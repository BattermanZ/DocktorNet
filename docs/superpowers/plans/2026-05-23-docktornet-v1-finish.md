# Docktornet v1 Finish — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the remaining gaps to ship docktornet v1: machine-ID flapping detection, Gatus label expansion (`group` + `telegram`), periodic full resync, container images + compose, and an interactive live cutover.

**Architecture:** Server-side additions only on the Rust side: a small `FlappingTracker` in `AppState`, new optional fields on `GatusMonitor`, a `tokio::sync::Mutex<()>` inside `Reconciler` shared between snapshot ingest and a new periodic `full_resync_loop`, and new `GET`-by-id calls on the NPM and Cloudflare clients. Deployment is two cargo-chef multi-stage Dockerfiles targeting `gcr.io/distroless/cc-debian13:nonroot`, plus per-role compose files. Cutover is captured in a reusable runbook plus a single interactive final task.

**Tech Stack:** Rust 2024, Tokio, Axum, SQLx SQLite, Reqwest, Serde, serde_yaml, Mockito, Tempfile, cargo-chef, Docker, distroless.

**Spec:** `docs/superpowers/specs/2026-05-23-docktornet-v1-finish-design.md`.

**Branch:** continue on `development`. Each task ends with a commit.

---

## Conventions used in this plan

- `unsafe_code = "forbid"` already set at the workspace root — do not change.
- Async tests use `#[tokio::test]`; sync tests use `#[test]`.
- Commit messages follow the existing repo style: subject `type(scope): summary`, body with `-` bullets.
- All commands assume the repo root (`/home/battermanz/coding/docktornet`) as the working directory unless explicitly noted.
- Run `cargo fmt --all` before each commit; `cargo clippy --workspace --all-targets -- -D warnings` after each commit.

---

## File structure

Create:

- `crates/server/src/flapping.rs` — `FlappingTracker`.
- `crates/server/Dockerfile`
- `crates/agent/Dockerfile`
- `.dockerignore`
- `deploy/main-host.compose.yaml`
- `deploy/agent.compose.yaml`
- `docs/runbooks/v1-cutover.md`
- `docs/deployment.md`

Modify:

- `crates/core/src/labels.rs` — add `group`, `telegram` fields, parsing.
- `crates/server/src/integrations/gatus.rs` — render new fields.
- `crates/server/src/integrations/npm.rs` — add `get_proxy_host`.
- `crates/server/src/integrations/cloudflare.rs` — add `get_record`.
- `crates/server/src/lib.rs` — export `flapping`.
- `crates/server/src/handlers/snapshot.rs` — wire flapping tracker, accept `ConnectInfo`.
- `crates/server/src/routes.rs` — include flapping tracker in state, add connect_info adapter for tests.
- `crates/server/src/reconcile.rs` — `reconcile_lock`, `full_resync`, helpers.
- `crates/server/src/main.rs` — build tracker, spawn resync loop, use `into_make_service_with_connect_info`.
- `.env.example` — document `DOCKTORNET_FULL_RESYNC_INTERVAL` and confirm shared file.

---

## Task 1: Flapping detection

**Files:**
- Create: `crates/server/src/flapping.rs`
- Modify: `crates/server/src/lib.rs`
- Modify: `crates/server/src/handlers/snapshot.rs`
- Modify: `crates/server/src/routes.rs`
- Modify: `crates/server/src/main.rs`

The tracker is a small standalone struct with one method (`observe`) that the snapshot handler calls. The handler receives the source IP through `ConnectInfo<SocketAddr>`.

- [ ] **Step 1.1: Write failing tests** in `crates/server/src/flapping.rs`

```rust
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use docktornet_core::MachineId;

pub struct FlappingTracker {
    last_seen: Mutex<HashMap<MachineId, (IpAddr, Instant)>>,
    window: Duration,
}

impl FlappingTracker {
    pub fn new(window: Duration) -> Self {
        Self {
            last_seen: Mutex::new(HashMap::new()),
            window,
        }
    }

    /// Record an observation of `machine_id` from `ip` at `now`.
    /// Returns `Some(previous_ip)` if a different IP was seen for the
    /// same machine within the window, else `None`.
    pub fn observe(&self, machine_id: &MachineId, ip: IpAddr, now: Instant) -> Option<IpAddr> {
        let mut map = self.last_seen.lock().expect("flapping mutex poisoned");
        let prev = map.get(machine_id).cloned();
        map.insert(machine_id.clone(), (ip, now));
        match prev {
            Some((prev_ip, prev_seen)) if prev_ip != ip && now.duration_since(prev_seen) < self.window => {
                Some(prev_ip)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn first_observation_returns_none() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        assert_eq!(t.observe(&MachineId::new("m1"), ip("10.0.0.1"), Instant::now()), None);
    }

    #[test]
    fn same_ip_returns_none() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        let id = MachineId::new("m1");
        let now = Instant::now();
        t.observe(&id, ip("10.0.0.1"), now);
        assert_eq!(t.observe(&id, ip("10.0.0.1"), now + Duration::from_secs(10)), None);
    }

    #[test]
    fn different_ip_within_window_returns_previous() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        let id = MachineId::new("m1");
        let now = Instant::now();
        t.observe(&id, ip("10.0.0.1"), now);
        let prev = t.observe(&id, ip("10.0.0.2"), now + Duration::from_secs(30));
        assert_eq!(prev, Some(ip("10.0.0.1")));
    }

    #[test]
    fn different_ip_after_window_returns_none() {
        let t = FlappingTracker::new(Duration::from_secs(60));
        let id = MachineId::new("m1");
        let now = Instant::now();
        t.observe(&id, ip("10.0.0.1"), now);
        assert_eq!(
            t.observe(&id, ip("10.0.0.2"), now + Duration::from_secs(120)),
            None
        );
    }

    #[test]
    fn machines_are_isolated() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        let now = Instant::now();
        t.observe(&MachineId::new("m1"), ip("10.0.0.1"), now);
        assert_eq!(
            t.observe(&MachineId::new("m2"), ip("10.0.0.2"), now + Duration::from_secs(1)),
            None
        );
    }
}
```

- [ ] **Step 1.2: Add module export** in `crates/server/src/lib.rs`

```rust
pub mod auth;
pub mod config;
pub mod db;
pub mod flapping;
pub mod handlers;
pub mod integrations;
pub mod reconcile;
pub mod routes;
pub mod storage;
```

- [ ] **Step 1.3: Run tests, expect PASS**

```bash
cargo test -p docktornet-server flapping::tests
```

Expected: 5 tests pass.

- [ ] **Step 1.4: Wire tracker into `AppState`** in `crates/server/src/handlers/snapshot.rs`

Replace the file with:

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::extract::{ConnectInfo, State};
use axum::http::StatusCode;
use docktornet_core::Snapshot;
use tracing::{info, warn};

use crate::flapping::FlappingTracker;
use crate::reconcile::Reconciler;

#[derive(Clone)]
pub struct AppState {
    pub reconciler: Reconciler,
    pub secret: Arc<String>,
    pub flapping: Arc<FlappingTracker>,
}

pub async fn ingest(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(snap): Json<Snapshot>,
) -> StatusCode {
    if let Some(prev) = state.flapping.observe(&snap.machine_id, addr.ip(), Instant::now()) {
        warn!(
            machine_id = %snap.machine_id.as_str(),
            prev_ip = %prev,
            new_ip = %addr.ip(),
            "machine id seen from multiple source IPs within window"
        );
    }
    info!(machine = %snap.machine_id.as_str(), containers = snap.containers.len(), "snapshot received");
    match state.reconciler.reconcile_snapshot(&snap).await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            warn!(error = %e, "failed to reconcile snapshot");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
```

- [ ] **Step 1.5: Update router tests** in `crates/server/src/routes.rs`

Tests previously created `AppState { reconciler, secret }`. Update `make_app` to also include the flapping tracker, and switch all `oneshot` calls to use `into_make_service_with_connect_info` so `ConnectInfo` extractors work. Replace the `tests` module with:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use docktornet_core::{ContainerEntry, MachineId, Snapshot};
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;
    use tower::ServiceExt;

    use crate::db;
    use crate::flapping::FlappingTracker;
    use crate::reconcile::{ReconcileOptions, Reconciler};
    use crate::storage::list_services_for_machine;

    async fn make_app() -> (Router, crate::db::Db, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = Reconciler::new(
            pool.clone(),
            ReconcileOptions {
                dry_run: true,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
        );
        let state = AppState {
            reconciler,
            secret: Arc::new("s3cret".into()),
            flapping: Arc::new(FlappingTracker::new(Duration::from_secs(300))),
        };
        (build(state), pool, dir)
    }

    fn body(snap: &Snapshot) -> Body {
        Body::from(serde_json::to_vec(snap).unwrap())
    }

    fn snap() -> Snapshot {
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "u".into());
        labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
        Snapshot {
            machine_id: MachineId::new("m1"),
            taken_at: 1,
            containers: vec![ContainerEntry { name: "a".into(), labels }],
        }
    }

    fn req(method: &str, token: Option<&str>, snap: &Snapshot) -> Request<Body> {
        let mut b = Request::builder()
            .method(method)
            .uri("/api/v1/snapshot")
            .header("content-type", "application/json");
        if let Some(t) = token {
            b = b.header("authorization", format!("Bearer {t}"));
        }
        // The handler uses ConnectInfo<SocketAddr>; inject it via the request extensions.
        let mut request = b.body(body(snap)).unwrap();
        request
            .extensions_mut()
            .insert(axum::extract::ConnectInfo::<SocketAddr>(
                "127.0.0.1:9999".parse().unwrap(),
            ));
        request
    }

    #[tokio::test]
    async fn ingest_rejects_wrong_token() {
        let (app, _, _dir) = make_app().await;
        let resp = app.oneshot(req("POST", Some("wrongtoken"), &snap())).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn ingest_requires_bearer() {
        let (app, _, _dir) = make_app().await;
        let resp = app.oneshot(req("POST", None, &snap())).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn ingest_persists_snapshot() {
        let (app, pool, _dir) = make_app().await;
        let resp = app.oneshot(req("POST", Some("s3cret"), &snap())).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        let rows = list_services_for_machine(&pool, "m1").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "m1/a");
    }
}
```

> Note: in axum 0.8, injecting `ConnectInfo` via request extensions works for `tower::ServiceExt::oneshot` tests because the `ConnectInfo` extractor reads from extensions when not behind `into_make_service_with_connect_info`. If the test compiles but the handler returns 500 because the extractor fails, switch the test rig to a real `TcpListener` + `axum::serve(... .into_make_service_with_connect_info::<SocketAddr>())` pattern (same approach as `crates/server/tests/e2e_snapshot_push.rs`).

- [ ] **Step 1.6: Wire tracker into `main.rs`**

Replace `crates/server/src/main.rs` with:

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use docktornet_server::flapping::FlappingTracker;
use docktornet_server::{config, db, routes};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = config::ServerConfig::from_env()?;
    let pool = db::connect(&cfg.db_path).await?;
    let npm = docktornet_server::integrations::npm::NpmClient::new(cfg.npm.clone());
    let cloudflare =
        docktornet_server::integrations::cloudflare::CloudflareClient::new(cfg.cloudflare.clone());
    let reconciler = docktornet_server::reconcile::Reconciler::with_clients(
        pool,
        docktornet_server::reconcile::ReconcileOptions {
            dry_run: cfg.dry_run,
            adopt_existing: cfg.adopt_existing,
            gatus_config_dir: cfg.gatus_config_dir.clone(),
        },
        npm,
        cloudflare,
    );
    let app = routes::build(docktornet_server::handlers::snapshot::AppState {
        reconciler,
        secret: Arc::new(cfg.secret),
        flapping: Arc::new(FlappingTracker::new(Duration::from_secs(300))),
    });

    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    info!(listen = %cfg.listen, "server ready");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        tokio::signal::ctrl_c().await.ok();
    })
    .await?;
    Ok(())
}
```

- [ ] **Step 1.7: Update e2e test to expect `ConnectInfo` wiring**

Read `crates/server/tests/e2e_snapshot_push.rs` and update the `AppState` construction to include the new `flapping: Arc::new(FlappingTracker::new(Duration::from_secs(300)))` field. Switch the `axum::serve` call to `axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())`. The test already binds a real TCP listener, so connect_info will populate naturally.

Run:
```bash
cargo test --workspace
```
Expected: all tests pass.

- [ ] **Step 1.8: Run clippy + fmt**

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

- [ ] **Step 1.9: Commit**

```bash
git add crates/server src
git commit -m "feat(server): warn when a machine id appears from multiple source IPs

- Add FlappingTracker keyed by MachineId with a 5-minute window
- Capture source IP via axum ConnectInfo and emit warn log on change
- Wire tracker into AppState and main; update tests"
```

---

## Task 2: Gatus label expansion in core

**Files:**
- Modify: `crates/core/src/labels.rs`

Two new optional fields on `GatusMonitor`. Parser updates plus tests.

- [ ] **Step 2.1: Extend the struct and `is_gatus_field` allowlist**

Replace `GatusMonitor` and `is_gatus_field` in `crates/core/src/labels.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatusMonitor {
    pub index: u8,
    pub url: String,
    pub interval: String,
    pub conditions: Vec<String>,
    pub group: Option<String>,
    pub telegram: bool,
}

fn is_gatus_field(field: &str) -> bool {
    matches!(
        field,
        "url" | "interval" | "conditions" | "group" | "telegram"
    )
}
```

- [ ] **Step 2.2: Extend `build_gatus` to parse the new fields**

Replace `build_gatus` with:

```rust
fn build_gatus(index: u8, fields: &BTreeMap<String, String>) -> Result<GatusMonitor, String> {
    let url = fields.get("url").ok_or("missing url")?.clone();
    let interval = fields.get("interval").ok_or("missing interval")?.clone();
    let mut conditions: Vec<String> = match fields.get("conditions") {
        Some(conditions) => conditions
            .split(',')
            .map(|condition| condition.trim().to_string())
            .filter(|condition| !condition.is_empty())
            .collect(),
        None => vec!["[STATUS] == 200".to_string()],
    };
    if conditions.is_empty() {
        conditions.push("[STATUS] == 200".to_string());
    }

    let group = fields.get("group").cloned().filter(|g| !g.is_empty());
    let telegram = match fields.get("telegram") {
        Some(value) => parse_bool(value)?,
        None => true,
    };

    Ok(GatusMonitor {
        index,
        url,
        interval,
        conditions,
        group,
        telegram,
    })
}
```

- [ ] **Step 2.3: Update existing tests that build `GatusMonitor` literals or assert on it**

Existing tests `gatus_with_default_conditions`, `gatus_explicit_conditions_split_on_comma`, and `gatus_empty_explicit_conditions_use_default` only assert on `.conditions`; they remain correct. Add new test cases at the end of the `tests` module:

```rust
    #[test]
    fn gatus_telegram_defaults_to_true_when_absent() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
        ]));
        assert_eq!(p.gatus.len(), 1);
        assert!(p.gatus[0].telegram);
        assert_eq!(p.gatus[0].group, None);
    }

    #[test]
    fn gatus_telegram_false_is_respected() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.0.telegram", "false"),
        ]));
        assert_eq!(p.gatus.len(), 1);
        assert!(!p.gatus[0].telegram);
    }

    #[test]
    fn gatus_group_is_pass_through_when_set() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.0.group", "media"),
        ]));
        assert_eq!(p.gatus[0].group, Some("media".to_string()));
    }

    #[test]
    fn gatus_invalid_telegram_emits_warning_and_skips() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.0.telegram", "maybe"),
        ]));
        assert!(p.gatus.is_empty());
        assert_eq!(p.warnings.len(), 1);
        assert!(p.warnings[0].contains("gatus.0"));
    }
```

- [ ] **Step 2.4: Run tests**

```bash
cargo test -p docktornet-core labels::tests
```

Expected: all prior tests still pass; 4 new tests pass.

- [ ] **Step 2.5: Run workspace tests**

```bash
cargo test --workspace
```

> Some downstream Gatus-related tests in server may fail because `GatusMonitor` literals must now include the two new fields. Fix them in the next task; for now, only `cargo test -p docktornet-core` must pass.

- [ ] **Step 2.6: Commit**

```bash
git add crates/core/src/labels.rs
git commit -m "feat(core): add gatus group and telegram labels

- Add optional group pass-through to GatusMonitor
- Add telegram boolean (default true) to GatusMonitor
- Reject invalid telegram values as monitor-level warnings"
```

---

## Task 3: Gatus renderer — emit group override + alerts block

**Files:**
- Modify: `crates/server/src/integrations/gatus.rs`

The current renderer hard-codes `group: <service_id>`. The new schema lets the operator override it via the label and adds an optional `alerts` block when `telegram` is true.

- [ ] **Step 3.1: Replace `GatusEndpoint` and `render_gatus_yaml`**

Replace the relevant section of `crates/server/src/integrations/gatus.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GatusFile {
    pub endpoints: Vec<GatusEndpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GatusEndpoint {
    pub name: String,
    pub group: String,
    pub url: String,
    pub interval: String,
    pub conditions: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<GatusAlert>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GatusAlert {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(rename = "send-on-resolved")]
    pub send_on_resolved: bool,
}

pub fn render_gatus_yaml(
    service_id: &str,
    monitors: &[GatusMonitor],
) -> Result<String, serde_yaml::Error> {
    let file = GatusFile {
        endpoints: monitors
            .iter()
            .map(|monitor| GatusEndpoint {
                name: format!("{service_id}/{}", monitor.index),
                group: monitor.group.clone().unwrap_or_else(|| service_id.to_string()),
                url: monitor.url.clone(),
                interval: monitor.interval.clone(),
                conditions: monitor.conditions.clone(),
                alerts: if monitor.telegram {
                    vec![GatusAlert {
                        kind: "telegram".to_string(),
                        send_on_resolved: true,
                    }]
                } else {
                    Vec::new()
                },
            })
            .collect(),
    };

    serde_yaml::to_string(&file)
}
```

- [ ] **Step 3.2: Update existing renderer test fixture and add new ones**

Replace the `renders_expected_yaml` test and add three new tests:

```rust
    #[test]
    fn renders_expected_yaml_with_default_group_and_telegram_alert() {
        let monitors = vec![GatusMonitor {
            index: 0,
            url: "https://app.example.com".to_string(),
            interval: "30s".to_string(),
            conditions: vec!["[STATUS] == 200".to_string()],
            group: None,
            telegram: true,
        }];

        let yaml = render_gatus_yaml("m1/app", &monitors).unwrap();
        let expected = concat!(
            "endpoints:\n",
            "- name: m1/app/0\n",
            "  group: m1/app\n",
            "  url: https://app.example.com\n",
            "  interval: 30s\n",
            "  conditions:\n",
            "  - '[STATUS] == 200'\n",
            "  alerts:\n",
            "  - type: telegram\n",
            "    send-on-resolved: true\n",
        );

        assert_eq!(yaml, expected);
    }

    #[test]
    fn renders_group_override_when_set() {
        let monitors = vec![GatusMonitor {
            index: 0,
            url: "https://app.example.com".to_string(),
            interval: "30s".to_string(),
            conditions: vec!["[STATUS] == 200".to_string()],
            group: Some("media".to_string()),
            telegram: true,
        }];

        let yaml = render_gatus_yaml("m1/app", &monitors).unwrap();
        assert!(yaml.contains("group: media\n"));
        assert!(!yaml.contains("group: m1/app\n"));
    }

    #[test]
    fn omits_alerts_block_when_telegram_is_false() {
        let monitors = vec![GatusMonitor {
            index: 0,
            url: "https://app.example.com".to_string(),
            interval: "30s".to_string(),
            conditions: vec!["[STATUS] == 200".to_string()],
            group: None,
            telegram: false,
        }];

        let yaml = render_gatus_yaml("m1/app", &monitors).unwrap();
        assert!(!yaml.contains("alerts"));
        assert!(!yaml.contains("telegram"));
    }
```

- [ ] **Step 3.3: Fix any other test in the workspace that constructs `GatusMonitor` literally**

Search for stale literals and add the new fields:

```bash
grep -rn "GatusMonitor {" crates/
```

Expected places to touch (current as of this plan): inside `crates/server/src/integrations/gatus.rs` (already covered) and `crates/server/src/integrations/intended.rs` if any literal exists there. For every literal found, add `group: None, telegram: true,`.

- [ ] **Step 3.4: Run tests**

```bash
cargo test --workspace
```

Expected: all pass.

- [ ] **Step 3.5: Run clippy + fmt**

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

- [ ] **Step 3.6: Commit**

```bash
git add crates/server/src/integrations/gatus.rs crates/server/src/integrations/intended.rs
git commit -m "feat(gatus): emit telegram alerts and honor group override

- Render alerts: [{type: telegram, send-on-resolved: true}] when label opts in
- Skip the alerts block entirely when telegram is disabled
- Use the label-provided group when set, fall back to service id"
```

---

## Task 4: NPM and Cloudflare GET-by-id methods

**Files:**
- Modify: `crates/server/src/integrations/npm.rs`
- Modify: `crates/server/src/integrations/cloudflare.rs`

The full resync needs to ask each external system "does the resource I think I own still exist?". Both clients are missing that primitive.

- [ ] **Step 4.1: Add NPM `get_proxy_host` returning `Option<ProxyHost>`**

Inspect the existing `create_proxy_host`/`update_proxy_host` to find the existing response shape. Add at the bottom of the `impl NpmClient` block:

```rust
    pub async fn get_proxy_host(
        &self,
        token: &str,
        id: i64,
    ) -> Result<Option<ProxyHost>, NpmError> {
        let url = self
            .config
            .url
            .join(&format!("api/nginx/proxy-hosts/{id}"))?;
        let resp = self
            .http
            .get(url)
            .bearer_auth(token)
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp = resp.error_for_status()?;
        Ok(Some(resp.json::<ProxyHost>().await?))
    }
```

If `ProxyHost` does not already exist as a struct in `npm.rs`, add a minimal one capturing the fields docktornet cares about for drift detection (id + certificate_id):

```rust
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ProxyHost {
    pub id: i64,
    #[serde(default)]
    pub certificate_id: Option<i64>,
}
```

> Read the existing `npm.rs` first; if a type with the same name exists, do not duplicate it — extend it.

- [ ] **Step 4.2: Write a mockito test for NPM `get_proxy_host`**

Add to the existing `#[cfg(test)] mod tests` in `crates/server/src/integrations/npm.rs`:

```rust
    #[tokio::test]
    async fn get_proxy_host_returns_none_on_404() {
        let mut server = mockito::Server::new_async().await;
        let m = server
            .mock("GET", "/api/nginx/proxy-hosts/42")
            .with_status(404)
            .create_async()
            .await;

        let client = NpmClient::new(NpmConfig {
            url: server.url().parse().unwrap(),
            user: "u".into(),
            password: "p".into(),
        });
        let got = client.get_proxy_host("tok", 42).await.unwrap();
        m.assert_async().await;
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn get_proxy_host_returns_some_with_cert_id() {
        let mut server = mockito::Server::new_async().await;
        let m = server
            .mock("GET", "/api/nginx/proxy-hosts/7")
            .with_status(200)
            .with_body(r#"{"id":7,"certificate_id":12}"#)
            .create_async()
            .await;

        let client = NpmClient::new(NpmConfig {
            url: server.url().parse().unwrap(),
            user: "u".into(),
            password: "p".into(),
        });
        let got = client.get_proxy_host("tok", 7).await.unwrap().unwrap();
        m.assert_async().await;
        assert_eq!(got.id, 7);
        assert_eq!(got.certificate_id, Some(12));
    }
```

- [ ] **Step 4.3: Add Cloudflare `get_record` returning `Option<DnsRecord>`**

Add to the existing `impl CloudflareClient` block:

```rust
    pub async fn get_record(
        &self,
        id: &str,
    ) -> Result<Option<DnsRecord>, CloudflareError> {
        let url = format!(
            "https://api.cloudflare.com/client/v4/zones/{}/dns_records/{}",
            self.config.zone_id, id
        );
        let resp = self
            .http
            .get(&url)
            .bearer_auth(&self.config.api_token)
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp = resp.error_for_status()?;
        let env: CloudflareEnvelope<DnsRecord> = resp.json().await?;
        Ok(env.result)
    }
```

> Read `cloudflare.rs` first to confirm: (a) the envelope type name, (b) the `DnsRecord` struct fields, (c) whether `error_for_status()` matches the existing style. If the envelope has a different name, use it; if there is no envelope, deserialize directly.

- [ ] **Step 4.4: Write mockito tests for Cloudflare `get_record`**

Add to the Cloudflare tests module:

```rust
    #[tokio::test]
    async fn get_record_returns_none_on_404() {
        let mut server = mockito::Server::new_async().await;
        let m = server
            .mock("GET", "/client/v4/zones/zone-id/dns_records/recX")
            .with_status(404)
            .create_async()
            .await;

        let client = CloudflareClient::with_base(
            CloudflareConfig { api_token: "t".into(), zone_id: "zone-id".into() },
            server.url(),
        );
        let got = client.get_record("recX").await.unwrap();
        m.assert_async().await;
        assert!(got.is_none());
    }
```

> If the existing `CloudflareClient` hard-codes `https://api.cloudflare.com` instead of accepting a base URL, refactor it to accept an optional `base: String` (or `&str`) in a `with_base` constructor that the tests use. Default constructor keeps the production URL. Apply the same pattern to any other test in the file that already uses mockito.

- [ ] **Step 4.5: Run tests**

```bash
cargo test -p docktornet-server integrations::npm
cargo test -p docktornet-server integrations::cloudflare
```

Expected: all existing tests pass + the new ones.

- [ ] **Step 4.6: Run clippy + fmt + commit**

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
git add crates/server/src/integrations
git commit -m "feat(integrations): add get-by-id for npm proxy hosts and cf dns records

- Add NpmClient::get_proxy_host returning None on 404
- Add CloudflareClient::get_record returning None on 404
- Cover both with mockito tests"
```

---

## Task 5: Reconcile mutex shared by ingest and resync

**Files:**
- Modify: `crates/server/src/reconcile.rs`

A single `tokio::sync::Mutex<()>` makes diff cycles and full resyncs mutually exclusive without otherwise changing their behaviour.

- [ ] **Step 5.1: Add the lock field and acquire it in `reconcile_snapshot`**

Edit `crates/server/src/reconcile.rs`:

```rust
use std::sync::Arc;
use tokio::sync::Mutex;
// ... existing imports

#[derive(Clone)]
pub struct Reconciler {
    db: Db,
    options: ReconcileOptions,
    npm: Option<crate::integrations::npm::NpmClient>,
    cloudflare: Option<crate::integrations::cloudflare::CloudflareClient>,
    lock: Arc<Mutex<()>>,
}

impl Reconciler {
    pub fn new(db: Db, options: ReconcileOptions) -> Self {
        Self {
            db,
            options,
            npm: None,
            cloudflare: None,
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn with_clients(
        db: Db,
        options: ReconcileOptions,
        npm: crate::integrations::npm::NpmClient,
        cloudflare: crate::integrations::cloudflare::CloudflareClient,
    ) -> Self {
        Self {
            db,
            options,
            npm: Some(npm),
            cloudflare: Some(cloudflare),
            lock: Arc::new(Mutex::new(())),
        }
    }
```

Wrap the body of `reconcile_snapshot` so the very first line acquires the lock:

```rust
    pub async fn reconcile_snapshot(&self, snap: &Snapshot) -> Result<(), ReconcileError> {
        let _guard = self.lock.lock().await;
        // ... rest of the existing body unchanged
    }
```

- [ ] **Step 5.2: Run existing tests**

```bash
cargo test -p docktornet-server reconcile::tests
```

Expected: all existing reconcile tests still pass — the lock is held for the duration of one call and immediately released, so single-threaded tests don't observe contention.

- [ ] **Step 5.3: Commit**

```bash
git add crates/server/src/reconcile.rs
git commit -m "feat(reconcile): serialize diff and resync paths via a shared mutex

- Add tokio::sync::Mutex on Reconciler held for one reconcile call
- Acquire in reconcile_snapshot to prepare for full_resync sharing the same lock"
```

---

## Task 6: `Reconciler::full_resync`

**Files:**
- Modify: `crates/server/src/reconcile.rs`

Adds the verify-and-repair pass. Reuses existing storage helpers and integration clients. Failures on individual resources are logged and skipped; the cycle continues.

- [ ] **Step 6.1: Add a helper that enumerates all tracked services**

Add to `crates/server/src/storage.rs`:

```rust
pub async fn list_all_services(pool: &Pool<Sqlite>) -> sqlx::Result<Vec<(String, String)>> {
    sqlx::query_as("SELECT id, labels_json FROM services ORDER BY id")
        .fetch_all(pool)
        .await
}
```

And a paired test in the same module:

```rust
    #[tokio::test]
    async fn list_all_services_returns_every_machine() {
        let p = pool().await;
        upsert_snapshot(&p, &Snapshot { machine_id: MachineId::new("m1"), taken_at: 1, containers: vec![entry("a", &[("docktornet.x","1")])] }).await.unwrap();
        upsert_snapshot(&p, &Snapshot { machine_id: MachineId::new("m2"), taken_at: 1, containers: vec![entry("b", &[("docktornet.y","2")])] }).await.unwrap();
        let all = list_all_services(&p).await.unwrap();
        let ids: Vec<String> = all.into_iter().map(|(id,_)| id).collect();
        assert_eq!(ids, vec!["m1/a", "m2/b"]);
    }
```

Run:
```bash
cargo test -p docktornet-server storage::tests::list_all_services_returns_every_machine
```
Expected: passes.

- [ ] **Step 6.2: Implement `full_resync`**

Append to `impl Reconciler` in `crates/server/src/reconcile.rs`:

```rust
    pub async fn full_resync(&self) -> Result<ResyncReport, ReconcileError> {
        let _guard = self.lock.lock().await;
        let mut report = ResyncReport::default();

        let services = crate::storage::list_all_services(&self.db).await?;
        for (service_id, labels_json) in services {
            let raw: std::collections::BTreeMap<String, String> =
                serde_json::from_str(&labels_json).unwrap_or_default();
            let parsed = docktornet_core::labels::parse(&raw);

            // --- NPM ---
            for host in &parsed.npm {
                if let Err(err) = self
                    .resync_npm_host(&service_id, host)
                    .await
                {
                    tracing::warn!(service_id, index = host.index, error = %err, "npm resync failed");
                    report.failed += 1;
                } else {
                    report.verified += 1;
                }
            }

            // --- Cloudflare ---
            if let Some(dns) = &parsed.cf_dns {
                if let Err(err) = self.resync_cloudflare(&service_id, dns).await {
                    tracing::warn!(service_id, error = %err, "cloudflare resync failed");
                    report.failed += 1;
                } else {
                    report.verified += 1;
                }
            }

            // --- Gatus ---
            if !parsed.gatus.is_empty() {
                match self.resync_gatus(&service_id, &parsed.gatus).await {
                    Ok(rewrote) => {
                        if rewrote { report.repaired += 1 } else { report.verified += 1 }
                    }
                    Err(err) => {
                        tracing::warn!(service_id, error = %err, "gatus resync failed");
                        report.failed += 1;
                    }
                }
            }
        }

        tracing::info!(
            verified = report.verified,
            repaired = report.repaired,
            failed = report.failed,
            "full resync complete"
        );
        Ok(report)
    }

    async fn resync_npm_host(
        &self,
        service_id: &str,
        host: &docktornet_core::labels::NpmHost,
    ) -> Result<(), ReconcileError> {
        let Some(client) = &self.npm else { return Ok(()); };
        let Some(npm_id) =
            crate::storage::get_npm_host(&self.db, service_id, host.index.into()).await?
        else { return Ok(()); };
        if self.options.dry_run {
            tracing::info!(service_id, npm_id, "dry-run: would verify npm host");
            return Ok(());
        }

        let token = client.token().await.map_err(|e| ReconcileError::External(e.into()))?;
        let existing = client.get_proxy_host(&token, npm_id).await
            .map_err(|e| ReconcileError::External(e.into()))?;
        let certs = client.certificates(&token).await
            .map_err(|e| ReconcileError::External(e.into()))?;
        let desired_cert = host
            .cert_id
            .map(i64::from)
            .or_else(|| crate::integrations::npm::matching_cert_id(&host.host, &certs));
        let payload = crate::integrations::npm::proxy_payload(host, "127.0.0.1", desired_cert);

        match existing {
            None => {
                tracing::info!(service_id, npm_id, "npm host missing, recreating");
                let created = client.create_proxy_host(&token, &payload).await
                    .map_err(|e| ReconcileError::External(e.into()))?;
                crate::storage::set_npm_host(&self.db, service_id, host.index.into(), created.id).await?;
            }
            Some(found) if found.certificate_id != desired_cert => {
                tracing::info!(service_id, npm_id, ?desired_cert, "npm cert drifted, updating");
                client.update_proxy_host(&token, npm_id, &payload).await
                    .map_err(|e| ReconcileError::External(e.into()))?;
            }
            Some(_) => {}
        }
        Ok(())
    }

    async fn resync_cloudflare(
        &self,
        service_id: &str,
        dns: &docktornet_core::labels::CloudflareDns,
    ) -> Result<(), ReconcileError> {
        let Some(client) = &self.cloudflare else { return Ok(()); };
        let Some(record_id) = crate::storage::get_cf_dns_record(&self.db, service_id).await? else { return Ok(()); };
        if self.options.dry_run {
            tracing::info!(service_id, record_id = %record_id, "dry-run: would verify cf record");
            return Ok(());
        }

        let zone = client.zone().await.map_err(|e| ReconcileError::External(e.into()))?;
        let payload = crate::integrations::cloudflare::dns_payload(dns, &zone.name);
        let existing = client.get_record(&record_id).await
            .map_err(|e| ReconcileError::External(e.into()))?;

        match existing {
            None => {
                tracing::info!(service_id, "cf record missing, recreating");
                let created = client.create_record(&payload).await
                    .map_err(|e| ReconcileError::External(e.into()))?;
                crate::storage::set_cf_dns_record(&self.db, service_id, &created.id).await?;
            }
            Some(found) if !cf_record_matches(&found, &payload) => {
                tracing::info!(service_id, "cf record drifted, updating");
                client.update_record(&record_id, &payload).await
                    .map_err(|e| ReconcileError::External(e.into()))?;
            }
            Some(_) => {}
        }
        Ok(())
    }

    async fn resync_gatus(
        &self,
        service_id: &str,
        monitors: &[docktornet_core::labels::GatusMonitor],
    ) -> Result<bool, ReconcileError> {
        let target = crate::integrations::gatus::service_file_path(
            &self.options.gatus_config_dir,
            service_id,
        );
        let intended = crate::integrations::gatus::render_gatus_yaml(service_id, monitors)
            .map_err(|e| ReconcileError::External(e.into()))?;
        let current = tokio::fs::read_to_string(&target).await.ok();
        if current.as_deref() == Some(intended.as_str()) {
            return Ok(false);
        }
        if self.options.dry_run {
            tracing::info!(service_id, path = %target.display(), "dry-run: would rewrite gatus file");
            return Ok(false);
        }
        tracing::info!(service_id, path = %target.display(), "gatus file drifted, rewriting");
        crate::integrations::gatus::write_service_file(
            &self.options.gatus_config_dir,
            service_id,
            monitors,
        )
        .await
        .map_err(ReconcileError::External)?;
        Ok(true)
    }
```

Add the report struct and the cf comparison helper near the top of the file (after `ReconcileOptions`):

```rust
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ResyncReport {
    pub verified: usize,
    pub repaired: usize,
    pub failed: usize,
}

fn cf_record_matches(
    found: &crate::integrations::cloudflare::DnsRecord,
    intended: &crate::integrations::cloudflare::DnsRecordPayload,
) -> bool {
    found.name == intended.name
        && found.content == intended.content
        && found.proxied == intended.proxied
}
```

> If `DnsRecord` does not currently expose `name`, `content`, or `proxied` as public fields, add them by reading the existing struct in `cloudflare.rs` and widening visibility / adding the fields with serde defaults. Keep the change minimal.

- [ ] **Step 6.3: Tests — gatus resync rewrites missing or drifted file**

Add to the `#[cfg(test)] mod tests` in `crates/server/src/reconcile.rs`:

```rust
    #[tokio::test]
    async fn full_resync_rewrites_missing_gatus_file() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = Reconciler::new(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: gatus_dir.clone(),
            },
        );

        // Seed via a snapshot ingest so SQLite + file are in a consistent starting state.
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
        labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                taken_at: 1,
                containers: vec![ContainerEntry { name: "app".into(), labels }],
            })
            .await
            .unwrap();

        // Delete the file out from under us.
        let path = gatus::service_file_path(&gatus_dir, "m1/app");
        assert!(path.exists());
        std::fs::remove_file(&path).unwrap();

        let report = reconciler.full_resync().await.unwrap();
        assert_eq!(report.repaired, 1);
        assert_eq!(report.failed, 0);
        assert!(path.exists());
    }

    #[tokio::test]
    async fn full_resync_is_a_noop_when_state_is_already_correct() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = Reconciler::new(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: gatus_dir.clone(),
            },
        );

        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
        labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                taken_at: 1,
                containers: vec![ContainerEntry { name: "app".into(), labels }],
            })
            .await
            .unwrap();

        let report = reconciler.full_resync().await.unwrap();
        assert_eq!(report.repaired, 0);
        assert_eq!(report.failed, 0);
        assert_eq!(report.verified, 1);
    }

    #[tokio::test]
    async fn full_resync_dry_run_does_not_rewrite_drifted_file() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        // Build state via a non-dry-run reconciler first, then resync with dry_run = true.
        let writer = Reconciler::new(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: gatus_dir.clone(),
            },
        );
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
        labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
        writer
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                taken_at: 1,
                containers: vec![ContainerEntry { name: "app".into(), labels }],
            })
            .await
            .unwrap();

        let path = gatus::service_file_path(&gatus_dir, "m1/app");
        std::fs::write(&path, "drifted").unwrap();

        let dryrun = Reconciler::new(
            pool,
            ReconcileOptions {
                dry_run: true,
                adopt_existing: false,
                gatus_config_dir: gatus_dir,
            },
        );
        let report = dryrun.full_resync().await.unwrap();
        assert_eq!(report.repaired, 0);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "drifted");
    }
```

- [ ] **Step 6.4: Run tests**

```bash
cargo test -p docktornet-server reconcile::tests
```

Expected: existing tests still pass + 3 new ones.

- [ ] **Step 6.5: Run clippy + fmt + commit**

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
git add crates/server
git commit -m "feat(reconcile): add full_resync verify-and-repair pass

- Iterate every tracked service and verify NPM, CF DNS, and Gatus state
- Recreate missing NPM hosts and CF records; re-apply drifted cert assignments
- Rewrite Gatus files whose content does not match intended yaml
- Skip writes under dry-run; emit a per-cycle counts log line"
```

---

## Task 7: Spawn the resync loop from `main`

**Files:**
- Modify: `crates/server/src/main.rs`

`DOCKTORNET_FULL_RESYNC_INTERVAL` already lives in env. Check whether `ServerConfig` parses it; if not, add the field.

- [ ] **Step 7.1: Add the field to `ServerConfig` if missing**

Read `crates/server/src/config.rs`. If `full_resync_interval: Duration` is not yet present, add it:

```rust
pub struct ServerConfig {
    // ... existing fields
    pub full_resync_interval: std::time::Duration,
}
```

Parse via the same `humantime_serde`-style helper used elsewhere, or with a small inline parser:

```rust
        let full_resync_interval = match env.get("DOCKTORNET_FULL_RESYNC_INTERVAL") {
            Some(s) => humantime::parse_duration(s).map_err(|_| ConfigError::InvalidBool {
                name: "DOCKTORNET_FULL_RESYNC_INTERVAL",
                value: s.clone(),
            })?,
            None => std::time::Duration::from_secs(3600),
        };
```

If `humantime` is not yet a direct dep of the server crate, add to `crates/server/Cargo.toml`:

```toml
humantime = "2"
```

Update existing config tests to cover default (1h) and explicit values. Run:

```bash
cargo test -p docktornet-server config::tests
```

- [ ] **Step 7.2: Spawn the loop in `main.rs`**

Just after the listener is bound, before `axum::serve`:

```rust
    {
        let reconciler = docktornet_server::handlers::snapshot::AppState::clone(&app_state).reconciler.clone();
        let interval = cfg.full_resync_interval;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // Skip the immediate tick — first fire is one interval after startup.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if let Err(err) = reconciler.full_resync().await {
                    tracing::warn!(error = %err, "full resync failed; will retry next cycle");
                }
            }
        });
    }
```

> Note: the existing `main.rs` builds an `AppState` inline. Restructure to bind it to a local first so it can be cloned for the spawn:

```rust
    let app_state = docktornet_server::handlers::snapshot::AppState {
        reconciler,
        secret: Arc::new(cfg.secret),
        flapping: Arc::new(FlappingTracker::new(Duration::from_secs(300))),
    };
    let reconciler_for_resync = app_state.reconciler.clone();
    let app = routes::build(app_state);

    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    info!(listen = %cfg.listen, "server ready");

    let resync_interval = cfg.full_resync_interval;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(resync_interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(err) = reconciler_for_resync.full_resync().await {
                tracing::warn!(error = %err, "full resync failed; will retry next cycle");
            }
        }
    });

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        tokio::signal::ctrl_c().await.ok();
    })
    .await?;
```

- [ ] **Step 7.3: Build + run + smoke**

```bash
cargo build -p docktornet-server
DOCKTORNET_LISTEN=127.0.0.1:5363 \
DOCKTORNET_SECRET=s3cret \
DOCKTORNET_DB_PATH=/tmp/dn-resync-smoke.db \
DOCKTORNET_GATUS_CONFIG_DIR=/tmp/dn-resync-gatus \
DOCKTORNET_NPM_URL=http://npm:81 \
DOCKTORNET_NPM_USER=admin@example.com \
DOCKTORNET_NPM_PASSWORD=changeme \
DOCKTORNET_CF_API_TOKEN=fake \
DOCKTORNET_CF_ZONE_ID=fake \
DOCKTORNET_FULL_RESYNC_INTERVAL=10s \
DOCKTORNET_DRY_RUN=true \
cargo run -p docktornet-server &
SERVER_PID=$!
sleep 15
kill $SERVER_PID
```

Expected: at least one "full resync complete" log line (verified=0, repaired=0, failed=0 because nothing is tracked yet). The server exits cleanly on SIGTERM.

- [ ] **Step 7.4: Update `.env.example`**

Add (or move into a clearly labelled block) at the bottom of the existing `.env.example`:

```env
# --- Server ---
# How often to run the verify-and-repair pass over all tracked resources.
DOCKTORNET_FULL_RESYNC_INTERVAL=1h
```

- [ ] **Step 7.5: Commit**

```bash
git add crates/server .env.example
git commit -m "feat(server): spawn periodic full resync loop

- Add DOCKTORNET_FULL_RESYNC_INTERVAL config (default 1h)
- Spawn a tokio interval task that calls Reconciler::full_resync
- First fire is one interval after startup; failures log and keep cycling"
```

---

## Task 8: Server Dockerfile

**Files:**
- Create: `crates/server/Dockerfile`
- Create: `.dockerignore`

- [ ] **Step 8.1: Write `.dockerignore`** at the repo root

```
target/
docktornet-backups/
data/
.env
*.db
.git/
```

- [ ] **Step 8.2: Write `crates/server/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json --bin docktornet-server

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json --bin docktornet-server
COPY . .
RUN cargo build --release --bin docktornet-server

FROM gcr.io/distroless/cc-debian13:nonroot AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/docktornet-server /usr/local/bin/docktornet-server
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/docktornet-server"]
```

- [ ] **Step 8.3: Build and smoke-test the server image**

```bash
docker build -f crates/server/Dockerfile -t docktornet-server:dev .
docker run --rm docktornet-server:dev || true
```

Expected: `docker build` succeeds. `docker run --rm docktornet-server:dev` fails fast with `Error: missing environment variable 'DOCKTORNET_LISTEN'` (or similar), proving the binary executes from inside the distroless image. Exit code from the container is non-zero — the trailing `|| true` keeps the shell happy.

- [ ] **Step 8.4: Commit**

```bash
git add .dockerignore crates/server/Dockerfile
git commit -m "feat(server): add rootless distroless Dockerfile via cargo-chef

- Multi-stage build pinned to lukemathwalker/cargo-chef:latest-rust-1
- Runtime image is gcr.io/distroless/cc-debian13:nonroot
- Build only the docktornet-server binary; .dockerignore filters secrets and build artifacts"
```

---

## Task 9: Agent Dockerfile

**Files:**
- Create: `crates/agent/Dockerfile`

- [ ] **Step 9.1: Write `crates/agent/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json --bin docktornet-agent

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json --bin docktornet-agent
COPY . .
RUN cargo build --release --bin docktornet-agent

FROM gcr.io/distroless/cc-debian13:nonroot AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/docktornet-agent /usr/local/bin/docktornet-agent
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/docktornet-agent"]
```

- [ ] **Step 9.2: Build and smoke-test**

```bash
docker build -f crates/agent/Dockerfile -t docktornet-agent:dev .
docker run --rm docktornet-agent:dev || true
```

Expected: build succeeds; run fails fast on missing env. Same pattern as the server.

- [ ] **Step 9.3: Commit**

```bash
git add crates/agent/Dockerfile
git commit -m "feat(agent): add rootless distroless Dockerfile via cargo-chef

- Same shape as the server image: cargo-chef + distroless cc-debian13:nonroot
- Builds only the docktornet-agent binary"
```

---

## Task 10: Compose files + deployment docs

**Files:**
- Create: `deploy/main-host.compose.yaml`
- Create: `deploy/agent.compose.yaml`
- Create: `docs/deployment.md`

- [ ] **Step 10.1: Write `deploy/main-host.compose.yaml`**

```yaml
services:
  docktornet-server:
    image: ghcr.io/battermanz/docktornet-server:latest
    restart: unless-stopped
    env_file: ../.env
    user: "65532:65532"
    volumes:
      - ./data:/data
      - /etc/gatus/config.d:/etc/gatus/config.d
    ports:
      - "5363:5363"

  docktornet-agent:
    image: ghcr.io/battermanz/docktornet-agent:latest
    restart: unless-stopped
    env_file: ../.env
    user: "65532:${DOCKER_GID:-988}"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    depends_on:
      - docktornet-server
```

- [ ] **Step 10.2: Write `deploy/agent.compose.yaml`**

```yaml
services:
  docktornet-agent:
    image: ghcr.io/battermanz/docktornet-agent:latest
    restart: unless-stopped
    env_file: ../.env
    user: "65532:${DOCKER_GID:-988}"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
```

> `${DOCKER_GID}` lets the operator export the host's docker group id (e.g. `getent group docker | cut -d: -f3`). 988 is a common default on Debian/Ubuntu but varies by distro — documented in `docs/deployment.md`.

- [ ] **Step 10.3: Write `docs/deployment.md`**

```markdown
# Deployment

Docktornet ships two images, both rootless and distroless:

- `ghcr.io/battermanz/docktornet-server` — runs on the main host only.
- `ghcr.io/battermanz/docktornet-agent` — runs on every host that exposes containers, including the main host.

## Images

Build locally:

```bash
docker build -f crates/server/Dockerfile -t docktornet-server:dev .
docker build -f crates/agent/Dockerfile  -t docktornet-agent:dev  .
```

Publishing images to GHCR is out of scope for v1 — tag and push manually for now.

## Configuration

Both binaries read the same `.env` file. Keys irrelevant to a given role are ignored.

| Var | Server | Agent |
| --- | --- | --- |
| `DOCKTORNET_LISTEN` | required | — |
| `DOCKTORNET_SECRET` | required | required |
| `DOCKTORNET_DB_PATH` | required | — |
| `DOCKTORNET_GATUS_CONFIG_DIR` | required | — |
| `DOCKTORNET_FULL_RESYNC_INTERVAL` | optional (`1h`) | — |
| `DOCKTORNET_DRY_RUN` | optional (`true`) | — |
| `DOCKTORNET_ADOPT_EXISTING` | optional (`false`) | — |
| `DOCKTORNET_NPM_*` | required | — |
| `DOCKTORNET_CF_*` | required | — |
| `DOCKTORNET_SERVER_URL` | — | required |
| `DOCKTORNET_POLL_INTERVAL` | — | required |
| `DOCKTORNET_MACHINE_ID` | — | required |
| `DOCKTORNET_SOCKETS` | — | required |

## Compose layouts

**Main host (server + agent in one stack):** `deploy/main-host.compose.yaml`

```bash
cd deploy
export DOCKER_GID=$(getent group docker | cut -d: -f3)
docker compose -f main-host.compose.yaml up -d
```

**Secondary hosts (agent only):** `deploy/agent.compose.yaml`

```bash
cd deploy
export DOCKER_GID=$(getent group docker | cut -d: -f3)
docker compose -f agent.compose.yaml up -d
```

For rootless Podman hosts, set `DOCKER_GID` to the gid of the directory containing `podman.sock` (typically the operator's own gid) and adjust the volume path in the compose file.

## Filesystem permissions

The server container runs as uid 65532 (`nonroot`). The two bind mounts must be writable by that uid:

```bash
sudo install -d -o 65532 -g 65532 ./data
sudo chown -R 65532:65532 /etc/gatus/config.d
```

The agent only needs **read** access to the docker/podman socket. Setting the container's secondary group to the docker gid is enough — no chmod required.

## Gatus base configuration

Docktornet only writes per-service files. The base Gatus config (web/storage/alerting) must be defined by the operator in a non-managed file in the same directory, e.g. `/etc/gatus/config.d/00-base.yaml`:

```yaml
web:
  port: 8080
storage:
  type: sqlite
  path: /data/gatus.db
alerting:
  telegram:
    token: "<bot token>"
    id: "<chat id>"
```

`alerting.telegram` is **required** if any service has `docktornet.gatus.N.telegram=true` (the default). Without it, Gatus startup fails when it encounters the per-service alert references.
```

- [ ] **Step 10.4: Commit**

```bash
git add deploy docs/deployment.md
git commit -m "docs(deploy): add compose files and deployment guide

- deploy/main-host.compose.yaml runs server + agent on the main host
- deploy/agent.compose.yaml runs agent-only on secondary hosts
- docs/deployment.md documents env keys, gids, and required gatus base config"
```

---

## Task 11: Cutover runbook

**Files:**
- Create: `docs/runbooks/v1-cutover.md`

- [ ] **Step 11.1: Write the runbook**

```bash
mkdir -p docs/runbooks
```

```markdown
# Docktornet v1 Cutover Runbook

Use this runbook to safely flip docktornet from dry-run to live writes against NPM, Cloudflare DNS, and Gatus. Reusable for re-cutovers; not a one-off checklist.

## 1. Pre-flight

- [ ] `git status` is clean on `development`.
- [ ] `just test` passes.
- [ ] `just lint` passes.
- [ ] `docker build -f crates/server/Dockerfile -t docktornet-server:dev .` succeeds.
- [ ] `docker build -f crates/agent/Dockerfile -t docktornet-agent:dev .` succeeds.
- [ ] At least one labelled container exists on a host you can monitor closely (a "canary" service).

## 2. Backup

```bash
./scripts/backup_external_state.sh
ls docktornet-backups/$(date +%F)/
git tag pre-cutover-$(date +%F)
```

Verify the backup directory contains:

- `npm-proxy-hosts.json`
- `cloudflare-dns.json`
- `gatus-config.tar.gz`

Do **not** proceed if any file is missing.

## 3. Shadow phase (dry-run)

- [ ] In `.env`: set `DOCKTORNET_DRY_RUN=true`.
- [ ] Deploy `deploy/main-host.compose.yaml`.
- [ ] Tail logs for at least one `DOCKTORNET_FULL_RESYNC_INTERVAL` (default 1h):

```bash
docker compose -f deploy/main-host.compose.yaml logs -f docktornet-server
```

Expected:
- `server ready` and periodic `snapshot received` lines.
- `full resync complete` exactly once per interval.
- `dry-run: would ...` lines describing the actions that *would* run against NPM/CF/Gatus.

Bail out if anything in the dry-run plan looks wrong (unexpected creates, deletes, or updates).

## 4. Single-service cutover

Pick **one** low-stakes service to flip first. The simplest way is:

1. Stop the server only (keep agents running):

   ```bash
   docker compose -f deploy/main-host.compose.yaml stop docktornet-server
   ```

2. In `.env`: `DOCKTORNET_DRY_RUN=false`.
3. Restart:

   ```bash
   docker compose -f deploy/main-host.compose.yaml up -d docktornet-server
   ```

4. Verify the canary service in:
   - NPM admin UI — the proxy host exists and points where intended.
   - Cloudflare dashboard — the CNAME exists with the right target / proxy state.
   - Gatus dashboard — the endpoint appears, the telegram alert (if enabled) fires on a forced failure.

If anything is wrong: set `DRY_RUN=true`, restart, then jump to §7 Rollback.

## 5. Full cutover

After the canary is healthy:

- Leave `DRY_RUN=false`.
- The next diff cycle (≤ 30 s) reconciles every other tracked service.
- Watch logs for ~10 minutes; spot-check 2–3 services across NPM/CF/Gatus.

## 6. Post-cutover

- [ ] Push:
  ```bash
  git push origin development
  git push batterforge development
  ```
- [ ] After 24 hours of clean operation, tag `v1.0.0`:
  ```bash
  git tag v1.0.0
  git push origin v1.0.0
  git push batterforge v1.0.0
  ```

## 7. Rollback

Use the most recent `docktornet-backups/<date>/` directory.

1. Stop the server:
   ```bash
   docker compose -f deploy/main-host.compose.yaml stop docktornet-server
   ```

2. Restore NPM hosts. Snippet (adjust to backup JSON shape):
   ```bash
   jq -c '.[]' docktornet-backups/<date>/npm-proxy-hosts.json | \
   while read -r host; do
     curl -fsS -X POST "$DOCKTORNET_NPM_URL/api/nginx/proxy-hosts" \
       -H "Authorization: Bearer $NPM_TOKEN" \
       -H "Content-Type: application/json" \
       -d "$host"
   done
   ```

3. Restore Cloudflare records:
   ```bash
   jq -c '.[]' docktornet-backups/<date>/cloudflare-dns.json | \
   while read -r rec; do
     curl -fsS -X POST \
       "https://api.cloudflare.com/client/v4/zones/$DOCKTORNET_CF_ZONE_ID/dns_records" \
       -H "Authorization: Bearer $DOCKTORNET_CF_API_TOKEN" \
       -H "Content-Type: application/json" \
       -d "$rec"
   done
   ```

4. Restore Gatus config:
   ```bash
   sudo tar -xzf docktornet-backups/<date>/gatus-config.tar.gz -C /etc/gatus/config.d
   ```

5. Wipe docktornet tracked state so it cannot try to manage resources it no longer "owns":
   ```bash
   rm ./data/docktornet.db
   ```

6. Bring docktornet back up in dry-run for a second look before reattempting:
   ```bash
   # .env: DOCKTORNET_DRY_RUN=true
   docker compose -f deploy/main-host.compose.yaml up -d
   ```
```

- [ ] **Step 11.2: Commit**

```bash
git add docs/runbooks
git commit -m "docs(runbook): add v1 cutover runbook

- Pre-flight, backup, shadow, canary, full cutover, post-cutover, rollback sections
- Designed as reusable for future re-cutovers, not a one-shot checklist"
```

---

## Task 12: Interactive cutover walkthrough

This task is **not** code or docs — it is an instruction to the executing agent.

- [ ] **Step 12.1: Walk through `docs/runbooks/v1-cutover.md` with the operator**

For each numbered section in the runbook:

1. Read the section aloud (paste it in the response).
2. Run the listed commands one at a time, showing output to the operator.
3. **Stop and wait for explicit operator confirmation** before moving to the next section. Phrases that authorize moving on: "ok", "continue", "next". Anything else is treated as a halt.
4. Never run §4 or §5 (anything that flips `DRY_RUN=false`) without an explicit, separately worded approval from the operator naming the section.
5. If anything in the output deviates from "Expected" in the runbook, stop, show the diff, and ask the operator whether to halt or proceed.

No commit comes from this task — it is purely operational. If observations during the walkthrough reveal a defect, capture it in a follow-up issue (or a separate plan), do not patch live.

---

## Task 13: Final quality gate

- [ ] **Step 13.1: Full test suite**

```bash
cargo test --workspace
```

Expected: all green.

- [ ] **Step 13.2: Clippy**

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: zero warnings.

- [ ] **Step 13.3: Format check**

```bash
cargo fmt --all -- --check
```

Expected: no diff.

- [ ] **Step 13.4: Image build sanity**

```bash
docker build -f crates/server/Dockerfile -t docktornet-server:dev .
docker build -f crates/agent/Dockerfile  -t docktornet-agent:dev  .
```

Expected: both build clean.

- [ ] **Step 13.5: Push branch**

```bash
git push origin development
git push batterforge development
```

Expected: both succeed.

---

## Self-Review

### Spec coverage

| Spec section | Covered by |
| --- | --- |
| §2 Flapping detection | Task 1 |
| §3 Gatus label expansion | Tasks 2, 3 |
| §3.4 Operator obligation (alerting.telegram in base config) | Task 10 (`docs/deployment.md`) |
| §4 Periodic full resync (scheduler, mutex, per-integration behaviour, dry-run, failure isolation) | Tasks 4, 5, 6, 7 |
| §5 Container images and compose | Tasks 8, 9, 10 |
| §6 Live cutover (runbook + interactive task) | Tasks 11, 12 |
| §7 SQLite schema impact (none) | n/a |
| §8 Config additions (none beyond DOCKTORNET_FULL_RESYNC_INTERVAL already in env) | Task 7 |

Every numbered spec section maps to at least one task.

### Placeholder scan

- No "TBD" / "TODO" / "implement later".
- No "add appropriate error handling" — every error path uses the existing typed errors (`ReconcileError::External`, `NpmError`, `CloudflareError`, `ConfigError`).
- No "write tests for the above" without code — each test step has runnable code.
- No "similar to Task N" references — Tasks 8 and 9 each repeat their Dockerfile inline.
- Two places where code is contingent on the existing struct shape (NPM `ProxyHost`, Cloudflare `DnsRecord`) explicitly direct the executor to read the file first and add/widen fields if the assumed names differ. This is intentional: the code I would have written blindly is dead on arrival without that check.

### Type consistency

- `GatusMonitor` gains `group: Option<String>` and `telegram: bool` in Task 2; every later renderer test (Task 3) and reconcile test (Task 6) constructs literals with those two fields. Task 3.3 explicitly greps for stale literals.
- `Reconciler::new` / `Reconciler::with_clients` both gain the `lock` field in Task 5. Tests don't construct `Reconciler` literally — they use the constructors — so they remain valid.
- `AppState` gains `flapping: Arc<FlappingTracker>` in Task 1; the router test, e2e test, and main all update consistently.
- `FlappingTracker::new(Duration)` is the only constructor; the wall-clock instant is supplied by callers (`Instant::now()` in production, controlled in tests).
- `ResyncReport` is introduced in Task 6 and is the only return type of `full_resync`; the loop in Task 7 discards `Ok(_)` cleanly.

No inconsistencies found.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-23-docktornet-v1-finish.md`. Two execution options:

1. **Subagent-Driven (recommended)** — fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
