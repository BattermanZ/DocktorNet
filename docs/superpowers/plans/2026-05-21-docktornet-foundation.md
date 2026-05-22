# Docktornet Foundation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish the Cargo workspace, shared `core` crate, working `agent` binary that polls Docker/Podman sockets and pushes snapshots, and a `server` binary that authenticates pushes and persists snapshots to SQLite. No reconciliation logic; end state is an end-to-end demo where containers appear in the server's SQLite within 30 seconds of starting.

**Architecture:** Rust workspace with three crates (`core`, `agent`, `server`). Async via Tokio. `bollard` for Docker/Podman socket talk (they share the same Engine API). `axum` for the server HTTP listener. `sqlx` for SQLite. `reqwest` for the agent's outbound HTTP. Bearer-token auth between agent and server. Configuration via `.env` files and `dotenvy`.

**Tech Stack:** Rust (edition 2024, latest stable), tokio, axum 0.8, reqwest 0.12, sqlx 0.8 (sqlite), bollard 0.18, serde + serde_json, tracing, thiserror, anyhow, dotenvy, humantime-serde, mockito (test-only), tempfile (test-only).

**Branch:** `development` (already created). Each task ends with a commit to `development`; both remotes get pushed at the end of each natural milestone (called out in dedicated tasks).

**Scope reminder:** Spec at `docs/superpowers/specs/2026-05-21-docktornet-design.md`. This plan covers the **foundation** only — reconciliation, NPM/CF DNS/Gatus integrations, dry-run, adoption, full resync, Dockerfiles and Compose are *out of scope* and will be covered by Plan 2 and Plan 3.

---

## Conventions used in this plan

- Every Rust source file uses `unsafe_code = "forbid"` at the crate root unless otherwise stated.
- Tests follow the `#[tokio::test]` pattern for async tests; synchronous tests use `#[test]`.
- Commit messages follow `AGENTS.md` §7: subject `type(scope): summary`, body uses `-` bullets only.
- All commands assume the working directory is the repo root (`/home/battermanz/coding/docktornet`) unless explicitly noted otherwise.
- When a step says "Run test, expect FAIL" and the test compiles but doesn't fail, that means the test isn't actually exercising the code path — re-read it.

---

## File Structure (created across all tasks)

```
docktornet/
├── Cargo.toml                         # workspace root
├── rust-toolchain.toml                # latest stable toolchain
├── .env.example                       # documentation
├── Justfile                           # common commands
├── crates/
│   ├── core/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                 # re-exports
│   │       ├── snapshot.rs            # Snapshot, ContainerEntry, MachineId types
│   │       ├── labels.rs              # label parsing (docktornet.* → typed structs)
│   │       └── ids.rs                 # service_id + sanitization
│   ├── agent/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs                # entry point: load config, run loop
│   │       ├── config.rs              # env → AgentConfig
│   │       ├── sockets.rs             # bollard wrapper, list containers
│   │       ├── snapshot.rs            # build Snapshot from containers
│   │       ├── push.rs                # HTTP client + retry
│   │       └── loop_.rs               # the periodic poll/push loop
│   └── server/
│       ├── Cargo.toml
│       ├── migrations/
│       │   └── 0001_init.sql          # services table only (Plan 1)
│       └── src/
│           ├── main.rs                # entry point
│           ├── config.rs              # env → ServerConfig
│           ├── db.rs                  # sqlx pool setup + migrations
│           ├── auth.rs                # bearer-token middleware
│           ├── routes.rs              # axum router
│           ├── handlers/
│           │   └── snapshot.rs        # POST /api/v1/snapshot
│           └── storage.rs             # services table insert/upsert/delete
└── tests/
    └── e2e/
        └── snapshot_push.rs           # integration test (workspace-level)
```

---

## Task 1: Workspace bootstrap

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `Justfile`
- Create: `.env.example`
- Create: `crates/core/Cargo.toml`
- Create: `crates/core/src/lib.rs`
- Create: `crates/agent/Cargo.toml`
- Create: `crates/agent/src/main.rs`
- Create: `crates/server/Cargo.toml`
- Create: `crates/server/src/main.rs`

- [ ] **Step 1.1: Write workspace `Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = [
    "crates/core",
    "crates/agent",
    "crates/server",
]

[workspace.package]
edition      = "2024"
license      = "AGPL-3.0-or-later"
authors      = ["BattermanZ"]
repository   = "https://github.com/BattermanZ/DocktorNet"

[workspace.dependencies]
# async
tokio            = { version = "1", features = ["macros", "rt-multi-thread", "signal", "time"] }
# serialization
serde            = { version = "1", features = ["derive"] }
serde_json       = "1"
# error handling
thiserror        = "2"
anyhow           = "1"
# logging
tracing          = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }
# config
dotenvy          = "0.15"
humantime-serde  = "1"
# http
axum             = "0.8"
reqwest          = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
# db
sqlx             = { version = "0.8", default-features = false, features = ["runtime-tokio", "sqlite", "macros", "migrate"] }
# docker / podman
bollard          = "0.18"

# test-only (declared here so versions are shared)
mockito          = "1"
tempfile         = "3"

[workspace.lints.rust]
unsafe_code = "forbid"
```

- [ ] **Step 1.2: Write `rust-toolchain.toml`**

```toml
[toolchain]
channel    = "stable"
components = ["rustfmt", "clippy"]
profile    = "minimal"
```

- [ ] **Step 1.3: Write `crates/core/Cargo.toml`**

```toml
[package]
name         = "docktornet-core"
version      = "0.1.0"
edition.workspace      = true
license.workspace      = true

[lib]
name = "docktornet_core"
path = "src/lib.rs"

[dependencies]
serde        = { workspace = true }
serde_json   = { workspace = true }
thiserror    = { workspace = true }

[lints]
workspace = true
```

- [ ] **Step 1.4: Write `crates/core/src/lib.rs`** (re-export stubs only)

```rust
pub mod snapshot;
pub mod labels;
pub mod ids;

pub use ids::{service_id, sanitize_service_id, MachineId};
pub use snapshot::{ContainerEntry, Snapshot};
```

> Note: this file references modules that don't exist yet. We'll create empty stub files so the workspace compiles.

- [ ] **Step 1.5: Create stub module files**

```rust
// crates/core/src/snapshot.rs
// will be filled in Task 2
```

```rust
// crates/core/src/labels.rs
// will be filled in Task 3
```

```rust
// crates/core/src/ids.rs
// will be filled in Task 2
```

Use `Write` for each with the placeholder comment as the only content.

- [ ] **Step 1.6: Write `crates/agent/Cargo.toml`**

```toml
[package]
name         = "docktornet-agent"
version      = "0.1.0"
edition.workspace      = true
license.workspace      = true

[[bin]]
name = "docktornet-agent"
path = "src/main.rs"

[dependencies]
docktornet-core = { path = "../core" }

tokio       = { workspace = true }
serde       = { workspace = true }
serde_json  = { workspace = true }
thiserror   = { workspace = true }
anyhow      = { workspace = true }
tracing     = { workspace = true }
tracing-subscriber = { workspace = true }
dotenvy     = { workspace = true }
humantime-serde = { workspace = true }
reqwest     = { workspace = true }
bollard     = { workspace = true }

[dev-dependencies]
mockito  = { workspace = true }
tempfile = { workspace = true }

[lints]
workspace = true
```

- [ ] **Step 1.7: Write `crates/agent/src/main.rs`** (stub)

```rust
fn main() {
    println!("docktornet-agent (placeholder)");
}
```

- [ ] **Step 1.8: Write `crates/server/Cargo.toml`**

```toml
[package]
name         = "docktornet-server"
version      = "0.1.0"
edition.workspace      = true
license.workspace      = true

[[bin]]
name = "docktornet-server"
path = "src/main.rs"

[dependencies]
docktornet-core = { path = "../core" }

tokio       = { workspace = true }
serde       = { workspace = true }
serde_json  = { workspace = true }
thiserror   = { workspace = true }
anyhow      = { workspace = true }
tracing     = { workspace = true }
tracing-subscriber = { workspace = true }
dotenvy     = { workspace = true }
humantime-serde = { workspace = true }
axum        = { workspace = true }
sqlx        = { workspace = true }

[dev-dependencies]
reqwest  = { workspace = true }
mockito  = { workspace = true }
tempfile = { workspace = true }

[lints]
workspace = true
```

- [ ] **Step 1.9: Write `crates/server/src/main.rs`** (stub)

```rust
fn main() {
    println!("docktornet-server (placeholder)");
}
```

- [ ] **Step 1.10: Write `Justfile`**

```just
default:
    @just --list

# format all crates
fmt:
    cargo fmt --all

# clippy across the workspace
lint:
    cargo clippy --workspace --all-targets -- -D warnings

# fast check
check:
    cargo check --workspace --all-targets

# run tests for whole workspace
test:
    cargo test --workspace

# run agent locally (requires .env in CWD)
run-agent:
    cargo run -p docktornet-agent

# run server locally (requires .env in CWD)
run-server:
    cargo run -p docktornet-server
```

- [ ] **Step 1.11: Write `.env.example`** (documentation only)

```env
# --- Server ---
DOCKTORNET_LISTEN=0.0.0.0:5363
DOCKTORNET_SECRET=changeme-please
DOCKTORNET_DB_PATH=./data/docktornet.db

# --- Agent ---
DOCKTORNET_SERVER_URL=http://127.0.0.1:5363
# DOCKTORNET_SECRET=changeme-please       # share with server
DOCKTORNET_POLL_INTERVAL=30s
DOCKTORNET_MACHINE_ID=batterdock
DOCKTORNET_SOCKETS=docker:/var/run/docker.sock
```

- [ ] **Step 1.12: Verify the workspace builds**

```bash
cargo check --workspace
```

Expected: succeeds with no warnings.

- [ ] **Step 1.13: Commit**

```bash
git add Cargo.toml rust-toolchain.toml Justfile .env.example crates/
git commit -m "chore(workspace): scaffold cargo workspace with core, agent, server crates

- Add Cargo workspace at repo root with three member crates
- Pin stable Rust toolchain via rust-toolchain.toml
- Define shared workspace dependencies and lints (unsafe_code = forbid)
- Add Justfile with fmt, lint, check, test, run-* targets
- Add .env.example documenting v1 environment variables
- Stub agent and server binaries with placeholder main functions"
```

---

## Task 2: Core types — `Snapshot`, `ContainerEntry`, `MachineId`, `service_id`

**Files:**
- Modify: `crates/core/src/snapshot.rs`
- Modify: `crates/core/src/ids.rs`
- Test: tests live inside the same files as `#[cfg(test)] mod tests { ... }` blocks.

- [ ] **Step 2.1: Write failing tests for `service_id` and `sanitize_service_id`** in `crates/core/src/ids.rs`

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MachineId(String);

impl MachineId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Returns the canonical service id `{machine_id}/{container}`.
pub fn service_id(machine: &MachineId, container: &str) -> String {
    format!("{}/{}", machine.as_str(), container)
}

/// Returns a filename-safe version of a service id.
/// Both components are slugified (any non-alphanumeric becomes `_`) and joined with `__`.
pub fn sanitize_service_id(id: &str) -> String {
    let (machine, container) = match id.split_once('/') {
        Some(pair) => pair,
        None => return slug(id),
    };
    format!("{}__{}", slug(machine), slug(container))
}

fn slug(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_id_joins_machine_and_container() {
        let m = MachineId::new("batterdock");
        assert_eq!(service_id(&m, "myapp"), "batterdock/myapp");
    }

    #[test]
    fn sanitize_replaces_slash_and_dots() {
        let id = sanitize_service_id("batter-dock/my.app");
        assert_eq!(id, "batter_dock__my_app");
    }

    #[test]
    fn sanitize_handles_missing_slash() {
        assert_eq!(sanitize_service_id("foo"), "foo");
    }

    #[test]
    fn machine_id_roundtrips_json() {
        let m = MachineId::new("batterdock");
        let s = serde_json::to_string(&m).unwrap();
        let back: MachineId = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
    }
}
```

> Note: this step writes the implementation **and** tests at once because the API is tiny and writing the test alone would require the same types in scope. The next step verifies it.

- [ ] **Step 2.2: Run tests for `ids` module**

```bash
cargo test -p docktornet-core ids::tests
```

Expected: 4 tests pass.

- [ ] **Step 2.3: Write `Snapshot` and `ContainerEntry` types** in `crates/core/src/snapshot.rs`

```rust
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ids::MachineId;

/// A single container reported by an agent. `labels` is the subset of the
/// container's Docker labels whose keys start with `docktornet.`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerEntry {
    pub name: String,
    pub labels: BTreeMap<String, String>,
}

/// A snapshot of all docktornet-relevant containers on one machine at one
/// point in time. Sent in the body of `POST /api/v1/snapshot`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub machine_id: MachineId,
    pub taken_at: u64,        // unix epoch seconds
    pub containers: Vec<ContainerEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_roundtrips_json() {
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "https://x".into());

        let snap = Snapshot {
            machine_id: MachineId::new("batterdock"),
            taken_at: 1716300000,
            containers: vec![ContainerEntry {
                name: "myapp".into(),
                labels,
            }],
        };

        let s = serde_json::to_string(&snap).unwrap();
        let back: Snapshot = serde_json::from_str(&s).unwrap();
        assert_eq!(snap, back);
    }

    #[test]
    fn empty_snapshot_is_valid() {
        let snap = Snapshot {
            machine_id: MachineId::new("batterdock"),
            taken_at: 1716300000,
            containers: vec![],
        };
        let s = serde_json::to_string(&snap).unwrap();
        assert!(s.contains("\"containers\":[]"));
    }
}
```

- [ ] **Step 2.4: Run snapshot tests**

```bash
cargo test -p docktornet-core snapshot::tests
```

Expected: 2 tests pass.

- [ ] **Step 2.5: Run the whole `core` test suite**

```bash
cargo test -p docktornet-core
```

Expected: 6 tests pass.

- [ ] **Step 2.6: Commit**

```bash
git add crates/core/src
git commit -m "feat(core): add Snapshot, ContainerEntry, MachineId, service_id

- Add MachineId newtype wrapping a String for type-safety
- Add service_id(machine, container) returning canonical {machine}/{container}
- Add sanitize_service_id(id) for filename-safe forms used by Gatus configs
- Add Snapshot and ContainerEntry serde structs used on the wire
- Cover all helpers with round-trip and slugification unit tests"
```

---

## Task 3: Core label parser

**Files:**
- Modify: `crates/core/src/labels.rs`

We parse the *raw* `docktornet.*` labels into typed structures. Plan 1 only ships parsing for the three v1 integrations: NPM, Cloudflare DNS, Gatus. Parser failures are non-fatal — bad labels for one integration must not poison the rest.

- [ ] **Step 3.1: Write failing tests** in `crates/core/src/labels.rs`

```rust
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedLabels {
    pub npm: Vec<NpmHost>,
    pub cf_dns: Option<CloudflareDns>,
    pub gatus: Vec<GatusMonitor>,
    /// Diagnostic messages for labels that could not be parsed. The
    /// parsing function never fails; it logs problems here so callers can
    /// surface them without breaking unrelated integrations.
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NpmHost {
    pub index: u8,
    pub host: String,
    pub port: u16,
    pub scheme: NpmScheme,
    pub ssl: bool,
    pub hsts: Option<bool>,
    pub http2: Option<bool>,
    pub cert_id: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NpmScheme { Http, Https }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudflareDns {
    pub name: String,
    pub target: String,
    pub proxied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatusMonitor {
    pub index: u8,
    pub url: String,
    pub interval: String,            // free-form, validated by Gatus itself
    pub conditions: Vec<String>,     // defaults to [STATUS] == 200 when empty
}

pub fn parse(labels: &BTreeMap<String, String>) -> ParsedLabels {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lbl(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn no_docktornet_labels_yields_empty() {
        let p = parse(&lbl(&[("foo", "bar")]));
        assert!(p.npm.is_empty());
        assert!(p.cf_dns.is_none());
        assert!(p.gatus.is_empty());
        assert!(p.warnings.is_empty());
    }

    #[test]
    fn single_npm_host_parses() {
        let p = parse(&lbl(&[
            ("docktornet.npm.0.host", "a.example.com"),
            ("docktornet.npm.0.port", "3000"),
            ("docktornet.npm.0.scheme", "http"),
            ("docktornet.npm.0.ssl", "true"),
        ]));
        assert_eq!(p.npm.len(), 1);
        let h = &p.npm[0];
        assert_eq!(h.index, 0);
        assert_eq!(h.host, "a.example.com");
        assert_eq!(h.port, 3000);
        assert_eq!(h.scheme, NpmScheme::Http);
        assert!(h.ssl);
        assert_eq!(h.hsts, None);
        assert_eq!(h.cert_id, None);
        assert!(p.warnings.is_empty());
    }

    #[test]
    fn multiple_npm_hosts_parse_by_index() {
        let p = parse(&lbl(&[
            ("docktornet.npm.0.host", "a"),
            ("docktornet.npm.0.port", "1"),
            ("docktornet.npm.0.scheme", "http"),
            ("docktornet.npm.0.ssl", "true"),
            ("docktornet.npm.2.host", "c"),
            ("docktornet.npm.2.port", "3"),
            ("docktornet.npm.2.scheme", "https"),
            ("docktornet.npm.2.ssl", "false"),
        ]));
        assert_eq!(p.npm.len(), 2);
        assert_eq!(p.npm[0].index, 0);
        assert_eq!(p.npm[1].index, 2);
    }

    #[test]
    fn npm_missing_required_field_emits_warning_and_skips() {
        let p = parse(&lbl(&[
            ("docktornet.npm.0.host", "a"),
            // missing port + scheme + ssl
        ]));
        assert!(p.npm.is_empty());
        assert_eq!(p.warnings.len(), 1);
        assert!(p.warnings[0].contains("npm.0"));
    }

    #[test]
    fn npm_optional_overrides_parse() {
        let p = parse(&lbl(&[
            ("docktornet.npm.0.host", "a"),
            ("docktornet.npm.0.port", "80"),
            ("docktornet.npm.0.scheme", "http"),
            ("docktornet.npm.0.ssl", "true"),
            ("docktornet.npm.0.hsts", "false"),
            ("docktornet.npm.0.http2", "false"),
            ("docktornet.npm.0.cert_id", "42"),
        ]));
        assert_eq!(p.npm.len(), 1);
        assert_eq!(p.npm[0].hsts, Some(false));
        assert_eq!(p.npm[0].http2, Some(false));
        assert_eq!(p.npm[0].cert_id, Some(42));
    }

    #[test]
    fn cf_dns_parses_when_all_fields_present() {
        let p = parse(&lbl(&[
            ("docktornet.cf.dns.name", "myapp"),
            ("docktornet.cf.dns.target", "home.example.com"),
            ("docktornet.cf.dns.proxied", "true"),
        ]));
        let dns = p.cf_dns.unwrap();
        assert_eq!(dns.name, "myapp");
        assert_eq!(dns.target, "home.example.com");
        assert!(dns.proxied);
    }

    #[test]
    fn cf_dns_partial_emits_warning() {
        let p = parse(&lbl(&[
            ("docktornet.cf.dns.name", "myapp"),
            // missing target
        ]));
        assert!(p.cf_dns.is_none());
        assert_eq!(p.warnings.len(), 1);
    }

    #[test]
    fn gatus_with_default_conditions() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
        ]));
        assert_eq!(p.gatus.len(), 1);
        assert_eq!(p.gatus[0].conditions, vec!["[STATUS] == 200"]);
    }

    #[test]
    fn gatus_explicit_conditions_split_on_comma() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.0.conditions", "[STATUS] == 200, [RESPONSE_TIME] < 500"),
        ]));
        assert_eq!(p.gatus[0].conditions, vec![
            "[STATUS] == 200".to_string(),
            "[RESPONSE_TIME] < 500".to_string(),
        ]);
    }

    #[test]
    fn unrelated_docktornet_keys_become_warnings() {
        let p = parse(&lbl(&[
            ("docktornet.unknown.0.foo", "bar"),
        ]));
        assert!(p.npm.is_empty());
        assert!(p.cf_dns.is_none());
        assert!(p.gatus.is_empty());
        assert_eq!(p.warnings.len(), 1);
        assert!(p.warnings[0].contains("docktornet.unknown"));
    }
}
```

- [ ] **Step 3.2: Run tests, expect FAIL**

```bash
cargo test -p docktornet-core labels::tests
```

Expected: tests fail at compile time because `parse` is `todo!()`. (Or at runtime if Rust compiles past the todo — either way, a failure is required here.)

- [ ] **Step 3.3: Implement `parse`** (replace the `todo!()` body with this code; tests stay unchanged)

```rust
pub fn parse(labels: &BTreeMap<String, String>) -> ParsedLabels {
    let mut out = ParsedLabels {
        npm: Vec::new(),
        cf_dns: None,
        gatus: Vec::new(),
        warnings: Vec::new(),
    };

    // Group "docktornet.<category>...<value>" keys by their *first* segment after
    // the docktornet prefix. Anything that doesn't match a known category becomes
    // a warning.
    let mut npm_groups: BTreeMap<u8, BTreeMap<String, String>> = BTreeMap::new();
    let mut gatus_groups: BTreeMap<u8, BTreeMap<String, String>> = BTreeMap::new();
    let mut cf_dns: BTreeMap<String, String> = BTreeMap::new();

    for (k, v) in labels {
        let Some(rest) = k.strip_prefix("docktornet.") else { continue };
        let parts: Vec<&str> = rest.split('.').collect();

        match parts.as_slice() {
            ["npm", idx, field] => match idx.parse::<u8>() {
                Ok(i) => {
                    npm_groups.entry(i).or_default().insert((*field).to_string(), v.clone());
                }
                Err(_) => {
                    out.warnings.push(format!("invalid npm index in key '{k}'"));
                }
            },
            ["gatus", idx, field] => match idx.parse::<u8>() {
                Ok(i) => {
                    gatus_groups.entry(i).or_default().insert((*field).to_string(), v.clone());
                }
                Err(_) => {
                    out.warnings.push(format!("invalid gatus index in key '{k}'"));
                }
            },
            ["cf", "dns", field] => {
                cf_dns.insert((*field).to_string(), v.clone());
            }
            _ => {
                out.warnings.push(format!("unrecognised docktornet key '{k}'"));
            }
        }
    }

    for (idx, fields) in npm_groups {
        match build_npm_host(idx, &fields) {
            Ok(h) => out.npm.push(h),
            Err(e) => out.warnings.push(format!("npm.{idx}: {e}")),
        }
    }

    for (idx, fields) in gatus_groups {
        match build_gatus(idx, &fields) {
            Ok(m) => out.gatus.push(m),
            Err(e) => out.warnings.push(format!("gatus.{idx}: {e}")),
        }
    }

    if !cf_dns.is_empty() {
        match build_cf_dns(&cf_dns) {
            Ok(d) => out.cf_dns = Some(d),
            Err(e) => out.warnings.push(format!("cf.dns: {e}")),
        }
    }

    out.npm.sort_by_key(|h| h.index);
    out.gatus.sort_by_key(|g| g.index);

    out
}

fn build_npm_host(index: u8, f: &BTreeMap<String, String>) -> Result<NpmHost, String> {
    let host = f.get("host").ok_or("missing host")?.clone();
    let port: u16 = f
        .get("port")
        .ok_or("missing port")?
        .parse()
        .map_err(|_| "port must be a u16".to_string())?;
    let scheme = match f.get("scheme").map(|s| s.as_str()) {
        Some("http")  => NpmScheme::Http,
        Some("https") => NpmScheme::Https,
        Some(other)   => return Err(format!("scheme must be http|https, got {other}")),
        None          => return Err("missing scheme".into()),
    };
    let ssl = parse_bool(f.get("ssl").ok_or("missing ssl")?)?;
    let hsts    = f.get("hsts").map(parse_bool).transpose()?;
    let http2   = f.get("http2").map(parse_bool).transpose()?;
    let cert_id = f.get("cert_id").map(|s| s.parse::<u32>()).transpose().map_err(|_| "cert_id must be u32".to_string())?;
    Ok(NpmHost { index, host, port, scheme, ssl, hsts, http2, cert_id })
}

fn build_gatus(index: u8, f: &BTreeMap<String, String>) -> Result<GatusMonitor, String> {
    let url = f.get("url").ok_or("missing url")?.clone();
    let interval = f.get("interval").ok_or("missing interval")?.clone();
    let conditions = match f.get("conditions") {
        Some(s) => s.split(',').map(|c| c.trim().to_string()).filter(|c| !c.is_empty()).collect(),
        None    => vec!["[STATUS] == 200".to_string()],
    };
    Ok(GatusMonitor { index, url, interval, conditions })
}

fn build_cf_dns(f: &BTreeMap<String, String>) -> Result<CloudflareDns, String> {
    let name    = f.get("name").ok_or("missing name")?.clone();
    let target  = f.get("target").ok_or("missing target")?.clone();
    let proxied = parse_bool(f.get("proxied").ok_or("missing proxied")?)?;
    Ok(CloudflareDns { name, target, proxied })
}

fn parse_bool(s: impl AsRef<str>) -> Result<bool, String> {
    match s.as_ref() {
        "true" | "1" | "yes"  => Ok(true),
        "false" | "0" | "no"  => Ok(false),
        other => Err(format!("expected bool, got '{other}'")),
    }
}
```

- [ ] **Step 3.4: Run tests, expect PASS**

```bash
cargo test -p docktornet-core labels::tests
```

Expected: 10 tests pass.

- [ ] **Step 3.5: Run `cargo clippy` for the workspace and fix any warnings**

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: no warnings.

- [ ] **Step 3.6: Commit**

```bash
git add crates/core/src/labels.rs
git commit -m "feat(core): parse docktornet.* labels into typed integration structs

- Add ParsedLabels with npm, cf_dns and gatus fields plus a warnings vec
- Parse docktornet.npm.<idx>.<field> into NpmHost (host, port, scheme, ssl, hsts, http2, cert_id)
- Parse docktornet.cf.dns.<field> into CloudflareDns (name, target, proxied)
- Parse docktornet.gatus.<idx>.<field> into GatusMonitor with default [STATUS] == 200 conditions
- Bad labels for one integration produce warnings without poisoning the rest
- Sort outputs by index to keep diffs stable
- Cover happy path, partial input, optional overrides and bad keys with unit tests"
```

---

## Task 4: Agent configuration

**Files:**
- Modify: `crates/agent/src/main.rs`
- Create: `crates/agent/src/config.rs`

- [ ] **Step 4.1: Write failing tests for `AgentConfig`** in `crates/agent/src/config.rs`

```rust
use std::time::Duration;

use serde::Deserialize;

use docktornet_core::MachineId;

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub server_url: String,
    pub secret: String,
    pub poll_interval: Duration,
    pub machine_id: MachineId,
    pub sockets: Vec<SocketSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocketSpec {
    pub runtime: Runtime,
    pub path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Runtime { Docker, Podman }

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing environment variable '{0}'")]
    Missing(&'static str),
    #[error("invalid value for {var}: {msg}")]
    Invalid { var: &'static str, msg: String },
}

#[derive(Debug, Deserialize)]
struct DurationStr(#[serde(with = "humantime_serde")] Duration);

impl AgentConfig {
    /// Load from the current process environment. Reads (in order):
    ///   DOCKTORNET_SERVER_URL, DOCKTORNET_SECRET, DOCKTORNET_POLL_INTERVAL,
    ///   DOCKTORNET_MACHINE_ID, DOCKTORNET_SOCKETS
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_iter(std::env::vars().collect::<std::collections::HashMap<_, _>>())
    }

    /// Same as `from_env` but reads from an explicit map (used by tests).
    pub fn from_iter(env: std::collections::HashMap<String, String>) -> Result<Self, ConfigError> {
        let get = |k: &'static str| env.get(k).cloned().ok_or(ConfigError::Missing(k));

        let server_url = get("DOCKTORNET_SERVER_URL")?;
        let secret     = get("DOCKTORNET_SECRET")?;

        let interval_str = get("DOCKTORNET_POLL_INTERVAL")?;
        let DurationStr(poll_interval) = serde_json::from_value(serde_json::Value::String(interval_str.clone()))
            .map_err(|e| ConfigError::Invalid { var: "DOCKTORNET_POLL_INTERVAL", msg: e.to_string() })?;

        let machine_id = MachineId::new(get("DOCKTORNET_MACHINE_ID")?);

        let sockets_raw = get("DOCKTORNET_SOCKETS")?;
        let sockets = parse_sockets(&sockets_raw)
            .map_err(|msg| ConfigError::Invalid { var: "DOCKTORNET_SOCKETS", msg })?;

        Ok(Self { server_url, secret, poll_interval, machine_id, sockets })
    }
}

fn parse_sockets(raw: &str) -> Result<Vec<SocketSpec>, String> {
    let mut out = Vec::new();
    for item in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (runtime, path) = item
            .split_once(':')
            .ok_or_else(|| format!("expected 'runtime:path', got '{item}'"))?;
        let runtime = match runtime {
            "docker" => Runtime::Docker,
            "podman" => Runtime::Podman,
            other    => return Err(format!("unknown runtime '{other}', expected docker|podman")),
        };
        out.push(SocketSpec { runtime, path: path.to_string() });
    }
    if out.is_empty() {
        return Err("at least one socket is required".into());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn happy_path_loads_all_fields() {
        let c = AgentConfig::from_iter(env(&[
            ("DOCKTORNET_SERVER_URL", "http://127.0.0.1:5363"),
            ("DOCKTORNET_SECRET", "s3cret"),
            ("DOCKTORNET_POLL_INTERVAL", "30s"),
            ("DOCKTORNET_MACHINE_ID", "batterdock"),
            ("DOCKTORNET_SOCKETS", "docker:/var/run/docker.sock,podman:/run/user/1000/podman/podman.sock"),
        ])).unwrap();
        assert_eq!(c.server_url, "http://127.0.0.1:5363");
        assert_eq!(c.secret, "s3cret");
        assert_eq!(c.poll_interval, Duration::from_secs(30));
        assert_eq!(c.machine_id.as_str(), "batterdock");
        assert_eq!(c.sockets.len(), 2);
        assert_eq!(c.sockets[0], SocketSpec { runtime: Runtime::Docker, path: "/var/run/docker.sock".into() });
        assert_eq!(c.sockets[1], SocketSpec { runtime: Runtime::Podman, path: "/run/user/1000/podman/podman.sock".into() });
    }

    #[test]
    fn missing_var_errors_clearly() {
        let err = AgentConfig::from_iter(env(&[])).unwrap_err();
        match err {
            ConfigError::Missing(v) => assert_eq!(v, "DOCKTORNET_SERVER_URL"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn unknown_runtime_errors() {
        let err = AgentConfig::from_iter(env(&[
            ("DOCKTORNET_SERVER_URL", "u"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_POLL_INTERVAL", "1s"),
            ("DOCKTORNET_MACHINE_ID", "m"),
            ("DOCKTORNET_SOCKETS", "wasm:/x"),
        ])).unwrap_err();
        match err {
            ConfigError::Invalid { var, .. } => assert_eq!(var, "DOCKTORNET_SOCKETS"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn empty_sockets_errors() {
        let err = AgentConfig::from_iter(env(&[
            ("DOCKTORNET_SERVER_URL", "u"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_POLL_INTERVAL", "1s"),
            ("DOCKTORNET_MACHINE_ID", "m"),
            ("DOCKTORNET_SOCKETS", ""),
        ])).unwrap_err();
        matches!(err, ConfigError::Invalid { var: "DOCKTORNET_SOCKETS", .. });
    }
}
```

- [ ] **Step 4.2: Wire the module into `main.rs`**

Modify `crates/agent/src/main.rs`:

```rust
mod config;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cfg = config::AgentConfig::from_env()?;
    info!(machine_id = %cfg.machine_id.as_str(), sockets = ?cfg.sockets, "agent starting (config only — loop not wired yet)");
    Ok(())
}
```

- [ ] **Step 4.3: Run agent tests**

```bash
cargo test -p docktornet-agent config::tests
```

Expected: 4 tests pass.

- [ ] **Step 4.4: Run agent binary with sample env to smoke-test**

```bash
DOCKTORNET_SERVER_URL=http://127.0.0.1:5363 \
DOCKTORNET_SECRET=s \
DOCKTORNET_POLL_INTERVAL=30s \
DOCKTORNET_MACHINE_ID=test \
DOCKTORNET_SOCKETS=docker:/var/run/docker.sock \
cargo run -p docktornet-agent
```

Expected: prints something like `agent starting (config only — loop not wired yet)` and exits successfully.

- [ ] **Step 4.5: Commit**

```bash
git add crates/agent/src
git commit -m "feat(agent): load AgentConfig from environment

- Add AgentConfig with server_url, secret, poll_interval (humantime), machine_id and sockets
- Parse DOCKTORNET_SOCKETS as comma-separated runtime:path pairs (docker|podman)
- Surface missing and invalid env vars via a typed ConfigError
- Initialise tracing in main and load .env via dotenvy when present
- Unit-test happy path, missing var, unknown runtime, empty sockets"
```

---

## Task 5: Agent — Docker/Podman socket polling

**Files:**
- Create: `crates/agent/src/sockets.rs`
- Modify: `crates/agent/src/main.rs`

`bollard` talks to any Docker-compatible engine API over a unix socket. We wrap it with a thin trait so tests can substitute a fake.

- [ ] **Step 5.1: Write failing tests** in `crates/agent/src/sockets.rs`

```rust
use std::collections::BTreeMap;

use async_trait::async_trait;
use bollard::container::ListContainersOptions;
use bollard::Docker;
use tracing::warn;

use crate::config::SocketSpec;

/// A container as it appears on the wire. Names come from the Docker API
/// stripped of the leading `/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawContainer {
    pub name: String,
    pub labels: BTreeMap<String, String>,
}

#[async_trait]
pub trait ContainerSource: Send + Sync {
    async fn list(&self) -> anyhow::Result<Vec<RawContainer>>;
}

/// Production implementation using bollard against a Unix socket.
pub struct BollardSource {
    docker: Docker,
    label: String,
}

impl BollardSource {
    pub fn connect(spec: &SocketSpec) -> anyhow::Result<Self> {
        let docker = Docker::connect_with_socket(&spec.path, 120, bollard::API_DEFAULT_VERSION)?;
        Ok(Self { docker, label: format!("{:?}:{}", spec.runtime, spec.path) })
    }
}

#[async_trait]
impl ContainerSource for BollardSource {
    async fn list(&self) -> anyhow::Result<Vec<RawContainer>> {
        let opts = ListContainersOptions::<String> { all: false, ..Default::default() };
        let containers = self.docker.list_containers(Some(opts)).await?;

        let mut out = Vec::with_capacity(containers.len());
        for c in containers {
            let Some(names) = c.names else {
                warn!(source = %self.label, "container with no names, skipping");
                continue;
            };
            let Some(first) = names.into_iter().next() else { continue };
            let name = first.trim_start_matches('/').to_string();
            let labels: BTreeMap<String, String> = c.labels.unwrap_or_default().into_iter().collect();
            out.push(RawContainer { name, labels });
        }
        Ok(out)
    }
}

/// Merge results from every configured source. Duplicate container names from
/// different sockets are kept (we trust the operator's setup).
pub async fn list_all(sources: &[Box<dyn ContainerSource>]) -> Vec<RawContainer> {
    let mut out = Vec::new();
    for src in sources {
        match src.list().await {
            Ok(mut v) => out.append(&mut v),
            Err(e) => warn!(error = %e, "source failed to list containers"),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake { items: Vec<RawContainer> }

    #[async_trait]
    impl ContainerSource for Fake {
        async fn list(&self) -> anyhow::Result<Vec<RawContainer>> { Ok(self.items.clone()) }
    }

    fn rc(name: &str, labels: &[(&str, &str)]) -> RawContainer {
        RawContainer {
            name: name.into(),
            labels: labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        }
    }

    #[tokio::test]
    async fn list_all_concatenates_sources_in_order() {
        let a: Box<dyn ContainerSource> = Box::new(Fake { items: vec![rc("a", &[])] });
        let b: Box<dyn ContainerSource> = Box::new(Fake { items: vec![rc("b", &[])] });
        let merged = list_all(&[a, b]).await;
        assert_eq!(merged.iter().map(|c| c.name.clone()).collect::<Vec<_>>(), vec!["a", "b"]);
    }

    struct Boom;
    #[async_trait]
    impl ContainerSource for Boom {
        async fn list(&self) -> anyhow::Result<Vec<RawContainer>> { anyhow::bail!("boom") }
    }

    #[tokio::test]
    async fn list_all_continues_when_one_source_fails() {
        let a: Box<dyn ContainerSource> = Box::new(Boom);
        let b: Box<dyn ContainerSource> = Box::new(Fake { items: vec![rc("b", &[])] });
        let merged = list_all(&[a, b]).await;
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].name, "b");
    }
}
```

- [ ] **Step 5.2: Add `async-trait` to agent dependencies**

Update `crates/agent/Cargo.toml`, adding to `[dependencies]`:

```toml
async-trait = "0.1"
```

- [ ] **Step 5.3: Wire the module into `main.rs`**

Modify `crates/agent/src/main.rs`:

```rust
mod config;
mod sockets;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::sockets::{BollardSource, ContainerSource, list_all};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cfg = config::AgentConfig::from_env()?;

    let mut sources: Vec<Box<dyn ContainerSource>> = Vec::new();
    for spec in &cfg.sockets {
        match BollardSource::connect(spec) {
            Ok(src) => sources.push(Box::new(src)),
            Err(e)  => tracing::error!(socket = %spec.path, error = %e, "failed to connect, skipping"),
        }
    }

    let containers = list_all(&sources).await;
    info!(count = containers.len(), "listed containers (loop not wired yet)");
    Ok(())
}
```

- [ ] **Step 5.4: Run tests**

```bash
cargo test -p docktornet-agent sockets::tests
```

Expected: 2 tests pass.

- [ ] **Step 5.5: Run the binary against a real socket to verify**

```bash
DOCKTORNET_SERVER_URL=http://127.0.0.1:5363 \
DOCKTORNET_SECRET=s \
DOCKTORNET_POLL_INTERVAL=30s \
DOCKTORNET_MACHINE_ID=$(hostname) \
DOCKTORNET_SOCKETS=docker:/var/run/docker.sock \
cargo run -p docktornet-agent
```

Expected: prints `listed containers count=<n>` where `<n>` matches `docker ps -q | wc -l`. On a Podman-only host, swap the socket to `podman:/run/user/$(id -u)/podman/podman.sock`.

- [ ] **Step 5.6: Commit**

```bash
git add crates/agent
git commit -m "feat(agent): list containers from Docker and Podman sockets

- Define ContainerSource async trait so sources are pluggable for testing
- Implement BollardSource using bollard's connect_with_socket against unix paths
- Strip leading slash from container names and collect labels into BTreeMap
- Add list_all() helper that concatenates sources and tolerates per-source failures
- Unit-test trait composition with two fakes (success + failure)
- Wire sources into main and log container count without yet building snapshots"
```

---

## Task 6: Agent — build `Snapshot` from raw containers, filtered to `docktornet.*`

**Files:**
- Create: `crates/agent/src/snapshot.rs`
- Modify: `crates/agent/src/main.rs`

- [ ] **Step 6.1: Write failing test** in `crates/agent/src/snapshot.rs`

```rust
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use docktornet_core::{ContainerEntry, MachineId, Snapshot};

use crate::sockets::RawContainer;

/// Build a Snapshot from the raw container list, keeping only containers that
/// declare at least one label starting with `docktornet.`. Labels without the
/// prefix are dropped to keep payloads small.
pub fn build(machine_id: &MachineId, raw: &[RawContainer]) -> Snapshot {
    let taken_at = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let mut containers = Vec::new();
    for c in raw {
        let labels: BTreeMap<String, String> = c
            .labels
            .iter()
            .filter(|(k, _)| k.starts_with("docktornet."))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if labels.is_empty() { continue }
        containers.push(ContainerEntry { name: c.name.clone(), labels });
    }
    containers.sort_by(|a, b| a.name.cmp(&b.name));
    Snapshot { machine_id: machine_id.clone(), taken_at, containers }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rc(name: &str, labels: &[(&str, &str)]) -> RawContainer {
        RawContainer {
            name: name.into(),
            labels: labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        }
    }

    #[test]
    fn keeps_only_docktornet_prefixed_labels_and_drops_unlabelled_containers() {
        let snap = build(
            &MachineId::new("m1"),
            &[
                rc("withlabel", &[("docktornet.gatus.0.url", "u"), ("com.docker.x", "y")]),
                rc("nolabels",  &[("com.docker.x", "y")]),
            ],
        );
        assert_eq!(snap.containers.len(), 1);
        assert_eq!(snap.containers[0].name, "withlabel");
        assert_eq!(snap.containers[0].labels.len(), 1);
        assert!(snap.containers[0].labels.contains_key("docktornet.gatus.0.url"));
    }

    #[test]
    fn containers_sorted_by_name_for_stable_diffs() {
        let snap = build(
            &MachineId::new("m1"),
            &[
                rc("zebra", &[("docktornet.x", "1")]),
                rc("apple", &[("docktornet.x", "1")]),
            ],
        );
        assert_eq!(snap.containers.iter().map(|c| c.name.clone()).collect::<Vec<_>>(), vec!["apple", "zebra"]);
    }
}
```

- [ ] **Step 6.2: Run tests, expect PASS** (the implementation was written in the same file)

```bash
cargo test -p docktornet-agent snapshot::tests
```

Expected: 2 tests pass.

- [ ] **Step 6.3: Wire `snapshot::build` into `main.rs`**

Replace `crates/agent/src/main.rs` with:

```rust
mod config;
mod sockets;
mod snapshot;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::sockets::{BollardSource, ContainerSource, list_all};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cfg = config::AgentConfig::from_env()?;

    let mut sources: Vec<Box<dyn ContainerSource>> = Vec::new();
    for spec in &cfg.sockets {
        match BollardSource::connect(spec) {
            Ok(src) => sources.push(Box::new(src)),
            Err(e)  => tracing::error!(socket = %spec.path, error = %e, "failed to connect, skipping"),
        }
    }

    let raw  = list_all(&sources).await;
    let snap = snapshot::build(&cfg.machine_id, &raw);
    info!(containers = snap.containers.len(), "snapshot built (push not wired yet)");
    Ok(())
}
```

- [ ] **Step 6.4: Run the binary smoke test**

```bash
DOCKTORNET_SERVER_URL=http://127.0.0.1:5363 \
DOCKTORNET_SECRET=s \
DOCKTORNET_POLL_INTERVAL=30s \
DOCKTORNET_MACHINE_ID=$(hostname) \
DOCKTORNET_SOCKETS=docker:/var/run/docker.sock \
cargo run -p docktornet-agent
```

Expected: `snapshot built containers=<n>` where `<n>` equals the number of running containers that have at least one `docktornet.*` label.

- [ ] **Step 6.5: Commit**

```bash
git add crates/agent
git commit -m "feat(agent): build Snapshot from raw containers filtered to docktornet.* labels

- Add snapshot::build that returns a docktornet_core::Snapshot
- Drop containers with no docktornet.* labels to keep payloads small
- Drop non-docktornet labels from kept containers for the same reason
- Sort containers by name so server-side diffs are deterministic
- Stamp taken_at with the current unix epoch
- Unit-test filtering and sorting behaviour"
```

---

## Task 7: Agent — HTTP push with bearer-token auth and retry/backoff

**Files:**
- Create: `crates/agent/src/push.rs`
- Modify: `crates/agent/src/main.rs`

- [ ] **Step 7.1: Write failing tests** in `crates/agent/src/push.rs`

```rust
use std::time::Duration;

use docktornet_core::Snapshot;
use reqwest::Client;
use tracing::{warn, info};

#[derive(Clone)]
pub struct PushClient {
    http: Client,
    base: String,
    secret: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PushError {
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("server returned {status}: {body}")]
    Status { status: u16, body: String },
}

impl PushClient {
    pub fn new(base: impl Into<String>, secret: impl Into<String>) -> anyhow::Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        Ok(Self { http, base: base.into(), secret: secret.into() })
    }

    pub async fn push(&self, snap: &Snapshot) -> Result<(), PushError> {
        let url = format!("{}/api/v1/snapshot", self.base.trim_end_matches('/'));
        let resp = self.http.post(&url)
            .bearer_auth(&self.secret)
            .json(snap)
            .send()
            .await?;
        let status = resp.status();
        if status.is_success() { return Ok(()) }
        let body = resp.text().await.unwrap_or_default();
        Err(PushError::Status { status: status.as_u16(), body })
    }

    /// Push with bounded exponential backoff: tries up to `attempts` times,
    /// waiting 1s, 4s, 16s, 64s ... (capped at 60s). Returns the final error
    /// if all attempts fail.
    pub async fn push_with_retry(&self, snap: &Snapshot, attempts: u32) -> Result<(), PushError> {
        let mut delay = Duration::from_secs(1);
        let mut last_err: Option<PushError> = None;
        for attempt in 1..=attempts {
            match self.push(snap).await {
                Ok(()) => {
                    if attempt > 1 { info!(attempt, "snapshot push recovered"); }
                    return Ok(());
                }
                Err(e) => {
                    warn!(attempt, max = attempts, error = %e, "snapshot push failed");
                    last_err = Some(e);
                    if attempt < attempts {
                        tokio::time::sleep(delay).await;
                        delay = (delay * 4).min(Duration::from_secs(60));
                    }
                }
            }
        }
        Err(last_err.expect("at least one attempt"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use docktornet_core::{ContainerEntry, MachineId};
    use mockito::Server;

    fn snap() -> Snapshot {
        Snapshot {
            machine_id: MachineId::new("m1"),
            taken_at: 1,
            containers: vec![ContainerEntry {
                name: "a".into(),
                labels: BTreeMap::new(),
            }],
        }
    }

    #[tokio::test]
    async fn push_sends_bearer_token_and_json_body() {
        let mut server = Server::new_async().await;
        let m = server.mock("POST", "/api/v1/snapshot")
            .match_header("authorization", "Bearer s3cret")
            .match_header("content-type", "application/json")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({ "machine_id": "m1" })))
            .with_status(204)
            .create_async()
            .await;

        let c = PushClient::new(server.url(), "s3cret").unwrap();
        c.push(&snap()).await.unwrap();
        m.assert_async().await;
    }

    #[tokio::test]
    async fn push_returns_status_error_on_4xx() {
        let mut server = Server::new_async().await;
        let _m = server.mock("POST", "/api/v1/snapshot")
            .with_status(401).with_body("nope")
            .create_async().await;

        let c = PushClient::new(server.url(), "wrong").unwrap();
        let err = c.push(&snap()).await.unwrap_err();
        match err {
            PushError::Status { status, body } => {
                assert_eq!(status, 401);
                assert_eq!(body, "nope");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn push_with_retry_recovers_after_one_failure() {
        let mut server = Server::new_async().await;
        let m1 = server.mock("POST", "/api/v1/snapshot")
            .with_status(503).expect(1)
            .create_async().await;
        let m2 = server.mock("POST", "/api/v1/snapshot")
            .with_status(204).expect(1)
            .create_async().await;

        let c = PushClient::new(server.url(), "s").unwrap();
        // shrink the initial delay path for tests: use the public API with
        // attempts=2 which still produces a brief sleep but the test tolerates it.
        c.push_with_retry(&snap(), 2).await.unwrap();
        m1.assert_async().await;
        m2.assert_async().await;
    }
}
```

- [ ] **Step 7.2: Run tests, expect PASS** (implementation lives in the same file as the tests)

```bash
cargo test -p docktornet-agent push::tests
```

Expected: 3 tests pass. The retry test sleeps ~1s; tolerate that.

- [ ] **Step 7.3: Wire push into `main.rs` as a one-shot push (loop comes next task)**

Replace `crates/agent/src/main.rs`:

```rust
mod config;
mod sockets;
mod snapshot;
mod push;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::sockets::{BollardSource, ContainerSource, list_all};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cfg = config::AgentConfig::from_env()?;
    let client = push::PushClient::new(&cfg.server_url, &cfg.secret)?;

    let mut sources: Vec<Box<dyn ContainerSource>> = Vec::new();
    for spec in &cfg.sockets {
        match BollardSource::connect(spec) {
            Ok(src) => sources.push(Box::new(src)),
            Err(e)  => tracing::error!(socket = %spec.path, error = %e, "failed to connect, skipping"),
        }
    }

    let raw  = list_all(&sources).await;
    let snap = snapshot::build(&cfg.machine_id, &raw);
    info!(containers = snap.containers.len(), "pushing snapshot (one-shot, loop wires next)");
    client.push_with_retry(&snap, 3).await?;
    Ok(())
}
```

- [ ] **Step 7.4: Commit**

```bash
git add crates/agent
git commit -m "feat(agent): push snapshots to the server with bounded exponential backoff

- Add PushClient using reqwest with a 10s request timeout
- POST snapshots to /api/v1/snapshot with bearer-token auth and JSON body
- Map non-2xx responses to a typed PushError::Status with status + body
- Add push_with_retry helper that retries with 1s, 4s, 16s, 60s backoff
- Mockito-based tests cover headers, error mapping and retry recovery
- Replace main with a one-shot push so the agent can talk to the server end-to-end"
```

---

## Task 8: Agent — periodic loop

**Files:**
- Create: `crates/agent/src/loop_.rs`
- Modify: `crates/agent/src/main.rs`

- [ ] **Step 8.1: Implement and unit-test the loop** in `crates/agent/src/loop_.rs`

```rust
use std::time::Duration;

use docktornet_core::MachineId;
use tokio::time::{Instant, sleep_until};
use tracing::{error, info};

use crate::push::PushClient;
use crate::snapshot;
use crate::sockets::ContainerSource;

/// Run the poll/push loop forever. Each cycle:
///   1. lists containers from every source
///   2. builds a Snapshot
///   3. pushes it with retry
/// Errors after retry exhaust are logged and the loop continues — we never
/// abort because of a transient server outage.
pub async fn run(
    machine: MachineId,
    sources: Vec<Box<dyn ContainerSource>>,
    client: PushClient,
    interval: Duration,
) -> ! {
    let mut next = Instant::now();
    loop {
        let raw  = crate::sockets::list_all(&sources).await;
        let snap = snapshot::build(&machine, &raw);
        match client.push_with_retry(&snap, 3).await {
            Ok(()) => info!(containers = snap.containers.len(), "snapshot pushed"),
            Err(e) => error!(error = %e, "snapshot push failed after retries; will try again next tick"),
        }
        next += interval;
        sleep_until(next).await;
    }
}

#[cfg(test)]
mod tests {
    // The loop body is exercised end-to-end in the workspace integration test
    // (Task 14). Here we only assert the function compiles with the expected
    // signature; behaviour is covered elsewhere.
    use super::*;
    fn _signature_check(
        m: MachineId,
        s: Vec<Box<dyn ContainerSource>>,
        c: PushClient,
        d: Duration,
    ) -> impl std::future::Future<Output = !> {
        run(m, s, c, d)
    }
}
```

> Note: `!` is the never-type, stable since 1.41 in return position. If clippy complains, switch the return type to `()` and `loop {}` at the end of `main` instead.

- [ ] **Step 8.2: Wire `loop_::run` into main**

Replace `crates/agent/src/main.rs`:

```rust
mod config;
mod sockets;
mod snapshot;
mod push;
mod loop_;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::sockets::{BollardSource, ContainerSource};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cfg = config::AgentConfig::from_env()?;
    let client = push::PushClient::new(&cfg.server_url, &cfg.secret)?;

    let mut sources: Vec<Box<dyn ContainerSource>> = Vec::new();
    for spec in &cfg.sockets {
        match BollardSource::connect(spec) {
            Ok(src) => sources.push(Box::new(src)),
            Err(e)  => tracing::error!(socket = %spec.path, error = %e, "failed to connect, skipping"),
        }
    }

    info!(machine_id = %cfg.machine_id.as_str(), sources = sources.len(), interval = ?cfg.poll_interval, "agent ready");
    loop_::run(cfg.machine_id, sources, client, cfg.poll_interval).await;
}
```

- [ ] **Step 8.3: Run `cargo check`**

```bash
cargo check -p docktornet-agent
```

Expected: succeeds. If the `!` return type fails to compile, change the loop to return `()` (and remove the `-> !` annotation; the function still loops forever).

- [ ] **Step 8.4: Run the workspace test suite to confirm nothing else regressed**

```bash
cargo test --workspace
```

Expected: all tests pass.

- [ ] **Step 8.5: Commit**

```bash
git add crates/agent
git commit -m "feat(agent): wire periodic poll/push loop

- Add loop_::run that lists containers, builds a snapshot and pushes every interval
- Sleep until the next tick instead of from each iteration's start to avoid drift
- Log push failures and continue rather than aborting on transient errors
- Replace one-shot main with a long-running loop driven by AgentConfig.poll_interval"
```

---

## Task 9: Server — configuration

**Files:**
- Create: `crates/server/src/config.rs`
- Modify: `crates/server/src/main.rs`

- [ ] **Step 9.1: Implement and test `ServerConfig`** in `crates/server/src/config.rs`

```rust
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub listen: String,        // e.g. "0.0.0.0:5363"
    pub secret: String,
    pub db_path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing environment variable '{0}'")]
    Missing(&'static str),
}

impl ServerConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_iter(std::env::vars().collect())
    }

    pub fn from_iter(env: HashMap<String, String>) -> Result<Self, ConfigError> {
        let get = |k: &'static str| env.get(k).cloned().ok_or(ConfigError::Missing(k));
        Ok(Self {
            listen:  get("DOCKTORNET_LISTEN")?,
            secret:  get("DOCKTORNET_SECRET")?,
            db_path: PathBuf::from(get("DOCKTORNET_DB_PATH")?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn happy_path() {
        let c = ServerConfig::from_iter(env(&[
            ("DOCKTORNET_LISTEN",  "0.0.0.0:5363"),
            ("DOCKTORNET_SECRET",  "s"),
            ("DOCKTORNET_DB_PATH", "/data/x.db"),
        ])).unwrap();
        assert_eq!(c.listen, "0.0.0.0:5363");
        assert_eq!(c.db_path, PathBuf::from("/data/x.db"));
    }

    #[test]
    fn missing_var() {
        let err = ServerConfig::from_iter(env(&[])).unwrap_err();
        match err { ConfigError::Missing(v) => assert_eq!(v, "DOCKTORNET_LISTEN") }
    }
}
```

- [ ] **Step 9.2: Replace `crates/server/src/main.rs`**

```rust
mod config;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cfg = config::ServerConfig::from_env()?;
    info!(listen = %cfg.listen, db = ?cfg.db_path, "server starting (HTTP not wired yet)");
    Ok(())
}
```

- [ ] **Step 9.3: Run tests**

```bash
cargo test -p docktornet-server config::tests
```

Expected: 2 pass.

- [ ] **Step 9.4: Commit**

```bash
git add crates/server/src
git commit -m "feat(server): load ServerConfig from environment

- Add ServerConfig with listen, secret and db_path fields
- Read DOCKTORNET_LISTEN, DOCKTORNET_SECRET, DOCKTORNET_DB_PATH from env
- Surface missing vars via typed ConfigError
- Initialise tracing and dotenv in main as a placeholder before HTTP wiring"
```

---

## Task 10: Server — SQLite schema and pool

**Files:**
- Create: `crates/server/migrations/0001_init.sql`
- Create: `crates/server/src/db.rs`
- Modify: `crates/server/src/main.rs`

- [ ] **Step 10.1: Write the initial migration**

```sql
-- crates/server/migrations/0001_init.sql
CREATE TABLE IF NOT EXISTS services (
    id          TEXT PRIMARY KEY,        -- "{machine_id}/{container_name}"
    machine_id  TEXT NOT NULL,
    container   TEXT NOT NULL,
    labels_json TEXT NOT NULL,
    last_seen   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_services_machine_id ON services(machine_id);
```

> Note: NPM/DNS resource tables come in Plan 2 — Plan 1 only persists the snapshot itself.

- [ ] **Step 10.2: Add sqlx CLI dependency requirement (no Rust change yet)** — verify the developer has `sqlx-cli` available for offline mode is **not** required since we use runtime migrations. Skip this if it's already present.

- [ ] **Step 10.3: Implement and test the db module** in `crates/server/src/db.rs`

```rust
use std::path::Path;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Sqlite};
use tracing::info;

pub type Db = Pool<Sqlite>;

pub async fn connect(db_path: &Path) -> anyhow::Result<Db> {
    if let Some(parent) = db_path.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent).await?;
        }
    }
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(opts)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    info!(?db_path, "database ready");
    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn connect_creates_db_and_runs_migrations() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let pool = connect(&path).await.unwrap();

        // Migration ran iff the `services` table exists.
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM services")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(row.0, 0);
    }
}
```

- [ ] **Step 10.4: Wire into main** (replace `crates/server/src/main.rs`)

```rust
mod config;
mod db;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cfg = config::ServerConfig::from_env()?;
    let _pool = db::connect(&cfg.db_path).await?;
    info!(listen = %cfg.listen, "server starting (HTTP routes not wired yet)");
    Ok(())
}
```

- [ ] **Step 10.5: Run tests**

```bash
cargo test -p docktornet-server db::tests
```

Expected: 1 test passes.

- [ ] **Step 10.6: Commit**

```bash
git add crates/server
git commit -m "feat(server): connect to SQLite with WAL and run migrations

- Add migrations/0001_init.sql defining the services table
- Add db::connect that creates the parent dir, opens SQLite in WAL mode and runs migrations
- Use a small connection pool (8) sized for homelab workloads
- Test that connect produces a queryable services table"
```

---

## Task 11: Server — bearer auth middleware

**Files:**
- Create: `crates/server/src/auth.rs`

- [ ] **Step 11.1: Implement and test** in `crates/server/src/auth.rs`

```rust
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use tracing::warn;

/// Shared state carried by the bearer-auth middleware.
#[derive(Clone)]
pub struct AuthState {
    pub secret: Arc<String>,
}

pub async fn require_bearer(
    State(state): State<AuthState>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let header = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let provided = header.and_then(|h| h.strip_prefix("Bearer "));
    if provided != Some(state.secret.as_str()) {
        warn!("rejected request with invalid or missing bearer token");
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn app() -> Router {
        let state = AuthState { secret: Arc::new("s3cret".into()) };
        Router::new()
            .route("/ping", get(|| async { "pong" }))
            .layer(axum::middleware::from_fn_with_state(state.clone(), require_bearer))
            .with_state(state)
    }

    #[tokio::test]
    async fn missing_header_returns_401() {
        let resp = app().oneshot(Request::builder().uri("/ping").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn correct_token_returns_200() {
        let req = Request::builder()
            .uri("/ping")
            .header("authorization", "Bearer s3cret")
            .body(Body::empty()).unwrap();
        let resp = app().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn wrong_token_returns_401() {
        let req = Request::builder()
            .uri("/ping")
            .header("authorization", "Bearer nope")
            .body(Body::empty()).unwrap();
        let resp = app().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
```

- [ ] **Step 11.2: Add `tower` as a dev-dependency** to `crates/server/Cargo.toml`

```toml
[dev-dependencies]
# ... existing ...
tower = { version = "0.5", features = ["util"] }
```

- [ ] **Step 11.3: Run tests**

```bash
cargo test -p docktornet-server auth::tests
```

Expected: 3 pass.

- [ ] **Step 11.4: Commit**

```bash
git add crates/server
git commit -m "feat(server): add bearer-token auth middleware

- Add AuthState carrying a shared secret pointer
- Add require_bearer middleware that checks Authorization: Bearer <secret>
- Reject missing or mismatched tokens with 401 and a warn log
- Cover missing/correct/wrong tokens with axum + tower oneshot tests"
```

---

## Task 12: Server — `services` table storage

**Files:**
- Create: `crates/server/src/storage.rs`

- [ ] **Step 12.1: Implement and test storage** in `crates/server/src/storage.rs`

```rust
use docktornet_core::Snapshot;
use sqlx::{Sqlite, Pool};

pub async fn upsert_snapshot(pool: &Pool<Sqlite>, snap: &Snapshot) -> sqlx::Result<()> {
    let now = snap.taken_at as i64;
    let machine = snap.machine_id.as_str();
    let seen_ids: Vec<String> = snap.containers.iter()
        .map(|c| format!("{}/{}", machine, c.name))
        .collect();

    let mut tx = pool.begin().await?;

    // upsert each container
    for c in &snap.containers {
        let id = format!("{}/{}", machine, c.name);
        let labels_json = serde_json::to_string(&c.labels).unwrap_or_else(|_| "{}".into());
        sqlx::query(
            r#"INSERT INTO services (id, machine_id, container, labels_json, last_seen)
               VALUES (?1, ?2, ?3, ?4, ?5)
               ON CONFLICT(id) DO UPDATE SET
                 labels_json = excluded.labels_json,
                 last_seen   = excluded.last_seen"#)
            .bind(&id)
            .bind(machine)
            .bind(&c.name)
            .bind(&labels_json)
            .bind(now)
            .execute(&mut *tx).await?;
    }

    // delete any services for this machine that were not in the snapshot
    if seen_ids.is_empty() {
        sqlx::query("DELETE FROM services WHERE machine_id = ?1")
            .bind(machine)
            .execute(&mut *tx).await?;
    } else {
        // Build the IN clause manually because sqlx doesn't expand Vec into IN args.
        let placeholders = std::iter::repeat("?").take(seen_ids.len()).collect::<Vec<_>>().join(",");
        let sql = format!("DELETE FROM services WHERE machine_id = ?1 AND id NOT IN ({placeholders})");
        let mut q = sqlx::query(&sql).bind(machine);
        for id in &seen_ids { q = q.bind(id); }
        q.execute(&mut *tx).await?;
    }

    tx.commit().await
}

pub async fn list_services_for_machine(pool: &Pool<Sqlite>, machine: &str) -> sqlx::Result<Vec<(String, String)>> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, labels_json FROM services WHERE machine_id = ?1 ORDER BY id")
        .bind(machine)
        .fetch_all(pool).await?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use docktornet_core::{ContainerEntry, MachineId};
    use tempfile::tempdir;

    use crate::db;

    fn entry(name: &str, labels: &[(&str, &str)]) -> ContainerEntry {
        ContainerEntry {
            name: name.into(),
            labels: labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<BTreeMap<_, _>>(),
        }
    }

    async fn pool() -> Pool<Sqlite> {
        let dir = Box::leak(Box::new(tempdir().unwrap()));
        db::connect(&dir.path().join("t.db")).await.unwrap()
    }

    #[tokio::test]
    async fn inserts_new_containers() {
        let p = pool().await;
        upsert_snapshot(&p, &Snapshot {
            machine_id: MachineId::new("m1"),
            taken_at: 100,
            containers: vec![entry("a", &[("docktornet.x", "1")])],
        }).await.unwrap();

        let rows = list_services_for_machine(&p, "m1").await.unwrap();
        assert_eq!(rows, vec![("m1/a".into(), "{\"docktornet.x\":\"1\"}".into())]);
    }

    #[tokio::test]
    async fn updates_existing_when_labels_change() {
        let p = pool().await;
        let m = MachineId::new("m1");
        upsert_snapshot(&p, &Snapshot {
            machine_id: m.clone(), taken_at: 100,
            containers: vec![entry("a", &[("docktornet.x", "1")])],
        }).await.unwrap();
        upsert_snapshot(&p, &Snapshot {
            machine_id: m, taken_at: 200,
            containers: vec![entry("a", &[("docktornet.x", "2")])],
        }).await.unwrap();

        let rows = list_services_for_machine(&p, "m1").await.unwrap();
        assert_eq!(rows[0].1, "{\"docktornet.x\":\"2\"}");
    }

    #[tokio::test]
    async fn removes_containers_not_in_latest_snapshot() {
        let p = pool().await;
        let m = MachineId::new("m1");
        upsert_snapshot(&p, &Snapshot {
            machine_id: m.clone(), taken_at: 100,
            containers: vec![
                entry("a", &[("docktornet.x", "1")]),
                entry("b", &[("docktornet.y", "2")]),
            ],
        }).await.unwrap();
        upsert_snapshot(&p, &Snapshot {
            machine_id: m, taken_at: 200,
            containers: vec![entry("a", &[("docktornet.x", "1")])],
        }).await.unwrap();

        let rows = list_services_for_machine(&p, "m1").await.unwrap();
        assert_eq!(rows.iter().map(|r| r.0.clone()).collect::<Vec<_>>(), vec!["m1/a"]);
    }

    #[tokio::test]
    async fn empty_snapshot_clears_machine() {
        let p = pool().await;
        let m = MachineId::new("m1");
        upsert_snapshot(&p, &Snapshot {
            machine_id: m.clone(), taken_at: 100,
            containers: vec![entry("a", &[("docktornet.x", "1")])],
        }).await.unwrap();
        upsert_snapshot(&p, &Snapshot {
            machine_id: m, taken_at: 200, containers: vec![],
        }).await.unwrap();

        let rows = list_services_for_machine(&p, "m1").await.unwrap();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn does_not_touch_other_machines() {
        let p = pool().await;
        upsert_snapshot(&p, &Snapshot {
            machine_id: MachineId::new("m1"), taken_at: 100,
            containers: vec![entry("a", &[("docktornet.x", "1")])],
        }).await.unwrap();
        upsert_snapshot(&p, &Snapshot {
            machine_id: MachineId::new("m2"), taken_at: 100, containers: vec![],
        }).await.unwrap();

        let rows = list_services_for_machine(&p, "m1").await.unwrap();
        assert_eq!(rows.len(), 1);
    }
}
```

- [ ] **Step 12.2: Add `storage` to `crates/server/src/main.rs`**

Edit `main.rs` and add `mod storage;` near the top (next to `mod db;`). The function is unused from main directly — the handler in Task 13 calls it.

- [ ] **Step 12.3: Run tests**

```bash
cargo test -p docktornet-server storage::tests
```

Expected: 5 pass.

- [ ] **Step 12.4: Commit**

```bash
git add crates/server
git commit -m "feat(server): persist snapshots into the services table

- Add storage::upsert_snapshot that inserts/updates containers and removes ones missing from the snapshot
- Use a transaction so each snapshot is applied atomically
- Build the NOT IN delete clause dynamically since sqlx does not expand Vec parameters
- Cover insert, update-on-change, delete-on-disappear, empty snapshot and machine isolation with tests"
```

---

## Task 13: Server — snapshot endpoint + router

**Files:**
- Create: `crates/server/src/handlers/snapshot.rs`
- Create: `crates/server/src/handlers/mod.rs`
- Create: `crates/server/src/routes.rs`
- Modify: `crates/server/src/main.rs`

- [ ] **Step 13.1: Create handler module** at `crates/server/src/handlers/mod.rs`

```rust
pub mod snapshot;
```

- [ ] **Step 13.2: Write handler** at `crates/server/src/handlers/snapshot.rs`

```rust
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use docktornet_core::Snapshot;
use tracing::{info, warn};

use crate::db::Db;
use crate::storage;

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub secret: Arc<String>,
}

pub async fn ingest(State(state): State<AppState>, Json(snap): Json<Snapshot>) -> StatusCode {
    info!(machine = %snap.machine_id.as_str(), containers = snap.containers.len(), "snapshot received");
    match storage::upsert_snapshot(&state.db, &snap).await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            warn!(error = %e, "failed to persist snapshot");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
```

- [ ] **Step 13.3: Wire the router** at `crates/server/src/routes.rs`

```rust
use std::sync::Arc;

use axum::routing::post;
use axum::Router;

use crate::auth::{AuthState, require_bearer};
use crate::db::Db;
use crate::handlers::snapshot::{AppState, ingest};

pub fn build(db: Db, secret: String) -> Router {
    let auth_state = AuthState { secret: Arc::new(secret.clone()) };
    let app_state = AppState { db, secret: auth_state.secret.clone() };

    Router::new()
        .route("/api/v1/snapshot", post(ingest))
        .layer(axum::middleware::from_fn_with_state(auth_state.clone(), require_bearer))
        .with_state(app_state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use docktornet_core::{ContainerEntry, MachineId, Snapshot};
    use tempfile::tempdir;
    use tower::ServiceExt;

    use crate::db;
    use crate::storage::list_services_for_machine;

    async fn make_app() -> (Router, Db) {
        let dir = Box::leak(Box::new(tempdir().unwrap()));
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let router = build(pool.clone(), "s3cret".into());
        (router, pool)
    }

    fn body(snap: &Snapshot) -> Body {
        Body::from(serde_json::to_vec(snap).unwrap())
    }

    fn snap() -> Snapshot {
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "u".into());
        Snapshot {
            machine_id: MachineId::new("m1"),
            taken_at: 1,
            containers: vec![ContainerEntry { name: "a".into(), labels }],
        }
    }

    #[tokio::test]
    async fn ingest_requires_bearer() {
        let (app, _) = make_app().await;
        let req = Request::builder().method("POST").uri("/api/v1/snapshot")
            .header("content-type", "application/json")
            .body(body(&snap())).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn ingest_persists_snapshot() {
        let (app, pool) = make_app().await;
        let req = Request::builder().method("POST").uri("/api/v1/snapshot")
            .header("authorization", "Bearer s3cret")
            .header("content-type", "application/json")
            .body(body(&snap())).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        let rows = list_services_for_machine(&pool, "m1").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "m1/a");
    }
}
```

- [ ] **Step 13.4: Replace `crates/server/src/main.rs`**

```rust
mod auth;
mod config;
mod db;
mod handlers;
mod routes;
mod storage;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cfg  = config::ServerConfig::from_env()?;
    let pool = db::connect(&cfg.db_path).await?;
    let app  = routes::build(pool, cfg.secret);

    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    info!(listen = %cfg.listen, "server ready");
    axum::serve(listener, app).await?;
    Ok(())
}
```

- [ ] **Step 13.5: Run all server tests**

```bash
cargo test -p docktornet-server
```

Expected: all tests (including config, db, auth, storage, routes) pass.

- [ ] **Step 13.6: Smoke-test the binary**

In one terminal:

```bash
DOCKTORNET_LISTEN=127.0.0.1:5363 \
DOCKTORNET_SECRET=s3cret \
DOCKTORNET_DB_PATH=/tmp/docktornet-smoke.db \
cargo run -p docktornet-server
```

In another terminal:

```bash
curl -fsS -X POST http://127.0.0.1:5363/api/v1/snapshot \
  -H "Authorization: Bearer s3cret" \
  -H "Content-Type: application/json" \
  -d '{"machine_id":"smoke","taken_at":1,"containers":[]}'
```

Expected: `curl` exits 0 (204 No Content). Stop the server with Ctrl-C. Inspect the SQLite db:

```bash
sqlite3 /tmp/docktornet-smoke.db 'SELECT * FROM services;'
```

Expected: no rows (we sent an empty snapshot). Re-run curl with one container and re-check:

```bash
curl -fsS -X POST http://127.0.0.1:5363/api/v1/snapshot \
  -H "Authorization: Bearer s3cret" -H "Content-Type: application/json" \
  -d '{"machine_id":"smoke","taken_at":2,"containers":[{"name":"hello","labels":{"docktornet.x":"y"}}]}'
sqlite3 /tmp/docktornet-smoke.db 'SELECT id, last_seen FROM services;'
```

Expected: one row `smoke/hello | 2`.

- [ ] **Step 13.7: Commit**

```bash
git add crates/server
git commit -m "feat(server): expose POST /api/v1/snapshot guarded by bearer auth

- Add AppState carrying the db pool and shared secret
- Add handlers::snapshot::ingest that persists snapshots via storage::upsert_snapshot
- Add routes::build that wires the bearer auth middleware to the ingest route
- Bind axum::serve to DOCKTORNET_LISTEN in main and run forever
- Unit-test that the endpoint rejects unauthenticated requests and persists authenticated ones"
```

---

## Task 14: End-to-end integration test

Workspace-level integration test that runs the real agent loop against the real server binary in-process.

**Files:**
- Create: `crates/server/tests/e2e_snapshot_push.rs`

(We put the test inside the server crate to reuse its module visibility cleanly. Workspace-level integration tests would force re-exporting more APIs.)

- [ ] **Step 14.1: Create test scaffold and write the test**

`crates/server/tests/e2e_snapshot_push.rs`:

```rust
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use docktornet_core::{ContainerEntry, MachineId, Snapshot};
use reqwest::Client;
use tempfile::tempdir;

#[tokio::test]
async fn agent_push_lands_in_sqlite_via_real_http() {
    // 1. Spin up the real server on a random local port using its public router.
    let dir = tempdir().unwrap();
    let pool = docktornet_server::db::connect(&dir.path().join("e2e.db")).await.unwrap();
    let app  = docktornet_server::routes::build(pool.clone(), "s3cret".into());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr     = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // 2. Build a Snapshot directly (we don't need a real docker socket).
    let mut labels = BTreeMap::new();
    labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
    let snap = Snapshot {
        machine_id: MachineId::new("e2e-host"),
        taken_at: 42,
        containers: vec![ContainerEntry { name: "demo".into(), labels }],
    };

    // 3. Push via reqwest (mirrors what the agent does).
    let http = Client::new();
    let resp = http.post(format!("http://{}/api/v1/snapshot", addr))
        .bearer_auth("s3cret")
        .json(&snap)
        .send().await.unwrap();
    assert_eq!(resp.status(), 204);

    // 4. Verify it landed in SQLite.
    let rows = docktornet_server::storage::list_services_for_machine(&pool, "e2e-host").await.unwrap();
    assert_eq!(rows.iter().map(|r| r.0.clone()).collect::<Vec<_>>(), vec!["e2e-host/demo"]);

    server_task.abort();
    let _ = tokio::time::timeout(Duration::from_secs(1), server_task).await;
    drop(Arc::new(())); // silence dead-import warning if any
}
```

> Note: this test requires `docktornet_server`'s modules to be visible. We need to expose them as a `lib` target as well as the existing `bin`.

- [ ] **Step 14.2: Expose a library target for the server crate**

Modify `crates/server/Cargo.toml` to add:

```toml
[lib]
name = "docktornet_server"
path = "src/lib.rs"
```

Create `crates/server/src/lib.rs` (re-exports only):

```rust
pub mod auth;
pub mod config;
pub mod db;
pub mod handlers;
pub mod routes;
pub mod storage;
```

Modify `crates/server/src/main.rs` to import from the lib instead of declaring modules itself:

```rust
use anyhow::Result;
use docktornet_server::{config, db, routes};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cfg  = config::ServerConfig::from_env()?;
    let pool = db::connect(&cfg.db_path).await?;
    let app  = routes::build(pool, cfg.secret);

    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    info!(listen = %cfg.listen, "server ready");
    axum::serve(listener, app).await?;
    Ok(())
}
```

- [ ] **Step 14.3: Run the e2e test**

```bash
cargo test -p docktornet-server --test e2e_snapshot_push
```

Expected: passes.

- [ ] **Step 14.4: Run the whole workspace test suite**

```bash
cargo test --workspace
```

Expected: every test in core, agent and server passes.

- [ ] **Step 14.5: Run clippy across the workspace**

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: no warnings.

- [ ] **Step 14.6: Commit**

```bash
git add crates/server
git commit -m "test(server): add end-to-end snapshot push test

- Expose server modules as docktornet_server lib so integration tests can drive them
- Bind a real axum server on 127.0.0.1:0 and POST a real Snapshot via reqwest
- Assert the response is 204 and the row lands in the services table
- Use tempfile for the SQLite path so the test leaves no state behind
- Switch main to consume the lib so binary and tests share one module tree"
```

---

## Task 15: Real-world smoke run + push to remotes

- [ ] **Step 15.1: Stand up the server locally**

```bash
DOCKTORNET_LISTEN=127.0.0.1:5363 \
DOCKTORNET_SECRET=s3cret \
DOCKTORNET_DB_PATH=$PWD/data/docktornet.db \
cargo run -p docktornet-server &
SERVER_PID=$!
```

Expected: log line `server ready listen=127.0.0.1:5363`.

- [ ] **Step 15.2: Stand up the agent locally** in a second terminal

```bash
DOCKTORNET_SERVER_URL=http://127.0.0.1:5363 \
DOCKTORNET_SECRET=s3cret \
DOCKTORNET_POLL_INTERVAL=5s \
DOCKTORNET_MACHINE_ID=$(hostname) \
DOCKTORNET_SOCKETS=docker:/var/run/docker.sock \
cargo run -p docktornet-agent
```

> If this is a Podman-only machine, swap the socket to `podman:/run/user/$(id -u)/podman/podman.sock` first.

Expected: every 5 seconds the agent logs `snapshot pushed containers=<n>` and the server logs `snapshot received`.

- [ ] **Step 15.3: Verify SQLite state**

```bash
sqlite3 $PWD/data/docktornet.db 'SELECT id, length(labels_json), last_seen FROM services;'
```

Expected: one row per running container that has a `docktornet.*` label. If your hosts don't yet have any labelled containers, add one to a non-critical service first and rerun — the agent's next tick will pick it up.

- [ ] **Step 15.4: Stop the agent and server** (Ctrl-C the agent, then `kill $SERVER_PID`).

- [ ] **Step 15.5: Push the foundation to both remotes**

```bash
git push origin development
git push batterforge development
```

Expected: both pushes succeed.

- [ ] **Step 15.6: Final commit (operational documentation)**

Create a short `README.md` at the repo root *only if it does not already exist* (this is the one allowed Markdown file outside `docs/` since the user asked us to push to public remotes):

```markdown
# Docktornet

Docker/Podman label-driven reconciliation for Nginx Proxy Manager, Cloudflare DNS, and Gatus.

See [the v1 spec](docs/superpowers/specs/2026-05-21-docktornet-design.md) for design details.

## Status

**v1 in progress.** Plan 1 (foundation) implemented: workspace, agent, server, SQLite snapshot ingest.

## Quick start (local dev)

```bash
just run-server  # in one terminal, with server.env loaded
just run-agent   # in another, with agent.env loaded
```
```

> Stop here if a README already exists in the repo — don't overwrite it. If yes:

```bash
git add README.md
git commit -m "docs(readme): add minimal README pointing to the v1 spec

- Add repo README so the GitHub mirror has a landing page
- Link to the v1 design spec in docs/superpowers/specs/
- Document quick-start commands using the Justfile"
git push origin development
git push batterforge development
```

---

## Self-review (run after the plan is written)

This section is for the plan author, not the executor — it is the gate before handing the plan off.

### Spec coverage

Spec sections vs tasks:

| Spec section | Covered by |
|---|---|
| §1 Purpose | n/a (descriptive) |
| §2.1 Binaries | Tasks 1, 4–8, 9–13 |
| §2.2 Workspace layout | Task 1 |
| §2.3 Communication (HTTP + Bearer) | Tasks 7, 11, 13 |
| §2.4 Container runtimes (Docker + Podman socket) | Task 5 |
| §2.5 Reconciliation model | **Deferred to Plan 3** (foundation only) |
| §3.1–3.4 Label schema | Task 3 (parsing only, no use yet) |
| §3.5 Service identity (incl. sanitisation) | Task 2 |
| §4 Configuration (.env-style) | Tasks 4, 9 |
| §4.3 Machine identity flapping check | **Deferred to Plan 3** (operational concern) |
| §5 SQLite schema | Task 10 (services table only — NPM/DNS tables deferred to Plan 2) |
| §6 Reconciliation logic | **Deferred to Plans 2 + 3** |
| §7 Filesystem layout | Task 10 (db path); Gatus dir comes in Plan 2 |
| §8 Deployment | **Deferred to Plan 3** (Dockerfiles) |

All Plan 1 scope is covered. Deferrals are intentional and called out at the top.

### Placeholder scan

Scanned the plan for the patterns the writing-plans skill forbids:

- No "TBD" / "TODO" / "implement later".
- No "add appropriate error handling" — every error path is explicit (`ConfigError`, `PushError`, `anyhow::Result` at the binary edge).
- No "write tests for the above" without code — every test has a runnable block.
- No "similar to Task N" — code is repeated where needed.
- No references to types/functions/methods that are not introduced earlier in this plan.

### Type consistency

Cross-checked the names used between tasks:

- `MachineId` introduced in Task 2 (ids.rs), used unchanged in Tasks 4, 6, 8, 9, 13, 14.
- `Snapshot`/`ContainerEntry` introduced in Task 2 (snapshot.rs), used unchanged in Tasks 6, 7, 12, 13, 14.
- `service_id` returns `"machine/container"` (Task 2); `storage::upsert_snapshot` rebuilds the same string with `format!("{}/{}", machine, c.name)` (Task 12) — consistent.
- `sanitize_service_id` introduced in Task 2; not used in Plan 1 (Gatus filenames are written in Plan 2). Re-export is in `core/lib.rs` so it's accessible when Plan 2 needs it.
- `AgentConfig` / `ServerConfig` field names are consistent across tasks.
- `AppState` (server) and `AuthState` (server) are distinct on purpose; the router (Task 13) constructs both from the same secret.

No inconsistencies found.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-21-docktornet-foundation.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
