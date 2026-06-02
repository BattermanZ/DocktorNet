# Docktornet Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add full v1 reconciliation for Nginx Proxy Manager proxy hosts, Cloudflare DNS CNAME records, and Gatus monitor files, starting with complete external backups before any development write touches live systems.

**Architecture:** The server remains the only component that owns external credentials. Snapshot ingest will persist the latest snapshot, derive intended resources from parsed `docktornet.*` labels, then reconcile each integration through focused modules with resource IDs tracked in SQLite. External writes are gated by `DOCKTORNET_DRY_RUN`, defaulting to `true`; Gatus is updated by atomic YAML file writes in a directory watched by Gatus.

**Tech Stack:** Rust 2024, Tokio, Axum, SQLx SQLite, Reqwest, Serde, Serde JSON, Serde YAML, Tempfile, Mockito, shell/curl/jq for one-off live backups.

---

## Scope

Plan 2 implements the v1 reconciliation behavior from `docs/superpowers/specs/2026-05-21-docktornet-design.md`:

- NPM proxy host create, update, delete, certificate lookup, and duplicate adoption.
- Cloudflare DNS CNAME create, update, delete, lookup, and duplicate adoption.
- Gatus YAML generation and removal.
- Dry-run mode that computes/logs actions without NPM, Cloudflare, or Gatus writes.
- SQLite tracking tables for externally managed NPM and Cloudflare resources.

This plan intentionally does not implement Cloudflare Tunnel, Cloudflare Access, multi-zone DNS, or container images.

## Required Operator Preflight

Before Task 1 is executed, confirm these local tools are installed:

```bash
curl --version
jq --version
```

The live external smoke tests already validated:

- Cloudflare token verify, zone read, DNS create/read/update/delete.
- NPM JWT auth, certificate list read, proxy host list read, proxy host create/read/delete.
- Gatus file hot reload after `config.yaml` changes.

Important Gatus configuration requirement: Docktornet will write one managed YAML file per service. Gatus only merges multiple `.yaml`/`.yml` files when `GATUS_CONFIG_PATH` points to a directory. The test Compose file should be updated before final live testing:

```yaml
environment:
  - TZ=Europe/Amsterdam
  - GATUS_CONFIG_PATH=/config
```

Keep a base config file such as `/home/battermanz/gatus/config/00-base.yaml` with `web` and `storage`, then let Docktornet write files like `/home/battermanz/gatus/config/batterdock__myapp.yaml`.

---

## File Structure

Create:

- `scripts/backup_external_state.sh` — backup NPM and Cloudflare zone state before development writes.
- `crates/server/migrations/0002_external_resources.sql` — `npm_hosts` and `cf_dns_records` tracking tables.
- `crates/server/src/integrations/mod.rs` — integration module exports.
- `crates/server/src/integrations/intended.rs` — intended resource structs derived from labels.
- `crates/server/src/integrations/npm.rs` — NPM API client, payload mapping, certificate matching.
- `crates/server/src/integrations/cloudflare.rs` — Cloudflare DNS API client.
- `crates/server/src/integrations/gatus.rs` — deterministic Gatus YAML renderer/writer.
- `crates/server/src/reconcile.rs` — orchestration across storage and integrations.

Modify:

- `.gitignore` — ignore local backup output.
- `.env.example` — document new server settings.
- `Cargo.toml` — add `serde_yaml` and `url` workspace dependencies.
- `crates/server/Cargo.toml` — add server dependencies and dev dependencies.
- `crates/server/src/lib.rs` — export new modules.
- `crates/server/src/config.rs` — add integration config and dry-run/adoption settings.
- `crates/server/src/handlers/snapshot.rs` — call reconciler instead of raw snapshot upsert.
- `crates/server/src/main.rs` — build `Reconciler` from config and pass it into router state.
- `crates/server/src/routes.rs` — accept full app state.
- `crates/server/src/storage.rs` — add diff/resource tracking helpers.
- `crates/server/src/db.rs` — test new tables exist after migrations.
- `crates/server/tests/e2e_snapshot_push.rs` — update state construction for dry-run reconciler.

---

## Task 1: Backup Live External State First

**Files:**
- Modify: `.gitignore`
- Create: `scripts/backup_external_state.sh`

- [ ] **Step 1.1: Add backup output to `.gitignore`**

Add this section:

```gitignore
# Local external API backups
docktornet-backups/
```

- [ ] **Step 1.2: Create the backup script**

Create `scripts/backup_external_state.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${1:-$ROOT/.env.local}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "missing env file: $ENV_FILE" >&2
  exit 2
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

need() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "missing $name in $ENV_FILE" >&2
    exit 2
  fi
}

for name in \
  DOCKTORNET_NPM_URL \
  DOCKTORNET_NPM_USER \
  DOCKTORNET_NPM_PASSWORD \
  DOCKTORNET_CF_API_TOKEN \
  DOCKTORNET_CF_ZONE_ID
do
  need "$name"
done

command -v curl >/dev/null || { echo "curl is required" >&2; exit 2; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
out="$ROOT/docktornet-backups/$stamp"
mkdir -p "$out/npm" "$out/cloudflare"

echo "writing backup to $out"

npm_token="$(
  jq -n \
    --arg identity "$DOCKTORNET_NPM_USER" \
    --arg secret "$DOCKTORNET_NPM_PASSWORD" \
    '{identity:$identity, secret:$secret}' |
  curl -fsS "$DOCKTORNET_NPM_URL/api/tokens" \
    -H "Content-Type: application/json" \
    --data-binary @- |
  jq -r ".token"
)"

if [[ -z "$npm_token" || "$npm_token" == "null" ]]; then
  echo "NPM token request did not return a token" >&2
  exit 1
fi

npm_get() {
  local path="$1"
  local file="$2"
  curl -fsS "$DOCKTORNET_NPM_URL$path" \
    -H "Authorization: Bearer $npm_token" |
    jq "." > "$out/npm/$file"
}

npm_get "/api/nginx/proxy-hosts" "proxy-hosts.json"
npm_get "/api/nginx/certificates" "certificates.json"
npm_get "/api/nginx/redirection-hosts" "redirection-hosts.json"
npm_get "/api/nginx/dead-hosts" "dead-hosts.json"
npm_get "/api/nginx/streams" "streams.json"
npm_get "/api/nginx/access-lists" "access-lists.json"
npm_get "/api/settings" "settings.json"

cf_api="https://api.cloudflare.com/client/v4"
cf_get() {
  local path="$1"
  local file="$2"
  curl -fsS "$cf_api$path" \
    -H "Authorization: Bearer $DOCKTORNET_CF_API_TOKEN" |
    jq "." > "$out/cloudflare/$file"
}

cf_get "/user/tokens/verify" "token-verify.json"
cf_get "/zones/$DOCKTORNET_CF_ZONE_ID" "zone.json"
cf_get "/zones/$DOCKTORNET_CF_ZONE_ID/settings" "zone-settings.json"

page=1
all="$out/cloudflare/dns-records.jsonl"
: > "$all"
while true; do
  body="$(
    curl -fsS "$cf_api/zones/$DOCKTORNET_CF_ZONE_ID/dns_records?per_page=100&page=$page" \
      -H "Authorization: Bearer $DOCKTORNET_CF_API_TOKEN"
  )"
  jq -c ".result[]" <<<"$body" >> "$all"
  total_pages="$(jq -r ".result_info.total_pages" <<<"$body")"
  if [[ "$page" -ge "$total_pages" ]]; then
    break
  fi
  page=$((page + 1))
done
jq -s "." "$all" > "$out/cloudflare/dns-records.json"
rm "$all"

find "$out" -type f -maxdepth 3 -print | sort
echo "backup complete: $out"
```

- [ ] **Step 1.3: Make the script executable**

Run:

```bash
chmod +x scripts/backup_external_state.sh
```

- [ ] **Step 1.4: Run the backup against live config**

Run:

```bash
./scripts/backup_external_state.sh .env.local
```

Expected:

```text
writing backup to .../docktornet-backups/<timestamp>
.../npm/proxy-hosts.json
.../npm/certificates.json
.../cloudflare/zone.json
.../cloudflare/dns-records.json
backup complete: .../docktornet-backups/<timestamp>
```

- [ ] **Step 1.5: Verify backup contents without printing secrets**

Run:

```bash
latest="$(find docktornet-backups -mindepth 1 -maxdepth 1 -type d | sort | tail -1)"
jq 'length' "$latest/npm/proxy-hosts.json"
jq 'length' "$latest/npm/certificates.json"
jq 'length' "$latest/cloudflare/dns-records.json"
jq -r '.result.name' "$latest/cloudflare/zone.json"
```

Expected: numeric counts for NPM proxy hosts, NPM certificates, Cloudflare DNS records, and the Cloudflare zone name.

- [ ] **Step 1.6: Commit backup tooling**

Run:

```bash
git add .gitignore scripts/backup_external_state.sh
git commit -m "chore(backups): add external state backup script" -m "- Add script to export NPM configuration before reconciliation development" -m "- Add script to export Cloudflare zone settings and DNS records" -m "- Ignore local backup output containing live environment state"
```

---

## Task 2: Add Dependencies and Server Config

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/server/Cargo.toml`
- Modify: `crates/server/src/config.rs`
- Modify: `.env.example`

- [ ] **Step 2.1: Add failing config tests**

Append tests to `crates/server/src/config.rs`:

```rust
#[test]
fn parses_integration_config_with_safe_defaults() {
    let c = ServerConfig::from_map(env(&[
        ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
        ("DOCKTORNET_SECRET", "s"),
        ("DOCKTORNET_DB_PATH", "/data/x.db"),
        ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
        ("DOCKTORNET_NPM_URL", "http://npm:81"),
        ("DOCKTORNET_NPM_USER", "admin@example.com"),
        ("DOCKTORNET_NPM_PASSWORD", "pw"),
        ("DOCKTORNET_CF_API_TOKEN", "token"),
        ("DOCKTORNET_CF_ZONE_ID", "zone"),
    ]))
    .unwrap();

    assert!(c.dry_run);
    assert!(!c.adopt_existing);
    assert_eq!(c.gatus_config_dir, PathBuf::from("/etc/gatus/config"));
    assert_eq!(c.npm.url.as_str(), "http://npm:81");
    assert_eq!(c.cloudflare.zone_id, "zone");
}

#[test]
fn parses_explicit_write_flags() {
    let c = ServerConfig::from_map(env(&[
        ("DOCKTORNET_LISTEN", "0.0.0.0:5363"),
        ("DOCKTORNET_SECRET", "s"),
        ("DOCKTORNET_DB_PATH", "/data/x.db"),
        ("DOCKTORNET_DRY_RUN", "false"),
        ("DOCKTORNET_ADOPT_EXISTING", "true"),
        ("DOCKTORNET_GATUS_CONFIG_DIR", "/etc/gatus/config"),
        ("DOCKTORNET_NPM_URL", "http://npm:81"),
        ("DOCKTORNET_NPM_USER", "admin@example.com"),
        ("DOCKTORNET_NPM_PASSWORD", "pw"),
        ("DOCKTORNET_CF_API_TOKEN", "token"),
        ("DOCKTORNET_CF_ZONE_ID", "zone"),
    ]))
    .unwrap();

    assert!(!c.dry_run);
    assert!(c.adopt_existing);
}
```

- [ ] **Step 2.2: Run config tests and expect failure**

Run:

```bash
cargo test -p docktornet-server config::tests::parses_integration_config_with_safe_defaults config::tests::parses_explicit_write_flags
```

Expected: fail because `ServerConfig` lacks the new fields.

- [ ] **Step 2.3: Add workspace dependencies**

In root `Cargo.toml`, add:

```toml
serde_yaml       = "0.9"
url              = "2"
```

- [ ] **Step 2.4: Add server dependencies**

In `crates/server/Cargo.toml`, add:

```toml
reqwest     = { workspace = true }
serde_yaml  = { workspace = true }
url         = { workspace = true }
```

- [ ] **Step 2.5: Implement config structs**

Replace `crates/server/src/config.rs` with:

```rust
use std::collections::HashMap;
use std::path::PathBuf;

use url::Url;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub listen: String,
    pub secret: String,
    pub db_path: PathBuf,
    pub dry_run: bool,
    pub adopt_existing: bool,
    pub gatus_config_dir: PathBuf,
    pub npm: NpmConfig,
    pub cloudflare: CloudflareConfig,
}

#[derive(Debug, Clone)]
pub struct NpmConfig {
    pub url: Url,
    pub user: String,
    pub password: String,
}

#[derive(Debug, Clone)]
pub struct CloudflareConfig {
    pub api_token: String,
    pub zone_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing environment variable '{0}'")]
    Missing(&'static str),
    #[error("environment variable '{name}' must be a bool, got '{value}'")]
    InvalidBool { name: &'static str, value: String },
    #[error("environment variable '{name}' must be a URL, got '{value}'")]
    InvalidUrl { name: &'static str, value: String },
}

impl ServerConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_map(std::env::vars().collect())
    }

    pub fn from_map(env: HashMap<String, String>) -> Result<Self, ConfigError> {
        let get = |k: &'static str| env.get(k).cloned().ok_or(ConfigError::Missing(k));
        let get_bool = |k: &'static str, default: bool| match env.get(k) {
            Some(value) => parse_bool(k, value),
            None => Ok(default),
        };
        let get_url = |k: &'static str| {
            let value = get(k)?;
            Url::parse(&value).map_err(|_| ConfigError::InvalidUrl { name: k, value })
        };

        Ok(Self {
            listen: get("DOCKTORNET_LISTEN")?,
            secret: get("DOCKTORNET_SECRET")?,
            db_path: PathBuf::from(get("DOCKTORNET_DB_PATH")?),
            dry_run: get_bool("DOCKTORNET_DRY_RUN", true)?,
            adopt_existing: get_bool("DOCKTORNET_ADOPT_EXISTING", false)?,
            gatus_config_dir: PathBuf::from(get("DOCKTORNET_GATUS_CONFIG_DIR")?),
            npm: NpmConfig {
                url: get_url("DOCKTORNET_NPM_URL")?,
                user: get("DOCKTORNET_NPM_USER")?,
                password: get("DOCKTORNET_NPM_PASSWORD")?,
            },
            cloudflare: CloudflareConfig {
                api_token: get("DOCKTORNET_CF_API_TOKEN")?,
                zone_id: get("DOCKTORNET_CF_ZONE_ID")?,
            },
        })
    }
}

fn parse_bool(name: &'static str, value: &str) -> Result<bool, ConfigError> {
    match value {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(ConfigError::InvalidBool {
            name,
            value: value.to_string(),
        }),
    }
}
```

Keep the existing test helper and existing `happy_path`/`missing_var` tests, then update `happy_path` to include all required integration env vars.

- [ ] **Step 2.6: Update `.env.example`**

Add:

```env
DOCKTORNET_FULL_RESYNC_INTERVAL=1h
DOCKTORNET_GATUS_CONFIG_DIR=/etc/gatus/config
DOCKTORNET_DRY_RUN=true
DOCKTORNET_ADOPT_EXISTING=false

DOCKTORNET_NPM_URL=http://npm:81
DOCKTORNET_NPM_USER=admin@example.com
DOCKTORNET_NPM_PASSWORD=changeme

DOCKTORNET_CF_API_TOKEN=...
DOCKTORNET_CF_ZONE_ID=...
```

- [ ] **Step 2.7: Run config tests**

Run:

```bash
cargo test -p docktornet-server config
```

Expected: pass.

- [ ] **Step 2.8: Commit**

Run:

```bash
git add Cargo.toml crates/server/Cargo.toml crates/server/src/config.rs .env.example Cargo.lock
git commit -m "feat(config): add reconciliation settings" -m "- Add server config for dry-run, adoption, Gatus, NPM, and Cloudflare" -m "- Default external writes to dry-run for safe startup" -m "- Document phase 2 environment variables"
```

---

## Task 3: Add External Resource Schema

**Files:**
- Create: `crates/server/migrations/0002_external_resources.sql`
- Modify: `crates/server/src/db.rs`

- [ ] **Step 3.1: Add failing migration test**

In `crates/server/src/db.rs`, extend `connect_creates_db_and_runs_migrations`:

```rust
let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM npm_hosts")
    .fetch_one(&pool)
    .await
    .unwrap();
assert_eq!(row.0, 0);

let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM cf_dns_records")
    .fetch_one(&pool)
    .await
    .unwrap();
assert_eq!(row.0, 0);
```

- [ ] **Step 3.2: Run migration test and expect failure**

Run:

```bash
cargo test -p docktornet-server db::tests::connect_creates_db_and_runs_migrations
```

Expected: fail because `npm_hosts` does not exist.

- [ ] **Step 3.3: Add migration**

Create `crates/server/migrations/0002_external_resources.sql`:

```sql
CREATE TABLE IF NOT EXISTS npm_hosts (
    service_id TEXT NOT NULL,
    npm_index  INTEGER NOT NULL,
    npm_id     INTEGER NOT NULL,
    PRIMARY KEY (service_id, npm_index),
    FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS cf_dns_records (
    service_id TEXT PRIMARY KEY,
    record_id  TEXT NOT NULL,
    FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE CASCADE
);
```

- [ ] **Step 3.4: Run migration test**

Run:

```bash
cargo test -p docktornet-server db::tests::connect_creates_db_and_runs_migrations
```

Expected: pass.

- [ ] **Step 3.5: Commit**

Run:

```bash
git add crates/server/migrations/0002_external_resources.sql crates/server/src/db.rs
git commit -m "feat(db): track external resources" -m "- Add tables for NPM proxy host IDs and Cloudflare DNS record IDs" -m "- Verify migrations create resource tracking tables"
```

---

## Task 4: Build Intended State From Labels

**Files:**
- Create: `crates/server/src/integrations/mod.rs`
- Create: `crates/server/src/integrations/intended.rs`
- Modify: `crates/server/src/lib.rs`

- [ ] **Step 4.1: Export integration modules**

Add to `crates/server/src/lib.rs`:

```rust
pub mod integrations;
```

Create `crates/server/src/integrations/mod.rs`:

```rust
pub mod intended;
```

- [ ] **Step 4.2: Write failing intended-state tests**

Create `crates/server/src/integrations/intended.rs` with tests first:

```rust
use docktornet_core::{ContainerEntry, MachineId, labels::ParsedLabels, service_id};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntendedService {
    pub service_id: String,
    pub machine_id: MachineId,
    pub container: String,
    pub labels: ParsedLabels,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn entry(name: &str, labels: &[(&str, &str)]) -> ContainerEntry {
        ContainerEntry {
            name: name.to_string(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>(),
        }
    }

    #[test]
    fn ignores_containers_without_docktornet_labels() {
        let services = intended_services(
            &MachineId::new("m1"),
            &[entry("plain", &[("traefik.enable", "true")])],
        );
        assert!(services.is_empty());
    }

    #[test]
    fn parses_opted_in_container() {
        let services = intended_services(
            &MachineId::new("m1"),
            &[entry(
                "app",
                &[
                    ("docktornet.npm.0.host", "app.example.com"),
                    ("docktornet.npm.0.port", "3000"),
                    ("docktornet.npm.0.scheme", "http"),
                    ("docktornet.npm.0.ssl", "true"),
                ],
            )],
        );
        assert_eq!(services.len(), 1);
        assert_eq!(services[0].service_id, "m1/app");
        assert_eq!(services[0].labels.npm[0].host, "app.example.com");
    }
}
```

- [ ] **Step 4.3: Run intended-state tests and expect failure**

Run:

```bash
cargo test -p docktornet-server integrations::intended
```

Expected: fail because `intended_services` does not exist.

- [ ] **Step 4.4: Implement intended-state builder**

Add above the tests:

```rust
pub fn intended_services(machine_id: &MachineId, containers: &[ContainerEntry]) -> Vec<IntendedService> {
    containers
        .iter()
        .filter_map(|container| {
            let parsed = docktornet_core::labels::parse(&container.labels);
            let has_intent =
                !parsed.npm.is_empty() || parsed.cf_dns.is_some() || !parsed.gatus.is_empty();
            has_intent.then(|| IntendedService {
                service_id: service_id(machine_id, &container.name),
                machine_id: machine_id.clone(),
                container: container.name.clone(),
                labels: parsed,
            })
        })
        .collect()
}
```

- [ ] **Step 4.5: Run intended-state tests**

Run:

```bash
cargo test -p docktornet-server integrations::intended
```

Expected: pass.

- [ ] **Step 4.6: Commit**

Run:

```bash
git add crates/server/src/lib.rs crates/server/src/integrations/mod.rs crates/server/src/integrations/intended.rs
git commit -m "feat(reconcile): derive intended services" -m "- Add intended-state builder from parsed docktornet labels" -m "- Ignore containers with no opted-in integrations"
```

---

## Task 5: Add Resource Tracking Storage Helpers

**Files:**
- Modify: `crates/server/src/storage.rs`

- [ ] **Step 5.1: Add failing storage tests**

Append tests in `crates/server/src/storage.rs`:

```rust
#[tokio::test]
async fn tracks_npm_host_ids() {
    let p = pool().await;
    set_npm_host(&p, "m1/app", 0, 58).await.unwrap();
    assert_eq!(get_npm_host(&p, "m1/app", 0).await.unwrap(), Some(58));
    delete_npm_host(&p, "m1/app", 0).await.unwrap();
    assert_eq!(get_npm_host(&p, "m1/app", 0).await.unwrap(), None);
}

#[tokio::test]
async fn tracks_cloudflare_record_ids() {
    let p = pool().await;
    set_cf_dns_record(&p, "m1/app", "abc").await.unwrap();
    assert_eq!(get_cf_dns_record(&p, "m1/app").await.unwrap(), Some("abc".to_string()));
    delete_cf_dns_record(&p, "m1/app").await.unwrap();
    assert_eq!(get_cf_dns_record(&p, "m1/app").await.unwrap(), None);
}
```

- [ ] **Step 5.2: Run storage tests and expect failure**

Run:

```bash
cargo test -p docktornet-server storage::tests::tracks_npm_host_ids storage::tests::tracks_cloudflare_record_ids
```

Expected: fail because helper functions do not exist.

- [ ] **Step 5.3: Add helper functions**

Add to `crates/server/src/storage.rs`:

```rust
pub async fn get_npm_host(
    pool: &Pool<Sqlite>,
    service_id: &str,
    npm_index: u8,
) -> sqlx::Result<Option<i64>> {
    sqlx::query_scalar(
        "SELECT npm_id FROM npm_hosts WHERE service_id = ? AND npm_index = ?",
    )
    .bind(service_id)
    .bind(i64::from(npm_index))
    .fetch_optional(pool)
    .await
}

pub async fn set_npm_host(
    pool: &Pool<Sqlite>,
    service_id: &str,
    npm_index: u8,
    npm_id: i64,
) -> sqlx::Result<()> {
    sqlx::query(
        r#"INSERT INTO npm_hosts (service_id, npm_index, npm_id)
           VALUES (?, ?, ?)
           ON CONFLICT(service_id, npm_index) DO UPDATE SET npm_id = excluded.npm_id"#,
    )
    .bind(service_id)
    .bind(i64::from(npm_index))
    .bind(npm_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete_npm_host(
    pool: &Pool<Sqlite>,
    service_id: &str,
    npm_index: u8,
) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM npm_hosts WHERE service_id = ? AND npm_index = ?")
        .bind(service_id)
        .bind(i64::from(npm_index))
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn get_cf_dns_record(
    pool: &Pool<Sqlite>,
    service_id: &str,
) -> sqlx::Result<Option<String>> {
    sqlx::query_scalar("SELECT record_id FROM cf_dns_records WHERE service_id = ?")
        .bind(service_id)
        .fetch_optional(pool)
        .await
}

pub async fn set_cf_dns_record(
    pool: &Pool<Sqlite>,
    service_id: &str,
    record_id: &str,
) -> sqlx::Result<()> {
    sqlx::query(
        r#"INSERT INTO cf_dns_records (service_id, record_id)
           VALUES (?, ?)
           ON CONFLICT(service_id) DO UPDATE SET record_id = excluded.record_id"#,
    )
    .bind(service_id)
    .bind(record_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete_cf_dns_record(pool: &Pool<Sqlite>, service_id: &str) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM cf_dns_records WHERE service_id = ?")
        .bind(service_id)
        .execute(pool)
        .await?;
    Ok(())
}
```

- [ ] **Step 5.4: Run storage tests**

Run:

```bash
cargo test -p docktornet-server storage
```

Expected: pass.

- [ ] **Step 5.5: Commit**

Run:

```bash
git add crates/server/src/storage.rs
git commit -m "feat(storage): add resource ID helpers" -m "- Add NPM proxy host tracking helpers" -m "- Add Cloudflare DNS record tracking helpers"
```

---

## Task 6: Implement NPM Client

**Files:**
- Modify: `crates/server/src/integrations/mod.rs`
- Create: `crates/server/src/integrations/npm.rs`

- [ ] **Step 6.1: Export module**

Add to `crates/server/src/integrations/mod.rs`:

```rust
pub mod npm;
```

- [ ] **Step 6.2: Write NPM mapping tests**

Create `crates/server/src/integrations/npm.rs`:

```rust
use docktornet_core::labels::{NpmHost, NpmScheme};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProxyHostPayload {
    pub domain_names: Vec<String>,
    pub forward_scheme: String,
    pub forward_host: String,
    pub forward_port: u16,
    pub access_list_id: i64,
    pub certificate_id: i64,
    pub ssl_forced: bool,
    pub caching_enabled: bool,
    pub block_exploits: bool,
    pub allow_websocket_upgrade: bool,
    pub http2_support: bool,
    pub hsts_enabled: bool,
    pub hsts_subdomains: bool,
    pub advanced_config: String,
    pub locations: Vec<serde_json::Value>,
    pub meta: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssl_payload_enables_expected_flags() {
        let host = NpmHost {
            index: 0,
            host: "app.example.com".to_string(),
            port: 3000,
            scheme: NpmScheme::Http,
            ssl: true,
            hsts: None,
            http2: None,
            cert_id: Some(42),
        };

        let payload = proxy_payload(&host, "172.17.0.10", Some(42));
        assert_eq!(payload.domain_names, vec!["app.example.com"]);
        assert_eq!(payload.forward_scheme, "http");
        assert_eq!(payload.forward_host, "172.17.0.10");
        assert_eq!(payload.forward_port, 3000);
        assert_eq!(payload.certificate_id, 42);
        assert!(payload.ssl_forced);
        assert!(payload.hsts_enabled);
        assert!(payload.http2_support);
    }

    #[test]
    fn wildcard_certificate_matches_subdomain() {
        let certs = vec![
            Certificate { id: 1, domain_names: vec!["*.example.com".to_string()] },
            Certificate { id: 2, domain_names: vec!["other.example.com".to_string()] },
        ];
        assert_eq!(matching_cert_id("app.example.com", &certs), Some(1));
    }

    #[test]
    fn exact_certificate_beats_wildcard() {
        let certs = vec![
            Certificate { id: 1, domain_names: vec!["*.example.com".to_string()] },
            Certificate { id: 2, domain_names: vec!["app.example.com".to_string()] },
        ];
        assert_eq!(matching_cert_id("app.example.com", &certs), Some(2));
    }
}
```

- [ ] **Step 6.3: Run NPM tests and expect failure**

Run:

```bash
cargo test -p docktornet-server integrations::npm
```

Expected: fail because `proxy_payload`, `Certificate`, and `matching_cert_id` do not exist.

- [ ] **Step 6.4: Implement payload and certificate matching**

Add to `npm.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Certificate {
    pub id: i64,
    pub domain_names: Vec<String>,
}

pub fn proxy_payload(host: &NpmHost, forward_host: &str, cert_id: Option<i64>) -> ProxyHostPayload {
    let ssl_enabled = host.ssl && cert_id.unwrap_or_default() > 0;
    let hsts = ssl_enabled && host.hsts.unwrap_or(true);
    let http2 = ssl_enabled && host.http2.unwrap_or(true);
    ProxyHostPayload {
        domain_names: vec![host.host.clone()],
        forward_scheme: match host.scheme {
            NpmScheme::Http => "http".to_string(),
            NpmScheme::Https => "https".to_string(),
        },
        forward_host: forward_host.to_string(),
        forward_port: host.port,
        access_list_id: 0,
        certificate_id: cert_id.unwrap_or(0),
        ssl_forced: ssl_enabled,
        caching_enabled: false,
        block_exploits: false,
        allow_websocket_upgrade: false,
        http2_support: http2,
        hsts_enabled: hsts,
        hsts_subdomains: false,
        advanced_config: String::new(),
        locations: Vec::new(),
        meta: serde_json::json!({
            "letsencrypt_agree": false,
            "dns_challenge": false
        }),
    }
}

pub fn matching_cert_id(hostname: &str, certs: &[Certificate]) -> Option<i64> {
    for cert in certs {
        if cert.domain_names.iter().any(|name| name == hostname) {
            return Some(cert.id);
        }
    }
    for cert in certs {
        if cert.domain_names.iter().any(|name| wildcard_matches(name, hostname)) {
            return Some(cert.id);
        }
    }
    None
}

fn wildcard_matches(pattern: &str, hostname: &str) -> bool {
    let Some(suffix) = pattern.strip_prefix("*.") else {
        return false;
    };
    hostname.ends_with(suffix)
        && hostname
            .trim_end_matches(suffix)
            .trim_end_matches('.')
            .split('.')
            .count()
            == 1
}
```

- [ ] **Step 6.5: Add HTTP client methods**

Add:

```rust
#[derive(Clone)]
pub struct NpmClient {
    base_url: url::Url,
    user: String,
    password: String,
    http: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
pub enum NpmError {
    #[error("NPM HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("NPM token response did not contain a token")]
    MissingToken,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ProxyHostResponse {
    pub id: i64,
    pub domain_names: Vec<String>,
}

impl NpmClient {
    pub fn new(config: crate::config::NpmConfig) -> Self {
        Self {
            base_url: config.url,
            user: config.user,
            password: config.password,
            http: reqwest::Client::new(),
        }
    }

    pub async fn token(&self) -> Result<String, NpmError> {
        let url = self.base_url.join("api/tokens").expect("valid NPM token URL");
        let body = serde_json::json!({
            "identity": self.user,
            "secret": self.password,
        });
        let response: TokenResponse = self
            .http
            .post(url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        response.token.ok_or(NpmError::MissingToken)
    }

    pub async fn certificates(&self, token: &str) -> Result<Vec<Certificate>, NpmError> {
        let url = self.base_url.join("api/nginx/certificates").expect("valid NPM certificates URL");
        Ok(self.http.get(url).bearer_auth(token).send().await?.error_for_status()?.json().await?)
    }

    pub async fn create_proxy_host(
        &self,
        token: &str,
        payload: &ProxyHostPayload,
    ) -> Result<ProxyHostResponse, NpmError> {
        let url = self.base_url.join("api/nginx/proxy-hosts").expect("valid NPM proxy-hosts URL");
        Ok(self.http.post(url).bearer_auth(token).json(payload).send().await?.error_for_status()?.json().await?)
    }

    pub async fn update_proxy_host(
        &self,
        token: &str,
        id: i64,
        payload: &ProxyHostPayload,
    ) -> Result<ProxyHostResponse, NpmError> {
        let url = self.base_url.join(&format!("api/nginx/proxy-hosts/{id}")).expect("valid NPM proxy-host URL");
        Ok(self.http.put(url).bearer_auth(token).json(payload).send().await?.error_for_status()?.json().await?)
    }

    pub async fn delete_proxy_host(&self, token: &str, id: i64) -> Result<(), NpmError> {
        let url = self.base_url.join(&format!("api/nginx/proxy-hosts/{id}")).expect("valid NPM proxy-host URL");
        self.http.delete(url).bearer_auth(token).send().await?.error_for_status()?;
        Ok(())
    }
}
```

- [ ] **Step 6.6: Run NPM tests**

Run:

```bash
cargo test -p docktornet-server integrations::npm
```

Expected: pass.

- [ ] **Step 6.7: Commit**

Run:

```bash
git add crates/server/src/integrations/mod.rs crates/server/src/integrations/npm.rs
git commit -m "feat(npm): add proxy host client" -m "- Add NPM proxy host payload mapping" -m "- Add certificate matching with exact and wildcard precedence" -m "- Add authenticated NPM client methods"
```

---

## Task 7: Implement Cloudflare DNS Client

**Files:**
- Modify: `crates/server/src/integrations/mod.rs`
- Create: `crates/server/src/integrations/cloudflare.rs`

- [ ] **Step 7.1: Export module**

Add to `crates/server/src/integrations/mod.rs`:

```rust
pub mod cloudflare;
```

- [ ] **Step 7.2: Write Cloudflare payload tests**

Create `crates/server/src/integrations/cloudflare.rs`:

```rust
use docktornet_core::labels::CloudflareDns;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DnsRecordPayload {
    #[serde(rename = "type")]
    pub record_type: String,
    pub name: String,
    pub content: String,
    pub proxied: bool,
    pub ttl: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_cname_payload() {
        let dns = CloudflareDns {
            name: "app".to_string(),
            target: "home.example.com".to_string(),
            proxied: true,
        };
        let payload = dns_payload(&dns, "example.com");
        assert_eq!(payload.record_type, "CNAME");
        assert_eq!(payload.name, "app.example.com");
        assert_eq!(payload.content, "home.example.com");
        assert!(payload.proxied);
        assert_eq!(payload.ttl, 1);
    }
}
```

- [ ] **Step 7.3: Run tests and expect failure**

Run:

```bash
cargo test -p docktornet-server integrations::cloudflare
```

Expected: fail because `dns_payload` does not exist.

- [ ] **Step 7.4: Implement payload and client**

Add to `cloudflare.rs`:

```rust
pub fn dns_payload(dns: &CloudflareDns, zone_name: &str) -> DnsRecordPayload {
    DnsRecordPayload {
        record_type: "CNAME".to_string(),
        name: format!("{}.{}", dns.name.trim_end_matches('.'), zone_name.trim_end_matches('.')),
        content: dns.target.clone(),
        proxied: dns.proxied,
        ttl: 1,
    }
}

#[derive(Clone)]
pub struct CloudflareClient {
    api_token: String,
    zone_id: String,
    http: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
pub enum CloudflareError {
    #[error("Cloudflare HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Cloudflare returned success=false")]
    Api,
}

#[derive(Debug, Deserialize)]
pub struct ZoneResponse {
    pub success: bool,
    pub result: Zone,
}

#[derive(Debug, Deserialize)]
pub struct Zone {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct DnsRecordResponse {
    pub success: bool,
    pub result: DnsRecord,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DnsRecord {
    pub id: String,
    pub name: String,
    pub content: String,
    pub proxied: bool,
}

#[derive(Debug, Deserialize)]
struct DnsRecordListResponse {
    success: bool,
    result: Vec<DnsRecord>,
}

impl CloudflareClient {
    pub fn new(config: crate::config::CloudflareConfig) -> Self {
        Self {
            api_token: config.api_token,
            zone_id: config.zone_id,
            http: reqwest::Client::new(),
        }
    }

    pub async fn zone(&self) -> Result<Zone, CloudflareError> {
        let response: ZoneResponse = self
            .http
            .get(format!("https://api.cloudflare.com/client/v4/zones/{}", self.zone_id))
            .bearer_auth(&self.api_token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        response.success.then_some(response.result).ok_or(CloudflareError::Api)
    }

    pub async fn find_record_by_name(&self, name: &str) -> Result<Option<DnsRecord>, CloudflareError> {
        let response: DnsRecordListResponse = self
            .http
            .get(format!(
                "https://api.cloudflare.com/client/v4/zones/{}/dns_records",
                self.zone_id
            ))
            .bearer_auth(&self.api_token)
            .query(&[("type", "CNAME"), ("name", name)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if !response.success {
            return Err(CloudflareError::Api);
        }
        Ok(response.result.into_iter().next())
    }

    pub async fn create_record(&self, payload: &DnsRecordPayload) -> Result<DnsRecord, CloudflareError> {
        let response: DnsRecordResponse = self
            .http
            .post(format!(
                "https://api.cloudflare.com/client/v4/zones/{}/dns_records",
                self.zone_id
            ))
            .bearer_auth(&self.api_token)
            .json(payload)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        response.success.then_some(response.result).ok_or(CloudflareError::Api)
    }

    pub async fn update_record(
        &self,
        id: &str,
        payload: &DnsRecordPayload,
    ) -> Result<DnsRecord, CloudflareError> {
        let response: DnsRecordResponse = self
            .http
            .put(format!(
                "https://api.cloudflare.com/client/v4/zones/{}/dns_records/{id}",
                self.zone_id
            ))
            .bearer_auth(&self.api_token)
            .json(payload)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        response.success.then_some(response.result).ok_or(CloudflareError::Api)
    }

    pub async fn delete_record(&self, id: &str) -> Result<(), CloudflareError> {
        self.http
            .delete(format!(
                "https://api.cloudflare.com/client/v4/zones/{}/dns_records/{id}",
                self.zone_id
            ))
            .bearer_auth(&self.api_token)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}
```

- [ ] **Step 7.5: Run Cloudflare tests**

Run:

```bash
cargo test -p docktornet-server integrations::cloudflare
```

Expected: pass.

- [ ] **Step 7.6: Commit**

Run:

```bash
git add crates/server/src/integrations/mod.rs crates/server/src/integrations/cloudflare.rs
git commit -m "feat(cloudflare): add DNS client" -m "- Add CNAME payload mapping" -m "- Add zone lookup and DNS record create/update/delete methods"
```

---

## Task 8: Implement Gatus File Writer

**Files:**
- Modify: `crates/server/src/integrations/mod.rs`
- Create: `crates/server/src/integrations/gatus.rs`

- [ ] **Step 8.1: Export module**

Add to `crates/server/src/integrations/mod.rs`:

```rust
pub mod gatus;
```

- [ ] **Step 8.2: Write failing Gatus tests**

Create `crates/server/src/integrations/gatus.rs`:

```rust
use std::path::PathBuf;

use docktornet_core::{labels::GatusMonitor, sanitize_service_id};
use serde::Serialize;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn renders_expected_yaml() {
        let yaml = render_gatus_yaml(
            "m1/app",
            &[GatusMonitor {
                index: 0,
                url: "https://app.example.com".to_string(),
                interval: "30s".to_string(),
                conditions: vec!["[STATUS] == 200".to_string()],
            }],
        )
        .unwrap();
        assert!(yaml.contains("endpoints:"));
        assert!(yaml.contains("name: m1/app/0"));
        assert!(yaml.contains("url: https://app.example.com"));
    }

    #[tokio::test]
    async fn writes_and_removes_service_file() {
        let dir = tempdir().unwrap();
        let path = service_file_path(dir.path(), "m1/app");
        write_service_file(
            dir.path(),
            "m1/app",
            &[GatusMonitor {
                index: 0,
                url: "https://app.example.com".to_string(),
                interval: "30s".to_string(),
                conditions: vec!["[STATUS] == 200".to_string()],
            }],
        )
        .await
        .unwrap();
        assert!(path.exists());
        remove_service_file(dir.path(), "m1/app").await.unwrap();
        assert!(!path.exists());
    }
}
```

- [ ] **Step 8.3: Run Gatus tests and expect failure**

Run:

```bash
cargo test -p docktornet-server integrations::gatus
```

Expected: fail because functions do not exist.

- [ ] **Step 8.4: Implement renderer and atomic writer**

Add to `gatus.rs`:

```rust
pub fn service_file_path(config_dir: impl AsRef<std::path::Path>, service_id: &str) -> PathBuf {
    config_dir
        .as_ref()
        .join(format!("{}.yaml", sanitize_service_id(service_id)))
}

pub fn render_gatus_yaml(
    service_id: &str,
    monitors: &[GatusMonitor],
) -> Result<String, serde_yaml::Error> {
    let endpoints = monitors
        .iter()
        .map(|monitor| GatusEndpoint {
            name: format!("{service_id}/{}", monitor.index),
            group: service_id.to_string(),
            url: monitor.url.clone(),
            interval: monitor.interval.clone(),
            conditions: monitor.conditions.clone(),
        })
        .collect();
    serde_yaml::to_string(&GatusFile { endpoints })
}

pub async fn write_service_file(
    config_dir: impl AsRef<std::path::Path>,
    service_id: &str,
    monitors: &[GatusMonitor],
) -> anyhow::Result<PathBuf> {
    let config_dir = config_dir.as_ref();
    tokio::fs::create_dir_all(config_dir).await?;
    let target = service_file_path(config_dir, service_id);
    let tmp = target.with_extension("yaml.tmp");
    let yaml = render_gatus_yaml(service_id, monitors)?;
    tokio::fs::write(&tmp, yaml).await?;
    tokio::fs::rename(&tmp, &target).await?;
    Ok(target)
}

pub async fn remove_service_file(
    config_dir: impl AsRef<std::path::Path>,
    service_id: &str,
) -> anyhow::Result<()> {
    let path = service_file_path(config_dir, service_id);
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}
```

- [ ] **Step 8.5: Run Gatus tests**

Run:

```bash
cargo test -p docktornet-server integrations::gatus
```

Expected: pass.

- [ ] **Step 8.6: Commit**

Run:

```bash
git add crates/server/src/integrations/mod.rs crates/server/src/integrations/gatus.rs
git commit -m "feat(gatus): write managed endpoint files" -m "- Render deterministic per-service Gatus YAML" -m "- Write monitor files atomically and remove stale service files"
```

---

## Task 9: Add Reconciler Skeleton With Dry-Run

**Files:**
- Create: `crates/server/src/reconcile.rs`
- Modify: `crates/server/src/lib.rs`

- [ ] **Step 9.1: Export reconciler**

Add to `crates/server/src/lib.rs`:

```rust
pub mod reconcile;
```

- [ ] **Step 9.2: Write dry-run test**

Create `crates/server/src/reconcile.rs`:

```rust
use std::path::PathBuf;

use docktornet_core::Snapshot;

use crate::db::Db;

#[derive(Clone)]
pub struct Reconciler {
    db: Db,
    options: ReconcileOptions,
}

#[derive(Debug, Clone)]
pub struct ReconcileOptions {
    pub dry_run: bool,
    pub adopt_existing: bool,
    pub gatus_config_dir: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db, storage};
    use docktornet_core::{ContainerEntry, MachineId};
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    #[tokio::test]
    async fn dry_run_persists_snapshot_without_writing_gatus_file() {
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

        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".to_string(), "https://example.com".to_string());
        labels.insert("docktornet.gatus.0.interval".to_string(), "30s".to_string());
        let snap = Snapshot {
            machine_id: MachineId::new("m1"),
            taken_at: 1,
            containers: vec![ContainerEntry { name: "app".to_string(), labels }],
        };

        reconciler.reconcile_snapshot(&snap).await.unwrap();

        let rows = storage::list_services_for_machine(&pool, "m1").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert!(!dir.path().join("gatus/m1__app.yaml").exists());
    }
}
```

- [ ] **Step 9.3: Run dry-run test and expect failure**

Run:

```bash
cargo test -p docktornet-server reconcile::tests::dry_run_persists_snapshot_without_writing_gatus_file
```

Expected: fail because constructor and reconcile method do not exist.

- [ ] **Step 9.4: Implement dry-run skeleton**

Add to `reconcile.rs`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("gatus file error: {0}")]
    Gatus(#[from] anyhow::Error),
}

impl Reconciler {
    pub fn new(db: Db, options: ReconcileOptions) -> Self {
        Self { db, options }
    }

    pub async fn reconcile_snapshot(&self, snap: &Snapshot) -> Result<(), ReconcileError> {
        crate::storage::upsert_snapshot(&self.db, snap).await?;
        let intended = crate::integrations::intended::intended_services(&snap.machine_id, &snap.containers);

        for service in intended {
            if self.options.dry_run {
                tracing::info!(
                    service_id = %service.service_id,
                    npm = service.labels.npm.len(),
                    cf_dns = service.labels.cf_dns.is_some(),
                    gatus = service.labels.gatus.len(),
                    "dry-run: would reconcile service"
                );
                continue;
            }

            if !service.labels.gatus.is_empty() {
                crate::integrations::gatus::write_service_file(
                    &self.options.gatus_config_dir,
                    &service.service_id,
                    &service.labels.gatus,
                )
                .await?;
            }
        }

        Ok(())
    }
}
```

- [ ] **Step 9.5: Run dry-run test**

Run:

```bash
cargo test -p docktornet-server reconcile
```

Expected: pass.

- [ ] **Step 9.6: Commit**

Run:

```bash
git add crates/server/src/lib.rs crates/server/src/reconcile.rs
git commit -m "feat(reconcile): add dry-run skeleton" -m "- Add reconciler entry point for snapshots" -m "- Persist snapshots while skipping external writes in dry-run"
```

---

## Task 10: Wire Reconciler Into HTTP Ingest

**Files:**
- Modify: `crates/server/src/handlers/snapshot.rs`
- Modify: `crates/server/src/routes.rs`
- Modify: `crates/server/src/main.rs`
- Modify: `crates/server/tests/e2e_snapshot_push.rs`

- [ ] **Step 10.1: Update handler state**

Replace `AppState` and `ingest` in `crates/server/src/handlers/snapshot.rs`:

```rust
use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use docktornet_core::Snapshot;
use tracing::{info, warn};

use crate::reconcile::Reconciler;

#[derive(Clone)]
pub struct AppState {
    pub reconciler: Reconciler,
    pub secret: Arc<String>,
}

pub async fn ingest(State(state): State<AppState>, Json(snap): Json<Snapshot>) -> StatusCode {
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

- [ ] **Step 10.2: Update routes builder**

Change `crates/server/src/routes.rs` to accept `AppState` directly:

```rust
use axum::{Router, routing::post};

use crate::auth::auth_layer;
use crate::handlers::snapshot::{self, AppState};

pub fn build(state: AppState) -> Router {
    let secret = state.secret.clone();
    Router::new()
        .route("/api/v1/snapshot", post(snapshot::ingest))
        .with_state(state)
        .layer(axum::middleware::from_fn(move |req, next| {
            auth_layer(req, next, secret.clone())
        }))
}
```

- [ ] **Step 10.3: Update main**

In `crates/server/src/main.rs`, build the reconciler:

```rust
let cfg = config::ServerConfig::from_env()?;
let pool = db::connect(&cfg.db_path).await?;
let reconciler = docktornet_server::reconcile::Reconciler::new(
    pool,
    docktornet_server::reconcile::ReconcileOptions {
        dry_run: cfg.dry_run,
        adopt_existing: cfg.adopt_existing,
        gatus_config_dir: cfg.gatus_config_dir.clone(),
    },
);
let app = routes::build(docktornet_server::handlers::snapshot::AppState {
    reconciler,
    secret: std::sync::Arc::new(cfg.secret),
});
```

- [ ] **Step 10.4: Update e2e test state**

In `crates/server/tests/e2e_snapshot_push.rs`, replace route construction:

```rust
let reconciler = docktornet_server::reconcile::Reconciler::new(
    pool.clone(),
    docktornet_server::reconcile::ReconcileOptions {
        dry_run: true,
        adopt_existing: false,
        gatus_config_dir: dir.path().join("gatus"),
    },
);
let app = docktornet_server::routes::build(docktornet_server::handlers::snapshot::AppState {
    reconciler,
    secret: std::sync::Arc::new("s3cret".into()),
});
```

- [ ] **Step 10.5: Run server tests**

Run:

```bash
cargo test -p docktornet-server
```

Expected: pass.

- [ ] **Step 10.6: Commit**

Run:

```bash
git add crates/server/src/handlers/snapshot.rs crates/server/src/routes.rs crates/server/src/main.rs crates/server/tests/e2e_snapshot_push.rs
git commit -m "feat(server): route ingest through reconciler" -m "- Wire snapshot HTTP ingest to reconciliation entry point" -m "- Keep e2e push test in dry-run mode"
```

---

## Task 11: Reconcile Gatus Writes and Removals

**Files:**
- Modify: `crates/server/src/reconcile.rs`
- Modify: `crates/server/src/storage.rs`

- [ ] **Step 11.1: Add stale-service helper test**

Add test to `storage.rs`:

```rust
#[tokio::test]
async fn returns_removed_services_for_machine() {
    let p = pool().await;
    let m = MachineId::new("m1");
    upsert_snapshot(
        &p,
        &Snapshot {
            machine_id: m.clone(),
            taken_at: 100,
            containers: vec![entry("a", &[("docktornet.gatus.0.url", "https://a"), ("docktornet.gatus.0.interval", "30s")])],
        },
    )
    .await
    .unwrap();

    let removed = removed_service_ids(&p, "m1", &["m1/b".to_string()]).await.unwrap();
    assert_eq!(removed, vec!["m1/a"]);
}
```

- [ ] **Step 11.2: Add removed-service helper**

Add:

```rust
pub async fn removed_service_ids(
    pool: &Pool<Sqlite>,
    machine: &str,
    current_ids: &[String],
) -> sqlx::Result<Vec<String>> {
    if current_ids.is_empty() {
        return sqlx::query_scalar("SELECT id FROM services WHERE machine_id = ? ORDER BY id")
            .bind(machine)
            .fetch_all(pool)
            .await;
    }

    let placeholders = std::iter::repeat_n("?", current_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT id FROM services WHERE machine_id = ? AND id NOT IN ({placeholders}) ORDER BY id"
    );
    let mut q = sqlx::query_scalar(&sql).bind(machine);
    for id in current_ids {
        q = q.bind(id);
    }
    q.fetch_all(pool).await
}
```

- [ ] **Step 11.3: Add Gatus removal test**

Add to `reconcile.rs` tests:

```rust
#[tokio::test]
async fn removes_gatus_file_when_service_disappears() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = Reconciler::new(
        pool,
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
    );

    let mut labels = BTreeMap::new();
    labels.insert("docktornet.gatus.0.url".to_string(), "https://example.com".to_string());
    labels.insert("docktornet.gatus.0.interval".to_string(), "30s".to_string());

    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1"),
            taken_at: 1,
            containers: vec![ContainerEntry { name: "app".to_string(), labels }],
        })
        .await
        .unwrap();
    assert!(dir.path().join("gatus/m1__app.yaml").exists());

    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1"),
            taken_at: 2,
            containers: vec![],
        })
        .await
        .unwrap();
    assert!(!dir.path().join("gatus/m1__app.yaml").exists());
}
```

- [ ] **Step 11.4: Update reconciler ordering**

Change `reconcile_snapshot` to compute removed IDs before `upsert_snapshot`:

```rust
pub async fn reconcile_snapshot(&self, snap: &Snapshot) -> Result<(), ReconcileError> {
    let intended = crate::integrations::intended::intended_services(&snap.machine_id, &snap.containers);
    let current_ids = intended
        .iter()
        .map(|service| service.service_id.clone())
        .collect::<Vec<_>>();
    let removed = crate::storage::removed_service_ids(
        &self.db,
        snap.machine_id.as_str(),
        &current_ids,
    )
    .await?;

    crate::storage::upsert_snapshot(&self.db, snap).await?;

    if self.options.dry_run {
        tracing::info!(removed = removed.len(), current = current_ids.len(), "dry-run: would reconcile snapshot");
        return Ok(());
    }

    for service_id in removed {
        crate::integrations::gatus::remove_service_file(&self.options.gatus_config_dir, &service_id).await?;
    }

    for service in intended {
        if !service.labels.gatus.is_empty() {
            crate::integrations::gatus::write_service_file(
                &self.options.gatus_config_dir,
                &service.service_id,
                &service.labels.gatus,
            )
            .await?;
        } else {
            crate::integrations::gatus::remove_service_file(&self.options.gatus_config_dir, &service.service_id).await?;
        }
    }

    Ok(())
}
```

- [ ] **Step 11.5: Run tests**

Run:

```bash
cargo test -p docktornet-server storage reconcile::tests::removes_gatus_file_when_service_disappears
```

Expected: pass.

- [ ] **Step 11.6: Commit**

Run:

```bash
git add crates/server/src/storage.rs crates/server/src/reconcile.rs
git commit -m "feat(gatus): reconcile monitor file lifecycle" -m "- Write Gatus files for intended monitors" -m "- Remove Gatus files when services disappear or labels are removed"
```

---

## Task 12: Reconcile Cloudflare DNS

**Files:**
- Modify: `crates/server/src/reconcile.rs`

- [ ] **Step 12.1: Extend reconciler dependencies**

Add fields:

```rust
pub struct Reconciler {
    db: Db,
    options: ReconcileOptions,
    cloudflare: Option<crate::integrations::cloudflare::CloudflareClient>,
}
```

Add constructor:

```rust
pub fn with_cloudflare(
    db: Db,
    options: ReconcileOptions,
    cloudflare: crate::integrations::cloudflare::CloudflareClient,
) -> Self {
    Self {
        db,
        options,
        cloudflare: Some(cloudflare),
    }
}
```

Update `new` to set `cloudflare: None`.

- [ ] **Step 12.2: Add reconcile methods**

Add:

```rust
async fn reconcile_cloudflare(
    &self,
    service_id: &str,
    dns: &docktornet_core::labels::CloudflareDns,
) -> Result<(), ReconcileError> {
    let Some(client) = &self.cloudflare else {
        tracing::warn!(service_id, "Cloudflare DNS labels present but Cloudflare client is not configured");
        return Ok(());
    };
    let zone = client.zone().await.map_err(|err| ReconcileError::External(err.into()))?;
    let payload = crate::integrations::cloudflare::dns_payload(dns, &zone.name);
    let existing_id = crate::storage::get_cf_dns_record(&self.db, service_id).await?;

    if let Some(id) = existing_id {
        client.update_record(&id, &payload).await.map_err(|err| ReconcileError::External(err.into()))?;
        return Ok(());
    }

    match client.create_record(&payload).await {
        Ok(record) => {
            crate::storage::set_cf_dns_record(&self.db, service_id, &record.id).await?;
            Ok(())
        }
        Err(err) if self.options.adopt_existing => {
            let record = client
                .find_record_by_name(&payload.name)
                .await
                .map_err(|err| ReconcileError::External(err.into()))?
                .ok_or_else(|| ReconcileError::External(anyhow::anyhow!(err)))?;
            client.update_record(&record.id, &payload).await.map_err(|err| ReconcileError::External(err.into()))?;
            crate::storage::set_cf_dns_record(&self.db, service_id, &record.id).await?;
            Ok(())
        }
        Err(err) => Err(ReconcileError::External(err.into())),
    }
}
```

Extend `ReconcileError`:

```rust
#[error("external integration error: {0}")]
External(#[from] anyhow::Error),
```

Call from the main intended-service loop:

```rust
if let Some(dns) = &service.labels.cf_dns {
    self.reconcile_cloudflare(&service.service_id, dns).await?;
} else {
    if let Some(record_id) = crate::storage::get_cf_dns_record(&self.db, &service.service_id).await? {
        if let Some(client) = &self.cloudflare {
            client.delete_record(&record_id).await.map_err(|err| ReconcileError::External(err.into()))?;
        }
        crate::storage::delete_cf_dns_record(&self.db, &service.service_id).await?;
    }
}
```

- [ ] **Step 12.3: Wire Cloudflare in main**

In `main.rs`, use `with_cloudflare`:

```rust
let cloudflare = docktornet_server::integrations::cloudflare::CloudflareClient::new(cfg.cloudflare.clone());
let reconciler = docktornet_server::reconcile::Reconciler::with_cloudflare(
    pool,
    docktornet_server::reconcile::ReconcileOptions {
        dry_run: cfg.dry_run,
        adopt_existing: cfg.adopt_existing,
        gatus_config_dir: cfg.gatus_config_dir.clone(),
    },
    cloudflare,
);
```

- [ ] **Step 12.4: Run targeted tests**

Run:

```bash
cargo test -p docktornet-server reconcile integrations::cloudflare storage::tests::tracks_cloudflare_record_ids
```

Expected: pass.

- [ ] **Step 12.5: Commit**

Run:

```bash
git add crates/server/src/reconcile.rs crates/server/src/main.rs
git commit -m "feat(cloudflare): reconcile DNS records" -m "- Create and update tracked Cloudflare CNAME records" -m "- Delete tracked DNS records when labels are removed" -m "- Support adoption lookup when enabled"
```

---

## Task 13: Reconcile NPM Proxy Hosts

**Files:**
- Modify: `crates/server/src/reconcile.rs`
- Modify: `crates/server/src/main.rs`

- [ ] **Step 13.1: Extend reconciler with NPM client**

Add field:

```rust
npm: Option<crate::integrations::npm::NpmClient>,
```

Add constructor:

```rust
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
    }
}
```

Update `new` to set both clients to `None`.

- [ ] **Step 13.2: Add NPM reconcile method**

Add:

```rust
async fn reconcile_npm(
    &self,
    service_id: &str,
    host: &docktornet_core::labels::NpmHost,
    forward_host: &str,
) -> Result<(), ReconcileError> {
    let Some(client) = &self.npm else {
        tracing::warn!(service_id, index = host.index, "NPM labels present but NPM client is not configured");
        return Ok(());
    };
    let token = client.token().await.map_err(|err| ReconcileError::External(err.into()))?;
    let certs = client.certificates(&token).await.map_err(|err| ReconcileError::External(err.into()))?;
    let cert_id = host
        .cert_id
        .map(i64::from)
        .or_else(|| crate::integrations::npm::matching_cert_id(&host.host, &certs));
    let payload = crate::integrations::npm::proxy_payload(host, forward_host, cert_id);

    if let Some(id) = crate::storage::get_npm_host(&self.db, service_id, host.index).await? {
        client.update_proxy_host(&token, id, &payload).await.map_err(|err| ReconcileError::External(err.into()))?;
        return Ok(());
    }

    let created = client
        .create_proxy_host(&token, &payload)
        .await
        .map_err(|err| ReconcileError::External(err.into()))?;
    crate::storage::set_npm_host(&self.db, service_id, host.index, created.id).await?;
    Ok(())
}
```

Use a conservative forward host for phase 2:

```rust
let forward_host = "127.0.0.1";
```

This is intentionally temporary because Plan 1 snapshots do not yet include container IP addresses. It allows payload/API behavior to be implemented and tested without inventing agent networking. A follow-up plan should add `ContainerEntry` network addresses and replace this with the container's reachable address.

- [ ] **Step 13.3: Call NPM reconcile loop**

Inside the intended-service loop:

```rust
let forward_host = "127.0.0.1";
for npm_host in &service.labels.npm {
    self.reconcile_npm(&service.service_id, npm_host, forward_host).await?;
}
```

- [ ] **Step 13.4: Wire NPM in main**

In `main.rs`:

```rust
let npm = docktornet_server::integrations::npm::NpmClient::new(cfg.npm.clone());
let cloudflare = docktornet_server::integrations::cloudflare::CloudflareClient::new(cfg.cloudflare.clone());
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
```

- [ ] **Step 13.5: Run targeted tests**

Run:

```bash
cargo test -p docktornet-server integrations::npm reconcile storage::tests::tracks_npm_host_ids
```

Expected: pass.

- [ ] **Step 13.6: Commit**

Run:

```bash
git add crates/server/src/reconcile.rs crates/server/src/main.rs
git commit -m "feat(npm): reconcile proxy hosts" -m "- Create and update tracked NPM proxy hosts" -m "- Auto-match certificates for SSL hosts" -m "- Store NPM proxy host IDs after successful creates"
```

---

## Task 14: Live Dry-Run and Controlled Write Test

**Files:**
- No repo files should be edited in this task.

- [ ] **Step 14.1: Run full local checks before live writes**

Run:

```bash
just fmt
just lint
just test
```

Expected: pass.

- [ ] **Step 14.2: Run backup again immediately before live write**

Run:

```bash
./scripts/backup_external_state.sh .env.local
```

Expected: backup completes and prints a new timestamped directory.

- [ ] **Step 14.3: Start server in dry-run mode**

Run:

```bash
set -a
source .env.local
set +a
DOCKTORNET_DRY_RUN=true cargo run -p docktornet-server
```

Expected: server starts and logs `server ready`.

- [ ] **Step 14.4: Push a test snapshot**

In a second terminal, send a snapshot with one NPM host, one Cloudflare DNS record, and one Gatus monitor:

```bash
curl -fsS -X POST "http://127.0.0.1:5363/api/v1/snapshot" \
  -H "Authorization: Bearer $DOCKTORNET_SECRET" \
  -H "Content-Type: application/json" \
  -d '{
    "machine_id": "batterdock",
    "taken_at": 1,
    "containers": [
      {
        "name": "docktornet-phase2-smoke",
        "labels": {
          "docktornet.npm.0.host": "docktornet-phase2-smoke.battercloud.cc",
          "docktornet.npm.0.port": "8080",
          "docktornet.npm.0.scheme": "http",
          "docktornet.npm.0.ssl": "false",
          "docktornet.cf.dns.name": "docktornet-phase2-smoke",
          "docktornet.cf.dns.target": "home.battercloud.cc",
          "docktornet.cf.dns.proxied": "false",
          "docktornet.gatus.0.url": "https://example.com",
          "docktornet.gatus.0.interval": "30s"
        }
      }
    ]
  }' -i
```

Expected: HTTP `204 No Content`; server logs dry-run actions; no NPM host, Cloudflare record, or Gatus file is created.

- [ ] **Step 14.5: Run controlled live write**

Stop the server, then run:

```bash
set -a
source .env.local
set +a
DOCKTORNET_DRY_RUN=false DOCKTORNET_ADOPT_EXISTING=false cargo run -p docktornet-server
```

Repeat the snapshot push command.

Expected:

- NPM proxy host is created for `docktornet-phase2-smoke.battercloud.cc`.
- Cloudflare CNAME is created for `docktornet-phase2-smoke.battercloud.cc`.
- Gatus file exists at `${DOCKTORNET_GATUS_CONFIG_DIR}/batterdock__docktornet_phase2_smoke.yaml`.
- Gatus API lists the new endpoint after hot reload.

- [ ] **Step 14.6: Verify live state**

Run:

```bash
curl -fsS "http://127.0.0.1:8084/api/v1/endpoints/statuses" | jq -r '.[].name' | rg 'batterdock/docktornet-phase2-smoke/0'
```

Expected: the smoke monitor name is printed.

- [ ] **Step 14.7: Verify deletion path**

Push an empty snapshot:

```bash
curl -fsS -X POST "http://127.0.0.1:5363/api/v1/snapshot" \
  -H "Authorization: Bearer $DOCKTORNET_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"machine_id":"batterdock","taken_at":2,"containers":[]}' -i
```

Expected:

- HTTP `204 No Content`.
- Gatus managed YAML for the smoke service is removed.
- Tracked NPM/Cloudflare resources are removed when deletion support has been implemented for both integrations.

- [ ] **Step 14.8: Commit final verification note if docs changed**

If no files changed, do not commit. If the implementation added a short verification doc, commit it:

```bash
git add docs/superpowers/specs/2026-05-21-docktornet-design.md
git commit -m "docs(reconcile): record live verification notes" -m "- Document phase 2 live smoke verification outcome"
```

---

## Task 15: Final Quality Gate

**Files:**
- All changed source, migration, script, and docs files.

- [ ] **Step 15.1: Run format**

Run:

```bash
just fmt
```

Expected: pass.

- [ ] **Step 15.2: Run lint**

Run:

```bash
just lint
```

Expected: pass.

- [ ] **Step 15.3: Run tests**

Run:

```bash
just test
```

Expected: pass.

- [ ] **Step 15.4: Inspect git status**

Run:

```bash
git status --short --branch
```

Expected: branch is `development`; only intentional changes remain.

- [ ] **Step 15.5: Push development**

Run:

```bash
git push batterforge development
```

Expected: push succeeds.

---

## Self-Review

- Spec coverage: NPM, Cloudflare DNS, Gatus, dry-run, adoption flag, and resource tracking are covered. Full hourly resync is intentionally deferred because v1 can operate correctly from snapshot diff cycles first; add a follow-up plan for scheduled full resync after live reconciliation is stable.
- Backup requirement: Task 1 exports NPM proxy hosts/certificates/access lists/settings and Cloudflare zone/settings/DNS records before development writes.
- Gatus hot reload: Plan uses directory config mode because Gatus merges multiple YAML files only when `GATUS_CONFIG_PATH` points to a directory.
- Risk noted: NPM forward host is temporarily `127.0.0.1` because current snapshots do not include container IP addresses. A follow-up plan should add network address capture to the agent before production use for multi-host proxying.
- Placeholder scan: no unresolved placeholder markers remain.
