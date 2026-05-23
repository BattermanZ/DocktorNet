# Docktornet v1 Finish — Design Spec

**Date:** 2026-05-23
**Status:** Draft
**Predecessors:** [v1 design](2026-05-21-docktornet-design.md), [foundation plan](../plans/2026-05-21-docktornet-foundation.md), [reconciliation plan](../plans/2026-05-22-docktornet-reconciliation.md)

## 1. Purpose

Close the remaining gaps between the shipped Plans 1+2 and the v1 spec, plus a small extension to the Gatus label schema requested during this brainstorm. After this work, docktornet v1 is feature-complete and running in production on the homelab.

### 1.1 In scope

In dependency order:

1. **Machine-ID flapping detection** (spec §4.3).
2. **Gatus label expansion** — `group` and `telegram` labels (new, not in original spec).
3. **Periodic full resync** (spec §6.2).
4. **Container images and compose files** (spec §8).
5. **Live cutover runbook** and the interactive cutover task that uses it.

### 1.2 Out of scope

Anything in spec §9 / §10: Cloudflare Tunnel ingress, Cloudflare Access, multi-zone DNS, HA, encryption-at-rest, web UI, Prometheus endpoint, event-driven Docker mode, image publishing automation / CI.

## 2. Flapping detection

The server warns loudly when snapshots for the same `machine_id` arrive from two different source IPs within a short window. Operator error, not a security boundary: snapshots are never rejected.

**Module:** `crates/server/src/flapping.rs`.

```rust
pub struct FlappingTracker {
    last_seen: Mutex<HashMap<MachineId, (IpAddr, Instant)>>,
    window: Duration,                    // 5 minutes
}

impl FlappingTracker {
    pub fn observe(&self, id: &MachineId, ip: IpAddr) -> Option<IpAddr> {
        // returns Some(previous_ip) when a flap is detected, else None
    }
}
```

**Wiring:**

- Held inside `AppState`.
- Snapshot route uses `axum::extract::ConnectInfo<SocketAddr>`. Main binds the listener with `.into_make_service_with_connect_info::<SocketAddr>()`.
- Handler calls `tracker.observe(&snap.machine_id, addr.ip())`; on `Some(prev)`, emits `tracing::warn!(machine_id, prev_ip = %prev, new_ip = %addr.ip(), "machine id seen from multiple source IPs")`.
- The entry is updated on every observation regardless of warn outcome.

**Tests:**

- Unit on `observe`: no warn for the first observation; no warn when the same IP repeats; warn when the IP changes within window; no warn when the IP changes after window expired.

## 3. Gatus label expansion

Two new optional fields on every Gatus monitor index.

### 3.1 Label schema additions

```yaml
docktornet.gatus.0.group: "media"             # optional; passes through to endpoints[].group
docktornet.gatus.0.telegram: "true"           # optional; default true; toggles per-endpoint telegram alert block
```

Both fields are **per-index** (mirror `url`, `interval`, `conditions`). Telegram defaults to `true` when the label is absent. `group` defaults to `None` (no `group:` line emitted).

### 3.2 `GatusMonitor` struct change

```rust
pub struct GatusMonitor {
    pub index: u8,
    pub url: String,
    pub interval: String,
    pub conditions: Vec<String>,
    pub group: Option<String>,    // new
    pub telegram: bool,           // new, default true
}
```

Parser:

- `group`: pass-through, optional, no validation.
- `telegram`: parsed via the existing `parse_bool` helper. Absent → `true`. Invalid value → warning, skip the monitor (consistent with other parse errors).

### 3.3 Renderer change

In `crates/server/src/integrations/gatus.rs`, per monitor emit:

```yaml
endpoints:
  - name: <sanitized_service_id>-<index>
    url: <url>
    interval: <interval>
    group: <group>                # only when Some
    conditions:
      - <each condition>
    alerts:                       # only when telegram == true
      - type: telegram
        send-on-resolved: true
```

The atomic temp-file write path is unchanged.

### 3.4 Operator obligation

Docktornet **does not** manage the global `alerting.telegram` block. The operator's base Gatus config file (e.g. `00-base.yaml`) must define:

```yaml
alerting:
  telegram:
    token: "<bot-token>"
    id: "<chat-id>"
```

This requirement is documented in `docs/deployment.md`.

### 3.5 Tests

- Parser: group present → Some; group absent → None; telegram absent → true; `telegram: "false"` → false; `telegram: "yes"` → true; invalid bool → warning + skip.
- Renderer fixtures: full monitor with group + alerts; no group; `telegram=false` omits the alerts block; multiple monitors per service interleave correctly.

## 4. Periodic full resync

Scheduled verify-and-repair pass that catches drift between SQLite-tracked resources and external systems (manual deletes, partial config changes, newly available wildcard certs).

### 4.1 Scheduler

A second tokio task spawned from `main` immediately after the HTTP listener starts:

```rust
tokio::spawn(reconcile::full_resync_loop(reconciler.clone(), cfg.full_resync_interval));
```

- Interval driven by `DOCKTORNET_FULL_RESYNC_INTERVAL` (already in config; default `1h`).
- First fire is **one interval after startup**, not at startup. Diff-cycle ingest already reconciles current state on boot.
- Loop body logs start + end of each cycle plus a per-integration count of (verified, repaired, failed).

### 4.2 Concurrency

The `Reconciler` gains a single `tokio::sync::Mutex<()>` (`reconcile_lock`). Both paths acquire it:

- Snapshot ingest (diff cycle).
- Full-resync loop.

Dry-run paths also acquire the lock for symmetry. With at most tens of services in the homelab, the contention cost is negligible compared to the simplicity of "only one reconcile operation in flight at a time".

### 4.3 Per-integration behaviour

For every service id docktornet tracks in SQLite:

**NPM** (`npm_hosts` table):

- `GET /api/nginx/proxy-hosts/{npm_id}`. 404 → recreate host from labels currently in `services.labels_json`, store new id in `npm_hosts`.
- Re-run certificate auto-match. If the chosen cert differs from what NPM reports for that host, `PUT` the host with the new cert.
- If `services.labels_json` no longer contains the matching `npm.N.*` index, skip — the diff cycle owns deletes.

**Cloudflare DNS** (`cf_dns_records` table):

- `GET /v4/zones/{zone}/dns_records/{record_id}`. 404 → recreate, replace stored `record_id`.
- If content/proxied differs from labels → `PUT` to align.

**Gatus** (no DB table — files are deterministic):

- For each tracked service id with Gatus monitors: render expected YAML, read existing file, compare bytes.
- If missing or mismatched → write via the existing atomic temp+rename path. Emit a `tracing::info!` line identifying the file (so operator hand-edits don't disappear silently).

### 4.4 Dry-run

`DOCKTORNET_DRY_RUN=true` → each "would create/update/rewrite" path logs the intended action and short-circuits before the external call or file write. Same flag, same code path as the diff-cycle reconciler.

### 4.5 Failure isolation

A failure on one resource is logged and skipped; the cycle continues. Failed entries stay in SQLite unchanged and get retried on the next cycle. No partial-success writes to SQLite.

### 4.6 Tests

- Unit (mockito + tempfile): NPM 404 → recreate observed; CF content mismatch → PUT observed; Gatus file missing → rewritten; Gatus file matching → no write.
- Integration: drive `Reconciler::full_resync()` directly with a seeded SQLite + mocked NPM/CF + temp Gatus dir; assert repairs land and unrelated state is untouched.

## 5. Container images and compose

Two slim, rootless, distroless images. cargo-chef cuts cold-build deps caching cost; multi-stage keeps the source tree and toolchain out of the final image.

### 5.1 Dockerfiles

One Dockerfile per binary at `crates/server/Dockerfile` and `crates/agent/Dockerfile`. Build context is the workspace root so chef sees the full `Cargo.lock`.

```dockerfile
# crates/server/Dockerfile
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

Agent Dockerfile is identical with the binary name swapped (`docktornet-agent`).

**Choices:**

- `--bin <name>` everywhere so each image links only the relevant binary.
- `cc-debian13:nonroot` ships glibc + a `nonroot` user (uid 65532). Compatible with our default `x86_64-unknown-linux-gnu` target, bundled `libsqlite3-sys`, and `reqwest`'s `rustls-tls` feature (no openssl runtime).
- No `EXPOSE` line for the agent.

### 5.2 `.dockerignore`

At repo root:

```
target/
docktornet-backups/
data/
.env
*.db
.git/
```

### 5.3 Compose files

Single shared `.env` at the repo root (both services read it). `from_env` parsing on each binary already ignores keys it doesn't recognise.

**`deploy/main-host.compose.yaml`** — server + agent on `batterdock`:

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
    user: "65532:<docker-gid>"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    depends_on:
      - docktornet-server
```

**`deploy/agent.compose.yaml`** — agent-only for secondary hosts:

```yaml
services:
  docktornet-agent:
    image: ghcr.io/battermanz/docktornet-agent:latest
    restart: unless-stopped
    env_file: ../.env
    user: "65532:<docker-or-podman-gid>"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
```

**Host filesystem permissions:** `./data` and `/etc/gatus/config.d` must be writable by uid 65532. Documented in the runbook.

### 5.4 Build verification

A plan task runs:

```bash
docker build -f crates/server/Dockerfile -t docktornet-server:dev .
docker build -f crates/agent/Dockerfile  -t docktornet-agent:dev .
docker run --rm docktornet-server:dev   # expected: fails fast on missing env vars
```

Image publishing to ghcr.io and any CI workflow are intentionally **out of scope**.

## 6. Live cutover

A reusable runbook plus a final interactive plan task that walks through it once.

### 6.1 Runbook: `docs/runbooks/v1-cutover.md`

Sections, in order:

1. **Pre-flight** — clean `git status`, `just test` and `just lint` green, both images build locally.
2. **Backup** — re-run `scripts/backup_external_state.sh`; verify the new `docktornet-backups/<date>/` contents; `git tag pre-cutover-$(date +%F)`.
3. **Shadow phase** — deploy `main-host.compose.yaml` with `DOCKTORNET_DRY_RUN=true`; run for at least one full resync interval (≥ 1 h); tail logs and confirm intended actions are sane.
4. **Single-service cutover** — pick one low-stakes labelled container; flip `DRY_RUN=false`; restart server only; verify the change in NPM UI, Cloudflare dashboard, Gatus dashboard.
5. **Full cutover** — leave `DRY_RUN=false`; remaining services reconcile on the next diff cycle (≤ 30 s); watch logs ~10 min.
6. **Post-cutover** — push `development` to both remotes; after 24 h of clean operation, tag `v1.0.0`.
7. **Rollback** — referenced from every preceding step. Stop server, restore NPM hosts and CF DNS records from the backup JSON via `curl` (snippets included), restore Gatus dir from the tarball, wipe `data/docktornet.db` to clear tracked-resource state.

### 6.2 Final plan task

The plan ends with a task titled "Walk through the v1 cutover runbook interactively". The executing agent stops at each phase, runs the listed commands, reports outputs, and waits for explicit operator confirmation before proceeding. No autonomous cutover.

## 7. SQLite schema impact

None. All four features (flapping, gatus expansion, full resync, deployment) work on the existing tables from Plan 1 and Plan 2.

## 8. Config additions

None beyond what Plan 2 already added. `DOCKTORNET_FULL_RESYNC_INTERVAL` (default `1h`) is already in config and `.env.example`; this plan starts using it.

## 9. Open questions

None.
