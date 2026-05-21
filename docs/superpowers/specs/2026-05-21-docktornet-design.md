# Docktornet — Design Spec

**Date:** 2026-05-21
**Status:** Draft

## 1. Purpose

Docktornet reads Docker and Podman container labels and automatically reconciles them into three external systems:

- **Nginx Proxy Manager (NPM)** — reverse proxy entries
- **Cloudflare DNS** — CNAME records
- **Gatus** — monitoring configuration

Services declare their public/internal routing and monitoring metadata directly via Docker labels. Docktornet turns service exposure into a mostly declarative workflow with eventual-consistency reconciliation.

The target environment is a homelab with one main Docker host plus a few additional machines (some running Podman rootless). The system is designed for tens of services, not hundreds.

**Cloudflare Tunnel and Cloudflare Access integration are out of scope for v1** — see §9 and §10. Both will be revisited in a future iteration that handles ingress rule management together with Access applications and policies as a single coherent feature.

## 2. Architecture

Two Rust binaries shipped in a single Cargo workspace.

### 2.1 Binaries

- **`docktornet-server`** — runs on the main host. Receives label snapshots from agents, diffs them against SQLite, drives reconciliation against the three external systems. Owns all credentials for external APIs. Single-instance.
- **`docktornet-agent`** — runs on every machine (including the main host itself). Polls one or more Docker/Podman sockets every 30s, parses `docktornet.*` labels, sends a full snapshot of its machine's containers to the server. Stateless.

### 2.2 Workspace layout

```
docktornet/
├── Cargo.toml              (workspace)
├── crates/
│   ├── agent/              (binary)
│   ├── server/             (binary)
│   ├── core/               (shared types, label schema, snapshot structs)
│   └── integrations/       (npm, cloudflare_dns, gatus)
```

### 2.3 Communication

- Transport: HTTP REST.
- Authentication: pre-shared secret in `Authorization: Bearer <secret>` header.
- Agent → server: `POST /api/v1/snapshot` with the full container snapshot for that machine.
- The server never initiates connections to agents.

### 2.4 Container runtimes

Both Docker and Podman expose the same REST API over a unix socket. The agent accepts a list of `runtime:path` pairs in its config and polls every socket each cycle, merging the container lists. Confirmed working on rootless Podman via `/run/user/$(id -u)/podman/podman.sock` (requires `systemctl --user enable --now podman.socket`).

### 2.5 Reconciliation model

**Stateful diff-based** with periodic full resync:

- **Diff cycle (every 30s):** the server compares each agent snapshot against the last known state in SQLite. Only changed/new/removed services trigger external API calls.
- **Full resync (default 1h, configurable):** the server verifies every resource it claims to own still exists in the external systems and matches the expected configuration. Drift is corrected by re-creating missing resources and re-applying intended state.

Docktornet **only manages resources whose IDs are tracked in SQLite**. Manually created NPM or DNS entries are never read, modified, or deleted unless explicitly adopted (see §6.4).

## 3. Label Schema

All three integrations are independently opt-in. A container with no `docktornet.*` labels is silently ignored.

### 3.1 NPM (multiple proxy hosts per service via numeric index)

```yaml
docktornet.npm.0.host: "myapp.example.com"
docktornet.npm.0.port: "3000"
docktornet.npm.0.scheme: "http"          # upstream scheme: http|https
docktornet.npm.0.ssl: "true"             # enables: force-HTTPS + HSTS + HTTP/2 by default
docktornet.npm.0.hsts: "true"            # optional override (default true when ssl=true)
docktornet.npm.0.http2: "true"           # optional override (default true when ssl=true)
docktornet.npm.0.cert_id: "42"           # optional override; otherwise auto-matched

docktornet.npm.1.host: "myapp.local"
docktornet.npm.1.port: "3000"
docktornet.npm.1.scheme: "http"
docktornet.npm.1.ssl: "false"
```

**Defaults when `ssl: "true"`:** force-HTTPS redirect, HSTS, HTTP/2 all on. Each is individually overridable. When `ssl: "false"`, all security headers are off.

**Certificate selection:** the server fetches NPM's certificate list (`GET /api/nginx/certificates`) and builds a domain → cert_id map. For each NPM entry needing SSL, docktornet matches the hostname against the list:
1. Exact match wins.
2. Wildcard match (`*.example.com` covers `myapp.example.com`) used otherwise.
3. If nothing matches, the proxy host is created without SSL and a warning is logged. Each full resync re-runs auto-match for every NPM host docktornet manages, so a wildcard added later is picked up on the next resync without requiring any label change.

An explicit `cert_id` label always overrides auto-matching.

### 3.2 Cloudflare DNS

```yaml
docktornet.cf.dns.name: "myapp"                  # subdomain (becomes myapp.<zone>)
docktornet.cf.dns.target: "home.example.com"     # CNAME target
docktornet.cf.dns.proxied: "true"
```

### 3.3 Gatus (multiple monitors per service via numeric index)

```yaml
docktornet.gatus.0.url: "https://myapp.example.com"
docktornet.gatus.0.interval: "1m"
docktornet.gatus.0.conditions: "[STATUS] == 200"    # optional, defaults to [STATUS] == 200

docktornet.gatus.1.url: "http://myapp.local:3000"
docktornet.gatus.1.interval: "30s"
```

### 3.4 Service identity

A service is uniquely identified by `{machine_id}/{container_name}`. This is the SQLite primary key and is also used to derive filesystem paths (sanitised — see §7).

A container rename triggers full reprovision (old service deleted, new one created). This is acceptable for homelab use; an explicit `docktornet.service_id` override label can be added in a later iteration if it becomes painful.

## 4. Configuration

Both binaries are configured via environment variables (compatible with Docker Compose `env_file:` and `.env` files). No TOML, no JSON.

### 4.1 Server (`server.env`)

```env
DOCKTORNET_LISTEN=0.0.0.0:8080
DOCKTORNET_SECRET=your-shared-secret
DOCKTORNET_FULL_RESYNC_INTERVAL=1h
DOCKTORNET_GATUS_CONFIG_DIR=/etc/gatus/config.d
DOCKTORNET_DB_PATH=/data/docktornet.db

# Safety / bootstrap
DOCKTORNET_DRY_RUN=true                # default true on first run
DOCKTORNET_ADOPT_EXISTING=false        # opt-in to adopt pre-existing resources

# NPM
DOCKTORNET_NPM_URL=http://npm:81
DOCKTORNET_NPM_USER=admin@example.com
DOCKTORNET_NPM_PASSWORD=changeme

# Cloudflare DNS (scoped API token: Zone.DNS:Edit on the target zone)
DOCKTORNET_CF_API_TOKEN=...
DOCKTORNET_CF_ZONE_ID=...
```

The Cloudflare client uses a scoped API token via `Authorization: Bearer <token>`, with `Zone:Read` + `DNS:Edit` permissions limited to the target zone. No Global API Key is required, no email header is sent. (Tunnel-related env vars are removed — see §9.)

### 4.2 Agent (`agent.env`)

```env
DOCKTORNET_SERVER_URL=http://192.168.31.233:8080
DOCKTORNET_SECRET=your-shared-secret
DOCKTORNET_POLL_INTERVAL=30s
DOCKTORNET_MACHINE_ID=batterdock

# Comma-separated runtime:path pairs
DOCKTORNET_SOCKETS=docker:/var/run/docker.sock,podman:/run/user/1000/podman/podman.sock
```

### 4.3 Machine identity

`DOCKTORNET_MACHINE_ID` is operator-supplied. The server logs a loud warning if snapshots for the same `machine_id` arrive from two different source IP addresses within a short window (flapping detection). It does not reject them — duplicate IDs are an operator error, not a security boundary. Operators must keep machine IDs unique.

## 5. SQLite Schema

```sql
-- one row per container per machine
CREATE TABLE services (
    id          TEXT PRIMARY KEY,        -- "{machine_id}/{container_name}"
    machine_id  TEXT NOT NULL,
    container   TEXT NOT NULL,
    labels_json TEXT NOT NULL,           -- the parsed docktornet.* labels
    last_seen   INTEGER NOT NULL         -- unix epoch
);

-- one row per NPM entry per service (multiple per service possible)
CREATE TABLE npm_hosts (
    service_id  TEXT NOT NULL,
    npm_index   INTEGER NOT NULL,        -- matches label index
    npm_id      INTEGER NOT NULL,        -- NPM proxy_host id
    PRIMARY KEY (service_id, npm_index),
    FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE CASCADE
);

CREATE TABLE cf_dns_records (
    service_id  TEXT PRIMARY KEY,
    record_id   TEXT NOT NULL,           -- Cloudflare DNS record id
    FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE CASCADE
);
```

Gatus files are deterministic at `{gatus_config_dir}/{sanitized_service_id}.yaml` and require no table.

## 6. Reconciliation Logic

### 6.1 Diff cycle (per agent snapshot)

1. Server receives snapshot for `machine_id`, containing N container labels.
2. Server loads `services` rows for that `machine_id` from SQLite.
3. For each container in snapshot:
   - **New** (no SQLite row): for each opted-in integration, create the external resource, store its ID in SQLite, insert `services` row.
   - **Changed** (`labels_json` differs from SQLite): for each integration, compute resource-level diff and patch/recreate as needed.
   - **Unchanged**: no-op.
4. For each `services` row whose container is missing from the snapshot: delete all external resources, then delete the SQLite rows (cascade).

### 6.2 Full resync (scheduled, default hourly)

For each service tracked in SQLite:

- **NPM hosts** — `GET /api/nginx/proxy-hosts/{id}` per stored id. If missing → recreate. Always re-run certificate auto-match: if a better (or newly available) cert now applies, update the host.
- **Cloudflare DNS** — `GET /v4/zones/{zone}/dns_records/{record_id}`. If missing → recreate. If content differs → update.
- **Gatus** — verify each expected file exists and matches expected content. Regenerate as needed.

The resync **never** deletes anything that is not tracked in SQLite.

### 6.3 Dry-run mode

When `DOCKTORNET_DRY_RUN=true`, the server computes the intended state and logs every reconcile action (create/update/delete payloads, file contents) but does not call any external API or write any Gatus file. Default value is `true` on a fresh install; the operator must flip to `false` to enable writes. Dry-run applies to all integrations uniformly.

### 6.4 Bootstrap collision policy

Both NPM and Cloudflare DNS reject creates with duplicate hostnames.

**Default (`DOCKTORNET_ADOPT_EXISTING=false`):**

- On NPM/DNS duplicate-error: log a clear warning identifying the resource and existing ID, do NOT update SQLite, retry on every subsequent cycle. Operator must either delete the conflicting resource or set adoption mode.

**Adoption mode (`DOCKTORNET_ADOPT_EXISTING=true`):**

- On NPM/DNS duplicate: look up the existing resource by hostname, store its ID in SQLite, and apply the label-declared config to it. From that point on, docktornet treats it as its own resource (including deletion when labels are removed).

Adoption is reversible only by deleting the relevant SQLite rows (manual operator action).

### 6.5 Error handling & retry

- External API calls use a bounded exponential backoff inside a single reconcile attempt (e.g., 3 tries: 1s, 4s, 16s).
- If a reconcile attempt fails after retries, SQLite is not updated for that resource — the failed step will be retried on the next diff or resync cycle.
- A reconcile attempt is **per-resource atomic**: if NPM succeeds but Cloudflare DNS fails, the NPM ID is still stored in SQLite. On the next cycle, only DNS will be retried. SQLite never records a partial success that would block retry.
- NPM JWT tokens are obtained via `POST /api/tokens` with user/password on startup and refreshed on 401 or token expiry. Token state is kept in memory only.

## 7. Filesystem Layout

| Path | Purpose |
|---|---|
| `${DOCKTORNET_DB_PATH}` | SQLite database (server only) |
| `${DOCKTORNET_GATUS_CONFIG_DIR}/*.yaml` | Per-service Gatus configs |

**Service-id sanitisation:** `service_id` is `{machine_id}/{container_name}`. For filenames, both components are slugified (replace non-alphanumeric with `_`) and joined with `__`: `batterdock__myapp.yaml`.

## 8. Deployment

Both binaries ship as container images. Reference Compose snippets:

**Server:**

```yaml
services:
  docktornet-server:
    image: docktornet-server:latest
    restart: unless-stopped
    env_file: ./server.env
    volumes:
      - ./data:/data
      - /etc/gatus/config.d:/etc/gatus/config.d
    ports:
      - "8080:8080"
```

**Agent:**

```yaml
services:
  docktornet-agent:
    image: docktornet-agent:latest
    restart: unless-stopped
    env_file: ./agent.env
    volumes:
      # adjust paths per machine; bind any sockets the agent needs read access to
      - /var/run/docker.sock:/var/run/docker.sock:ro
      # rootless podman, host-specific:
      - /run/user/1000/podman/podman.sock:/run/user/1000/podman/podman.sock:ro
```

The main host runs both the server and an agent (otherwise the server cannot see its own containers).

## 9. Out of Scope (v1)

- **Cloudflare Tunnel ingress management** — deferred to v2; will be designed jointly with Access (see §10).
- **Cloudflare Access applications and policies** — deferred to v2.
- High availability of the server (single-instance only)
- Encryption-at-rest for SQLite
- A web UI
- Metrics / Prometheus endpoint (logs to stdout are sufficient for v1)
- An `EventDriven` mode using the Docker events socket (polling is sufficient for the agreed cadence)
- Multi-zone Cloudflare DNS (one zone per server install)
- Per-service overrides of the `service_id`

## 10. v2 Considerations (informative)

Recorded here so the v1 architecture stays compatible with what's coming:

- **Cloudflare Tunnel** — single shared existing tunnel, ingress rules managed via Cloudflare API (`PUT /v4/accounts/{account_id}/cfd_tunnel/{tunnel_id}/configurations`). Wide-blast-radius PUT requires pre-write backup of current config to disk. Some Cloudflare PUT endpoints reportedly reject API tokens and require Global API Key + email; this needs reconfirming when v2 is built.
- **Cloudflare Access** — protect tunnel-exposed services with Access applications and policies. Likely design: named reusable policies defined in server config (`homelab-users`, `family`, `public-bypass`), referenced by services via a label like `docktornet.cf.access.policy=<name>`.
- **Compatibility** — the SQLite schema will gain `cf_tunnel_hostnames` and `cf_access_applications` tables. The `integrations/` crate will gain `cloudflare_tunnel` and `cloudflare_access` modules. Server config will gain account_id, tunnel_id, and possibly Global API Key fields. No v1 schema or label change is expected — v2 additions will be additive.

## 11. Open Questions

None remaining for v1.
