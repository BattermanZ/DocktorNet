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
| `DOCKTORNET_FORWARD_HOST` | — | required |
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

## Gatus configuration

Gatus must be configured to load from a directory rather than a single file, so it picks up the per-service files docktornet writes. Set `GATUS_CONFIG_FILE` to the config directory in your Gatus compose service:

```yaml
environment:
  - GATUS_CONFIG_FILE=/config
```

Docktornet only writes per-service files. The base Gatus config (web/storage/alerting) must be defined by the operator in a non-managed file in the same directory, e.g. `/etc/gatus/config.d/00-base.yaml`:

```yaml
web:
  port: 8080
storage:
  type: sqlite
  path: /data/gatus.db
alerting:
  custom:
    url: "https://api.telegram.org/bot${GATUS_TELEGRAM_TOKEN}/sendMessage"
    method: "POST"
    headers:
      Content-Type: application/json
    body: |
      {
        "chat_id": "${GATUS_TELEGRAM_CHAT_ID}",
        "parse_mode": "Markdown",
        "text": "[ALERT_TRIGGERED_OR_RESOLVED] *[ENDPOINT_NAME]*\n🔗 [ENDPOINT_URL]"
      }
    placeholders:
      ALERT_TRIGGERED_OR_RESOLVED:
        TRIGGERED: "🔴 DOWN —"
        RESOLVED: "🟢 BACK UP —"
    default-alert:
      send-on-resolved: true
```

`alerting.custom` is **required** if any service has `docktornet.gatus.N.telegram=true` (the default), because docktornet emits `type: custom` alert references. **The `alerting.custom` block must exist before the new docktornet binary runs** — Gatus validates each endpoint's alert type against a configured provider at config load, so a missing `alerting.custom` block makes Gatus fail to load once per-service files reference it. The env vars `GATUS_TELEGRAM_TOKEN` and `GATUS_TELEGRAM_CHAT_ID` must be available to the Gatus container.
