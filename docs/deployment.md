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
| `DOCKTORNET_DB_PATH` | absolute; the image sets `/data/docktornet.db`, required outside it | — |
| `DOCKTORNET_ALLOW_EPHEMERAL_DB` | optional (`false`); start even when the database is on the container's own filesystem | — |
| `DOCKTORNET_GATUS_CONFIG_DIR` | required | — |
| `DOCKTORNET_FULL_RESYNC_INTERVAL` | optional (`1h`); the first full resync runs at startup | — |
| `DOCKTORNET_DRY_RUN` | optional (`true`) | — |
| `DOCKTORNET_ADOPT_EXISTING` | optional (`false`); enables hostname adoption of unmarked resources, see [Ownership markers](#ownership-markers-and-adoption) | — |
| `DOCKTORNET_NPM_*` | required | — |
| `DOCKTORNET_CF_API_TOKEN`, `DOCKTORNET_CF_ZONE_ID` | required | — |
| `DOCKTORNET_CF_API_URL` | optional (`https://api.cloudflare.com/client/v4`); the dev stack points it at its stub | — |
| `DOCKTORNET_SERVER_URL` | — | required |
| `DOCKTORNET_POLL_INTERVAL` | — | required |
| `DOCKTORNET_MACHINE_ID` | — | required |
| `DOCKTORNET_FORWARD_HOST` | — | required |
| `DOCKTORNET_SOCKETS` | — | required |

### The database path

The server image sets `DOCKTORNET_DB_PATH=/data/docktornet.db` and ships an empty `/data` owned by uid 65532. Mount a volume there, a bind mount (`./data:/data`) or a named volume, and leave the variable unset. Outside the image the variable is required.

The server checks the path before it opens the database and exits if a check fails:

- **The path must be absolute.** A relative path would resolve against the image's working directory, `/app`, in the container layer.
- **Inside a container, the database must be on a volume.** The server finds the mount that holds the database directory in `/proc/self/mountinfo`. If it is the container's own root filesystem, every recreate would start the database empty, so the server refuses to start and names the directory to mount a volume at. Set `DOCKTORNET_ALLOW_EPHEMERAL_DB=true` to start anyway, for a throwaway test; the server then logs a warning. The image has no `VOLUME` instruction on purpose: an anonymous volume would hide a forgotten mount from this check. Outside a container, or if mountinfo can't be read, the check is skipped.
- **The path must name a file**, not end in `/`.
- **The directory, and any database files already in it, must be writable** by the server's uid. See [Filesystem permissions](#filesystem-permissions).

The `database ready` line in the startup log shows the resolved path, whether it is on a mount, and the mount point.

## Ownership markers and adoption

The server writes an ownership marker on every NPM proxy host and Cloudflare
DNS record it manages: the `docktornet` key in the host's `meta`, and the
comment `docktornet:<machine>/<container>` on the record. The format is fixed
by [ADR 0001](adr/0001-ownership-markers.md).

- **Losing the database.** When a service has no stored mapping, the server
  adopts a resource marked with its service ID before it creates one. This
  needs no flag, so a server started on an empty database takes back what it
  owned as each agent pushes, without duplicates.
- **Hostname adoption.** With `DOCKTORNET_ADOPT_EXISTING=true`, a create that
  fails because the hostname is taken adopts the existing resource, but only
  if it is unmarked. The server writes the marker as it adopts. It refuses a
  resource marked for another service and logs both service IDs. If adoption replaces a Cloudflare comment the server did not write, it
  logs the old text first.
- **Deletion.** Unchanged: the server deletes only what a stored mapping or
  pending removal points to, never a marked resource on its own.
- **Long service IDs.** The server never writes a comment over 100
  characters. It sends the record an empty comment instead, so the record works
  as before but has no marker, and it logs a warning on each pass.

> **Upgrading to a version with markers:** the first full resync after the
> upgrade, which runs at startup, finds every mapped proxy host and DNS record
> without a marker and updates each one once. Every proxy host update makes NPM
> reload nginx, so expect one reload per proxy host on that pass. Later resyncs
> leave marked resources alone.

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

The server container runs as uid 65532 (`nonroot`). The two bind mounts must be writable by that uid, or the server exits naming the directory and its uid:

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

Docktornet also writes `docktornet-classification.json` to this directory: the classification map an alert service reads for each endpoint's severity, area, host and alert thresholds. Gatus ignores it, since it loads only `.yaml` and `.yml` files. To report thresholds, the server reads `alerting.custom.default-alert` from the other YAML files in the directory, so keep the base config there rather than mounting it into the Gatus container alone. The [README](../README.md#gatus-classification-map) documents the format.
