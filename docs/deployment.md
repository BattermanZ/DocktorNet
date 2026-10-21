# Deployment

Docktornet ships two images, both rootless and distroless:

- `ghcr.io/battermanz/docktornet-server` — runs on the main host only.
- `ghcr.io/battermanz/docktornet-agent` — runs on every host that exposes containers, including the main host.

## Images

Build locally:

```bash
docker build -f crates/server/Dockerfile -t docktornet-server:<version> .
docker build -f crates/agent/Dockerfile  -t docktornet-agent:<version>  .
```

Publishing images to GHCR is out of scope for v1 — tag and push manually for now.

## Configuration

The server and the agent each read their own env file, next to the compose
file: `server.env` and `agent.env`. Copy them from `server.env.example` and
`agent.env.example` at the repository root. The agent's file holds no NPM or
Cloudflare keys. The agent container mounts the Docker socket, so it should
not also carry credentials it never uses.

`DOCKTORNET_SECRET` is the bearer token agents send with each snapshot. Set the
same value in `server.env` and in every `agent.env`. Both binaries refuse to
start when it is shorter than 32 characters or contains `changeme`, upper or
lower case,
and the error names the variable. Generate one with:

```bash
openssl rand -hex 32
```

| Var | Server | Agent |
| --- | --- | --- |
| `DOCKTORNET_LISTEN` | required | — |
| `DOCKTORNET_SECRET` | required; at least 32 characters, no `changeme` | required; the server's value |
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
| `DOCKTORNET_MACHINE_ID` | — | required; non-empty, with no `/`, whitespace or control characters. Give each agent its own: within 5 minutes of the first agent's last push, the server answers `409` to another agent pushing the same id with a different forward host |
| `DOCKTORNET_FORWARD_HOST` | — | required |
| `DOCKTORNET_SOCKETS` | — | required |

### Upgrading from a single `.env`

Earlier versions read one `.env` for both roles. To upgrade:

1. Create `server.env` from the server keys in the old `.env`, and `agent.env`
   from the agent keys plus `DOCKTORNET_SECRET`, using the two example files
   as the list of keys. Put both next to the compose file you run. On an
   agent-only host, `agent.env` sits next to `deploy/agent.compose.yaml`, not
   one directory up as `.env` did.
2. If the old secret is shorter than 32 characters or contains `changeme`, the
   new server and agents refuse to start. Generate a new one with
   `openssl rand -hex 32` and put it in `server.env` and in every host's
   `agent.env`, then recreate the server and all agents together. An agent
   still holding the old secret gets a 401 on every push until it has the new
   one.
3. Once both stacks are up, remove the `DOCKTORNET_*` keys from the old
   `.env`. Keep the file if it also sets compose variables such as
   `SERVER_IMAGE`, `AGENT_IMAGE` or `DOCKER_GID`: compose reads those from the
   `.env` next to the compose file, not from `env_file`.

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
> update a resource only when a field Docktornet sets, the marker included,
> has drifted.

## Auditing ownership

`docktornet-server audit` compares NPM and Cloudflare with the server's
ownership mappings and prints what does not line up. It changes nothing. It
opens the database read-only and runs no migrations. Apart from the NPM token
request it sends only GET requests, whatever `DOCKTORNET_DRY_RUN` says. It reads
the same environment as the server, so run it inside the server container,
next to the live server. The image has no shell, so call the binary directly:

```bash
docker exec docktornet-server docktornet-server audit > audit.json
```

The upstream probes below go out from the container, over the server's
network. Logs and errors go to stderr, and stdout carries only the report: one
JSON document with every key present, as an empty list when there is nothing
to report.

```json
{
  "npm": {
    "marked_unmapped": [],
    "pending_removal": [],
    "mapped_missing": [],
    "mapped_unmarked": [],
    "unmarked_unreachable": []
  },
  "cloudflare": {
    "marked_unmapped": [],
    "pending_removal": [],
    "mapped_missing": [],
    "mapped_unmarked": [],
    "unmarked_unmapped": []
  }
}
```

Every entry has the resource's `id`. Where the resource still exists, NPM
entries add `domain_names` and Cloudflare entries add `name`, plus `type`
everywhere except `pending_removal`.
Times are unix seconds, and each `*_secs_ago` counts back from when the audit
ran.

| Category | What it lists | Extra fields |
| --- | --- | --- |
| `marked_unmapped` | A resource whose marker names a service, but no mapping or pending removal points to it. A recent `machine_last_seen` means the machine still reports and the service has gone, so the resource is an orphan. | `service_id`, `npm_index` (NPM), `machine_last_seen` and `machine_last_seen_secs_ago`, null when the database does not know the machine |
| `pending_removal` | A row in `pending_npm_removals` or `pending_cf_removals`: a resource waiting out its grace period, or one whose deletion keeps failing. | `service_id`, `npm_index` (NPM), `last_seen`, `last_seen_secs_ago`, and `exists`, false when the resource is already gone |
| `mapped_missing` | A mapping that points to a proxy host or record that no longer exists. | `service_id`, `npm_index` (NPM) |
| `mapped_unmarked` | A mapped resource with no marker, or a marker naming another service or index. | `service_id`, `npm_index` (NPM), and what the resource carries: `marker` (NPM) or `comment` (Cloudflare) |
| `unmarked_unreachable` (NPM) | A proxy host with no marker and no mapping whose `forward_host:forward_port` does not take a TCP connect within 2 seconds: refused, unresolvable, unroutable or silent. At most 8 probes run at once. | `forward_host`, `forward_port` |
| `unmarked_unmapped` (Cloudflare) | A record of any type with no marker and no mapping. It is listed without a probe, since a record does not say which port to try, so hand-made records show up here too. | `content` |

A record whose service ID is too long for a marker (see
[Long service IDs](#ownership-markers-and-adoption)) is listed under
`mapped_unmarked`.

The exit code says what the audit found:

| Code | Meaning |
| --- | --- |
| `0` | Every list is empty. |
| `1` | At least one list has entries. |
| `2` | The audit failed, for example on config, opening the database, the NPM login or an API error. The error goes to stderr and stdout stays empty. |

The audit lists NPM and Cloudflare first and then reads the database in one
transaction. A service that is created or removed while the audit runs can
still show up in a list, so run it again before acting on a finding.

Running the binary with no argument starts the server as before.
`docktornet-server backup` is described under [Backups](#backups), and
`docktornet-server healthcheck` under [Health](#health). Any other argument
exits 2 with a usage line.

## Backups

`scripts/backup_external_state.sh` takes everything needed to put the main
host back as it was, in one directory per run. Run it on the main host, from
the repository checkout, with the server running. Its one argument is an env
file holding the server's NPM and Cloudflare keys, such as `deploy/server.env`:

```bash
./scripts/backup_external_state.sh deploy/server.env
```

Each run writes `docktornet-backups/<stamp>/`, where the stamp is the UTC start
time as `YYYYMMDDTHHMMSSZ`:

| Path | What it holds |
| --- | --- |
| `npm/*.json` | NPM proxy hosts, certificates, redirection hosts, dead hosts, streams, access lists and settings |
| `cloudflare/*.json` | The token check, the zone, its settings (skipped with a warning when the token gets a 403) and every DNS record |
| `docktornet.db` | A consistent copy of the server's database, with the ownership mappings and pending removals |
| `gatus-config.tar.gz` | The whole Gatus config directory, base config included |

The database copy comes from `docktornet-server backup`, which the script runs
with `docker exec -i <container> docktornet-server backup > docktornet.db`. The
subcommand reads only `DOCKTORNET_DB_PATH`, opens the database read-only and
takes the copy with SQLite's `VACUUM INTO`, so it is safe next to the live
server. It stages the copy in a temporary file next to the database, removes
that file whether or not the copy succeeds, and writes the copy to stdout.
Errors go to stderr, and it exits 1 on any failure. A backup killed by a
signal mid-copy cannot clean up, and leaves a `.docktornet-backup-*.db` file
next to the database. Delete it; the server never reads it.

The script works in `<stamp>.partial` and renames it to `<stamp>` only after
every part has succeeded. On a failure it exits non-zero, prints
`backup FAILED` and leaves the `.partial` directory for inspection. The server
can replace a Gatus file while `tar` reads the directory. The script accepts
`tar`'s exit 1 for that case, since every file is written under a temporary
name and renamed into place, so the archive holds each file whole.

After a successful run it keeps the newest `DOCKTORNET_BACKUP_KEEP` stamped
directories and deletes the older ones. It only counts and deletes directories
whose names match the stamp format, so `.partial` directories and anything else
in the backup directory are left alone. A failed run prunes nothing.

These optional settings come from the env file or the environment:

| Var | Default | Meaning |
| --- | --- | --- |
| `DOCKTORNET_BACKUP_DIR` | `docktornet-backups` in the checkout | Where the stamped directories go |
| `DOCKTORNET_BACKUP_SERVER_CONTAINER` | `docktornet-server` | The server container `docker exec` runs in |
| `DOCKTORNET_BACKUP_GATUS_DIR` | `/etc/gatus/config.d` | The Gatus config directory on the host |
| `DOCKTORNET_BACKUP_KEEP` | `14` | How many stamped directories to keep |
| `DOCKTORNET_CF_API_URL` | `https://api.cloudflare.com/client/v4` | The Cloudflare API, as for the server |

The script needs `curl`, `jq`, `tar` and access to `docker`. Nothing schedules
it. A daily cron line for the user that owns the checkout looks like this:

```cron
30 3 * * * cd /path/to/docktornet && ./scripts/backup_external_state.sh deploy/server.env >> "$HOME/docktornet-backup.log" 2>&1
```

### Restoring the database

Stop the server, remove the database's `-wal` and `-shm` files so SQLite does
not replay a stale log onto the restored file, put the copy in place owned by
uid 65532, then start the server. With the `./data` bind mount of
`deploy/main-host.compose.yaml`, from the repository root:

```bash
backup=docktornet-backups/<stamp>
docker compose -f deploy/main-host.compose.yaml stop docktornet-server
sudo rm -f deploy/data/docktornet.db-wal deploy/data/docktornet.db-shm
sudo install -o 65532 -g 65532 -m 0600 "$backup/docktornet.db" deploy/data/docktornet.db
docker compose -f deploy/main-host.compose.yaml start docktornet-server
```

With a named volume, do the same in the volume's directory, which
`docker volume inspect` prints as `Mountpoint`.

### Restoring the Gatus config directory

```bash
sudo tar -xzf docktornet-backups/<stamp>/gatus-config.tar.gz -C /etc/gatus/config.d
```

Extracting as root keeps the files' owners from the archive. It adds and
overwrites files but leaves any file created since the backup, so remove those
first for an exact restore. Gatus picks up the change at its next config check.

## Compose layouts

**Main host (server + agent in one stack):** `deploy/main-host.compose.yaml`

```bash
cd deploy
export SERVER_IMAGE=docktornet-server:<version> AGENT_IMAGE=docktornet-agent:<version>
export DOCKER_GID=$(getent group docker | cut -d: -f3)
docker compose -f main-host.compose.yaml up -d
```

**Secondary hosts (agent only):** `deploy/agent.compose.yaml`

```bash
cd deploy
export AGENT_IMAGE=docktornet-agent:<version>
export DOCKER_GID=$(getent group docker | cut -d: -f3)
docker compose -f agent.compose.yaml up -d
```

Both compose files require their image variables: `SERVER_IMAGE` and
`AGENT_IMAGE` on the main host, `AGENT_IMAGE` on an agent-only host. Compose
refuses every command without them, `down` and `logs` included, so a `.env`
next to the compose file is a better home than one shell's exports. The check
only rejects an unset or empty value. Set a release tag, not `latest`, so each
host runs the build you chose.

For rootless Podman hosts, set `DOCKER_GID` to the gid of the directory containing `podman.sock` (typically the operator's own gid) and adjust the volume path in the compose file.

## Health

The server service in `main-host.compose.yaml` has a compose healthcheck, and
the agent starts only once the server reports `healthy`. The image is
distroless, with no shell or curl, so the check runs
`docktornet-server healthcheck` from the image. It sends `GET /healthz` to the port in
`DOCKTORNET_LISTEN` on loopback (`0.0.0.0` becomes `127.0.0.1`, `::` becomes
`::1`, and a specific address is used as is). It exits 0 on HTTP 200 and 1 on
anything else, giving up after 3 seconds. It reads no other setting and sends
no token, so a broken NPM or Cloudflare never marks the container unhealthy.

```bash
docker inspect --format '{{.State.Health.Status}}' docktornet-server
```

For what the server is doing rather than whether it answers, call `/healthz`
with the bearer token:

```bash
curl -s -H "Authorization: Bearer $DOCKTORNET_SECRET" http://127.0.0.1:5363/healthz | jq
```

Besides `version` and `agents`, the authenticated response carries:

- `last_resync`: `null` until the first full resync finishes, then
  `finished_at`, `outcome` (`"ok"` or `"error"`) and the `verified`, `repaired`
  and `failed` counts. A resync that returned an error also has an `error`
  field with its message.
- `npm_breaker`: `open`, and `open_until`, the time NPM calls resume, or
  `null` while the breaker is closed. The breaker opens after three NPM
  failures in a row and stays open for a minute, during which the server
  leaves NPM alone.

`status` is `"degraded"` while the NPM breaker is open or when the server
cannot read its agent list, and `"ok"` otherwise.

## Filesystem permissions

The server container runs as uid 65532 (`nonroot`). The two bind mounts must be writable by that uid, or the server exits naming the directory and its uid:

```bash
sudo install -d -o 65532 -g 65532 ./data
sudo chown -R 65532:65532 /etc/gatus/config.d
```

The agent only needs **read** access to the docker/podman socket. Setting the container's secondary group to the docker gid is enough — no chmod required.

## Gatus configuration

Gatus must be configured to load from a directory rather than a single file, so it picks up the per-service files docktornet writes. Set `GATUS_CONFIG_PATH` to the config directory in your Gatus compose service. `GATUS_CONFIG_FILE` is its deprecated alias.

```yaml
environment:
  - GATUS_CONFIG_PATH=/config
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
