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
| `DOCKTORNET_CADDY_ADMIN_URL` | optional; Caddy's admin API, such as `http://caddy:2019`. Unset turns the Caddy integration off. See [Caddy](#caddy) | — |
| `DOCKTORNET_CADDY_ANCHOR_ID` | optional (`docktornet`); the `@id` of the anchor route, non-empty and without `/` | — |
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

The server writes an ownership marker on every NPM proxy host, Caddy route and
Cloudflare DNS record it manages: the `docktornet` key in the host's `meta`,
the route's `@id`, and the comment `docktornet:<machine>/<container>` on the
record. The format is fixed by [ADR 0001](adr/0001-ownership-markers.md).

- **Losing the database.** When a service has no stored mapping, the server
  adopts a resource marked with its service ID before it creates one. This
  needs no flag, so a server started on an empty database takes back what it
  owned as each agent pushes, without duplicates.
- **Hostname adoption.** With `DOCKTORNET_ADOPT_EXISTING=true`, a create that
  fails because the hostname is taken adopts the existing proxy host or record,
  but only if it is unmarked. The server writes the marker as it adopts. It
  refuses a resource marked for another service and logs both service IDs. If
  adoption replaces a Cloudflare comment the server did not write, it logs the
  old text first. Caddy routes have no hostname adoption, so the server never
  takes over a hand-written route.
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

`docktornet-server audit` compares NPM, Cloudflare and, when
`DOCKTORNET_CADDY_ADMIN_URL` is set, Caddy with the server's ownership mappings
and prints what does not line up. It changes nothing. It opens the database
read-only and runs no migrations. Apart from the NPM token request it sends
only GET requests, whatever `DOCKTORNET_DRY_RUN` says. It reads
the same environment as the server, so run it inside the server container,
next to the live server. The image has no shell, so call the binary directly:

```bash
docker exec docktornet-server docktornet-server audit > audit.json
docker exec docktornet-server docktornet-server audit --only caddy  # Caddy alone
```

`--only caddy` skips NPM and Cloudflare, and exits 2 when Caddy isn't
configured.

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
  },
  "caddy": {
    "marked_unmapped": [],
    "pending_removal": [],
    "mapped_missing": [],
    "marked_outside_anchor": []
  },
  "errors": []
}
```

The `caddy` section appears only when Caddy is configured, and under
`--only caddy` it is the only section besides `errors`. `errors` holds problems
that belong to no one resource. Today that is a missing anchor route:
`Caddy anchor route "<id>" not found`.

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

The Caddy section has its own four lists. A Caddy entry's `id` is the route's
`@id`, and `hosts` is what its host matcher holds. A marker can't be turned
back into a service ID, so `marked_unmapped` entries have none.

| Category | What it lists | Extra fields |
| --- | --- | --- |
| `marked_unmapped` (Caddy) | A route in the anchor whose `@id` starts with `docktornet--`, but no mapping or pending removal points to it. The server never deletes one of these; remove it by hand once you are sure. | `hosts` |
| `pending_removal` (Caddy) | A row in `pending_caddy_removals`: a Caddy route waiting out its grace period, or one whose deletion keeps failing. | `hosts`, `exists`, `service_id`, `caddy_index`, `last_seen`, `last_seen_secs_ago` |
| `mapped_missing` (Caddy) | A mapping whose route is nowhere in Caddy's config. The drift check puts it back within a minute, so a lasting entry means the drift check is failing or the server is in dry-run. | `service_id`, `caddy_index` |
| `marked_outside_anchor` | A route carrying a mapped service's marker somewhere outside the anchor route. The server won't add a second route with the same `@id`, so it leaves the service without one in the anchor. Delete or move the stray route. | `hosts`, `service_id`, `caddy_index` |

A record whose service ID is too long for a marker (see
[Long service IDs](#ownership-markers-and-adoption)) is listed under
`mapped_unmarked`.

The exit code says what the audit found:

| Code | Meaning |
| --- | --- |
| `0` | Every list is empty. |
| `1` | At least one list, `errors` included, has entries. |
| `2` | The audit failed, for example on config, opening the database, the NPM login or an API error. The error goes to stderr and stdout stays empty. |

The audit lists NPM, Cloudflare and Caddy's anchor route first and then reads
the database in one transaction. A service that is created or removed while the audit runs can
still show up in a list, so run it again before acting on a finding.

Running the binary with no argument starts the server as before.
`docktornet-server backup` is described under [Backups](#backups), and
`docktornet-server healthcheck` under [Health](#health). Any other argument
exits 2 with the usage line,
`usage: docktornet-server [audit [--only caddy] | backup | healthcheck]`.

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

With `DOCKTORNET_CADDY_ADMIN_URL` set, it also carries:

- `caddy_breaker`: the Caddy circuit breaker, in the same shape as
  `npm_breaker`. It opens after three Caddy failures in a row and stays open
  for a minute. Connection errors, timeouts and unexpected statuses count as
  failures. A route Caddy rejects with a 400 or a 500 fails on its own and
  doesn't count, so one bad route can't stall the rest.
- `caddy_anchor`: what the last read of the anchor route found. `"ok"` means
  Caddy has it, `"missing"` means Caddy answered that no route has the anchor's
  `@id`, and `"unknown"` means no read has happened yet, the last read failed,
  or the Caddy breaker is open.

Both are left out when Caddy isn't configured.

`status` is `"degraded"` while the NPM breaker or the Caddy breaker is open,
while `caddy_anchor` is `"missing"`, or when the server cannot read its agent
list, and `"ok"` otherwise. The unauthenticated response always says `"ok"`.

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

## Caddy

Caddy is an optional second reverse proxy beside NPM. A service picks it with
`docktornet.caddy.<index>.*` labels, listed in the
[README](../README.md#caddy-docktornetcaddyindex). NPM stays required.

Caddy runs on the same machine as the docktornet server, because its admin API
must stay on a private network between the two. It keeps services reachable
through an NPM outage only when that machine isn't NPM's, for example a second
site that runs the server.

The server manages Caddy routes the way it manages NPM proxy hosts: ownership
mappings, ownership markers, the grace period, pending removals, restore on
return, marker adoption, the audit and a circuit breaker of its own. Every
minute, and on each full resync, a drift check reads the anchor route and puts
back any Caddy route that was deleted or edited by hand. In dry-run it writes
nothing, the drift check included.

The server never runs, starts or configures Caddy. You run one Caddy, with a
bootstrap config that contains the anchor route, and the server writes only
inside that anchor. A failing or unreachable NPM never stops Caddy work, and
the reverse holds too.

### Bootstrap config

Save this as `caddy/bootstrap.json` next to the compose file:

```json
{
  "admin": {
    "listen": "0.0.0.0:2019",
    "origins": ["localhost:2019", "0.0.0.0:2019"]
  },
  "apps": {
    "http": {
      "servers": {
        "proxy": {
          "listen": [":80"],
          "automatic_https": { "disable": true },
          "trusted_proxies": { "source": "static", "ranges": ["172.31.253.0/24"] },
          "client_ip_headers": ["CF-Connecting-IP"],
          "routes": [
            {
              "@id": "docktornet",
              "handle": [{ "handler": "subroute", "routes": [] }]
            },
            {
              "handle": [{ "handler": "static_response", "status_code": 404 }]
            }
          ]
        }
      }
    }
  }
}
```

What each part is for:

- **A plain HTTP listener, automatic HTTPS off.** Caddy sits behind something
  that terminates TLS, such as a Cloudflare tunnel. With automatic HTTPS on,
  Caddy would try to get certificates and redirect to HTTPS, which fails or
  loops behind the tunnel.
- **The anchor route first.** Its `@id` is `DOCKTORNET_CADDY_ANCHOR_ID`,
  `docktornet` by default, and its only handler is a `subroute` with an explicit
  `"routes": []`. Without the `routes` key Caddy has no list to append to, and
  the server can neither read nor write the anchor's routes. The server reads
  and writes only that `routes` list. It never creates the anchor. When the
  anchor is missing, the server logs an error, reports
  `caddy_anchor: "missing"` in `/healthz`, and writes nothing.
- **Hand-written routes after the anchor.** Caddy tries routes in order, and
  every Caddy route the server writes is terminal. Put your own routes after
  the anchor; a catch-all placed before it would shadow every Caddy route. The
  last route above answers 404 for any hostname nothing else matched.
- **`admin.listen` on an address the server can reach.** The official image
  listens on `localhost:2019`, which another container can't reach.
  `0.0.0.0:2019` listens on every network Caddy is attached to, so attach Caddy
  only to networks you trust with the admin API. See
  [The admin API](#the-admin-api).
- **`trusted_proxies` and `client_ip_headers`** pass the visitor's address on
  to your apps. See [Client IPs](#client-ips).

Point the server at the admin API in `server.env`:

```bash
DOCKTORNET_CADDY_ADMIN_URL=http://caddy:2019
# DOCKTORNET_CADDY_ANCHOR_ID=docktornet  # only if the anchor's @id differs
```

### Running Caddy

Add Caddy to the server's compose file, and put it and the server on a network
of their own:

```yaml
services:
  docktornet-server:
    # ... as in deploy/main-host.compose.yaml, plus:
    networks: [default, caddy-admin]

  caddy:
    image: caddy:2.11.7
    container_name: caddy
    restart: unless-stopped
    command: ["caddy", "run", "--config", "/etc/caddy/bootstrap.json", "--resume"]
    ports:
      # Only the HTTP port, and only on loopback, for the tunnel. 2019 is never published.
      - "127.0.0.1:8080:80"
    volumes:
      - ./caddy/bootstrap.json:/etc/caddy/bootstrap.json:ro
      - ./caddy/config:/config
    networks: [caddy-admin]

networks:
  caddy-admin:
    ipam:
      # Fixed so trusted_proxies in the bootstrap can name it. Any free subnet
      # works; change both together.
      config:
        - subnet: 172.31.253.0/24
```

`--resume` makes Caddy start from its autosave, the last config it ran, instead
of the bootstrap file. Every change the server makes through the admin API is
autosaved, so after a restart Caddy serves its Caddy routes straight away, even
while the server is down or unreachable. Without `--resume`, a restarted Caddy
serves an empty anchor until the server's next drift check refills it, and not
at all while the server can't reach it. The autosave lives in Caddy's config
directory, `/config` in the official image, so keep that directory on a
persistent volume. On the first start there is no autosave yet, and Caddy loads
the bootstrap file.

Caddy dials each service at its agent's `DOCKTORNET_FORWARD_HOST` and the
label's port, so it needs to reach every machine whose services it routes. A
Caddy route to a service on another machine works, and depends on the network
between the two machines staying up.

### The admin API

The admin API has no authentication. Anyone who can reach it can rewrite or
stop your Caddy. Keep it on a private container network shared only with the
docktornet server, like `caddy-admin` above, and never publish port 2019.
Don't attach other containers, the tunnel connector included, to that network.

`admin.origins` is not access control. Caddy checks it only on requests that
carry an `Origin` header, which browsers send and the docktornet server and
`curl` don't. It stops a web page in someone's browser from driving the admin
API, and nothing else.

The `caddy` CLI inside the container does send an `Origin` header, so the
origins list has to name it, or `caddy reload` gets a 403. The CLI picks its
address, and the `Origin` it sends, like this:

- `caddy reload --config /etc/caddy/bootstrap.json` takes the address from the
  file's `admin.listen` and sends `http://0.0.0.0:2019`.
- A command given no config file, such as `caddy stop`, uses
  `localhost:2019`.
- `--address localhost:2019` on any command sends `http://localhost:2019`.

The bootstrap above allows both. If you'd rather list one, keep
`localhost:2019` and pass `--address localhost:2019` to every command.

### Changing the config later

Once an autosave exists, `--resume` ignores the bootstrap file, so editing it
changes nothing on the next restart. You have two ways to change settings:

- **The admin API, preferred.** Change the setting in place from a throwaway
  container on `caddy-admin`, whose name compose prefixes with the project
  name. For example, to set `trusted_proxies`:

  ```bash
  docker run --rm --network <project>_caddy-admin curlimages/curl:8.10.1 \
    -X PATCH -H 'Content-Type: application/json' -d '["172.31.253.0/24"]' \
    http://caddy:2019/config/apps/http/servers/proxy/trusted_proxies/ranges
  ```

  Caddy autosaves the change, and the Caddy routes in the anchor stay as they
  are. Leave changes inside the anchor to the server.
- **`caddy reload` with the bootstrap file.** This replaces the whole config,
  so the anchor comes back empty and every Caddy route stops answering until
  the drift check refills it, within a minute. The drift check puts back only
  the Caddy routes of services that are still running. A Caddy route whose
  service is in its grace period stays gone.

  ```bash
  docker exec caddy caddy reload --config /etc/caddy/bootstrap.json
  ```

  Expect that gap, or use the admin API.

### Behind a Cloudflare tunnel

The intended setup runs Caddy on the machine whose services should keep
working when NPM's machine or network is down, with a Cloudflare tunnel in
front of it. Run the tunnel connector, `cloudflared`, on the host, or in a
container with `network_mode: host`, so it reaches Caddy at
`http://localhost:8080` and stays off `caddy-admin`.

Give the tunnel one catch-all public hostname, `*.<your zone>`, with the service
`http://localhost:8080`. The tunnel matches ingress rules in order, top to
bottom, so put the catch-all after every rule the tunnel already has, or it
takes their traffic. In a locally managed tunnel's `config.yml` that means last
before the final `http_status:404` rule:

```yaml
ingress:
  - hostname: existing.example.com   # rules you already have stay first
    service: http://localhost:3000
  - hostname: "*.example.com"
    service: http://localhost:8080
  - service: http_status:404
```

Then point each service that moves to Caddy at the tunnel, through its
Cloudflare labels. The target is the tunnel's own hostname,
`<tunnel ID>.cfargotunnel.com`, and the record must be proxied:

```yaml
docktornet.caddy.0.host: app.example.com
docktornet.caddy.0.port: "8080"
docktornet.cf.dns.name: app
docktornet.cf.dns.target: <tunnel ID>.cfargotunnel.com
docktornet.cf.dns.proxied: "true"
```

Only hostnames whose DNS record points at the tunnel reach Caddy. Names that
still route through NPM keep records that point at NPM, so the catch-all never
sees them.

### Client IPs

Caddy's peer is the tunnel connector, not the visitor. Cloudflare sends the
visitor's address in `CF-Connecting-IP` and `X-Forwarded-For`, and the two
server settings in the bootstrap make Caddy use them:

- `trusted_proxies` lists the addresses Caddy accepts forwarded headers from.
  Caddy forwards `X-Forwarded-For` from a trusted peer and appends the peer's
  address, so the visitor's address comes first. From any other peer Caddy
  replaces the header with the peer's address. `cloudflared` on the host
  connects through the published port, so Caddy sees it at the gateway of
  `caddy-admin`, inside that network's subnet. The compose fragment fixes the
  subnet so the bootstrap can name it. If you change one, change the other, or
  Caddy stops trusting the tunnel. Caddy's JSON config doesn't accept the
  Caddyfile's `private_ranges` shorthand. Only processes on the host can reach
  a port published on loopback, so no one else can send Caddy a forged
  header.
- `client_ip_headers: ["CF-Connecting-IP"]` makes Caddy take the client
  address in its logs and `client_ip` matchers from that header, when the peer
  is trusted.

Caddy passes `CF-Connecting-IP` through to the app unchanged, so an app that
reads it gets the visitor's address either way.

### Moving a service from NPM to Caddy

A service can carry `npm.*` and `caddy.*` groups for the same hostname at once,
so it can move without a gap:

1. Add the `caddy.*` group beside the `npm.*` group, and change `cf.dns.target`
   to the tunnel hostname, proxied. Recreate the container. In one update the
   server creates the Caddy route and points the DNS record at the tunnel.
   Until Cloudflare serves the new record, traffic still reaches NPM, which
   still has the proxy host.
2. Check the service answers through the tunnel. If its Caddy route is
   missing, the request falls through to the bootstrap's last route and gets a
   404. The server log says why, for example `refusing caddy route` when
   another service's Caddy route already holds the hostname.
3. Remove the `npm.*` group and recreate the container. The server deletes the
   NPM proxy host in that update, with no grace period, because dropping a
   group is a label edit.

The server never moves anything between the two proxies on its own. Only
labels decide which proxy routes a service.
