# Changelog

## Unreleased

## v1.2.0 - 2026-10-22

### After upgrading

- Take a copy of `docktornet.db` before the first start. Migrations 0006 to 0010 run then, and a 1.1.0 server refuses the database they leave, so going back means putting the copy back.
- The server and the agent read separate files, `server.env` and `agent.env`, next to the compose file you run, where both used to read one `.env`. Split the old file using `server.env.example` and `agent.env.example` as the lists of keys. The agent's container, which mounts the Docker socket, no longer holds the NPM password or the Cloudflare token. [#12]
- `DOCKTORNET_SECRET` must be at least 32 characters and must not contain `changeme`, or the server and every agent refuse to start. Generate one with `openssl rand -hex 32`, put it in `server.env` and in every host's `agent.env`, and recreate the server and all agents together. [#12]
- The compose files take their images from `SERVER_IMAGE` and `AGENT_IMAGE` and refuse to start without them. The agent-only file used to fall back to `:latest`. Set a release tag, such as `docktornet-agent:1.2.0`. [#15]
- `DOCKTORNET_DB_PATH` must be an absolute path to a file. Inside a container the database must be on a volume, or the server refuses to start and names the directory to mount one at. The server image now sets the path to `/data/docktornet.db`, so mount a volume at `/data` and leave the variable unset. `DOCKTORNET_ALLOW_EPHEMERAL_DB=true` starts anyway, for a throwaway test. [#6]
- `DOCKTORNET_MACHINE_ID` must be non-empty, with no `/`, whitespace or control characters, or the agent refuses to start. [#11]
- The first full resync, which runs at startup, writes an ownership marker on every mapped NPM proxy host and Cloudflare record, once. NPM reloads nginx for each proxy host it updates, so expect one reload per host on that pass. [#5]

### Added

- Caddy can serve a service beside NPM or in its place. `docktornet.caddy.<index>.host`, `.port` and `.scheme` labels describe a route, and the server writes it through Caddy's admin API inside one anchor route of your config, named by `DOCKTORNET_CADDY_ADMIN_URL` and `DOCKTORNET_CADDY_ANCHOR_ID` (default `docktornet`). A host one service already routes is refused to another. Without the admin URL, Caddy labels are skipped with a warning, and NPM stays required. The deployment guide has a bootstrap config, how to keep the admin API off other networks, a Cloudflare tunnel setup and how to move a service from NPM to Caddy. [#20] [#24]
- A Caddy route follows the same removal lifecycle as an NPM proxy host: it keeps serving through the grace period after its service leaves, comes back if the service returns, and is deleted once the grace period ends. Caddy has its own circuit breaker. [#21]
- A Caddy route deleted or edited by hand, or wiped by `caddy reload`, is put back within a minute, from what the server stored and never from labels. A missing anchor is logged and never created. [#22]
- Every resource the server manages names its owner: `meta.docktornet` on an NPM proxy host, the comment `docktornet:<service_id>` on a Cloudflare record, and the `@id` of a Caddy route. A server that lost its database takes back its own resources by marker, with no adoption flag. Adoption by hostname now takes only unmarked resources, and a resource marked for another service is refused (ADR 0001). [#5] [#20]
- `docktornet-server audit` compares NPM, Cloudflare and, when configured, Caddy with the server's ownership records and prints one JSON report: resources marked but not recorded, pending removals, recorded resources that are missing or unmarked, and unmarked NPM hosts whose upstream does not answer. It exits 0 when clean, 1 with findings and 2 when it fails. It changes nothing, so it runs next to the live server with `docker exec`. `audit --only caddy` audits Caddy alone. [#8] [#23]
- `docktornet-server backup` writes a consistent copy of the database to stdout while the server runs. `scripts/backup_external_state.sh` now saves the database and the Gatus config directory beside NPM and Cloudflare, finishes a backup whole or leaves no backup at all, and keeps the newest `DOCKTORNET_BACKUP_KEEP` (default 14). [#13]
- `docktornet-server healthcheck` checks `/healthz` on loopback for compose, which the distroless image had nothing to probe with. The main-host and dev compose files use it, and their agents wait for a healthy server. [#14]
- Authenticated `/healthz` reports the server's `version`, each agent's `agent_version`, the last full resync's result, the NPM breaker's state and, with Caddy configured, the Caddy breaker's state and whether the anchor was found. It reports `degraded` while a breaker is open or the anchor is missing. The public response is unchanged. The server logs a warning when an agent's version differs from its own. [#6] [#14] [#23]
- `docktornet.gatus.<index>.severity` (`critical`, `warning` or `info`) and `.area` (`core`, `backups`, `storage`, `network`, `apps`, `deploys` or `agents`) label a monitor. `docktornet-classification.json`, next to the Gatus files, maps each endpoint's Gatus key to its service, host, severity, area and alert thresholds. An unknown value drops that monitor with a warning, so a typo cannot demote a critical endpoint. [#7]
- `DOCKTORNET_CF_API_URL` points the server at another Cloudflare API address, such as a test stub. An address off the `/client/v4` path is refused. [#17]

### Changed

- The full resync puts back every field Docktornet sets on an NPM proxy host, where it used to repair only the certificate. A managed host's port, upstream, scheme or flags edited in NPM's interface is set back within the hour. [#10]
- Removing a `docktornet.npm.<index>` group from a running container deletes its proxy host in the same apply, with no grace period, as removing a `docktornet.caddy.<index>` group does. A group with a typo keeps its host. [#18]
- Within five minutes of an agent's last push, another agent pushing the same machine id from another address with another forward host is answered `409`. [#11]
- Each machine has one apply running and one waiting, and a newer push replaces the waiting one. A slow NPM or Cloudflare used to queue one apply per poll. [#11]
- Both images pin their base images by digest and build with `--locked`, on Rust 1.99.1. [#15]

### Fixed

- A departed service's NPM proxy host, Cloudflare record and Gatus monitor go within a minute of the grace period ending, where they used to wait for the hourly resync. Gatus drops the deleted monitor too: it never noticed a deleted file, so the server now makes it reload. [#4]
- The grace period runs on the server's clock. An agent whose clock ran behind, or read zero, could have a proxy host or DNS record deleted before its grace period ended. A removal pending at the upgrade starts its grace period again. [#9]
- The full resync leaves NPM alone while its breaker is open, and NPM failures during a resync trip the breaker. [#9]
- A failed Gatus write no longer leaves its temp file behind, and stale temp files from earlier versions are swept. [#9]
- An NPM update keeps the keys NPM and its interface store in the host's `meta`. Each update used to wipe them. [#5]
- The full resync no longer rewrites every plain-HTTP proxy host on every pass, nor a host whose domains are listed in another order than NPM sorts them. [#5] [#10]
- An NPM boolean field that is neither a boolean, a number nor `"true"`, `"false"`, `"1"` or `"0"` fails the resync, where it was read as false. The Cloudflare zone is fetched once and kept. [#10]
- A malformed `docktornet.caddy.<index>` group, such as `port=80a`, keeps its live route, where it was deleted. [#25]
- A running container whose `docktornet.*` labels are all invalid has its resources removed once the grace period ends. Each push used to restart the grace period, so they stayed for good. [#26]
- The shared secret, the NPM password and the Cloudflare token no longer appear in debug output. A `server.env` at the repository root stays out of the image build. [#12]
- Podman builds both images. It refused the short name of the build image.
- The TLS library the server and the agent connect with, and the certificates it trusts, are past six published flaws (RUSTSEC-2026-0285, RUSTSEC-2026-0346, RUSTSEC-2026-0348, RUSTSEC-2026-0349, RUSTSEC-2026-0381 and RUSTSEC-2026-0382).

[#4]: https://github.com/BattermanZ/DocktorNet/issues/4
[#5]: https://github.com/BattermanZ/DocktorNet/issues/5
[#6]: https://github.com/BattermanZ/DocktorNet/issues/6
[#7]: https://github.com/BattermanZ/DocktorNet/issues/7
[#8]: https://github.com/BattermanZ/DocktorNet/issues/8
[#9]: https://github.com/BattermanZ/DocktorNet/issues/9
[#10]: https://github.com/BattermanZ/DocktorNet/issues/10
[#11]: https://github.com/BattermanZ/DocktorNet/issues/11
[#12]: https://github.com/BattermanZ/DocktorNet/issues/12
[#13]: https://github.com/BattermanZ/DocktorNet/issues/13
[#14]: https://github.com/BattermanZ/DocktorNet/issues/14
[#15]: https://github.com/BattermanZ/DocktorNet/issues/15
[#17]: https://github.com/BattermanZ/DocktorNet/issues/17
[#18]: https://github.com/BattermanZ/DocktorNet/issues/18
[#20]: https://github.com/BattermanZ/DocktorNet/issues/20
[#21]: https://github.com/BattermanZ/DocktorNet/issues/21
[#22]: https://github.com/BattermanZ/DocktorNet/issues/22
[#23]: https://github.com/BattermanZ/DocktorNet/issues/23
[#24]: https://github.com/BattermanZ/DocktorNet/issues/24
[#25]: https://github.com/BattermanZ/DocktorNet/issues/25
[#26]: https://github.com/BattermanZ/DocktorNet/issues/26

## v1.1.0 - 2026-07-19

Released before this changelog was kept. See the [v1.1.0 release notes](https://github.com/BattermanZ/DocktorNet/releases/tag/v1.1.0).

## v1.0.0 - 2026-06-02

Released before this changelog was kept. See the [v1.0.0 release notes](https://github.com/BattermanZ/DocktorNet/releases/tag/v1.0.0).
