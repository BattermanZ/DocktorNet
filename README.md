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

## Container labels

Docktornet reconciles state from Docker/Podman container labels. All keys are
prefixed with `docktornet.`; any other labels are ignored. Containers with no
`docktornet.*` labels are skipped entirely.

Boolean fields accept `true`/`1`/`yes` or `false`/`0`/`no`.

### Nginx Proxy Manager (`docktornet.npm.<index>.*`)

`<index>` is an integer (`0`, `1`, …) so a single container can publish
multiple proxy hosts.

| Label | Required | Type | Default | Notes |
|-------|----------|------|---------|-------|
| `docktornet.npm.<index>.host` | yes | string | — | Public hostname (FQDN). |
| `docktornet.npm.<index>.port` | yes | u16 | — | Upstream port. |
| `docktornet.npm.<index>.scheme` | yes | `http` \| `https` | — | Upstream scheme. |
| `docktornet.npm.<index>.ssl` | yes | bool | — | Enable SSL on the proxy host. |
| `docktornet.npm.<index>.hsts` | no | bool | unset | Enable HSTS. |
| `docktornet.npm.<index>.http2` | no | bool | unset | Enable HTTP/2 support. |
| `docktornet.npm.<index>.cert_id` | no | u32 | unset | Existing NPM certificate ID. |

### Cloudflare DNS (`docktornet.cf.dns.*`)

A single DNS record per container (not indexed).

| Label | Required | Type | Default | Notes |
|-------|----------|------|---------|-------|
| `docktornet.cf.dns.name` | yes | string | — | Record name. |
| `docktornet.cf.dns.target` | yes | string | — | Record target. |
| `docktornet.cf.dns.proxied` | yes | bool | — | Whether the record is proxied through Cloudflare. |

### Gatus monitors (`docktornet.gatus.<index>.*`)

`<index>` is an integer so a single container can define multiple monitors.

| Label | Required | Type | Default | Notes |
|-------|----------|------|---------|-------|
| `docktornet.gatus.<index>.url` | yes | string | — | Endpoint to monitor. |
| `docktornet.gatus.<index>.interval` | yes | string | — | Check interval (e.g. `30s`). |
| `docktornet.gatus.<index>.conditions` | no | string | `[STATUS] == 200` | Comma-separated condition list. |
| `docktornet.gatus.<index>.group` | no | string | none | Gatus group name. |
| `docktornet.gatus.<index>.telegram` | no | bool | `true` | Send Telegram alerts. |
