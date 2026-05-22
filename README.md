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
