# Caddy Reverse Proxy Provider — Design

**Date:** 2026-06-25
**Status:** Superseded by [2026-10-21 Caddy provider design](2026-10-21-caddy-provider-design.md)

## Summary

Add Caddy as a second reverse-proxy provider alongside the existing Nginx Proxy
Manager (NPM) integration. NPM is not replaced. Both run concurrently, and each
service opts into whichever provider it wants through its label namespace
(`docktornet.npm.*` vs `docktornet.caddy.*`). A single service may carry both
namespaces and be exposed through both proxies; the two integrations reconcile
independently.

## Goals

- Per-service provider selection: a container chooses NPM, Caddy, or both via labels.
- Reuse the existing reconciler structure (per-service loop, circuit breaker,
  grace period, full-resync orphan sweep, per-service failure isolation).
- Keep Caddy lighter than NPM by exploiting Caddy's strengths: automatic HTTPS
  (ACME), automatic websockets/HTTP/2, and deterministic config addressing.

## Non-goals

- No `ReverseProxyProvider` trait / abstraction. NPM (stateful REST of opaque
  ids) and Caddy (declarative config with caller-chosen ids) differ enough that
  a shared trait would be leaky. Caddy is added as its own integration beside
  NPM, sharing only label-parsing conventions and the breaker/grace patterns.
- docktornet does not run or own the Caddy instance. As with NPM and Gatus,
  Caddy is external infrastructure that docktornet points at.

## Ownership model

One shared Caddy instance serving all web traffic on the normal ports
(`:80`/`:443`), i.e. a single Caddy "server" block. docktornet manages only the
routes it creates, addressed by a deterministic `@id`, and never touches routes
it did not create (hand-written routes in the same server coexist safely).

## Architecture

Caddy becomes a fourth independent integration beside NPM, Cloudflare, and
Gatus. New/changed pieces, mirroring the NPM layout:

- `crates/server/src/integrations/caddy.rs` — `CaddyClient` for the Caddy admin
  API: get / create / update / delete a route by `@id`. Smaller than `npm.rs`:
  no auth token, no certificate list, no adoption logic.
- `crates/server/src/integrations/mod.rs` — add `pub mod caddy;`.
- `crates/server/src/config.rs` — `CaddyConfig { admin_url: Url, server: String }`
  plus env vars `DOCKTORNET_CADDY_ADMIN_URL` and `DOCKTORNET_CADDY_SERVER`. Both
  optional: when unset, the Caddy client is `None` (integration disabled).
- `crates/core/src/labels.rs` — `CaddyRoute` struct, `caddy: Vec<CaddyRoute>`
  field on `ParsedLabels`, a `["caddy", idx, field]` parse arm, `build_caddy_route`,
  and `is_caddy_field`.
- `crates/server/src/integrations/intended.rs` — `has_intent` also checks
  `!parsed.caddy.is_empty()`.
- `crates/server/src/reconcile.rs` — `Option<CaddyClient>` on `Reconciler`, a
  `reconcile_caddy` path in the per-service apply loop, a `resync_caddy` path,
  removal handling with grace, and a Caddy orphan sweep in `full_resync`. A
  dedicated `CircuitBreaker` for Caddy.
- `crates/server/src/main.rs` / `routes.rs` — construct the `CaddyClient` when
  configured and pass it into the reconciler.

**Key simplification vs NPM:** each route's `@id` is set deterministically from
the `service_id` and route index, so there is **no new SQLite table** — no
`caddy_routes`, no `pending_caddy_removals`. The existing `services` table
already records which services exist and which have left, which is sufficient to
derive every managed route id.

## Labels

A `docktornet.caddy.N.*` group parses into one `CaddyRoute`, mirroring `NpmHost`
(indexed, comma-separated multi-domain, warnings on bad input — never a hard
failure).

| Field | Required | Default | Meaning |
|-------|----------|---------|---------|
| `host` | yes | — | Public domain(s). Comma-separated = multiple domains on one route. |
| `port` | yes | — | Container port to forward to (`u16`). |
| `scheme` | no | `http` | Scheme to the **upstream** container (`http`/`https`). |
| `tls` | no | `true` | Whether Caddy serves the public side over HTTPS with an auto-provisioned cert. `false` = HTTP-only. |

Dropped relative to NPM (Caddy handles these automatically): `cert_id`, `hsts`,
`hsts_subdomains`, `http2`, `block_exploits`, `websockets`.

Typical usage:

```
docktornet.caddy.0.host   = app.example.com
docktornet.caddy.0.port   = 3000
docktornet.caddy.0.scheme = http
```

## Generated route

From a `CaddyRoute` plus the machine's `forward_host`, docktornet renders a
Caddy route object carrying `@id`:

```json
{
  "@id": "m1/app#0",
  "match": [{ "host": ["app.example.com", "app.other.org"] }],
  "handle": [{
    "handler": "reverse_proxy",
    "upstreams": [{ "dial": "10.0.0.1:3000" }]
  }]
}
```

- `@id` = `"<service_id>#<index>"`. One route per `caddy.N` group; all
  deterministically addressable.
- `dial` = `<forward_host>:<port>` — the same per-machine forwarding NPM does.
- `scheme = https` to the upstream adds
  `"transport": { "protocol": "http", "tls": {} }` to the `reverse_proxy` handler.
- `tls = false` makes the route HTTP-only (the domains are excluded from
  auto-HTTPS); `tls = true` (default) lets Caddy auto-provision certs for the
  domains. No certificate configuration is emitted by docktornet.

## Reconcile, removal & drift

**Create / update** (per service, in the existing apply loop). For each
`CaddyRoute` on a live service, compute the desired route JSON, then:

1. `GET /id/<service_id>#<index>` — does our route exist?
2. Absent → `POST /config/apps/http/servers/<server>/routes` with the route
   object (appends, carrying `@id`).
3. Present but different → `PATCH /id/<service_id>#<index>` with the new object.
4. Present and identical → no-op (avoid needless config reloads; same principle
   as NPM's `matches_payload`).

**Removal.** Removed services are already computed by `removed_service_ids`. For
each removed service, `DELETE /id/<service_id>#<index>` for each of its route
indices (known from the service's last-seen labels). Nothing needs to have been
stored — the id is derived.

**Grace period.** Reuse the existing ~5-minute grace concept so a briefly
restarting container does not flap its public route: defer the delete until the
service has been absent past the grace window. No durable table is required
because the id is deterministic.

**Orphan sweep (offline-machine backstop).** A machine that drops off entirely
never sends a final empty snapshot, so its routes would linger. `full_resync`
handles this declaratively: list Caddy's routes, filter to those whose `@id`
carries docktornet's marker (the `service_id#index` shape), and delete any whose
backing service no longer exists in the DB. This is analogous to the existing
Gatus orphan-file sweep, and it is also the eventual backstop for grace-deferred
deletes (rather than a precise per-removal timer).

**Circuit breaker.** Caddy admin calls are wrapped in the same `CircuitBreaker`
pattern NPM uses (its own instance), so a struggling Caddy is backed off rather
than hammered.

## Error handling

Follows existing reconciler conventions:

- Per-service isolation: a Caddy failure for one service is logged and counted,
  never aborts others.
- When `DOCKTORNET_CADDY_*` is unset, the client is `None`; a service carrying
  `caddy.*` labels logs a warning ("Caddy labels present but Caddy client is not
  configured") and is skipped — mirrors NPM.
- Admin-API calls use the same 30s request timeout as NPM, so a hung Caddy
  cannot stall the reconcile lock.
- The circuit breaker opens after repeated failures and skips Caddy for a cooldown.
- **Bootstrap assumption:** the target Caddy server (`DOCKTORNET_CADDY_SERVER`)
  must already exist with a `routes` array. docktornet manages routes *within*
  that server; it does not create the Caddy instance or the server block (just
  as NPM must already be running). Documented in deploy docs.

## Testing

Mirrors the NPM suite, using `mockito` to fake the admin API:

- **Label parsing** (`core`): required/optional fields, multi-domain comma split,
  bad-input warnings, default `scheme`/`tls`.
- **Client** (`caddy.rs`): create (POST), update (PATCH), delete, get-404
  returns `None`, request-timeout against an unresponsive server.
- **Reconcile** (`reconcile.rs`): skips PATCH when the route already matches;
  creates when absent; forwards to each machine's own `forward_host`; deletes on
  removal past grace; orphan sweep removes routes for a vanished machine; breaker
  opens on repeated failure.
- **Cross-provider**: a service carrying both `npm.*` and `caddy.*` labels
  reconciles through both independently.

## Deploy & docs

- Add `DOCKTORNET_CADDY_ADMIN_URL` and `DOCKTORNET_CADDY_SERVER` to
  `.env.example`.
- Document that Caddy is external (like NPM): its admin API (default `:2019`)
  must be reachable from the docktornet server, and the named server with a
  `routes` array must already exist. Include a minimal starter Caddy config
  snippet in the docs. Do **not** add Caddy to docktornet's compose files.
- Document the `docktornet.caddy.*` label set in the README.
```
