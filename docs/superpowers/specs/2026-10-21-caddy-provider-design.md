# Caddy provider design

**Date:** 2026-10-21
**Status:** Draft, pending review
**Supersedes:** [2026-06-25 Caddy reverse proxy provider design](2026-06-25-caddy-reverse-proxy-provider-design.md)

## Problem statement

Every proxy host docktornet manages lives in one Nginx Proxy Manager. When the
machine running NPM, or the network in front of it, goes down, every service
behind it goes dark, including services that run on other machines and are
healthy. An operator with a second site, such as a cloud VM that already runs
the docktornet server, has no way to give that site's services an ingress of
their own while keeping label-driven management. They would have to hand-write
proxy config there and lose the grace period, ownership markers, audit and drift
repair docktornet gives NPM hosts.

The June 2026 design for a Caddy provider does not fix this, and it no longer
fits the code.

- It removes a departed service's routes by deriving their ids from the
  `services` table, but the server deletes a service's row the moment it leaves
  a snapshot. Nothing would be left to time the grace period from.
- Its orphan sweep deletes routes it finds by listing, which breaks the rule
  that docktornet deletes only what a stored mapping or pending removal points
  to. It also misses its target, because a machine that stops reporting keeps
  its services rows indefinitely.
- Its route `@id`, `<service_id>#<index>`, contains a `/`, which Caddy's
  `/id/<id>` endpoint reads as a path separator.
- It appends routes to a whole Caddy server, so a hand-written catch-all route
  shadows every route docktornet adds after it.
- Its `tls` label defaults to on, which makes Caddy request certificates and
  redirect to HTTPS. Behind a tunnel or another TLS-terminating front, that
  fails or loops.

## Solution

Docktornet gains Caddy as a second reverse proxy beside NPM. A service chooses
NPM, Caddy or both through its labels. `docktornet.npm.<index>.*` works as
today, and a new `docktornet.caddy.<index>.*` set routes through Caddy. NPM
stays mandatory config, and Caddy is optional.

The operator runs one Caddy, typically on the machine whose services should
survive the loss of the NPM site. Caddy speaks plain HTTP behind something that
terminates TLS, such as a Cloudflare tunnel. It persists its own config with
`caddy run --resume`, so its routes come back after a restart even while the
docktornet server is unreachable.

Docktornet manages Caddy routes the way it manages NPM proxy hosts and
Cloudflare DNS records, with:

- an ownership mapping and pending removals in SQLite;
- an ownership marker on the route itself, its `@id`;
- the shared grace period, sweep, retry and restore-on-return;
- marker adoption after a database loss;
- an audit section.

Every route it owns sits inside one anchor route. The anchor route is a route
the operator's bootstrap config places first in the Caddy server, holding an
empty list of sub-routes that only docktornet writes to. Hand-written routes go
after the anchor. A check every minute keeps the routes in Caddy equal to what
docktornet last decided.

## User stories

1. As an operator, I want to label a container with `docktornet.caddy.0.host` and `docktornet.caddy.0.port`, so that Caddy routes its hostname to it without hand-written Caddy config.
2. As an operator, I want to list several hostnames in one `host` label, comma-separated, so that one route serves all of them, as with NPM.
3. As an operator, I want `scheme` to default to `http`, so that the common case needs two labels.
4. As an operator, I want `scheme=https` to reach a self-signed upstream, so that apps that only speak HTTPS work through Caddy as they do through NPM.
5. As an operator, I want a service to carry both `npm.*` and `caddy.*` labels, even for the same hostname, so that I can move it between proxies without a gap.
6. As an operator, I want a malformed `caddy.*` label to drop only that route and log a warning naming it, so that one typo doesn't take down the service's other routes.
7. As an operator, I want Caddy routes to forward to the agent's `DOCKTORNET_FORWARD_HOST` and the label's port, so that Caddy reaches each machine's containers the same way NPM does.
8. As an operator, I want Caddy config to be optional, so that upgrading docktornet without configuring Caddy changes nothing.
9. As an operator, I want `caddy.*` labels on a server with no Caddy configured to log a warning and be skipped, so that I notice the missing config without a failed reconcile.
10. As an operator, I want docktornet to write only inside one anchor route that I place in my bootstrap config, so that my hand-written routes and catch-all keep working and keep their order.
11. As an operator, I want the anchor's `@id` to default to `docktornet` and be configurable, so that I can run it beside an existing config that already uses that id.
12. As an operator, I want Caddy to keep serving its routes after a restart while the docktornet server is unreachable, so that the site survives an outage at the other site plus a reboot.
13. As an operator, I want docktornet to leave a route untouched when it already matches the labels, so that Caddy doesn't reload for nothing.
14. As an operator, I want a changed label to update the route in place, so that a port or host change applies within one agent poll.
15. As an operator, I want a second claim on a hostname already held by another service's Caddy route, in any letter case, to be refused with an error naming both services, so that a duplicate can't silently shadow the first route.
16. As an operator, I want the earlier owner to keep a contested hostname, so that routing stays stable when a route is recreated.
17. As an operator, I want a refused route to be retried on later applies, so that it comes up once the clash is resolved.
18. As an operator, I want a service that disappears briefly to keep its Caddy route through the grace period, so that a restart doesn't drop public traffic.
19. As an operator, I want a service that stays gone past the grace period to lose its Caddy route within about a minute, so that dead hostnames stop routing.
20. As an operator, I want a service that returns within the grace period to keep its route and mapping, so that a restart leaves no trace.
21. As an operator, I want dropping one `caddy.N` group from a running service to remove that route in the same apply, so that a deliberate label edit takes effect without waiting out a grace period.
22. As an operator, I want a failed route deletion to be retried every minute, so that a Caddy hiccup doesn't leave a route behind for good.
23. As an operator, I want removals that come due while Caddy is unreachable to wait as pending removals, so that they happen once Caddy is back.
24. As an operator, I want every docktornet route to carry an `@id` naming its service, so that ownership survives a lost database.
25. As an operator, I want docktornet to adopt a route whose `@id` names the service when no mapping exists, so that restoring from an empty database needs no flag and no manual cleanup.
26. As an operator, I want docktornet never to delete a route it only found by listing, so that the Caddy provider keeps the safety rule NPM and Cloudflare follow.
27. As an operator, I want a route deleted or edited by hand inside the anchor to be put back within a minute, so that Caddy can't drift from the labels for long.
28. As an operator, I want docktornet to report a missing anchor route rather than recreate it, so that it never decides where in my config its routes go.
29. As an operator, I want Caddy failures to open their own circuit breaker, so that a struggling Caddy is backed off without affecting NPM, Cloudflare or Gatus.
30. As an operator, I want one route that Caddy rejects to fail on its own, so that it can't open the breaker and stall every other route.
31. As an operator, I want an unreachable NPM to leave Caddy reconciling, so that the site with Caddy keeps getting route changes while the NPM site is down.
32. As an operator, I want dry-run to make no Caddy writes, including from the drift check, so that dry-run stays safe to point at a live Caddy.
33. As an operator, I want `/healthz` to show the Caddy breaker and the anchor's state, so that my monitoring sees a broken Caddy integration.
34. As an operator, I want `/healthz` to report `degraded` when the Caddy breaker is open or the anchor is missing, so that the existing health check catches it.
35. As an operator, I want the Caddy health fields to appear only for authenticated callers and only when Caddy is configured, so that the public response stays as it is.
36. As an operator, I want `docktornet-server audit` to list docktornet-marked routes with no mapping, pending Caddy removals, mapped routes missing from Caddy and marked routes sitting outside the anchor, so that I can find leftover routes after a database loss or a config rebuild.
37. As an operator, I want `audit --only caddy`, so that I can audit Caddy alone.
38. As an operator, I want the audit to report a missing anchor as an error, so that a broken bootstrap shows up there too.
39. As an operator, I want the deployment guide to include a working bootstrap config, so that I can start Caddy correctly the first time.
40. As an operator, I want the guide to explain keeping the admin API on a private network shared only with the docktornet server, so that I don't expose an unauthenticated API that controls my ingress.
41. As an operator, I want the guide to cover running Caddy behind a Cloudflare tunnel, including rule order and passing the real client IP to apps, so that I can reproduce the intended setup.
42. As an operator, I want the guide to warn that reloading the bootstrap file empties the anchor until the next drift check, so that a config change doesn't surprise me with a short outage.
43. As an operator, I want the README to document the `caddy.*` labels, so that I can write them without reading the code.
44. As a developer, I want the live dev stack to include a real Caddy, so that I can check that Caddy accepts the routes docktornet renders.
45. As a developer, I want the Caddy provider to reuse the reconcile, grace, sweep and breaker patterns, so that it behaves like the other providers.

## Implementation decisions

### Shape

- Caddy is its own integration beside NPM, Cloudflare and Gatus. There is no
  shared reverse-proxy trait. NPM is a REST API of opaque ids, and Caddy is a
  config tree with ids the caller picks, so a shared trait would leak.
- One Caddy instance per docktornet server, reached through its admin API.
  Docktornet doesn't run, start or bootstrap Caddy.
- The server reconciles Caddy in the same per-machine apply, full resync and
  pending-removal sweep as the other providers, with the same per-service and
  per-provider failure isolation. An NPM failure or an open NPM breaker never
  stops the Caddy work, and the reverse holds too.

### Labels

- `docktornet.caddy.<index>.host` is required. It holds one or more FQDNs,
  comma-separated.
- `docktornet.caddy.<index>.port` is required, a `u16`.
- `docktornet.caddy.<index>.scheme` is optional, `http` or `https`, and
  defaults to `http`. It is the scheme Caddy uses toward the upstream.
- There is no `tls` label. NPM's toggles are dropped too, namely `hsts`,
  `hsts_subdomains`, `http2`, `block_exploits`, `websockets` and `cert_id`.
  Caddy proxies websockets by default, and the front that terminates TLS owns
  HSTS and HTTP/2.
- Parsing follows the `npm.*` rules. Groups are indexed, hosts are split on
  commas, and a malformed group is dropped with a warning while the rest of the
  container's labels still parse. A container with only `caddy.*` labels counts
  as having intent.
- Caddy hosts are lowercased when parsed, because Caddy matches hosts without
  regard to case and the clash check compares strings. A host that appears in
  two `caddy.N` groups of the same container stays on the lower index, and the
  later group is dropped with a warning.
- Caddy labels are part of the per-machine intent hash already, because the
  hash covers every `docktornet.*` pair.

### Configuration

- `DOCKTORNET_CADDY_ADMIN_URL` is optional. When it is unset, the integration
  is off, and a service with `caddy.*` labels logs a warning and is skipped.
- `DOCKTORNET_CADDY_ANCHOR_ID` is optional and defaults to `docktornet`. It is
  the `@id` of the anchor route.
- NPM config stays mandatory.

### Anchor route and bootstrap contract

- The operator's bootstrap config defines a route with `"@id": "<anchor id>"`
  whose single handler is a `subroute` with an explicit `"routes": []`. Caddy
  can't append into a subroute that has no `routes` key and rejects the
  config. The anchor should be the first route of the server that serves the
  proxied hostnames. Docktornet reads and writes only inside that `routes`
  array.
- The bootstrap is expected to turn automatic HTTPS off for that server and to
  listen on plain HTTP.
- The bootstrap has to set `admin.listen` to an address the docktornet server
  can reach, such as `0.0.0.0:2019`. The official image's default,
  `localhost:2019`, can't be reached from another container.
- `admin.origins` only checks requests that carry an `Origin` header, which
  browsers send and docktornet doesn't. It blocks a web page from driving the
  admin API, and it is not access control. The deployment guide lists the
  origins the `caddy` CLI inside the container needs, so `caddy reload` still
  works.
- Caddy is expected to run with `--resume`, so changes made through the admin
  API survive restarts. Docktornet doesn't check this. The deployment guide
  states it, along with keeping Caddy's config directory on a persistent
  volume, because that is where the autosave lives.
- Once an autosave exists, `--resume` ignores later edits to the bootstrap
  file. `caddy reload` with the bootstrap file replaces the whole config and
  empties the anchor. The drift check refills it within a minute. The
  deployment guide says to expect that gap, or to change settings outside the
  anchor through the admin API instead.

### Rendered route

For each `caddy.N` group of a live service, docktornet renders:

```json
{
  "@id": "docktornet--<machine slug>__<container slug>--<16 hex>--<N>",
  "match": [{ "host": ["app.example.com", "app.example.org"] }],
  "handle": [{
    "handler": "reverse_proxy",
    "upstreams": [{ "dial": "<forward_host>:<port>" }]
  }],
  "terminal": true
}
```

- With `scheme=https`, the handler also gets an HTTP transport with TLS on and
  `insecure_skip_verify` set. NPM doesn't verify upstream certificates either,
  and the dial target is an address no upstream certificate names.
- An IPv6 forward host is bracketed in `dial`.

### Ownership marker

- The route's `@id` is its ownership marker. It is `docktornet--`, then the
  service ID as `sanitize_service_id` already slugs it for Gatus filenames,
  then `--` and the 16-hex stable hash of the exact service ID that Gatus
  filenames use, then `--` and the decimal index. The marker builder calls the
  existing slug function and the same hash function, and copies neither.
- The format avoids `/`, `#` and anything else that needs escaping in a URL
  path segment. Machine IDs may hold any character except `/`, whitespace and
  control characters, so the raw service ID can't be used.
- The hash keeps ids distinct when two service IDs slug the same. The id isn't
  reversible. Adoption and the audit compute the id from a known service and
  look it up, and never parse it back.
- The `docktornet--` prefix is how the audit recognises docktornet's routes.
- ADR 0001 gains this format as the Caddy marker. Caddy decodes route JSON
  strictly, so a route can't carry a free-form `meta` object the way an NPM
  host can.

### Admin API calls

- Read the anchor's routes with `GET /id/<anchor>/handle/0/routes`. Both the
  apply and the drift check decide whether a route exists from this read, so
  they always agree on where a route counts as present.
- Read one route with `GET /id/<marker>`. A 404 means it doesn't exist
  anywhere in the config. It is used only when a marker is missing from the
  anchor, to tell an absent route from one outside the anchor.
- Create by appending with `POST /id/<anchor>/handle/0/routes`.
- Update in place with `PATCH /id/<marker>`.
- Delete with `DELETE /id/<marker>`. A 404 counts as already gone.
- Requests share NPM's 30-second timeout.
- Caddy returns stored JSON with its keys sorted, so every comparison between
  a rendered route and Caddy's copy is between parsed JSON values, never
  strings.
- Caddy rejects an `@id` that already exists anywhere in its config with a 400,
  and a config it can't load with a 500. Both are per-route failures.
- The client sends no credentials. The admin API's protection is network
  isolation, covered in the deployment guide.

### Schema

One new migration adds two tables.

- `caddy_routes` is the ownership mapping, keyed by service ID and Caddy index
  and cascading from `services` like `npm_hosts`. It stores no remote id,
  because the marker is derived. It holds the hosts the route has claimed, for
  the clash check, and the route as last rendered after a successful claim,
  which the drift check restores. A refused claim changes neither, so the
  drift check can't write hosts the clash check refused.
- `pending_caddy_removals` is the pending removal, keyed the same way, holding
  the claimed hosts and a server-time `last_seen`. Like `pending_npm_removals`,
  it has no foreign key to `services`.

### Create, update and adoption in the apply

For each live service with Caddy work in a machine's apply, docktornet reads
the anchor's routes once, then does the following.

1. Dropped indices go first. A `caddy_routes` row whose index is no longer in
   the labels is a label edit, not a departure, so it gets no grace period.
   Docktornet deletes the route and the row in this apply. If the delete fails,
   or the breaker is open, the row moves to `pending_caddy_removals` already
   past grace, so the next sweep retries it.
2. Each remaining `caddy.N` group claims its hosts. In one transaction, the
   group's hosts are checked against every `caddy_routes` and
   `pending_caddy_removals` row of other services. The service's own rows don't
   count, so a host can move between its indices in one edit. NPM hosts never
   count either, so a service may route the same hostname through both
   proxies. On a clash the group is skipped and an error names the requesting
   service and the owner. It counts as a failure, so the intent hash isn't
   cached and later applies retry. With no clash, the mapping row is inserted
   or updated with the new hosts and rendered route. Claiming before writing is
   safe because the marker is deterministic. If the write that follows fails,
   the drift check repairs it.
3. Each claimed group is written.
   - If the anchor holds the route and it matches, nothing happens.
   - If the anchor holds it and it differs, docktornet PATCHes it.
   - If it is missing from the anchor, `GET /id/<marker>` checks the rest of
     the config. On a 404, docktornet POSTs it into the anchor. If the route
     exists outside the anchor, docktornet writes nothing, logs an error with
     the marker, and counts a failure. The audit lists the route.

   Finding the service's marker in the anchor with no prior mapping is marker
   adoption. It needs no flag.
4. There is no hostname adoption for Caddy.

### Removal, grace and restore

These copy the NPM and Cloudflare lifecycle exactly.

- When a service leaves, its `caddy_routes` rows are captured before the
  cascade deletes them. Routes past the grace period are deleted in the apply.
  Every other due removal becomes a `pending_caddy_removals` row. That covers
  routes within grace, routes skipped because the breaker is open or no client
  is configured, and routes whose delete failed.
- The 60-second pending-removal sweep and the full resync delete pending rows
  past grace. They skip a row whose service is stored again, and they keep a
  row whose delete failed.
- When snapshots coalesce, the newer pending apply keeps the Caddy rows the
  older one captured, the way it already keeps NPM hosts and the Cloudflare
  record.
- A service that returns gets its mapping rows back from its pending rows, and
  those pending rows are deleted. Only rows whose index is still labelled come
  back. The rest stay pending and are deleted past grace.
- During its grace period a pending route still serves traffic and still holds
  its hosts for the clash check.

### Drift check

- On the 60-second background tick that runs the pending-removal sweep, and in
  every full resync, docktornet reads the anchor's routes once. This is skipped
  while the Caddy breaker is open, and in dry-run it writes nothing.
- For every `caddy_routes` row of a stored service, it compares the row's
  stored route with the anchor's route of the same `@id` as parsed JSON. It
  PATCHes the route if it differs. If the route is missing, it follows the
  apply's write rule. It POSTs when `GET /id/<marker>` is a 404, and logs and
  counts a failure when the route sits outside the anchor. The drift check
  never renders from labels, so it can't apply a change whose claim was
  refused.
- Routes in the anchor with no mapping, and routes of pending removals, are
  left alone. The audit reports the former.
- If the anchor read returns 404, docktornet logs an error, records the anchor
  as missing for `/healthz`, and writes nothing. It never creates the anchor.

### Circuit breaker

- Caddy gets its own breaker instance, with NPM's threshold of 3 consecutive
  failures and its 60-second cooldown.
- Connection errors, timeouts and unexpected statuses feed it. Caddy's
  per-route 400 and load-failure 500 don't, and neither does a 404 on delete.
  A route Caddy rejects is that route's failure, or one bad route would stall
  every other route.
- An apply that skipped Caddy because of its breaker doesn't cache the intent
  hash, so the work is retried.

### Health endpoint

- Authenticated callers get `caddy_breaker`, in the same shape as
  `npm_breaker`, and `caddy_anchor`. The anchor field is `ok`, `missing`, or
  `unknown` before the first check and while the breaker is open.
- Both are omitted when Caddy isn't configured.
- `status` is `degraded` when the Caddy breaker is open or the anchor is
  missing.
- The unauthenticated response is unchanged.

### Audit

- The report gains a `caddy` section, present when Caddy is configured, with
  four lists.
  - `marked_unmapped` holds `docktornet--` routes in the anchor with no
    mapping and no pending removal.
  - `pending_removal`
  - `mapped_missing` holds mapping rows whose route is absent.
  - `marked_outside_anchor` holds routes carrying a mapped service's marker
    that sit outside the anchor, which the apply refuses to duplicate.
- A missing anchor is a top-level error in the report.
- `--only caddy` limits the audit to this section.
- NPM's `mapped_unmarked` and `unmarked_unreachable` have no Caddy equivalent.
  The marker is the address, and there is no hostname adoption.

## Testing decisions

A good test drives docktornet through a public entry point and checks what it
did to the world around it. That means the HTTP calls a fake Caddy received,
the rows in the database, and the `/healthz` and audit output. It doesn't
assert on intermediate structs, so a refactor that keeps behaviour keeps the
tests green.

### Seams

1. The main seam is the `Reconciler` against a mockito Caddy admin API. It
   mirrors the existing NPM and Cloudflare reconcile tests, which build a
   reconciler on mock servers, push snapshots, move past the grace period, and
   run the sweep or full resync. The NPM grace, restore, sweep, breaker,
   forward-host and in-sync tests are the prior art. Cases:
   - create, no-op and patch;
   - forwarding to each machine's own forward host;
   - `scheme=https` rendering;
   - host clash refused, the earlier owner kept, and the refused route retried
     after the clash clears;
   - a clash with a pending removal;
   - one service routing the same hostname through NPM and Caddy with no
     clash;
   - marker adoption after the mapping is lost;
   - removal within grace kept and past grace deleted;
   - restore when the service returns;
   - a dropped index deleted in the same apply, and parked past grace when
     the delete fails;
   - a host moved between two indices of one service in one edit, with no
     clash;
   - hosts differing only in case counted as a clash;
   - a marked route outside the anchor left alone, logged, and never POSTed
     again, with the breaker staying closed;
   - a 400 or load-failure 500 from Caddy failing that route without opening
     the breaker;
   - coalesced snapshots keeping a Caddy capture, shown by the route's DELETE
     after grace;
   - a failed delete retried by the sweep;
   - the drift check recreating a deleted route and reverting an edited one;
   - the drift check not applying hosts whose claim was refused;
   - a matching route with Caddy's key order counted as in sync, with no PATCH;
   - the drift check leaving unmapped `docktornet--` routes alone;
   - a missing anchor reported and never created;
   - the breaker opening and skipping Caddy while NPM and Cloudflare still run;
   - an NPM outage not stopping Caddy;
   - dry-run making no Caddy writes;
   - `caddy.*` labels with Caddy unconfigured being skipped with a warning.
2. The label parser, with the `npm.*` parser tests as prior art. It covers
   required and optional fields, comma-split hosts, lowercasing, the `scheme`
   default, malformed groups dropped with a warning, a host repeated across a
   container's groups, and a `caddy.*`-only container having intent.
3. The marker builder, a public function in core beside `sanitize_service_id`,
   with the Gatus filename tests as prior art. It covers the format, two
   service IDs that slug the same getting distinct markers, and machine IDs
   containing `#`, `?`, `%` or `:` producing a valid path segment.
4. The audit CLI and `/healthz` tests, with the existing ones as prior art.
   They cover the new section and fields, their absence when Caddy is
   unconfigured, and `degraded` on an open breaker or a missing anchor.
5. The live dev stack. `deploy/dev.compose.yaml` gains a pinned Caddy image
   and a dev bootstrap config, which puts the anchor first with
   `"routes": []`, turns automatic HTTPS off, and listens for admin requests on
   all interfaces while publishing the admin port nowhere. The docktornet
   server gets `DOCKTORNET_CADDY_ADMIN_URL`. AGENTS.md's live-test steps gain a
   `caddy.*` label on the test container and reads of the anchor's routes and
   the proxied response. This is the only seam that proves real Caddy accepts
   the rendered JSON, since Caddy decodes strictly, and that the `/id/...`
   paths resolve.

## Out of scope

- Running docktornet with NPM unconfigured, which a Caddy-only deployment
  needs. That's a separate issue if wanted.
- More than one Caddy instance, or picking a Caddy per machine.
- Docktornet creating or repairing the anchor route, the Caddy server block or
  the Caddy process.
- TLS on Caddy itself. That covers certificates, ACME, a direct-to-Caddy 443
  path and any `tls` label.
- Upstream certificate verification, `tls_server_name` and SNI labels.
- Hostname adoption of hand-written Caddy routes.
- Managing Cloudflare tunnel ingress rules. The operator adds the catch-all
  rule once.
- Removing the services of a machine that stops reporting. No provider does
  this today, and this design doesn't add it.
- A Unix-socket admin API transport.
- Moving any deployment's existing services from NPM to Caddy. That's the
  operator's runbook, done after release.
- Pruning a dropped `npm.N` group. NPM doesn't do it today, which is a bug of
  its own, tracked separately.

## Further notes

### Intended deployment

Caddy and the docktornet server run on the same machine and share a private
container network. Caddy's admin API is reachable only on that network.

A Cloudflare tunnel on that machine gets one catch-all `*.<zone>` public
hostname pointing at Caddy's HTTP port. Tunnel ingress rules are matched in
order, so the catch-all goes after every existing rule. Names that already
route through NPM keep their own DNS records pointing at NPM, so the tunnel
never sees their traffic.

A service moving to Caddy swaps its `npm.*` labels for `caddy.*` and points
`cf.dns.target` at the tunnel's `cfargotunnel.com` hostname, proxied. One apply
updates the DNS record and creates the route, so the switch has no gap. The old
NPM proxy host stays in NPM, because NPM doesn't prune a dropped group, and the
operator deletes it by hand until that bug is fixed.

### Client IPs

Behind a tunnel, the bootstrap config should set `trusted_proxies` and
`client_ip_headers: ["CF-Connecting-IP"]`, or apps will see the tunnel's
address. The deployment guide covers it.

### Cross-machine routes

A `caddy.*` label on a service on another machine is allowed. Its route then
depends on the network path between the two machines staying up.

### Glossary

`GLOSSARY.md` gains *Caddy route* and *anchor route*, and *managed resource*
and *ownership marker* widen to cover Caddy routes. These land with the first
slice, which introduces the terms.

### Stale comments

The doc comments that call the pending sweeps a backstop for offline machines
are wrong today, because an offline machine never produces pending rows.
Correct them while touching that code.
