---
status: accepted
---

# Ownership markers live on the resource, in NPM `meta`, the Cloudflare `comment` and the Caddy route `@id`

The ownership mapping in SQLite is the only record of which NPM proxy hosts and Cloudflare DNS records docktornet owns, so losing the database loses ownership (#5). Each managed resource now also carries an ownership marker naming its owner. That marker is the durable proof of ownership: a marked resource is adopted for the service it names without `DOCKTORNET_ADOPT_EXISTING`, a missing or wrong marker counts as drift, and the database stays the working copy.

Amended on 2026-10-21 to add the Caddy route marker, for the [Caddy provider](../superpowers/specs/2026-10-21-caddy-provider-design.md).

The format is fixed because markers outlive any one version of docktornet:

- **NPM proxy host:** the key `docktornet` in `meta`, holding `{"service_id": "<machine>/<container>", "npm_index": <n>}`. The key is lowercase because NPM's UI converts camelCase keys. NPM replaces `meta` wholesale on update, so docktornet reads it, merges its own key and writes it back, and leaves other keys alone.
- **Cloudflare DNS record:** `comment` set to `docktornet:<machine>/<container>`. Lookups filter with `comment.exact` and then compare case-sensitively, because Cloudflare's filters ignore case. If the marker would go over the plan's comment limit (100 characters on Free), docktornet writes no marker and logs a warning.
- **Caddy route:** the route's `@id`, `docktornet--<machine slug>__<container slug>--<hash>--<n>`. The slugs are `sanitize_service_id`'s, which turns every non-alphanumeric character of each half of the service ID into `_`. The hash is the 16-hex stable hash of the exact service ID that Gatus filenames use, and `<n>` is the route's index. The marker is also the route's address in Caddy's admin API, at `/id/<marker>`, so it never contains `/` or anything else that needs escaping in a URL path. The hash keeps two service IDs that slug the same apart. The marker can't be read back into a service ID, so adoption and the audit compute it from a known service and look it up.

## Considered options

- **Cloudflare tags:** not available on the Free plan. Comments work on every plan, so one code path covers all of them.
- **A dedicated NPM user owning every docktornet host (`owner_user_id`):** it marks the host as docktornet's, not which service and index own it, so it can't drive marker adoption.
- **A comment line in NPM `advanced_config`:** that field is pasted into the live nginx server block, and bad content takes the host offline.
- **A `meta` object on the Caddy route, like NPM's:** Caddy decodes route JSON strictly and rejects unknown fields, so `@id` is the only place a route can carry a marker.
- **The raw service ID as the Caddy `@id`:** Caddy reads `/id/<id>` as one path segment after decoding it, so the `/` in a service ID breaks the lookup, and machine IDs may hold `#`, `?` or `%`.
- **Hashing long service IDs into the comment:** that makes markers unreadable to the audit and to people, for an edge case that real container names rarely hit.
