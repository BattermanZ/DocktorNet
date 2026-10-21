# Docktornet

Docktornet turns Docker labels on containers into NPM proxy hosts, Caddy routes, Cloudflare DNS records and Gatus monitors, and removes them when the containers go away.

## Language

### Services

**Service**:
One labelled container on one machine, the unit docktornet reconciles.
_Avoid_: app, stack, container (when the labels are meant)

**Service ID**:
The `machine/container` string that names a service everywhere docktornet records it.
_Avoid_: container ID, service name

**Grace period**:
How long a service may be missing from its machine's snapshots before docktornet removes its managed resources. It stops a container restart from looking like a removal.
_Avoid_: timeout, debounce

**Pending removal**:
A managed resource docktornet has decided to delete, kept until the deletion succeeds. One whose service has left also waits out the grace period first; an NPM proxy host or Caddy route whose labels were dropped from a live service waits for none.
_Avoid_: orphan (that is a resource no mapping points to)

### Ownership

**Managed resource**:
An NPM proxy host, Caddy route or Cloudflare DNS record that docktornet owns on behalf of one service.
_Avoid_: docktornet resource, our host

**Ownership mapping**:
Docktornet's database record linking a service to its managed resources. It is the working copy of ownership.
_Avoid_: ownership table, DB ownership

**Ownership marker**:
A tag written on the managed resource itself, naming the service ID that owns it. It is the durable proof of ownership and outlives the database. A Caddy route's marker is its `@id`, which identifies the service without spelling out its service ID.
_Avoid_: tag, label (those mean Docker labels here), metadata

**Adoption**:
Taking over a resource that already exists in NPM, Caddy or Cloudflare instead of creating a new one. Adoption always writes an ownership marker.
_Avoid_: import, claim

**Marker adoption**:
Adoption of a resource whose ownership marker names the service. Always allowed.

**Hostname adoption**:
Adoption of an unmarked resource because its hostname matches a service's labels. Allowed only when `DOCKTORNET_ADOPT_EXISTING` is on, and never for a resource whose marker names another service.
_Avoid_: legacy adoption

### Caddy

**Caddy route**:
A route docktornet writes into the operator's Caddy for one `caddy.<index>` label group of a service, sending the group's hostnames to the service's machine.
_Avoid_: Caddy host, site, proxy host (that is NPM's)

**Anchor route**:
The one route in the operator's own Caddy config whose sub-routes docktornet owns. Every Caddy route sits inside it, ahead of the operator's hand-written routes.
_Avoid_: docktornet server block, parent route

### Monitoring

**Classification map**:
The `docktornet-classification.json` file the server writes next to the Gatus service files. For each Gatus endpoint it gives the severity, area, host and alert thresholds an alert service needs. Its `host` is the machine ID of the reporting agent, not an NPM host.
_Avoid_: alert map, label map

**Severity**:
How urgent a Gatus endpoint's alerts are: `critical`, `warning` or `info`.
_Avoid_: priority, level

**Area**:
The part of the estate a Gatus endpoint watches, such as `backups` or `network`.
_Avoid_: category, component
