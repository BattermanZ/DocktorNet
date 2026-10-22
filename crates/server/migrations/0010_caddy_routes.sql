-- The Caddy ownership mapping.  A route's address in Caddy is its ownership
-- marker, derived from the service id and index, so no remote id is stored.
-- hosts_json holds the hosts the route has claimed, for the host clash check.
-- route_json holds the route as last rendered after a successful claim, the
-- copy the drift check puts back.  A refused claim changes neither.
CREATE TABLE IF NOT EXISTS caddy_routes (
    service_id  TEXT NOT NULL,
    caddy_index INTEGER NOT NULL,
    hosts_json  TEXT NOT NULL,
    route_json  TEXT NOT NULL,
    PRIMARY KEY (service_id, caddy_index),
    FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE CASCADE
);

-- Caddy routes awaiting deletion.  The hosts stay claimed until the route is
-- gone.  Like pending_npm_removals, no foreign key to services: the service row
-- may be gone by the time a row lands here.
CREATE TABLE IF NOT EXISTS pending_caddy_removals (
    service_id  TEXT NOT NULL,
    caddy_index INTEGER NOT NULL,
    hosts_json  TEXT NOT NULL,
    last_seen   INTEGER NOT NULL,
    PRIMARY KEY (service_id, caddy_index)
);
