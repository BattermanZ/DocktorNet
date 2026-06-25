-- Durable record of NPM proxy hosts awaiting pruning after their service left a
-- snapshot.  Mirrors the role the on-disk gatus file plays for monitors: it lets
-- a within-grace removal be deferred and swept later, even though the npm_hosts
-- mapping is cascade-deleted the moment the service row is removed.  No foreign
-- key to services on purpose -- the service row is gone by the time a row lands
-- here.
CREATE TABLE IF NOT EXISTS pending_npm_removals (
    service_id TEXT NOT NULL,
    npm_index  INTEGER NOT NULL,
    npm_id     INTEGER NOT NULL,
    last_seen  INTEGER NOT NULL,
    PRIMARY KEY (service_id, npm_index)
);
