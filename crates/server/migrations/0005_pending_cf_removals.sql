-- Durable record of Cloudflare DNS records awaiting deletion after their
-- service left a snapshot.  Mirrors pending_npm_removals: it lets a within-grace
-- removal be deferred and swept later, even though the cf_dns_records mapping is
-- cascade-deleted the moment the service row is removed.  Without this table the
-- record_id is lost once upsert_snapshot deletes the service, orphaning the
-- public DNS record forever.  No foreign key to services on purpose -- the
-- service row is gone by the time a row lands here.
CREATE TABLE IF NOT EXISTS pending_cf_removals (
    service_id TEXT PRIMARY KEY,
    record_id  TEXT NOT NULL,
    last_seen  INTEGER NOT NULL
);
