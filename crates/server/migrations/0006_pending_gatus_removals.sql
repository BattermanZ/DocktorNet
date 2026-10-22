-- When a service whose Gatus file is kept through the grace period left its
-- snapshot.  The orphan sweep reads the grace period from here.  The file's
-- mtime cannot stand in for it: the file is rewritten only when its monitors
-- change, and raising the mtime to mark a departure would make Gatus reload.
-- left_at is server time.  No foreign key to services on purpose -- the service
-- row is gone by the time a row lands here.
CREATE TABLE IF NOT EXISTS pending_gatus_removals (
    service_id TEXT PRIMARY KEY,
    left_at    INTEGER NOT NULL
);
