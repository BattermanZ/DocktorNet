-- Each service's entries in the Gatus classification map, as JSON, from the
-- last time its Gatus file was written or verified.  No foreign key to
-- services on purpose: a departed service's file is kept through the grace
-- period, and its endpoints stay in the map until the file is gone.
CREATE TABLE IF NOT EXISTS gatus_classifications (
    service_id     TEXT PRIMARY KEY,
    endpoints_json TEXT NOT NULL
);
