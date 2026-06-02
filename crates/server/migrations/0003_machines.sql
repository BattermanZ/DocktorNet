CREATE TABLE IF NOT EXISTS machines (
    machine_id   TEXT PRIMARY KEY,
    forward_host TEXT NOT NULL,
    last_seen    INTEGER NOT NULL
);
