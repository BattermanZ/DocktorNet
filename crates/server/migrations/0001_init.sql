-- Initial schema for docktornet services
CREATE TABLE IF NOT EXISTS services (
    id          TEXT PRIMARY KEY,        -- "{machine_id}/{container_name}"
    machine_id  TEXT NOT NULL,
    container   TEXT NOT NULL,
    labels_json TEXT NOT NULL,
    last_seen   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_services_machine_id ON services(machine_id);
