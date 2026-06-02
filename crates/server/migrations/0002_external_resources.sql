CREATE TABLE IF NOT EXISTS npm_hosts (
    service_id TEXT NOT NULL,
    npm_index  INTEGER NOT NULL,
    npm_id     INTEGER NOT NULL,
    PRIMARY KEY (service_id, npm_index),
    FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS cf_dns_records (
    service_id TEXT PRIMARY KEY,
    record_id  TEXT NOT NULL,
    FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE CASCADE
);
