use std::collections::HashSet;

use docktornet_core::{Snapshot, service_id};
use sqlx::{Pool, Sqlite, SqliteExecutor};

/// Maximum number of bind parameters to place in a single statement. SQLite's
/// compile-time `SQLITE_MAX_VARIABLE_NUMBER` defaults to 32766; staying well
/// under it lets id-list queries chunk safely no matter how many containers a
/// machine reports.
const MAX_BIND_PARAMS: usize = 20_000;

pub async fn upsert_snapshot(pool: &Pool<Sqlite>, snap: &Snapshot) -> sqlx::Result<()> {
    let now = snap.taken_at as i64;
    let machine = snap.machine_id.as_str();
    let seen_ids: Vec<String> = snap
        .containers
        .iter()
        .map(|c| service_id(&snap.machine_id, &c.name))
        .collect();

    let mut tx = pool.begin().await?;

    // upsert the machine row so its forward host is available to the DB-only
    // resync path; kept in the same transaction as the services it owns.
    sqlx::query(
        r#"INSERT INTO machines (machine_id, forward_host, last_seen, agent_version)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(machine_id) DO UPDATE SET
             forward_host  = excluded.forward_host,
             last_seen     = excluded.last_seen,
             agent_version = excluded.agent_version"#,
    )
    .bind(machine)
    .bind(&snap.forward_host)
    .bind(now)
    .bind(&snap.agent_version)
    .execute(&mut *tx)
    .await?;

    // upsert each container
    for c in &snap.containers {
        let id = service_id(&snap.machine_id, &c.name);
        let labels_json = serde_json::to_string(&c.labels).unwrap_or_else(|_| "{}".into());
        sqlx::query(
            r#"INSERT INTO services (id, machine_id, container, labels_json, last_seen)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 labels_json = excluded.labels_json,
                 last_seen   = excluded.last_seen"#,
        )
        .bind(&id)
        .bind(machine)
        .bind(&c.name)
        .bind(&labels_json)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    }

    // Delete any services for this machine that were not in the snapshot.
    // Rather than build a `NOT IN (...)` clause with one bind per seen id —
    // which overflows SQLITE_MAX_VARIABLE_NUMBER on machines with very many
    // containers — fetch the machine's current ids and delete the stale ones
    // individually. The number of removed containers per snapshot is small.
    let seen: HashSet<&str> = seen_ids.iter().map(String::as_str).collect();
    let existing: Vec<String> = sqlx::query_scalar("SELECT id FROM services WHERE machine_id = ?")
        .bind(machine)
        .fetch_all(&mut *tx)
        .await?;
    for id in &existing {
        if !seen.contains(id.as_str()) {
            sqlx::query("DELETE FROM services WHERE id = ?")
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
    }

    tx.commit().await
}

/// A machine as its latest snapshot left it.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct Machine {
    pub machine_id: String,
    pub forward_host: String,
    pub last_seen: i64,
    pub agent_version: Option<String>,
}

pub async fn list_machines(db: impl SqliteExecutor<'_>) -> sqlx::Result<Vec<Machine>> {
    sqlx::query_as(
        "SELECT machine_id, forward_host, last_seen, agent_version FROM machines ORDER BY machine_id",
    )
    .fetch_all(db)
    .await
}

pub async fn list_services_for_machine(
    pool: &Pool<Sqlite>,
    machine: &str,
) -> sqlx::Result<Vec<(String, String)>> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT id, labels_json FROM services WHERE machine_id = ? ORDER BY id")
            .bind(machine)
            .fetch_all(pool)
            .await?;
    Ok(rows)
}

pub async fn list_all_services(pool: &Pool<Sqlite>) -> sqlx::Result<Vec<(String, String, String)>> {
    sqlx::query_as(
        "SELECT s.id, s.labels_json, m.forward_host \
         FROM services s JOIN machines m ON s.machine_id = m.machine_id \
         ORDER BY s.id",
    )
    .fetch_all(pool)
    .await
}

pub async fn get_services_last_seen(
    pool: &Pool<Sqlite>,
    service_ids: &[String],
) -> sqlx::Result<Vec<(String, i64)>> {
    if service_ids.is_empty() {
        return Ok(vec![]);
    }
    // Chunk the id list so the bind count never exceeds SQLite's variable
    // limit, then re-sort to restore the overall `ORDER BY id`.
    let mut out: Vec<(String, i64)> = Vec::new();
    for chunk in service_ids.chunks(MAX_BIND_PARAMS) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("SELECT id, last_seen FROM services WHERE id IN ({placeholders})");
        let mut q = sqlx::query_as::<_, (String, i64)>(&sql);
        for id in chunk {
            q = q.bind(id.as_str());
        }
        out.extend(q.fetch_all(pool).await?);
    }
    out.sort();
    Ok(out)
}

pub async fn removed_service_ids(
    pool: &Pool<Sqlite>,
    machine: &str,
    current_ids: &[String],
) -> sqlx::Result<Vec<String>> {
    // Fetch every id for the machine and subtract the current set in memory.
    // A `NOT IN (...)` clause would bind one parameter per current id and
    // overflow SQLITE_MAX_VARIABLE_NUMBER when a machine has very many
    // containers. The result stays ordered because the query is `ORDER BY id`.
    let all: Vec<String> =
        sqlx::query_scalar("SELECT id FROM services WHERE machine_id = ? ORDER BY id")
            .bind(machine)
            .fetch_all(pool)
            .await?;
    let current: HashSet<&str> = current_ids.iter().map(String::as_str).collect();
    Ok(all
        .into_iter()
        .filter(|id| !current.contains(id.as_str()))
        .collect())
}

pub async fn get_npm_host(
    pool: &Pool<Sqlite>,
    service_id: &str,
    npm_index: i64,
) -> sqlx::Result<Option<i64>> {
    sqlx::query_as("SELECT npm_id FROM npm_hosts WHERE service_id = ? AND npm_index = ?")
        .bind(service_id)
        .bind(npm_index)
        .fetch_optional(pool)
        .await
        .map(|row| row.map(|(npm_id,)| npm_id))
}

pub async fn set_npm_host(
    pool: &Pool<Sqlite>,
    service_id: &str,
    npm_index: i64,
    npm_id: i64,
) -> sqlx::Result<()> {
    sqlx::query(
        r#"INSERT INTO npm_hosts (service_id, npm_index, npm_id)
           VALUES (?, ?, ?)
           ON CONFLICT(service_id, npm_index) DO UPDATE SET
             npm_id = excluded.npm_id"#,
    )
    .bind(service_id)
    .bind(npm_index)
    .bind(npm_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete_npm_host(
    pool: &Pool<Sqlite>,
    service_id: &str,
    npm_index: i64,
) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM npm_hosts WHERE service_id = ? AND npm_index = ?")
        .bind(service_id)
        .bind(npm_index)
        .execute(pool)
        .await?;
    Ok(())
}

/// Return every `(service_id, npm_index, npm_id)` mapping for the given
/// services, ordered by `(service_id, npm_index)`.  Used to capture NPM host
/// ids for services about to leave a snapshot, before `upsert_snapshot`'s
/// cascade deletes the `npm_hosts` rows.
pub async fn get_npm_hosts_for_services(
    pool: &Pool<Sqlite>,
    service_ids: &[String],
) -> sqlx::Result<Vec<(String, i64, i64)>> {
    if service_ids.is_empty() {
        return Ok(Vec::new());
    }
    // Chunk to stay under SQLite's bind-parameter limit, then re-sort to
    // restore the overall `ORDER BY service_id, npm_index`.
    let mut out: Vec<(String, i64, i64)> = Vec::new();
    for chunk in service_ids.chunks(MAX_BIND_PARAMS) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT service_id, npm_index, npm_id FROM npm_hosts \
             WHERE service_id IN ({placeholders})"
        );
        let mut q = sqlx::query_as::<_, (String, i64, i64)>(&sql);
        for id in chunk {
            q = q.bind(id);
        }
        out.extend(q.fetch_all(pool).await?);
    }
    out.sort();
    Ok(out)
}

/// Every `(service_id, npm_index, npm_id)` ownership mapping, ordered by
/// `(service_id, npm_index)`.
pub async fn list_npm_hosts(db: impl SqliteExecutor<'_>) -> sqlx::Result<Vec<(String, i64, i64)>> {
    sqlx::query_as(
        "SELECT service_id, npm_index, npm_id FROM npm_hosts ORDER BY service_id, npm_index",
    )
    .fetch_all(db)
    .await
}

/// Record an NPM proxy host whose service has left a snapshot but whose grace
/// period has not yet elapsed, so it can be pruned later by the pending-removal sweep.
pub async fn add_pending_npm_removal(
    pool: &Pool<Sqlite>,
    service_id: &str,
    npm_index: i64,
    npm_id: i64,
    last_seen: i64,
) -> sqlx::Result<()> {
    sqlx::query(
        r#"INSERT INTO pending_npm_removals (service_id, npm_index, npm_id, last_seen)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(service_id, npm_index) DO UPDATE SET
             npm_id    = excluded.npm_id,
             last_seen = excluded.last_seen"#,
    )
    .bind(service_id)
    .bind(npm_index)
    .bind(npm_id)
    .bind(last_seen)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_pending_npm_removals(
    db: impl SqliteExecutor<'_>,
) -> sqlx::Result<Vec<(String, i64, i64, i64)>> {
    sqlx::query_as(
        "SELECT service_id, npm_index, npm_id, last_seen FROM pending_npm_removals \
         ORDER BY service_id, npm_index",
    )
    .fetch_all(db)
    .await
}

pub async fn delete_pending_npm_removal(
    pool: &Pool<Sqlite>,
    service_id: &str,
    npm_index: i64,
) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM pending_npm_removals WHERE service_id = ? AND npm_index = ?")
        .bind(service_id)
        .bind(npm_index)
        .execute(pool)
        .await?;
    Ok(())
}

/// Return the `(npm_index, npm_id)` pairs recorded in `pending_npm_removals`
/// for a single service.  Used when a service reappears within the grace
/// period: the pending row is the only surviving record of the NPM host id
/// (the `npm_hosts` mapping was cascade-deleted when the service left), so it
/// is restored into `npm_hosts` before reconcile runs — otherwise reconcile
/// would create a duplicate proxy host and orphan the original.
pub async fn get_pending_npm_removals_for_service(
    pool: &Pool<Sqlite>,
    service_id: &str,
) -> sqlx::Result<Vec<(i64, i64)>> {
    sqlx::query_as(
        "SELECT npm_index, npm_id FROM pending_npm_removals \
         WHERE service_id = ? ORDER BY npm_index",
    )
    .bind(service_id)
    .fetch_all(pool)
    .await
}

/// Drop every pending removal for a service.  Called when a service reappears,
/// so a host that is live again is never pruned.
pub async fn clear_pending_npm_removals_for_service(
    pool: &Pool<Sqlite>,
    service_id: &str,
) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM pending_npm_removals WHERE service_id = ?")
        .bind(service_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Return every `(service_id, record_id)` Cloudflare mapping for the given
/// services, ordered by `service_id`.  Used to capture DNS record ids for
/// services about to leave a snapshot, before `upsert_snapshot`'s cascade
/// deletes the `cf_dns_records` rows.
pub async fn get_cf_records_for_services(
    pool: &Pool<Sqlite>,
    service_ids: &[String],
) -> sqlx::Result<Vec<(String, String)>> {
    if service_ids.is_empty() {
        return Ok(Vec::new());
    }
    // Chunk to stay under SQLite's bind-parameter limit, then re-sort to
    // restore the overall `ORDER BY service_id`.
    let mut out: Vec<(String, String)> = Vec::new();
    for chunk in service_ids.chunks(MAX_BIND_PARAMS) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT service_id, record_id FROM cf_dns_records \
             WHERE service_id IN ({placeholders})"
        );
        let mut q = sqlx::query_as::<_, (String, String)>(&sql);
        for id in chunk {
            q = q.bind(id);
        }
        out.extend(q.fetch_all(pool).await?);
    }
    out.sort();
    Ok(out)
}

/// Record a Cloudflare DNS record whose service has left a snapshot but whose
/// grace period has not yet elapsed (or whose deletion failed), so it can be
/// deleted later by the pending-removal sweep.
pub async fn add_pending_cf_removal(
    pool: &Pool<Sqlite>,
    service_id: &str,
    record_id: &str,
    last_seen: i64,
) -> sqlx::Result<()> {
    sqlx::query(
        r#"INSERT INTO pending_cf_removals (service_id, record_id, last_seen)
           VALUES (?, ?, ?)
           ON CONFLICT(service_id) DO UPDATE SET
             record_id = excluded.record_id,
             last_seen = excluded.last_seen"#,
    )
    .bind(service_id)
    .bind(record_id)
    .bind(last_seen)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_pending_cf_removals(
    db: impl SqliteExecutor<'_>,
) -> sqlx::Result<Vec<(String, String, i64)>> {
    sqlx::query_as(
        "SELECT service_id, record_id, last_seen FROM pending_cf_removals \
         ORDER BY service_id",
    )
    .fetch_all(db)
    .await
}

/// Return the Cloudflare `record_id` recorded in `pending_cf_removals` for a
/// single service, if any.  The counterpart to
/// `get_pending_npm_removals_for_service`: when a service reappears within the
/// grace period this restores the `cf_dns_records` mapping so reconcile updates
/// the existing DNS record instead of creating a duplicate.
pub async fn get_pending_cf_removal(
    pool: &Pool<Sqlite>,
    service_id: &str,
) -> sqlx::Result<Option<String>> {
    sqlx::query_as("SELECT record_id FROM pending_cf_removals WHERE service_id = ?")
        .bind(service_id)
        .fetch_optional(pool)
        .await
        .map(|row| row.map(|(record_id,)| record_id))
}

pub async fn delete_pending_cf_removal(pool: &Pool<Sqlite>, service_id: &str) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM pending_cf_removals WHERE service_id = ?")
        .bind(service_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Record when a service whose Gatus file is kept through the grace period left
/// its snapshot, in server time, so the pending-removal sweep knows when the
/// grace period ends.
pub async fn add_pending_gatus_removal(
    pool: &Pool<Sqlite>,
    service_id: &str,
    left_at: i64,
) -> sqlx::Result<()> {
    sqlx::query(
        r#"INSERT INTO pending_gatus_removals (service_id, left_at)
           VALUES (?, ?)
           ON CONFLICT(service_id) DO UPDATE SET left_at = excluded.left_at"#,
    )
    .bind(service_id)
    .bind(left_at)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_pending_gatus_removals(pool: &Pool<Sqlite>) -> sqlx::Result<Vec<(String, i64)>> {
    sqlx::query_as("SELECT service_id, left_at FROM pending_gatus_removals ORDER BY service_id")
        .fetch_all(pool)
        .await
}

pub async fn delete_pending_gatus_removal(
    pool: &Pool<Sqlite>,
    service_id: &str,
) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM pending_gatus_removals WHERE service_id = ?")
        .bind(service_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Store a service's classification map entries, replacing any earlier ones.
pub async fn set_gatus_classification(
    pool: &Pool<Sqlite>,
    service_id: &str,
    endpoints_json: &str,
) -> sqlx::Result<()> {
    sqlx::query(
        r#"INSERT INTO gatus_classifications (service_id, endpoints_json)
           VALUES (?, ?)
           ON CONFLICT(service_id) DO UPDATE SET endpoints_json = excluded.endpoints_json"#,
    )
    .bind(service_id)
    .bind(endpoints_json)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_gatus_classifications(
    pool: &Pool<Sqlite>,
) -> sqlx::Result<Vec<(String, String)>> {
    sqlx::query_as(
        "SELECT service_id, endpoints_json FROM gatus_classifications ORDER BY service_id",
    )
    .fetch_all(pool)
    .await
}

pub async fn delete_gatus_classification(
    pool: &Pool<Sqlite>,
    service_id: &str,
) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM gatus_classifications WHERE service_id = ?")
        .bind(service_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn get_cf_dns_record(
    pool: &Pool<Sqlite>,
    service_id: &str,
) -> sqlx::Result<Option<String>> {
    sqlx::query_as("SELECT record_id FROM cf_dns_records WHERE service_id = ?")
        .bind(service_id)
        .fetch_optional(pool)
        .await
        .map(|row| row.map(|(record_id,)| record_id))
}

/// Every `(service_id, record_id)` ownership mapping, ordered by `service_id`.
pub async fn list_cf_dns_records(
    db: impl SqliteExecutor<'_>,
) -> sqlx::Result<Vec<(String, String)>> {
    sqlx::query_as("SELECT service_id, record_id FROM cf_dns_records ORDER BY service_id")
        .fetch_all(db)
        .await
}

pub async fn set_cf_dns_record(
    pool: &Pool<Sqlite>,
    service_id: &str,
    record_id: &str,
) -> sqlx::Result<()> {
    sqlx::query(
        r#"INSERT INTO cf_dns_records (service_id, record_id)
           VALUES (?, ?)
           ON CONFLICT(service_id) DO UPDATE SET
             record_id = excluded.record_id"#,
    )
    .bind(service_id)
    .bind(record_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete_cf_dns_record(pool: &Pool<Sqlite>, service_id: &str) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM cf_dns_records WHERE service_id = ?")
        .bind(service_id)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use docktornet_core::{ContainerEntry, MachineId};
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn entry(name: &str, labels: &[(&str, &str)]) -> ContainerEntry {
        ContainerEntry {
            name: name.into(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>(),
        }
    }

    async fn pool() -> Pool<Sqlite> {
        let dir = Box::leak(Box::new(tempdir().unwrap()));
        db::connect(&dir.path().join("t.db")).await.unwrap()
    }

    #[tokio::test]
    async fn inserts_new_containers() {
        let p = pool().await;
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.10".into(),
                taken_at: 100,
                agent_version: None,
                containers: vec![entry("a", &[("docktornet.x", "1")])],
            },
        )
        .await
        .unwrap();

        let rows = list_services_for_machine(&p, "m1").await.unwrap();
        assert_eq!(
            rows,
            vec![("m1/a".into(), "{\"docktornet.x\":\"1\"}".into())]
        );
    }

    #[tokio::test]
    async fn updates_existing_when_labels_change() {
        let p = pool().await;
        let m = MachineId::new("m1");
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: m.clone(),
                forward_host: "10.0.0.10".into(),
                taken_at: 100,
                agent_version: None,
                containers: vec![entry("a", &[("docktornet.x", "1")])],
            },
        )
        .await
        .unwrap();
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: m,
                forward_host: "10.0.0.10".into(),
                taken_at: 200,
                agent_version: None,
                containers: vec![entry("a", &[("docktornet.x", "2")])],
            },
        )
        .await
        .unwrap();

        let rows = list_services_for_machine(&p, "m1").await.unwrap();
        assert_eq!(rows[0].1, "{\"docktornet.x\":\"2\"}");
    }

    #[tokio::test]
    async fn removes_containers_not_in_latest_snapshot() {
        let p = pool().await;
        let m = MachineId::new("m1");
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: m.clone(),
                forward_host: "10.0.0.10".into(),
                taken_at: 100,
                agent_version: None,
                containers: vec![
                    entry("a", &[("docktornet.x", "1")]),
                    entry("b", &[("docktornet.y", "2")]),
                ],
            },
        )
        .await
        .unwrap();
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: m,
                forward_host: "10.0.0.10".into(),
                taken_at: 200,
                agent_version: None,
                containers: vec![entry("a", &[("docktornet.x", "1")])],
            },
        )
        .await
        .unwrap();

        let rows = list_services_for_machine(&p, "m1").await.unwrap();
        assert_eq!(
            rows.iter().map(|r| r.0.clone()).collect::<Vec<_>>(),
            vec!["m1/a"]
        );
    }

    #[tokio::test]
    async fn empty_snapshot_clears_machine() {
        let p = pool().await;
        let m = MachineId::new("m1");
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: m.clone(),
                forward_host: "10.0.0.10".into(),
                taken_at: 100,
                agent_version: None,
                containers: vec![entry("a", &[("docktornet.x", "1")])],
            },
        )
        .await
        .unwrap();
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: m,
                forward_host: "10.0.0.10".into(),
                taken_at: 200,
                agent_version: None,
                containers: vec![],
            },
        )
        .await
        .unwrap();

        let rows = list_services_for_machine(&p, "m1").await.unwrap();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn does_not_touch_other_machines() {
        let p = pool().await;
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.10".into(),
                taken_at: 100,
                agent_version: None,
                containers: vec![entry("a", &[("docktornet.x", "1")])],
            },
        )
        .await
        .unwrap();
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m2"),
                forward_host: "10.0.0.10".into(),
                taken_at: 100,
                agent_version: None,
                containers: vec![],
            },
        )
        .await
        .unwrap();

        let rows = list_services_for_machine(&p, "m1").await.unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn returns_removed_services_for_machine() {
        let p = pool().await;
        let m = MachineId::new("m1");
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: m.clone(),
                forward_host: "10.0.0.10".into(),
                taken_at: 100,
                agent_version: None,
                containers: vec![entry(
                    "a",
                    &[
                        ("docktornet.gatus.0.url", "https://a"),
                        ("docktornet.gatus.0.interval", "30s"),
                    ],
                )],
            },
        )
        .await
        .unwrap();

        let removed = removed_service_ids(&p, "m1", &["m1/b".to_string()])
            .await
            .unwrap();
        assert_eq!(removed, vec!["m1/a"]);
    }

    #[tokio::test]
    async fn tracks_npm_host_ids() {
        let p = pool().await;
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.10".into(),
                taken_at: 100,
                agent_version: None,
                containers: vec![entry("app", &[])],
            },
        )
        .await
        .unwrap();
        set_npm_host(&p, "m1/app", 0, 58).await.unwrap();
        assert_eq!(get_npm_host(&p, "m1/app", 0).await.unwrap(), Some(58));
        delete_npm_host(&p, "m1/app", 0).await.unwrap();
        assert_eq!(get_npm_host(&p, "m1/app", 0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn lists_npm_hosts_for_a_set_of_services() {
        let p = pool().await;
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.10".into(),
                taken_at: 100,
                agent_version: None,
                containers: vec![entry("a", &[]), entry("b", &[])],
            },
        )
        .await
        .unwrap();
        set_npm_host(&p, "m1/a", 0, 10).await.unwrap();
        set_npm_host(&p, "m1/a", 1, 11).await.unwrap();
        set_npm_host(&p, "m1/b", 0, 20).await.unwrap();

        // Only the requested services are returned, ordered (service_id, index).
        let hosts = get_npm_hosts_for_services(&p, &["m1/a".to_string()])
            .await
            .unwrap();
        assert_eq!(
            hosts,
            vec![("m1/a".to_string(), 0, 10), ("m1/a".to_string(), 1, 11)]
        );
    }

    #[tokio::test]
    async fn get_npm_hosts_for_services_returns_empty_for_empty_input() {
        let p = pool().await;
        let hosts = get_npm_hosts_for_services(&p, &[]).await.unwrap();
        assert!(hosts.is_empty());
    }

    #[tokio::test]
    async fn pending_npm_removals_round_trip() {
        let p = pool().await;
        // No FK to services: pending rows must survive after the service is gone.
        add_pending_npm_removal(&p, "m1/a", 0, 10, 100)
            .await
            .unwrap();
        add_pending_npm_removal(&p, "m1/a", 1, 11, 100)
            .await
            .unwrap();
        add_pending_npm_removal(&p, "m1/b", 0, 20, 150)
            .await
            .unwrap();

        let pending = list_pending_npm_removals(&p).await.unwrap();
        assert_eq!(
            pending,
            vec![
                ("m1/a".to_string(), 0, 10, 100),
                ("m1/a".to_string(), 1, 11, 100),
                ("m1/b".to_string(), 0, 20, 150),
            ]
        );

        // Deleting a single (service, index) entry leaves the rest.
        delete_pending_npm_removal(&p, "m1/a", 0).await.unwrap();
        let pending = list_pending_npm_removals(&p).await.unwrap();
        assert_eq!(
            pending,
            vec![
                ("m1/a".to_string(), 1, 11, 100),
                ("m1/b".to_string(), 0, 20, 150),
            ]
        );

        // Clearing by service removes all of that service's pending entries.
        clear_pending_npm_removals_for_service(&p, "m1/a")
            .await
            .unwrap();
        let pending = list_pending_npm_removals(&p).await.unwrap();
        assert_eq!(pending, vec![("m1/b".to_string(), 0, 20, 150)]);
    }

    #[tokio::test]
    async fn add_pending_npm_removal_upserts_on_conflict() {
        let p = pool().await;
        add_pending_npm_removal(&p, "m1/a", 0, 10, 100)
            .await
            .unwrap();
        // Same (service, index) again updates npm_id/last_seen rather than erroring.
        add_pending_npm_removal(&p, "m1/a", 0, 99, 200)
            .await
            .unwrap();
        let pending = list_pending_npm_removals(&p).await.unwrap();
        assert_eq!(pending, vec![("m1/a".to_string(), 0, 99, 200)]);
    }

    #[tokio::test]
    async fn list_machines_returns_every_machine() {
        let p = pool().await;
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 100,
                agent_version: None,
                containers: vec![],
            },
        )
        .await
        .unwrap();
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m2"),
                forward_host: "10.0.0.2".into(),
                taken_at: 200,
                agent_version: None,
                containers: vec![],
            },
        )
        .await
        .unwrap();

        let machines = list_machines(&p).await.unwrap();
        assert_eq!(
            machines,
            vec![
                Machine {
                    machine_id: "m1".into(),
                    forward_host: "10.0.0.1".into(),
                    last_seen: 100,
                    agent_version: None,
                },
                Machine {
                    machine_id: "m2".into(),
                    forward_host: "10.0.0.2".into(),
                    last_seen: 200,
                    agent_version: None,
                },
            ]
        );
    }

    #[tokio::test]
    async fn a_machine_keeps_its_latest_agent_version() {
        let p = pool().await;
        let snap = |taken_at, agent_version: Option<&str>| Snapshot {
            machine_id: MachineId::new("m1"),
            forward_host: "10.0.0.1".into(),
            taken_at,
            containers: vec![],
            agent_version: agent_version.map(str::to_string),
        };
        upsert_snapshot(&p, &snap(100, Some("1.1.0")))
            .await
            .unwrap();
        upsert_snapshot(&p, &snap(200, Some("1.2.0")))
            .await
            .unwrap();
        assert_eq!(
            list_machines(&p).await.unwrap()[0].agent_version.as_deref(),
            Some("1.2.0")
        );

        // An agent rolled back to a release without version reporting.
        upsert_snapshot(&p, &snap(300, None)).await.unwrap();
        assert_eq!(list_machines(&p).await.unwrap()[0].agent_version, None);
    }

    #[tokio::test]
    async fn list_all_services_returns_every_machine() {
        let p = pool().await;
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.10".into(),
                taken_at: 1,
                agent_version: None,
                containers: vec![entry("a", &[("docktornet.x", "1")])],
            },
        )
        .await
        .unwrap();
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m2"),
                forward_host: "10.0.0.10".into(),
                taken_at: 1,
                agent_version: None,
                containers: vec![entry("b", &[("docktornet.y", "2")])],
            },
        )
        .await
        .unwrap();
        let all = list_all_services(&p).await.unwrap();
        let ids: Vec<String> = all.iter().map(|(id, _, _)| id.clone()).collect();
        assert_eq!(ids, vec!["m1/a", "m2/b"]);
        let hosts: Vec<String> = all.into_iter().map(|(_, _, fh)| fh).collect();
        assert_eq!(hosts, vec!["10.0.0.10", "10.0.0.10"]);
    }

    #[tokio::test]
    async fn get_services_last_seen_returns_timestamps() {
        let p = pool().await;
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.10".into(),
                taken_at: 42,
                agent_version: None,
                containers: vec![entry("app", &[])],
            },
        )
        .await
        .unwrap();

        let rows = get_services_last_seen(&p, &["m1/app".to_string()])
            .await
            .unwrap();
        assert_eq!(rows, vec![("m1/app".to_string(), 42_i64)]);
    }

    #[tokio::test]
    async fn get_services_last_seen_returns_empty_for_empty_input() {
        let p = pool().await;
        let rows = get_services_last_seen(&p, &[]).await.unwrap();
        assert!(rows.is_empty());
    }

    // A machine with very many containers produces an id list longer than
    // SQLITE_MAX_VARIABLE_NUMBER (32766).  The old IN/NOT IN builders bound one
    // parameter per id, so past that count SQLite rejects the statement with
    // "too many SQL variables" and every query for that machine fails.  These
    // tests exercise the id-list functions with an oversized input; they must
    // succeed rather than error.
    const OVER_LIMIT: usize = 40_000;

    #[tokio::test]
    async fn get_services_last_seen_handles_huge_id_list() {
        let p = pool().await;
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.10".into(),
                taken_at: 42,
                agent_version: None,
                containers: vec![entry("app", &[])],
            },
        )
        .await
        .unwrap();

        let mut ids: Vec<String> = (0..OVER_LIMIT).map(|i| format!("m1/c{i}")).collect();
        ids.push("m1/app".to_string());

        let rows = get_services_last_seen(&p, &ids).await.unwrap();
        assert_eq!(rows, vec![("m1/app".to_string(), 42_i64)]);
    }

    #[tokio::test]
    async fn removed_service_ids_handles_huge_current_list() {
        let p = pool().await;
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.10".into(),
                taken_at: 100,
                agent_version: None,
                containers: vec![entry("a", &[]), entry("b", &[])],
            },
        )
        .await
        .unwrap();

        // A large "still present" set that includes a but not b: b must be
        // reported removed without the query blowing the variable limit.
        let mut current: Vec<String> = (0..OVER_LIMIT).map(|i| format!("m1/x{i}")).collect();
        current.push("m1/a".to_string());

        let removed = removed_service_ids(&p, "m1", &current).await.unwrap();
        assert_eq!(removed, vec!["m1/b".to_string()]);
    }

    #[tokio::test]
    async fn tracks_cloudflare_record_ids() {
        let p = pool().await;
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.10".into(),
                taken_at: 100,
                agent_version: None,
                containers: vec![entry("app", &[])],
            },
        )
        .await
        .unwrap();
        set_cf_dns_record(&p, "m1/app", "abc").await.unwrap();
        assert_eq!(
            get_cf_dns_record(&p, "m1/app").await.unwrap(),
            Some("abc".to_string())
        );
        delete_cf_dns_record(&p, "m1/app").await.unwrap();
        assert_eq!(get_cf_dns_record(&p, "m1/app").await.unwrap(), None);
    }

    #[tokio::test]
    async fn lists_cf_records_for_a_set_of_services() {
        let p = pool().await;
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.10".into(),
                taken_at: 100,
                agent_version: None,
                containers: vec![entry("a", &[]), entry("b", &[])],
            },
        )
        .await
        .unwrap();
        set_cf_dns_record(&p, "m1/a", "rec-a").await.unwrap();
        set_cf_dns_record(&p, "m1/b", "rec-b").await.unwrap();

        // Only the requested services are returned.
        let records = get_cf_records_for_services(&p, &["m1/a".to_string()])
            .await
            .unwrap();
        assert_eq!(records, vec![("m1/a".to_string(), "rec-a".to_string())]);

        // Empty input short-circuits to an empty result.
        assert!(
            get_cf_records_for_services(&p, &[])
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn pending_cf_removals_round_trip() {
        let p = pool().await;
        // No FK to services: pending rows must survive after the service is gone.
        add_pending_cf_removal(&p, "m1/a", "rec-a", 100)
            .await
            .unwrap();
        add_pending_cf_removal(&p, "m1/b", "rec-b", 150)
            .await
            .unwrap();

        let pending = list_pending_cf_removals(&p).await.unwrap();
        assert_eq!(
            pending,
            vec![
                ("m1/a".to_string(), "rec-a".to_string(), 100),
                ("m1/b".to_string(), "rec-b".to_string(), 150),
            ]
        );

        // Re-adding the same service upserts record_id/last_seen rather than erroring.
        add_pending_cf_removal(&p, "m1/a", "rec-a2", 200)
            .await
            .unwrap();
        assert_eq!(
            list_pending_cf_removals(&p).await.unwrap(),
            vec![
                ("m1/a".to_string(), "rec-a2".to_string(), 200),
                ("m1/b".to_string(), "rec-b".to_string(), 150),
            ]
        );

        // Deleting by service removes that service's pending entry.
        delete_pending_cf_removal(&p, "m1/a").await.unwrap();
        assert_eq!(
            list_pending_cf_removals(&p).await.unwrap(),
            vec![("m1/b".to_string(), "rec-b".to_string(), 150)]
        );
    }

    #[tokio::test]
    async fn pending_gatus_removals_round_trip() {
        let p = pool().await;
        add_pending_gatus_removal(&p, "m1/b", 150).await.unwrap();
        add_pending_gatus_removal(&p, "m1/a", 100).await.unwrap();
        // Leaving again moves left_at rather than erroring.
        add_pending_gatus_removal(&p, "m1/a", 200).await.unwrap();
        assert_eq!(
            list_pending_gatus_removals(&p).await.unwrap(),
            vec![("m1/a".to_string(), 200), ("m1/b".to_string(), 150)]
        );

        delete_pending_gatus_removal(&p, "m1/a").await.unwrap();
        assert_eq!(
            list_pending_gatus_removals(&p).await.unwrap(),
            vec![("m1/b".to_string(), 150)]
        );
    }
}
