use docktornet_core::{Snapshot, service_id};
use sqlx::{Pool, Sqlite};

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
        r#"INSERT INTO machines (machine_id, forward_host, last_seen)
           VALUES (?, ?, ?)
           ON CONFLICT(machine_id) DO UPDATE SET
             forward_host = excluded.forward_host,
             last_seen    = excluded.last_seen"#,
    )
    .bind(machine)
    .bind(&snap.forward_host)
    .bind(now)
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

    // delete any services for this machine that were not in the snapshot
    if seen_ids.is_empty() {
        sqlx::query("DELETE FROM services WHERE machine_id = ?")
            .bind(machine)
            .execute(&mut *tx)
            .await?;
    } else {
        // Build the IN clause manually because sqlx does not expand Vec into IN args.
        let placeholders = std::iter::repeat_n("?", seen_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql =
            format!("DELETE FROM services WHERE machine_id = ? AND id NOT IN ({placeholders})");
        let mut q = sqlx::query(&sql).bind(machine);
        for id in &seen_ids {
            q = q.bind(id);
        }
        q.execute(&mut *tx).await?;
    }

    tx.commit().await
}

pub async fn list_machines(pool: &Pool<Sqlite>) -> sqlx::Result<Vec<(String, String, i64)>> {
    sqlx::query_as(
        "SELECT machine_id, forward_host, last_seen FROM machines ORDER BY machine_id",
    )
    .fetch_all(pool)
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

pub async fn removed_service_ids(
    pool: &Pool<Sqlite>,
    machine: &str,
    current_ids: &[String],
) -> sqlx::Result<Vec<String>> {
    if current_ids.is_empty() {
        return sqlx::query_scalar("SELECT id FROM services WHERE machine_id = ? ORDER BY id")
            .bind(machine)
            .fetch_all(pool)
            .await;
    }

    let placeholders = std::iter::repeat_n("?", current_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT id FROM services WHERE machine_id = ? AND id NOT IN ({placeholders}) ORDER BY id"
    );
    let mut q = sqlx::query_scalar(&sql).bind(machine);
    for id in current_ids {
        q = q.bind(id);
    }
    q.fetch_all(pool).await
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
    async fn list_machines_returns_every_machine() {
        let p = pool().await;
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 100,
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
                containers: vec![],
            },
        )
        .await
        .unwrap();

        let machines = list_machines(&p).await.unwrap();
        assert_eq!(machines.len(), 2);
        assert_eq!(machines[0].0, "m1");
        assert_eq!(machines[0].1, "10.0.0.1");
        assert_eq!(machines[0].2, 100);
        assert_eq!(machines[1].0, "m2");
        assert_eq!(machines[1].1, "10.0.0.2");
        assert_eq!(machines[1].2, 200);
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
    async fn tracks_cloudflare_record_ids() {
        let p = pool().await;
        upsert_snapshot(
            &p,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.10".into(),
                taken_at: 100,
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
}
