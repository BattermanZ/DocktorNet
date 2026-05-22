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
                taken_at: 100,
                containers: vec![],
            },
        )
        .await
        .unwrap();

        let rows = list_services_for_machine(&p, "m1").await.unwrap();
        assert_eq!(rows.len(), 1);
    }
}
