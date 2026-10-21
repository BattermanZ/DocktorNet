use std::path::Path;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Sqlite};

pub type Db = Pool<Sqlite>;

pub async fn connect(db_path: &Path) -> anyhow::Result<Db> {
    if let Some(parent) = db_path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(opts)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    Ok(pool)
}

/// Open an existing database read-only, without creating it or running
/// migrations, so a command can read it next to a live server.
pub async fn connect_read_only(db_path: &Path) -> anyhow::Result<Db> {
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .read_only(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await?;
    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn connect_creates_db_and_runs_migrations() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let pool = connect(&path).await.unwrap();

        // Migration ran iff the core tables exist.
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM services")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.0, 0);

        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM npm_hosts")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.0, 0);

        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM cf_dns_records")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.0, 0);
    }

    #[tokio::test]
    async fn read_only_connection_refuses_writes_and_a_missing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        connect(&path).await.unwrap().close().await;

        let pool = connect_read_only(&path).await.unwrap();
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM machines")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.0, 0);
        sqlx::query(
            "INSERT INTO machines (machine_id, forward_host, last_seen) VALUES ('m1', 'h', 1)",
        )
        .execute(&pool)
        .await
        .unwrap_err();

        assert!(
            connect_read_only(&dir.path().join("missing.db"))
                .await
                .is_err()
        );
        assert!(!dir.path().join("missing.db").exists());
    }

    #[tokio::test]
    async fn existing_machines_read_back_with_no_agent_version_after_upgrade() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // A database as the previous release left it: every migration before
        // agent versions, and a machine row.
        let old = migrated_before(&path, 7).await;
        sqlx::query(
            "INSERT INTO machines (machine_id, forward_host, last_seen) VALUES ('m1', '10.0.0.1', 100)",
        )
        .execute(&old)
        .await
        .unwrap();
        old.close().await;

        let pool = connect(&path).await.unwrap();
        let machines = crate::storage::list_machines(&pool).await.unwrap();
        assert_eq!(machines.len(), 1);
        assert_eq!(machines[0].machine_id, "m1");
        assert_eq!(machines[0].agent_version, None);
    }

    /// A database at `path` with every migration before `version` applied.
    async fn migrated_before(path: &Path, version: i64) -> Db {
        let all = sqlx::migrate!("./migrations");
        let before = sqlx::migrate::Migrator {
            migrations: all
                .migrations
                .iter()
                .filter(|m| m.version < version)
                .cloned()
                .collect::<Vec<_>>()
                .into(),
            ..sqlx::migrate::Migrator::DEFAULT
        };
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        let old = SqlitePoolOptions::new().connect_with(opts).await.unwrap();
        before.run(&old).await.unwrap();
        old
    }

    #[tokio::test]
    async fn upgrade_restamps_agent_clock_times_with_the_server_clock() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Rows as the previous release left them: last_seen from an agent
        // whose clock runs an hour behind the server's.
        let old = migrated_before(&path, 9).await;
        let agent_time = now - 3600;
        for statement in [
            "INSERT INTO machines (machine_id, forward_host, last_seen) VALUES ('m1', '10.0.0.1', ?)",
            "INSERT INTO services (id, machine_id, container, labels_json, last_seen) \
             VALUES ('m1/app', 'm1', 'app', '{\"docktornet.gatus.0.url\":\"https://x\",\"docktornet.gatus.0.interval\":\"30s\"}', ?)",
            "INSERT INTO pending_npm_removals (service_id, npm_index, npm_id, last_seen) VALUES ('m1/old', 0, 50, ?)",
            "INSERT INTO pending_cf_removals (service_id, record_id, last_seen) VALUES ('m1/old', 'rec-1', ?)",
            // Already in server time before the upgrade.
            "INSERT INTO pending_gatus_removals (service_id, left_at) VALUES ('m1/gone', ?)",
        ] {
            sqlx::query(statement)
                .bind(agent_time)
                .execute(&old)
                .await
                .unwrap();
        }
        old.close().await;

        let pool = connect(&path).await.unwrap();
        let upgraded = |last_seen: i64| (now..=now + 5).contains(&last_seen);

        // Pending removals restart their grace period from the upgrade.
        let (_, _, _, npm_last_seen) = crate::storage::list_pending_npm_removals(&pool)
            .await
            .unwrap()[0];
        assert!(
            upgraded(npm_last_seen),
            "npm pending removal at {npm_last_seen}"
        );
        let (_, _, cf_last_seen) = crate::storage::list_pending_cf_removals(&pool)
            .await
            .unwrap()
            .remove(0);
        assert!(
            upgraded(cf_last_seen),
            "cf pending removal at {cf_last_seen}"
        );
        // Gatus departures and the machine's last contact are left alone.
        assert_eq!(
            crate::storage::list_pending_gatus_removals(&pool)
                .await
                .unwrap(),
            vec![("m1/gone".to_string(), agent_time)]
        );
        assert_eq!(
            crate::storage::list_machines(&pool).await.unwrap()[0].last_seen,
            agent_time
        );

        // The service leaves in the first push after the upgrade: it gets its
        // grace period rather than being judged an hour gone.
        let reconciler = crate::reconcile::Reconciler::new(
            pool.clone(),
            crate::reconcile::ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
        );
        reconciler
            .reconcile_snapshot(&docktornet_core::Snapshot {
                machine_id: docktornet_core::MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: now as u64,
                agent_version: None,
                containers: vec![],
            })
            .await
            .unwrap();
        let departures = crate::storage::list_pending_gatus_removals(&pool)
            .await
            .unwrap();
        let app_left_at = departures
            .iter()
            .find(|(service_id, _)| service_id == "m1/app")
            .map(|&(_, left_at)| left_at)
            .expect("the departure must be kept through its grace period");
        assert!(upgraded(app_left_at), "service last seen at {app_left_at}");
    }
}
