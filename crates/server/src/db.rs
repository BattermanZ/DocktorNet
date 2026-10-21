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
        let all = sqlx::migrate!("./migrations");
        let before = sqlx::migrate::Migrator {
            migrations: all
                .migrations
                .iter()
                .filter(|m| m.version < 7)
                .cloned()
                .collect::<Vec<_>>()
                .into(),
            ..sqlx::migrate::Migrator::DEFAULT
        };
        let opts = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let old = SqlitePoolOptions::new().connect_with(opts).await.unwrap();
        before.run(&old).await.unwrap();
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
}
