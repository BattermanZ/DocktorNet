use std::path::Path;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Sqlite};
use tracing::info;

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
    info!(?db_path, "database ready");
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
}
