//! `docktornet-server backup`, run as a real process.

use std::path::Path;
use std::process::{Command, Output};

use docktornet_server::db;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, SqliteConnection};
use tempfile::tempdir;

const BIN: &str = env!("CARGO_BIN_EXE_docktornet-server");

/// Run `backup` with only the database path in its environment.
fn backup(db_path: &Path) -> Output {
    // The binary loads a `.env` from its working directory or any parent.
    let cwd = tempdir().unwrap();
    Command::new(BIN)
        .arg("backup")
        .current_dir(cwd.path())
        .env_clear()
        .env("DOCKTORNET_DB_PATH", db_path)
        .output()
        .unwrap()
}

/// The names in `dir`, sorted.
fn entries(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn backup_writes_a_consistent_copy_of_a_live_database_to_stdout() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("docktornet.db");
    // Held open across the backup, like a running server, so the rows sit in
    // the WAL rather than the main file.
    let live = db::connect(&path).await.unwrap();
    sqlx::query(
        "INSERT INTO services (id, machine_id, container, labels_json, last_seen)
         VALUES ('m1/web', 'm1', 'web', '{}', 1)",
    )
    .execute(&live)
    .await
    .unwrap();
    sqlx::query("INSERT INTO npm_hosts (service_id, npm_index, npm_id) VALUES ('m1/web', 0, 42)")
        .execute(&live)
        .await
        .unwrap();
    sqlx::query("INSERT INTO cf_dns_records (service_id, record_id) VALUES ('m1/web', 'rec-1')")
        .execute(&live)
        .await
        .unwrap();
    let before = entries(dir.path());

    let output = backup(&path);

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(entries(dir.path()), before, "no staging file left behind");

    let out_dir = tempdir().unwrap();
    let copy = out_dir.path().join("x.db");
    std::fs::write(&copy, &output.stdout).unwrap();
    let mut conn = SqliteConnection::connect_with(
        &SqliteConnectOptions::new().filename(&copy).read_only(true),
    )
    .await
    .unwrap();
    let (check,): (String,) = sqlx::query_as("PRAGMA integrity_check")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(check, "ok");
    let npm: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT service_id, npm_index, npm_id FROM npm_hosts")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(npm, vec![("m1/web".to_string(), 0, 42)]);
    let cf: Vec<(String, String)> =
        sqlx::query_as("SELECT service_id, record_id FROM cf_dns_records")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(cf, vec![("m1/web".to_string(), "rec-1".to_string())]);
}

#[test]
fn backup_of_a_missing_database_fails_without_creating_anything() {
    let dir = tempdir().unwrap();

    let output = backup(&dir.path().join("docktornet.db"));

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(entries(dir.path()).is_empty(), "{:?}", entries(dir.path()));
}

#[test]
fn backup_of_a_corrupt_database_fails_and_removes_its_staging_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("docktornet.db");
    std::fs::write(&path, vec![0x5a; 8192]).unwrap();

    let output = backup(&path);

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    // Opening succeeds and the copy fails, so the staging file existed.
    assert!(String::from_utf8_lossy(&output.stderr).contains("copy the database"));
    assert_eq!(entries(dir.path()), vec!["docktornet.db".to_string()]);
}

#[test]
fn backup_without_a_database_path_fails_and_names_the_variable() {
    let cwd = tempdir().unwrap();
    let output = Command::new(BIN)
        .arg("backup")
        .current_dir(cwd.path())
        .env_clear()
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("DOCKTORNET_DB_PATH"));
}
