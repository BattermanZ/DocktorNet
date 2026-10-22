//! `docktornet-server backup`: write a consistent copy of the database to
//! stdout.
//!
//! The copy comes from SQLite's `VACUUM INTO` on a read-only connection, so it
//! can run next to a live server. It needs only `DOCKTORNET_DB_PATH`.

use std::io::Write;
use std::path::Path;

use anyhow::Context;

use crate::audit::describe;
use crate::config::DbConfig;
use crate::db;

/// The `backup` subcommand: read the database path from the environment,
/// copy the database to stdout and return the exit code, 0 on success and 1
/// on any failure.
pub async fn cli() -> i32 {
    let result = match DbConfig::from_env() {
        Ok(cfg) => backup(&cfg.db_path, &mut std::io::stdout().lock()).await,
        Err(e) => Err(anyhow::Error::from(e).context("read config")),
    };
    match result {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("docktornet-server backup: {}", describe(&e));
            1
        }
    }
}

/// Copy the database at `db_path` into `out`.
///
/// The copy is staged in a temporary file next to the database, which is
/// removed whether or not the copy succeeds. A process killed by a signal
/// cannot remove it. Nothing reaches `out` unless the copy succeeded.
pub async fn backup(db_path: &Path, out: &mut impl Write) -> anyhow::Result<()> {
    let pool = db::connect_read_only(db_path)
        .await
        .with_context(|| format!("open database {}", db_path.display()))?;
    let dir = db_path
        .parent()
        .context("the database path has no directory")?;
    let staging = tempfile::Builder::new()
        .prefix(".docktornet-backup-")
        .suffix(".db")
        .tempfile_in(dir)
        .with_context(|| format!("create a staging file in {}", dir.display()))?;
    let staging_path = staging
        .path()
        .to_str()
        .context("the database directory is not valid UTF-8")?;
    // VACUUM INTO writes into an existing file as long as it is empty, so
    // the tempfile holds the name until the copy lands in it.
    let copied = sqlx::query("VACUUM INTO ?")
        .bind(staging_path)
        .execute(&pool)
        .await
        .context("copy the database");
    pool.close().await;
    copied?;

    let mut copy = staging.reopen().context("read the copy")?;
    std::io::copy(&mut copy, out).context("write the copy")?;
    out.flush().context("write the copy")?;
    Ok(())
}
