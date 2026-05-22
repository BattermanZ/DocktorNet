mod auth;
mod config;
mod db;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = config::ServerConfig::from_env()?;
    let _pool = db::connect(&cfg.db_path).await?;
    info!(listen = %cfg.listen, "server starting (HTTP routes not wired yet)");
    Ok(())
}
