mod config;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cfg = config::ServerConfig::from_env()?;
    info!(listen = %cfg.listen, db = ?cfg.db_path, "server starting (HTTP not wired yet)");
    Ok(())
}
