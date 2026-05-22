mod config;

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

    let cfg = config::AgentConfig::from_env()?;
    info!(
        machine_id = %cfg.machine_id.as_str(),
        sockets = ?cfg.sockets,
        "agent starting (config only — loop not wired yet)"
    );
    Ok(())
}
