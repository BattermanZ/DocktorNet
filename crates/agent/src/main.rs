mod config;
mod sockets;
mod snapshot;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::sockets::{BollardSource, ContainerSource, list_all};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = config::AgentConfig::from_env()?;
    let mut sources: Vec<Box<dyn ContainerSource>> = Vec::new();
    for spec in &cfg.sockets {
        match BollardSource::connect(spec) {
            Ok(source) => sources.push(Box::new(source)),
            Err(error) => {
                tracing::error!(socket = %spec.path, %error, "failed to connect, skipping");
            }
        }
    }

    let raw = list_all(&sources).await;
    let snap = snapshot::build(&cfg.machine_id, &raw);
    info!(
        containers = snap.containers.len(),
        "snapshot built (push not wired yet)"
    );
    Ok(())
}
