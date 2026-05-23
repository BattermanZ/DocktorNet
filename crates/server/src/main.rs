use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Result;
use docktornet_server::flapping::FlappingTracker;
use docktornet_server::{config, db, routes};
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
    let pool = db::connect(&cfg.db_path).await?;
    let npm = docktornet_server::integrations::npm::NpmClient::new(cfg.npm.clone());
    let cloudflare =
        docktornet_server::integrations::cloudflare::CloudflareClient::new(cfg.cloudflare.clone());
    let reconciler = docktornet_server::reconcile::Reconciler::with_clients(
        pool,
        docktornet_server::reconcile::ReconcileOptions {
            dry_run: cfg.dry_run,
            adopt_existing: cfg.adopt_existing,
            gatus_config_dir: cfg.gatus_config_dir.clone(),
        },
        npm,
        cloudflare,
    );
    let app_state = docktornet_server::handlers::snapshot::AppState {
        reconciler,
        secret: std::sync::Arc::new(cfg.secret),
        flapping: std::sync::Arc::new(FlappingTracker::new(Duration::from_secs(300))),
    };
    let reconciler_for_resync = app_state.reconciler.clone();
    let app = routes::build(app_state);

    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    info!(listen = %cfg.listen, "server ready");

    let resync_interval = cfg.full_resync_interval;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(resync_interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(err) = reconciler_for_resync.full_resync().await {
                tracing::warn!(error = %err, "full resync failed; will retry next cycle");
            }
        }
    });

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        tokio::signal::ctrl_c().await.ok();
    })
    .await?;
    Ok(())
}
