use anyhow::Result;
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
    let app = routes::build(docktornet_server::handlers::snapshot::AppState {
        reconciler,
        secret: std::sync::Arc::new(cfg.secret),
    });

    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    info!(listen = %cfg.listen, "server ready");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;
    Ok(())
}
