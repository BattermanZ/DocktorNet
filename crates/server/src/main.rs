use std::net::SocketAddr;
use std::time::Duration;

const FLAPPING_WINDOW: Duration = Duration::from_secs(300);

use anyhow::Result;
use docktornet_server::flapping::FlappingTracker;
use docktornet_server::{config, db, routes};
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Returns `true` when binding `listen` exposes the plaintext HTTP transport
/// beyond the local host.
///
/// Docktornet terminates no TLS itself, so the bearer token and the full
/// snapshot travel in cleartext on every push. A loopback bind keeps that
/// traffic on the local host; any wildcard bind (`0.0.0.0` / `::`) or a
/// specific LAN address puts the cleartext token on a shared network segment
/// and warrants an operator warning to front the service with TLS.
fn bind_exposes_plaintext(listen: &str) -> bool {
    match listen.parse::<SocketAddr>() {
        Ok(addr) => !addr.ip().is_loopback(),
        Err(_) => {
            // Not an ip:port literal (e.g. "localhost:5363" or a bare host).
            // Only an explicit loopback hostname is treated as safe.
            let host = listen.rsplit_once(':').map_or(listen, |(h, _)| h);
            !host.eq_ignore_ascii_case("localhost")
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    tokio::select! {
        () = ctrl_c => {},
        _ = sigterm => {},
    }
    info!("shutdown signal received");
}

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
    let cloudflare = docktornet_server::integrations::cloudflare::CloudflareClient::with_base_url(
        cfg.cloudflare.clone(),
        cfg.cloudflare_api_url.as_str(),
    );
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
        flapping: std::sync::Arc::new(FlappingTracker::new(FLAPPING_WINDOW)),
    };
    let reconciler_for_resync = app_state.reconciler.clone();
    let app = routes::build(app_state);

    if bind_exposes_plaintext(&cfg.listen) {
        tracing::warn!(
            listen = %cfg.listen,
            "binding a non-loopback address but serving plaintext HTTP; the bearer token and snapshot travel in cleartext. Terminate TLS at a reverse proxy or bind a loopback address"
        );
    }

    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    info!(listen = %cfg.listen, "server ready");

    docktornet_server::background::spawn(reconciler_for_resync, cfg.full_resync_interval);

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_and_lan_binds_are_flagged_as_plaintext_exposed() {
        // The default deployment bind and any concrete LAN address expose the
        // cleartext bearer token / snapshot to the network and must warn.
        assert!(bind_exposes_plaintext("0.0.0.0:5363"));
        assert!(bind_exposes_plaintext("[::]:5363"));
        assert!(bind_exposes_plaintext("192.168.1.10:5363"));
    }

    #[test]
    fn loopback_binds_are_not_flagged() {
        // Loopback keeps the plaintext traffic on the local host.
        assert!(!bind_exposes_plaintext("127.0.0.1:5363"));
        assert!(!bind_exposes_plaintext("[::1]:5363"));
        assert!(!bind_exposes_plaintext("localhost:5363"));
    }
}
