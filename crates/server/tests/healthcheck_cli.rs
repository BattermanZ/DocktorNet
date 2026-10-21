//! `docktornet-server healthcheck`, run as a real process.

use std::net::SocketAddr;
use std::process::{Command, Output};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use axum::routing::get;
use docktornet_server::agent_versions::AgentVersionTracker;
use docktornet_server::flapping::FlappingTracker;
use docktornet_server::handlers::snapshot::AppState;
use docktornet_server::reconcile::{ReconcileOptions, Reconciler};
use tempfile::{TempDir, tempdir};

const BIN: &str = env!("CARGO_BIN_EXE_docktornet-server");

/// Run `healthcheck` with `DOCKTORNET_LISTEN` alone in its environment, or
/// nothing at all for `None`.
async fn healthcheck(listen: Option<String>) -> (Output, Duration) {
    tokio::task::spawn_blocking(move || {
        // The binary loads a `.env` from its working directory or any parent.
        let cwd = tempdir().unwrap();
        let mut command = Command::new(BIN);
        command
            .arg("healthcheck")
            .current_dir(cwd.path())
            .env_clear();
        if let Some(listen) = listen {
            command.env("DOCKTORNET_LISTEN", listen);
        }
        let started = Instant::now();
        let output = command.output().unwrap();
        (output, started.elapsed())
    })
    .await
    .unwrap()
}

/// Serve `app` on a free loopback port and return the port.
async fn serve(app: axum::Router) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    port
}

/// The server's real router, over a fresh database.
async fn server_app() -> (axum::Router, TempDir) {
    let dir = tempdir().unwrap();
    let pool = docktornet_server::db::connect(&dir.path().join("t.db"))
        .await
        .unwrap();
    let reconciler = Reconciler::new(
        pool,
        ReconcileOptions {
            dry_run: true,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
    );
    let app = docktornet_server::routes::build(AppState {
        reconciler,
        secret: Arc::new("s3cret".into()),
        flapping: Arc::new(FlappingTracker::new(Duration::from_secs(300))),
        agent_versions: Arc::new(AgentVersionTracker::new("0.0.0")),
    });
    (app, dir)
}

/// A port nothing listens on.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test]
async fn passes_with_the_wildcard_listen_address_the_compose_files_use() {
    let (app, _dir) = server_app().await;
    let port = serve(app).await;

    // Linux connects to 0.0.0.0 on loopback whether or not the probe rewrites
    // it, so the unit tests in `healthcheck.rs` pin the rewrite itself.
    let (output, _) = healthcheck(Some(format!("0.0.0.0:{port}"))).await;

    assert_eq!(output.status.code(), Some(0), "{output:?}");
}

#[tokio::test]
async fn passes_against_a_server_bound_to_a_specific_address() {
    let (app, _dir) = server_app().await;
    let port = serve(app).await;

    let (output, _) = healthcheck(Some(format!("127.0.0.1:{port}"))).await;

    assert_eq!(output.status.code(), Some(0), "{output:?}");
}

#[tokio::test]
async fn fails_quickly_when_nothing_listens() {
    let (output, elapsed) = healthcheck(Some(format!("0.0.0.0:{}", free_port()))).await;

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(elapsed < Duration::from_secs(3), "took {elapsed:?}");
}

#[tokio::test]
async fn fails_on_a_non_200_answer() {
    let app = axum::Router::new().route(
        "/healthz",
        get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
    );
    let port = serve(app).await;

    let (output, _) = healthcheck(Some(format!("127.0.0.1:{port}"))).await;

    assert_eq!(output.status.code(), Some(1), "{output:?}");
}

#[tokio::test]
async fn fails_within_the_timeout_when_the_server_never_answers() {
    // Accepts the connection, then says nothing.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            held.push(socket);
        }
    });

    let (output, elapsed) = healthcheck(Some(format!("127.0.0.1:{port}"))).await;

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(
        (Duration::from_secs(2)..Duration::from_secs(5)).contains(&elapsed),
        "took {elapsed:?}"
    );
}

#[tokio::test]
async fn fails_without_a_listen_address() {
    let (output, _) = healthcheck(None).await;

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("DOCKTORNET_LISTEN"), "{stderr}");
}
