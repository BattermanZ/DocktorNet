use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use docktornet_core::{ContainerEntry, MachineId, Snapshot};
use docktornet_server::agent_versions::AgentVersionTracker;
use docktornet_server::flapping::FlappingTracker;
use reqwest::Client;
use tempfile::tempdir;

#[tokio::test]
async fn agent_push_lands_in_sqlite_via_real_http() {
    let dir = tempdir().unwrap();
    let pool = docktornet_server::db::connect(&dir.path().join("e2e.db"))
        .await
        .unwrap();
    let reconciler = docktornet_server::reconcile::Reconciler::new(
        pool.clone(),
        docktornet_server::reconcile::ReconcileOptions {
            dry_run: true,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
    );
    let app = docktornet_server::routes::build(docktornet_server::handlers::snapshot::AppState {
        reconciler,
        secret: std::sync::Arc::new("s3cret".into()),
        flapping: std::sync::Arc::new(FlappingTracker::new(Duration::from_secs(300))),
        agent_versions: std::sync::Arc::new(AgentVersionTracker::new("0.0.0")),
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let mut labels = BTreeMap::new();
    labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
    let snap = Snapshot {
        machine_id: MachineId::new("e2e-host").unwrap(),
        forward_host: "10.0.0.10".into(),
        taken_at: 42,
        agent_version: None,
        containers: vec![ContainerEntry {
            name: "demo".into(),
            labels,
        }],
    };

    let http = Client::new();
    let resp = http
        .post(format!("http://{}/api/v1/snapshot", addr))
        .bearer_auth("s3cret")
        .json(&snap)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    let rows = docktornet_server::storage::list_services_for_machine(&pool, "e2e-host")
        .await
        .unwrap();
    assert_eq!(
        rows.iter().map(|r| r.0.clone()).collect::<Vec<_>>(),
        vec!["e2e-host/demo"]
    );

    server_task.abort();
    let _ = tokio::time::timeout(Duration::from_secs(1), server_task).await;
}
