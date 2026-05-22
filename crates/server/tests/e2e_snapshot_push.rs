use std::collections::BTreeMap;
use std::time::Duration;

use docktornet_core::{ContainerEntry, MachineId, Snapshot};
use reqwest::Client;
use tempfile::tempdir;

#[tokio::test]
async fn agent_push_lands_in_sqlite_via_real_http() {
    // 1. Spin up the real server on a random local port using its public router.
    let dir = tempdir().unwrap();
    let pool = docktornet_server::db::connect(&dir.path().join("e2e.db")).await.unwrap();
    let app  = docktornet_server::routes::build(pool.clone(), "s3cret".into());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr     = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // 2. Build a Snapshot directly (we don't need a real docker socket).
    let mut labels = BTreeMap::new();
    labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
    let snap = Snapshot {
        machine_id: MachineId::new("e2e-host"),
        taken_at: 42,
        containers: vec![ContainerEntry { name: "demo".into(), labels }],
    };

    // 3. Push via reqwest (mirrors what the agent does).
    let http = Client::new();
    let resp = http.post(format!("http://{}/api/v1/snapshot", addr))
        .bearer_auth("s3cret")
        .json(&snap)
        .send().await.unwrap();
    assert_eq!(resp.status(), 204);

    // 4. Verify it landed in SQLite.
    let rows = docktornet_server::storage::list_services_for_machine(&pool, "e2e-host").await.unwrap();
    assert_eq!(rows.iter().map(|r| r.0.clone()).collect::<Vec<_>>(), vec!["e2e-host/demo"]);

    server_task.abort();
    let _ = tokio::time::timeout(Duration::from_secs(1), server_task).await;
}
