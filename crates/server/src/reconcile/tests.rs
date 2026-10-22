use super::*;
use crate::integrations::gatus;
use crate::{db, storage};
use docktornet_core::{ContainerEntry, MachineId};
use std::collections::BTreeMap;
use tempfile::tempdir;

#[tokio::test]
async fn dry_run_persists_snapshot_without_writing_gatus_file() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = Reconciler::new(
        pool.clone(),
        ReconcileOptions {
            dry_run: true,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
    );

    let mut labels = BTreeMap::new();
    labels.insert(
        "docktornet.gatus.0.url".to_string(),
        "https://example.com".to_string(),
    );
    labels.insert("docktornet.gatus.0.interval".to_string(), "30s".to_string());
    let snap = Snapshot {
        machine_id: MachineId::new("m1").unwrap(),
        forward_host: "127.0.0.1".into(),
        taken_at: 1,
        agent_version: None,
        containers: vec![ContainerEntry {
            name: "app".to_string(),
            labels,
        }],
    };

    reconciler.reconcile_snapshot(&snap).await.unwrap();

    let rows = storage::list_services_for_machine(&pool, "m1")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(!dir.path().join("gatus").exists());
}

#[tokio::test]
async fn full_resync_rewrites_missing_gatus_file() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = Reconciler::new(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: gatus_dir.clone(),
        },
    );

    // Seed via a snapshot ingest so SQLite + file are in a consistent starting state.
    let mut labels = BTreeMap::new();
    labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
    labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "127.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels,
            }],
        })
        .await
        .unwrap();

    // Delete the file out from under us.
    let path = gatus::service_file_path(&gatus_dir, "m1/app");
    assert!(path.exists());
    std::fs::remove_file(&path).unwrap();

    let report = reconciler.full_resync().await.unwrap();
    assert_eq!(report.repaired, 1);
    assert_eq!(report.failed, 0);
    assert!(path.exists());
}

#[tokio::test]
async fn full_resync_is_a_noop_when_state_is_already_correct() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = Reconciler::new(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: gatus_dir.clone(),
        },
    );

    let mut labels = BTreeMap::new();
    labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
    labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "127.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels,
            }],
        })
        .await
        .unwrap();

    let report = reconciler.full_resync().await.unwrap();
    assert_eq!(report.repaired, 0);
    assert_eq!(report.failed, 0);
    assert_eq!(report.verified, 1);
}

#[tokio::test]
async fn full_resync_dry_run_does_not_rewrite_drifted_file() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    // Build state via a non-dry-run reconciler first, then resync with dry_run = true.
    let writer = Reconciler::new(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: gatus_dir.clone(),
        },
    );
    let mut labels = BTreeMap::new();
    labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
    labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
    writer
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "127.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels,
            }],
        })
        .await
        .unwrap();

    let path = gatus::service_file_path(&gatus_dir, "m1/app");
    std::fs::write(&path, "drifted").unwrap();

    let dryrun = Reconciler::new(
        pool,
        ReconcileOptions {
            dry_run: true,
            adopt_existing: false,
            gatus_config_dir: gatus_dir,
        },
    );
    let report = dryrun.full_resync().await.unwrap();
    assert_eq!(report.repaired, 0);
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "drifted");
}

#[tokio::test]
async fn gatus_file_survives_when_service_temporarily_absent() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = Reconciler::new(
        pool,
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: gatus_dir.clone(),
        },
    );

    let mut labels = BTreeMap::new();
    labels.insert(
        "docktornet.gatus.0.url".to_string(),
        "https://example.com".to_string(),
    );
    labels.insert("docktornet.gatus.0.interval".to_string(), "30s".to_string());

    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "127.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".to_string(),
                labels,
            }],
        })
        .await
        .unwrap();
    let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
    assert!(service_file.exists());

    // Snapshot taken only 30 seconds after the service was last seen:
    // still within the 5-minute grace period, so the file must survive.
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "127.0.0.1".into(),
            taken_at: 31,
            agent_version: None,
            containers: vec![],
        })
        .await
        .unwrap();
    assert!(
        service_file.exists(),
        "gatus file must not be deleted within the grace period"
    );
}

#[tokio::test]
async fn gatus_file_removed_when_service_absent_past_grace_period() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = Reconciler::new(
        pool,
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: gatus_dir.clone(),
        },
    );

    let mut labels = BTreeMap::new();
    labels.insert(
        "docktornet.gatus.0.url".to_string(),
        "https://example.com".to_string(),
    );
    labels.insert("docktornet.gatus.0.interval".to_string(), "30s".to_string());

    reconciler.set_now(1);
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "127.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".to_string(),
                labels,
            }],
        })
        .await
        .unwrap();
    let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
    assert!(service_file.exists());

    // Snapshot taken well past the 5-minute grace period.
    reconciler.set_now(1 + GATUS_REMOVAL_GRACE_SECS + 1);
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "127.0.0.1".into(),
            taken_at: (1 + GATUS_REMOVAL_GRACE_SECS + 1) as u64,
            agent_version: None,
            containers: vec![],
        })
        .await
        .unwrap();
    assert!(
        !service_file.exists(),
        "gatus file must be deleted after the grace period"
    );
}

/// A snapshot of machine `m1`, forwarding to 10.0.0.1, holding `containers`.
fn m1_snapshot(containers: Vec<ContainerEntry>) -> Snapshot {
    Snapshot {
        machine_id: MachineId::new("m1").unwrap(),
        forward_host: "10.0.0.1".into(),
        taken_at: 1,
        agent_version: None,
        containers,
    }
}

/// Ingest `snap` with the server's clock at `at`, returning the worker if
/// the ingest started one.
async fn ingest_at(reconciler: &Reconciler, at: i64, snap: &Snapshot) -> Option<ApplyWorker> {
    reconciler.set_now(at);
    reconciler.ingest_snapshot(snap).await.unwrap()
}

fn gatus_entry(name: &str) -> ContainerEntry {
    ContainerEntry {
        name: name.into(),
        labels: gatus_labels(),
    }
}

/// An NPM stand-in that counts token requests, the first call of every
/// apply that reaches NPM, and holds every request until `release` is sent,
/// then fails it.
async fn blocking_npm() -> (
    String,
    Arc<std::sync::atomic::AtomicUsize>,
    tokio::sync::watch::Sender<bool>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let tokens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (release, released) = tokio::sync::watch::channel(false);
    let counted = tokens.clone();
    tokio::spawn(async move {
        loop {
            let (mut conn, _) = listener.accept().await.unwrap();
            let counted = counted.clone();
            let mut released = released.clone();
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut buf = [0u8; 1024];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match conn.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => head.extend_from_slice(&buf[..n]),
                    }
                }
                if head.starts_with(b"POST /api/tokens") {
                    counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                let _ = released.wait_for(|released| *released).await;
                let _ = conn
                    .write_all(
                        b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                    )
                    .await;
            });
        }
    });
    (url, tokens, release)
}

#[tokio::test]
async fn identical_pushes_behind_a_blocked_apply_run_at_most_two_applies() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let (npm_url, tokens, release) = blocking_npm().await;
    let (npm, cf) = test_clients(&npm_url);
    let reconciler = Reconciler::with_clients(
        pool,
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );
    let snap = m1_snapshot(vec![ContainerEntry {
        name: "app".into(),
        labels: npm_host_labels("a.example.com"),
    }]);

    let worker = ingest_at(&reconciler, 1, &snap).await.unwrap();
    let running = tokio::spawn(worker.run());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokens.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the first apply never reached NPM"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The pushes wait for the persist lock the blocked apply holds, as
    // their handlers would.
    reconciler.set_now(2);
    let pushes: Vec<_> = (2..=10)
        .map(|_| {
            let reconciler = reconciler.clone();
            let snap = snap.clone();
            tokio::spawn(async move { reconciler.ingest_snapshot(&snap).await.unwrap() })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(200)).await;
    release.send(true).unwrap();
    for push in pushes {
        assert!(
            push.await.unwrap().is_none(),
            "a worker is already applying for m1"
        );
    }
    running.await.unwrap();

    assert_eq!(tokens.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_removal_only_a_replaced_snapshot_carried_lands_in_pending_removals() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = gatus_reconciler(pool.clone(), &dir.path().join("gatus"));
    push(&reconciler, 1, vec![gatus_entry("x")]).await;
    storage::set_npm_host(&pool, "m1/x", 0, 50).await.unwrap();
    storage::set_cf_dns_record(&pool, "m1/x", "rec-1")
        .await
        .unwrap();

    // x leaves in the first snapshot, which is replaced before it applies.
    let worker = ingest_at(&reconciler, 10, &m1_snapshot(vec![]))
        .await
        .unwrap();
    assert!(
        ingest_at(&reconciler, 20, &m1_snapshot(vec![gatus_entry("y")]))
            .await
            .is_none()
    );
    worker.run().await;

    assert_eq!(
        storage::list_pending_npm_removals(&pool).await.unwrap(),
        vec![("m1/x".to_string(), 0, 50, 1)]
    );
    assert_eq!(
        storage::list_pending_cf_removals(&pool).await.unwrap(),
        vec![("m1/x".to_string(), "rec-1".to_string(), 1)]
    );
}

#[tokio::test]
async fn a_removal_only_a_replaced_snapshot_carried_is_deleted_past_grace() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut server = cf_server_with_zone().await;
    let _token = server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .create_async()
        .await;
    let _certs = server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .create_async()
        .await;
    let npm_delete = server
        .mock("DELETE", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body("true")
        .expect(1)
        .create_async()
        .await;
    let cf_delete = server
        .mock("DELETE", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(cf_one(serde_json::json!({"id": "rec-1"})))
        .expect(1)
        .create_async()
        .await;
    let (npm, cf) = test_clients_with_cf(&server.url(), &server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );
    push(&reconciler, 1, vec![gatus_entry("x")]).await;
    storage::set_npm_host(&pool, "m1/x", 0, 50).await.unwrap();
    storage::set_cf_dns_record(&pool, "m1/x", "rec-1")
        .await
        .unwrap();

    let past_grace = 1 + GATUS_REMOVAL_GRACE_SECS + 10;
    let worker = ingest_at(&reconciler, past_grace, &m1_snapshot(vec![]))
        .await
        .unwrap();
    assert!(
        ingest_at(
            &reconciler,
            past_grace + 5,
            &m1_snapshot(vec![gatus_entry("y")])
        )
        .await
        .is_none()
    );
    worker.run().await;

    npm_delete.assert_async().await;
    cf_delete.assert_async().await;
    assert!(
        storage::list_pending_npm_removals(&pool)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        storage::list_pending_cf_removals(&pool)
            .await
            .unwrap()
            .is_empty()
    );
}

/// An NPM and Cloudflare stand-in for a service `m1/x` that owns NPM host
/// 50 and Cloudflare record rec-1.  It answers the token, certificate and
/// zone lookups; the delete mocks are returned for the caller to assert.
async fn x_resources_server(deletes: usize) -> (mockito::ServerGuard, Vec<mockito::Mock>) {
    let mut server = cf_server_with_zone().await;
    server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect_at_least(0)
        .create_async()
        .await;
    server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .expect_at_least(0)
        .create_async()
        .await;
    server
        .mock("GET", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(cf_one(serde_json::json!({
            "id": "rec-1", "name": "app.example.com", "content": "home.example.com",
            "proxied": false, "comment": "docktornet:m1/x"
        })))
        .expect_at_least(0)
        .create_async()
        .await;
    let npm_delete = server
        .mock("DELETE", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body("true")
        .expect(deletes)
        .create_async()
        .await;
    let cf_delete = server
        .mock("DELETE", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(cf_one(serde_json::json!({"id": "rec-1"})))
        .expect(deletes)
        .create_async()
        .await;
    (server, vec![npm_delete, cf_delete])
}

fn reconciler_on(
    pool: Db,
    gatus_dir: &std::path::Path,
    server: &mockito::ServerGuard,
) -> Reconciler {
    let (npm, cf) = test_clients_with_cf(&server.url(), &server.url());
    Reconciler::with_clients(
        pool,
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: gatus_dir.to_path_buf(),
        },
        npm,
        cf,
    )
}

/// `x` with a Gatus monitor and a Cloudflare record.
fn x_entry() -> ContainerEntry {
    let mut labels = gatus_labels();
    labels.extend(cf_dns_labels("app", "home.example.com"));
    ContainerEntry {
        name: "x".into(),
        labels,
    }
}

#[tokio::test]
async fn a_service_back_in_the_newest_snapshot_keeps_its_resources() {
    // x leaves, past its grace period, in a snapshot that is replaced by
    // one holding x again before it applies.  The apply must restore x,
    // not delete what it owns.
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let (mut server, deletes) = x_resources_server(0).await;
    // x's NPM host, in sync with its npm.0 group.
    server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(
            serde_json::json!({
                "id": 50, "domain_names": ["x.example.com"], "forward_scheme": "http",
                "forward_host": "10.0.0.1", "forward_port": 3000, "certificate_id": 0,
                "ssl_forced": 0, "http2_support": 0, "hsts_enabled": 0,
                "hsts_subdomains": 0, "block_exploits": 1, "allow_websocket_upgrade": 1,
                "meta": {"docktornet": {"service_id": "m1/x", "npm_index": 0}}
            })
            .to_string(),
        )
        .create_async()
        .await;
    let reconciler = reconciler_on(pool.clone(), &gatus_dir, &server);
    let x = || {
        let mut x = x_entry();
        x.labels.extend(npm_host_labels("x.example.com"));
        x
    };
    storage::upsert_snapshot(&pool, &m1_snapshot(vec![x()]), 1)
        .await
        .unwrap();
    storage::set_npm_host(&pool, "m1/x", 0, 50).await.unwrap();
    storage::set_cf_dns_record(&pool, "m1/x", "rec-1")
        .await
        .unwrap();

    let past_grace = 1 + GATUS_REMOVAL_GRACE_SECS + 10;
    let worker = ingest_at(&reconciler, past_grace, &m1_snapshot(vec![]))
        .await
        .unwrap();
    assert!(
        ingest_at(&reconciler, past_grace + 5, &m1_snapshot(vec![x()]))
            .await
            .is_none()
    );
    worker.run().await;

    for mock in deletes {
        mock.assert_async().await;
    }
    assert!(gatus::service_file_path(&gatus_dir, "m1/x").exists());
    assert_eq!(
        storage::get_npm_host(&pool, "m1/x", 0).await.unwrap(),
        Some(50)
    );
    assert_eq!(
        storage::get_cf_dns_record(&pool, "m1/x").await.unwrap(),
        Some("rec-1".to_string())
    );
    assert!(
        storage::list_pending_npm_removals(&pool)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        storage::list_pending_cf_removals(&pool)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_service_that_returns_and_leaves_again_while_coalesced_keeps_its_grace() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = gatus_reconciler(pool.clone(), &dir.path().join("gatus"));
    push(&reconciler, 1, vec![gatus_entry("x")]).await;
    storage::set_npm_host(&pool, "m1/x", 0, 50).await.unwrap();
    storage::set_cf_dns_record(&pool, "m1/x", "rec-1")
        .await
        .unwrap();
    // x leaves: its removal waits out the grace period from t=1.
    ingest_at(&reconciler, 10, &m1_snapshot(vec![]))
        .await
        .unwrap()
        .run()
        .await;

    // x is back at t=20 and gone again at t=30, both behind one apply.
    let worker = ingest_at(&reconciler, 20, &m1_snapshot(vec![gatus_entry("x")]))
        .await
        .unwrap();
    assert!(
        ingest_at(&reconciler, 30, &m1_snapshot(vec![]))
            .await
            .is_none()
    );
    worker.run().await;

    // x was seen at t=20, so its grace period runs from there.
    assert_eq!(
        storage::list_pending_npm_removals(&pool).await.unwrap(),
        vec![("m1/x".to_string(), 0, 50, 20)]
    );
    assert_eq!(
        storage::list_pending_cf_removals(&pool).await.unwrap(),
        vec![("m1/x".to_string(), "rec-1".to_string(), 20)]
    );
}

#[tokio::test]
async fn sweep_deletes_what_a_running_service_without_intent_left_behind() {
    // x keeps running but its only docktornet label is malformed, so it has
    // left.  Its removal must still go through.
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let (server, deletes) = x_resources_server(1).await;
    let reconciler = reconciler_on(pool.clone(), &gatus_dir, &server);
    storage::upsert_snapshot(&pool, &m1_snapshot(vec![x_entry()]), 1)
        .await
        .unwrap();
    storage::set_npm_host(&pool, "m1/x", 0, 50).await.unwrap();
    storage::set_cf_dns_record(&pool, "m1/x", "rec-1")
        .await
        .unwrap();
    let malformed = ContainerEntry {
        name: "x".into(),
        labels: [(
            "docktornet.npm.0.host".to_string(),
            "a.example.com".to_string(),
        )]
        .into(),
    };
    push(&reconciler, 2, vec![malformed]).await;

    reconciler.set_now(2 + GATUS_REMOVAL_GRACE_SECS + 60);
    reconciler.sweep_pending_removals().await.unwrap();

    for mock in deletes {
        mock.assert_async().await;
    }
}

#[tokio::test]
async fn later_pushes_of_a_service_without_intent_leave_its_grace_period_alone() {
    // x had intent at t=1, then keeps running with only a malformed label.
    // Its grace period runs from t=1, however often the agent pushes it.
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let (server, deletes) = x_resources_server(1).await;
    let reconciler = reconciler_on(pool.clone(), &gatus_dir, &server);
    storage::upsert_snapshot(&pool, &m1_snapshot(vec![x_entry()]), 1)
        .await
        .unwrap();
    storage::set_npm_host(&pool, "m1/x", 0, 50).await.unwrap();
    storage::set_cf_dns_record(&pool, "m1/x", "rec-1")
        .await
        .unwrap();
    std::fs::create_dir_all(&gatus_dir).unwrap();
    let gatus_file = gatus::service_file_path(&gatus_dir, "m1/x");
    std::fs::write(&gatus_file, "endpoints: []\n").unwrap();
    let malformed = ContainerEntry {
        name: "x".into(),
        labels: [(
            "docktornet.npm.0.host".to_string(),
            "a.example.com".to_string(),
        )]
        .into(),
    };
    for at in [2, 100, 200] {
        push(&reconciler, at, vec![malformed.clone()]).await;
    }
    assert_eq!(
        storage::list_pending_gatus_removals(&pool).await.unwrap(),
        vec![("m1/x".to_string(), 1)]
    );

    reconciler.set_now(1 + GATUS_REMOVAL_GRACE_SECS);
    reconciler.sweep_pending_removals().await.unwrap();

    for mock in deletes {
        mock.assert_async().await;
    }
    assert!(
        !gatus_file.exists(),
        "the gatus file outlived the grace period"
    );
}

#[tokio::test]
async fn a_push_after_the_worker_finished_starts_a_new_worker() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = gatus_reconciler(pool, &dir.path().join("gatus"));
    let worker = ingest_at(&reconciler, 1, &m1_snapshot(vec![gatus_entry("x")]))
        .await
        .unwrap();
    worker.run().await;
    assert!(
        ingest_at(&reconciler, 2, &m1_snapshot(vec![gatus_entry("x")]))
            .await
            .is_some(),
        "the push would be stranded"
    );
}

#[tokio::test]
async fn a_dropped_worker_lets_the_next_push_start_one() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = gatus_reconciler(pool, &gatus_dir);
    let worker = ingest_at(&reconciler, 1, &m1_snapshot(vec![gatus_entry("x")]))
        .await
        .unwrap();
    drop(worker);
    let worker = ingest_at(&reconciler, 2, &m1_snapshot(vec![gatus_entry("y")]))
        .await
        .expect("the dropped worker must hand m1 back");
    worker.run().await;
    assert!(gatus::service_file_path(&gatus_dir, "m1/y").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_pushes_end_with_the_newest_snapshot_applied() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = gatus_reconciler(pool, &gatus_dir);
    let entry = |i: u32| {
        let mut labels = gatus_labels();
        labels.insert("docktornet.gatus.0.url".into(), format!("https://v{i}"));
        ContainerEntry {
            name: "app".into(),
            labels,
        }
    };
    for i in 0..50 {
        if let Some(worker) = ingest_at(&reconciler, 1, &m1_snapshot(vec![entry(i)])).await {
            tokio::spawn(worker.run());
        }
        if i % 3 == 0 {
            tokio::task::yield_now().await;
        }
    }
    reconciler.wait_for_applies().await;
    let file = std::fs::read_to_string(gatus::service_file_path(&gatus_dir, "m1/app")).unwrap();
    assert!(file.contains("https://v49"), "{file}");
}

#[tokio::test]
async fn sweep_keeps_a_pending_removal_of_a_service_that_is_back() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut server = mockito::Server::new_async().await;
    let mut untouched = Vec::new();
    for method in ["GET", "POST", "PUT", "DELETE"] {
        untouched.push(
            server
                .mock(method, mockito::Matcher::Any)
                .expect(0)
                .create_async()
                .await,
        );
    }
    let (npm, cf) = test_clients_with_cf(&server.url(), &server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );
    // x is stored again still labelling npm.0, and its removal waits for the
    // apply to restore it.
    let mut labels = gatus_labels();
    labels.extend(npm_host_labels("x.example.com"));
    storage::upsert_snapshot(
        &pool,
        &m1_snapshot(vec![ContainerEntry {
            name: "x".into(),
            labels,
        }]),
        1,
    )
    .await
    .unwrap();
    storage::add_pending_npm_removal(&pool, "m1/x", 0, 50, 1)
        .await
        .unwrap();
    storage::add_pending_cf_removal(&pool, "m1/x", "rec-1", 1)
        .await
        .unwrap();
    reconciler.set_now(1 + GATUS_REMOVAL_GRACE_SECS + 60);

    reconciler.sweep_pending_removals().await.unwrap();

    for mock in untouched {
        mock.assert_async().await;
    }
    assert_eq!(
        storage::list_pending_npm_removals(&pool)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        storage::list_pending_cf_removals(&pool)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn full_resync_removes_orphan_gatus_file() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = Reconciler::new(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: gatus_dir.clone(),
        },
    );

    let mut labels = BTreeMap::new();
    labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
    labels.insert("docktornet.gatus.0.interval".into(), "30s".into());

    // Seed the service.
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "127.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels,
            }],
        })
        .await
        .unwrap();
    let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
    assert!(service_file.exists());

    // Remove the service within the grace period so the file is kept.
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "127.0.0.1".into(),
            taken_at: 31,
            agent_version: None,
            containers: vec![],
        })
        .await
        .unwrap();
    assert!(service_file.exists(), "file should survive grace period");

    // Let the grace period recorded for its departure run out.
    end_grace(&reconciler, "m1/app").await;

    // full_resync should sweep the orphan file (service is no longer in DB).
    let report = reconciler.full_resync().await.unwrap();
    assert_eq!(report.failed, 0);
    assert!(
        !service_file.exists(),
        "full_resync must remove the orphan gatus file"
    );
}

#[tokio::test]
async fn full_resync_keeps_orphan_gatus_file_within_grace() {
    // A container that briefly leaves the snapshot (e.g. a restart) has its
    // service row cascade-deleted from the DB, so it is no longer "expected"
    // by the orphan sweep — yet apply_inner deliberately keeps its gatus file
    // during the grace period to preserve Gatus alert state.  A full_resync
    // that coincides with the grace window must NOT delete the file.
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = Reconciler::new(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: gatus_dir.clone(),
        },
    );

    let mut labels = BTreeMap::new();
    labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
    labels.insert("docktornet.gatus.0.interval".into(), "30s".into());

    // Seed the service.
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "127.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels,
            }],
        })
        .await
        .unwrap();
    let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
    assert!(service_file.exists());

    // Remove the service within the grace period so apply_inner keeps the
    // file (its mtime stays recent, standing in for last-seen).
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "127.0.0.1".into(),
            taken_at: 31,
            agent_version: None,
            containers: vec![],
        })
        .await
        .unwrap();
    assert!(service_file.exists(), "file should survive grace period");

    // A coinciding full_resync must respect the grace period and leave the
    // still-in-grace file in place.
    let report = reconciler.full_resync().await.unwrap();
    assert_eq!(report.failed, 0);
    assert!(
        service_file.exists(),
        "full_resync must not delete a gatus file that is still within the grace period"
    );
}

/// Push a snapshot of machine `m1` holding `containers`, with the server's
/// clock and the agent's `taken_at` both at `at`.
async fn push(reconciler: &Reconciler, at: u64, containers: Vec<ContainerEntry>) {
    reconciler.set_now(at as i64);
    push_stamped(reconciler, at, containers).await;
}

/// Push a snapshot of machine `m1` holding `containers`, stamped `taken_at`
/// by the agent, leaving the server's clock alone.
async fn push_stamped(reconciler: &Reconciler, taken_at: u64, containers: Vec<ContainerEntry>) {
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at,
            containers,
            agent_version: None,
        })
        .await
        .unwrap();
}

fn gatus_labels() -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
    labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
    labels
}

fn set_mtime(path: &std::path::Path, mtime: std::time::SystemTime) {
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(mtime)
        .unwrap();
}

/// Backdate a departure that apply_inner recorded so its grace period is over.
async fn end_grace(reconciler: &Reconciler, service_id: &str) {
    storage::add_pending_gatus_removal(
        reconciler.pool(),
        service_id,
        reconciler.clock.now() - GATUS_REMOVAL_GRACE_SECS - 60,
    )
    .await
    .unwrap();
}

fn aged_past_grace() -> std::time::SystemTime {
    std::time::SystemTime::now()
        - std::time::Duration::from_secs((GATUS_REMOVAL_GRACE_SECS + 60) as u64)
}

#[tokio::test]
async fn pending_removal_sweep_removes_gatus_file_without_full_resync() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = Reconciler::new(
        pool,
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: gatus_dir.clone(),
        },
    );
    let app = || ContainerEntry {
        name: "app".into(),
        labels: gatus_labels(),
    };

    // Present at t=1, gone at t=31 (within grace), and still gone after it.
    push(&reconciler, 1, vec![app()]).await;
    let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
    assert!(service_file.exists());
    push(&reconciler, 31, vec![]).await;
    assert!(service_file.exists(), "file should survive grace period");
    push(
        &reconciler,
        (31 + GATUS_REMOVAL_GRACE_SECS + 30) as u64,
        vec![],
    )
    .await;
    assert!(
        service_file.exists(),
        "a later snapshot no longer names the service, so it cannot remove the file"
    );

    // Grace has passed since the service left.
    end_grace(&reconciler, "m1/app").await;
    reconciler.sweep_pending_removals().await.unwrap();

    assert!(
        !service_file.exists(),
        "the short sweep must remove the gatus file once grace has passed"
    );
}

#[tokio::test]
async fn pending_removal_sweep_removes_old_temp_files_only() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = gatus_reconciler(pool, &gatus_dir);
    std::fs::create_dir_all(&gatus_dir).unwrap();
    let file = |name: &str, aged: bool| {
        let path = gatus_dir.join(name);
        std::fs::write(&path, "x").unwrap();
        if aged {
            set_mtime(&path, aged_past_grace());
        }
        path
    };
    let old_service_tmp = file("x--0123456789abcdef.yaml.tmp-1-2-3", true);
    let old_map_tmp = file("docktornet-classification.json.tmp-1-2-4", true);
    let young_tmp = file("y--0123456789abcdef.yaml.tmp-1-2-5", false);
    let not_ours = [
        file("notes.yaml.tmp", true),
        file("x.yaml.tmp-1-2", true),
        file("x.yaml.tmp-1-2-z", true),
        // An operator's own dated copy of their base config.
        file("00-base.yaml.tmp-2026-10-21", true),
    ];

    reconciler.sweep_pending_removals().await.unwrap();

    assert!(!old_service_tmp.exists());
    assert!(!old_map_tmp.exists());
    assert!(young_tmp.exists(), "a write may still be using it");
    for path in not_ours {
        assert!(path.exists(), "{} is not a temp file", path.display());
    }
}

#[tokio::test]
async fn a_delayed_apply_decides_on_the_time_of_ingest() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = gatus_reconciler(pool, &gatus_dir);
    push(
        &reconciler,
        1,
        vec![ContainerEntry {
            name: "app".into(),
            labels: gatus_labels(),
        }],
    )
    .await;

    // The service is gone 5 s later, but the apply only runs after the
    // grace period, held up behind a slow pass.
    reconciler.set_now(6);
    let gone = Snapshot {
        machine_id: MachineId::new("m1").unwrap(),
        forward_host: "10.0.0.1".into(),
        taken_at: 6,
        agent_version: None,
        containers: vec![],
    };
    let worker = reconciler.ingest_snapshot(&gone).await.unwrap().unwrap();
    reconciler.set_now(6 + GATUS_REMOVAL_GRACE_SECS + 60);
    worker.run().await;

    assert!(
        gatus::service_file_path(&gatus_dir, "m1/app").exists(),
        "the service was gone 5 s when ingested, so its removal is deferred"
    );
}

/// Create the reload trigger with an old mtime and return that mtime.
fn age_reload_trigger(gatus_dir: &std::path::Path) -> std::time::SystemTime {
    let trigger = gatus::reload_trigger_path(gatus_dir);
    std::fs::create_dir_all(gatus_dir).unwrap();
    std::fs::write(&trigger, "{}\n").unwrap();
    let old = aged_past_grace();
    set_mtime(&trigger, old);
    old
}

fn reload_trigger_mtime(gatus_dir: &std::path::Path) -> std::time::SystemTime {
    std::fs::metadata(gatus::reload_trigger_path(gatus_dir))
        .unwrap()
        .modified()
        .unwrap()
}

fn gatus_reconciler(pool: Db, gatus_dir: &std::path::Path) -> Reconciler {
    Reconciler::new(
        pool,
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: gatus_dir.to_path_buf(),
        },
    )
}

#[tokio::test]
async fn pending_removal_sweep_keeps_long_unchanged_gatus_file_within_grace() {
    // A stable service's file is only rewritten when its monitors change,
    // so its mtime can be far older than the grace period.  A restart that
    // drops it from one snapshot must not cost it its monitor.
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = gatus_reconciler(pool, &gatus_dir);
    let app = || ContainerEntry {
        name: "app".into(),
        labels: gatus_labels(),
    };

    push(&reconciler, 1, vec![app()]).await;
    let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
    set_mtime(&service_file, aged_past_grace());
    push(&reconciler, 31, vec![]).await;

    reconciler.sweep_pending_removals().await.unwrap();
    reconciler.full_resync().await.unwrap();

    assert!(
        service_file.exists(),
        "a service absent for less than the grace period keeps its gatus file"
    );
    assert!(!gatus::reload_trigger_path(&gatus_dir).exists());
}

#[tokio::test]
async fn sweep_between_ingest_and_apply_keeps_gatus_file_within_grace() {
    // The ingest handler persists a snapshot, then applies it later in a
    // background task.  A sweep that runs in between must already know the
    // service only just left.
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = gatus_reconciler(pool, &gatus_dir);
    push(
        &reconciler,
        1,
        vec![ContainerEntry {
            name: "app".into(),
            labels: gatus_labels(),
        }],
    )
    .await;
    let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
    set_mtime(&service_file, aged_past_grace());

    let gone = Snapshot {
        machine_id: MachineId::new("m1").unwrap(),
        forward_host: "10.0.0.1".into(),
        taken_at: 31,
        agent_version: None,
        containers: vec![],
    };
    let worker = reconciler.ingest_snapshot(&gone).await.unwrap().unwrap();
    reconciler.sweep_pending_removals().await.unwrap();
    worker.run().await;

    assert!(
        service_file.exists(),
        "a sweep between ingest and apply must respect the grace period"
    );
}

#[tokio::test]
async fn service_returning_after_a_failed_removal_apply_is_reconciled() {
    // The apply that records a removal fails (NPM is down).  When the
    // service comes back with the same labels it had, that snapshot must
    // not be skipped as unchanged, or its pending NPM removal survives and
    // the sweep deletes a live proxy host.
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let token_ok = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .create_async()
        .await;
    let _get = npm_server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(
            r#"{"id":50,"domain_names":["a.example.com"],"forward_scheme":"http","forward_host":"10.0.0.1","forward_port":3000,"certificate_id":0,"ssl_forced":false,"http2_support":false,"hsts_enabled":false,"block_exploits":true,"allow_websocket_upgrade":true,"enabled":true,"meta":{"docktornet":{"service_id":"m1/app","npm_index":0}}}"#,
        )
        .create_async()
        .await;
    let delete = npm_server
        .mock("DELETE", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body("{}")
        .expect(0)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );
    let app = || ContainerEntry {
        name: "app".into(),
        labels: npm_host_labels("a.example.com"),
    };

    // Present and reconciled, so its intent hash is cached.
    storage::upsert_snapshot(
        &pool,
        &Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![app()],
        },
        1,
    )
    .await
    .unwrap();
    storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();
    push(&reconciler, 2, vec![app()]).await;

    // Gone within grace while NPM fails: the apply errors.
    token_ok.remove_async().await;
    let token_down = npm_server
        .mock("POST", "/api/tokens")
        .with_status(500)
        .create_async()
        .await;
    let result = reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 32,
            agent_version: None,
            containers: vec![],
        })
        .await;
    assert!(result.is_err());
    assert_eq!(
        storage::list_pending_npm_removals(&pool)
            .await
            .unwrap()
            .len(),
        1
    );

    // Back with the same labels, NPM healthy again.
    token_down.remove_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .create_async()
        .await;
    push(&reconciler, 62, vec![app()]).await;

    assert!(
        storage::list_pending_npm_removals(&pool)
            .await
            .unwrap()
            .is_empty(),
        "the returning service's pending removal must be cleared"
    );
    assert_eq!(
        storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
        Some(50)
    );
    reconciler.sweep_pending_removals().await.unwrap();
    delete.assert_async().await;
}

#[tokio::test]
async fn removal_past_grace_touches_gatus_reload_trigger() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = gatus_reconciler(pool, &gatus_dir);
    let app = ContainerEntry {
        name: "app".into(),
        labels: gatus_labels(),
    };

    push(&reconciler, 1, vec![app]).await;
    let before = age_reload_trigger(&gatus_dir);
    push(
        &reconciler,
        (1 + GATUS_REMOVAL_GRACE_SECS + 1) as u64,
        vec![],
    )
    .await;

    assert!(!gatus::service_file_path(&gatus_dir, "m1/app").exists());
    assert!(reload_trigger_mtime(&gatus_dir) > before);
}

#[tokio::test]
async fn orphan_sweep_touches_gatus_reload_trigger() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = gatus_reconciler(pool, &gatus_dir);
    let app = ContainerEntry {
        name: "app".into(),
        labels: gatus_labels(),
    };

    push(&reconciler, 1, vec![app]).await;
    push(&reconciler, 31, vec![]).await;
    let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
    end_grace(&reconciler, "m1/app").await;
    let before = age_reload_trigger(&gatus_dir);

    reconciler.sweep_pending_removals().await.unwrap();

    assert!(!service_file.exists());
    assert!(reload_trigger_mtime(&gatus_dir) > before);
    assert!(
        gatus::reload_trigger_path(&gatus_dir).exists(),
        "the orphan sweep must never delete the trigger"
    );
}

#[tokio::test]
async fn dropping_gatus_labels_touches_gatus_reload_trigger() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = gatus_reconciler(pool, &gatus_dir);

    let mut labels = gatus_labels();
    labels.extend(cf_dns_labels("app", "home.example.com"));
    push(
        &reconciler,
        1,
        vec![ContainerEntry {
            name: "app".into(),
            labels,
        }],
    )
    .await;
    let before = age_reload_trigger(&gatus_dir);

    // Still running, but its Gatus labels are gone.
    push(
        &reconciler,
        31,
        vec![ContainerEntry {
            name: "app".into(),
            labels: cf_dns_labels("app", "home.example.com"),
        }],
    )
    .await;

    assert!(!gatus::service_file_path(&gatus_dir, "m1/app").exists());
    assert!(reload_trigger_mtime(&gatus_dir) > before);
}

#[tokio::test]
async fn pass_without_gatus_deletion_leaves_reload_trigger_alone() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = gatus_reconciler(pool, &gatus_dir);
    let app = || ContainerEntry {
        name: "app".into(),
        labels: gatus_labels(),
    };

    push(&reconciler, 1, vec![app()]).await;
    // A restart within grace keeps the file, so Gatus has nothing to reload.
    push(&reconciler, 31, vec![]).await;
    assert_eq!(
        storage::list_pending_gatus_removals(reconciler.pool())
            .await
            .unwrap()
            .len(),
        1
    );
    push(&reconciler, 61, vec![app()]).await;
    reconciler.sweep_pending_removals().await.unwrap();

    assert!(!gatus::reload_trigger_path(&gatus_dir).exists());
    assert!(
        storage::list_pending_gatus_removals(reconciler.pool())
            .await
            .unwrap()
            .is_empty(),
        "a returning service's departure is forgotten"
    );
}

#[tokio::test]
async fn dry_run_pending_removal_sweep_writes_nothing() {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let reconciler = gatus_reconciler(pool.clone(), &gatus_dir);
    push(
        &reconciler,
        1,
        vec![ContainerEntry {
            name: "app".into(),
            labels: gatus_labels(),
        }],
    )
    .await;
    push(&reconciler, 31, vec![]).await;
    let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
    end_grace(&reconciler, "m1/app").await;

    let dryrun = Reconciler::new(
        pool,
        ReconcileOptions {
            dry_run: true,
            adopt_existing: false,
            gatus_config_dir: gatus_dir.clone(),
        },
    );
    dryrun.sweep_pending_removals().await.unwrap();

    assert!(service_file.exists());
    assert!(!gatus::reload_trigger_path(&gatus_dir).exists());
}

#[tokio::test]
async fn pending_removal_sweep_deletes_cf_record_without_full_resync() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut cf_server = mockito::Server::new_async().await;
    let delete = cf_server
        .mock("DELETE", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(r#"{"success":true}"#)
        .expect(1)
        .create_async()
        .await;

    let (npm, cf) = test_clients_with_cf(&cf_server.url(), &cf_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );
    let app = || ContainerEntry {
        name: "app".into(),
        labels: cf_dns_labels("app", "home.example.com"),
    };

    // Present at t=1, owning a Cloudflare record.
    storage::upsert_snapshot(
        &pool,
        &Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![app()],
        },
        1,
    )
    .await
    .unwrap();
    storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
        .await
        .unwrap();
    push(&reconciler, 31, vec![]).await;
    push(
        &reconciler,
        (31 + GATUS_REMOVAL_GRACE_SECS + 30) as u64,
        vec![],
    )
    .await;
    assert_eq!(
        storage::list_pending_cf_removals(&pool).await.unwrap(),
        vec![("m1/app".to_string(), "rec-1".to_string(), 1)],
        "no later snapshot names the service, so the record waits in pending"
    );

    reconciler.sweep_pending_removals().await.unwrap();

    delete.assert_async().await;
    assert!(
        storage::list_pending_cf_removals(&pool)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn pending_removal_sweep_prunes_npm_host_without_full_resync() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .create_async()
        .await;
    let delete = npm_server
        .mock("DELETE", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body("{}")
        .expect(1)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );
    let app = || ContainerEntry {
        name: "app".into(),
        labels: npm_host_labels("a.example.com"),
    };

    // Present at t=1, owning an NPM proxy host.
    storage::upsert_snapshot(
        &pool,
        &Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![app()],
        },
        1,
    )
    .await
    .unwrap();
    storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();
    push(&reconciler, 31, vec![]).await;
    push(
        &reconciler,
        (31 + GATUS_REMOVAL_GRACE_SECS + 30) as u64,
        vec![],
    )
    .await;
    assert_eq!(
        storage::list_pending_npm_removals(&pool).await.unwrap(),
        vec![("m1/app".to_string(), 0, 50, 1)],
        "no later snapshot names the service, so the host waits in pending"
    );

    reconciler.sweep_pending_removals().await.unwrap();

    delete.assert_async().await;
    assert!(
        storage::list_pending_npm_removals(&pool)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn pending_removal_sweep_clears_an_npm_host_already_gone_without_feeding_the_breaker() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .create_async()
        .await;
    // Someone already deleted the host in NPM.
    let delete = npm_server
        .mock("DELETE", "/api/nginx/proxy-hosts/50")
        .with_status(404)
        .with_body(r#"{"error":{"code":404,"message":"Not Found"}}"#)
        .expect(NPM_BREAKER_THRESHOLD as usize)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );
    reconciler.set_now(1 + GATUS_REMOVAL_GRACE_SECS + 60);

    // As many sweeps as it takes failures to open the breaker, each with a
    // removal past its grace period whose delete finds nothing.
    for sweep in 0..NPM_BREAKER_THRESHOLD {
        storage::add_pending_npm_removal(&pool, "m1/app", 0, 50, 1)
            .await
            .unwrap();

        let report = reconciler.sweep_pending_removals().await.unwrap();

        assert_eq!(report.failed, 0, "sweep {sweep}: {report:?}");
        assert!(
            storage::list_pending_npm_removals(&pool)
                .await
                .unwrap()
                .is_empty(),
            "sweep {sweep}: a host already gone clears its pending row"
        );
        assert_eq!(
            reconciler.npm_breaker.consecutive_failures(),
            0,
            "sweep {sweep}"
        );
        assert_eq!(reconciler.npm_breaker_open_until(), None, "sweep {sweep}");
    }
    delete.assert_async().await;
}

#[tokio::test]
async fn npm_forwards_to_each_machines_own_address() {
    use mockito::Matcher;

    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .expect_at_least(1)
        .create_async()
        .await;
    // No host is marked for the service, so marker adoption finds nothing.
    let _list = npm_server
        .mock("GET", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body("[]")
        .create_async()
        .await;
    // One expectation per machine, matching its own forward_host.
    let create_a = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .match_body(Matcher::PartialJson(
            serde_json::json!({ "forward_host": "10.0.0.1" }),
        ))
        .with_status(200)
        .with_body(r#"{"id":1,"domain_names":["a.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;
    let create_b = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .match_body(Matcher::PartialJson(
            serde_json::json!({ "forward_host": "10.0.0.2" }),
        ))
        .with_status(200)
        .with_body(r#"{"id":2,"domain_names":["b.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;

    let npm = crate::integrations::npm::NpmClient::new(crate::config::NpmConfig {
        url: url::Url::parse(&npm_server.url()).unwrap(),
        user: "u".into(),
        password: "p".into(),
    });
    let cf =
        crate::integrations::cloudflare::CloudflareClient::new(crate::config::CloudflareConfig {
            api_token: "t".into(),
            zone_id: "z".into(),
        });

    let reconciler = Reconciler::with_clients(
        pool,
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    let npm_labels = |host: &str| {
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.npm.0.host".to_string(), host.to_string());
        labels.insert("docktornet.npm.0.port".to_string(), "3000".to_string());
        labels.insert("docktornet.npm.0.scheme".to_string(), "http".to_string());
        labels.insert("docktornet.npm.0.ssl".to_string(), "false".to_string());
        labels
    };

    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_labels("a.example.com"),
            }],
        })
        .await
        .unwrap();

    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m2").unwrap(),
            forward_host: "10.0.0.2".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_labels("b.example.com"),
            }],
        })
        .await
        .unwrap();

    create_a.assert_async().await;
    create_b.assert_async().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_ingests_for_two_machines_both_persist() {
    use mockito::Matcher;

    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .expect_at_least(1)
        .create_async()
        .await;
    let _list = npm_server
        .mock("GET", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body("[]")
        .create_async()
        .await;
    let create_a = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .match_body(Matcher::PartialJson(
            serde_json::json!({ "forward_host": "10.0.0.1" }),
        ))
        .with_status(200)
        .with_body(r#"{"id":1,"domain_names":["a.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;
    let create_b = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .match_body(Matcher::PartialJson(
            serde_json::json!({ "forward_host": "10.0.0.2" }),
        ))
        .with_status(200)
        .with_body(r#"{"id":2,"domain_names":["b.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    // Both ingests wait at the barrier and run at once; the applies start
    // only once both snapshots are queued, so neither push's pending apply
    // can be taken before the other machine's push lands.
    let start = Arc::new(tokio::sync::Barrier::new(2));
    let ingest = |machine: &str, forward_host: &str, host: &str| {
        let reconciler = reconciler.clone();
        let start = start.clone();
        let snap = Snapshot {
            machine_id: MachineId::new(machine).unwrap(),
            forward_host: forward_host.into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels(host),
            }],
        };
        tokio::spawn(async move {
            start.wait().await;
            reconciler.ingest_snapshot(&snap).await.unwrap()
        })
    };
    let m1 = ingest("m1", "10.0.0.1", "a.example.com");
    let m2 = ingest("m2", "10.0.0.2", "b.example.com");
    let workers = [m1.await.unwrap(), m2.await.unwrap()];
    assert!(
        workers.iter().all(Option::is_some),
        "each machine's first push starts its own worker"
    );
    for worker in workers.into_iter().flatten() {
        tokio::spawn(worker.run());
    }
    reconciler.wait_for_applies().await;

    for (machine, npm_id) in [("m1", 1), ("m2", 2)] {
        let service_id = format!("{machine}/app");
        let services = storage::list_services_for_machine(&pool, machine)
            .await
            .unwrap();
        assert_eq!(
            services
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>(),
            vec![service_id.as_str()],
            "{machine}'s service persists"
        );
        assert_eq!(
            storage::get_npm_host(&pool, &service_id, 0).await.unwrap(),
            Some(npm_id),
            "{machine}'s NPM mapping persists"
        );
    }
    create_a.assert_async().await;
    create_b.assert_async().await;
}

#[tokio::test]
async fn npm_skips_update_when_host_already_in_sync() {
    // A host that is already mapped and whose NPM state matches the payload
    // must be fetched and left untouched — no PUT (which would reload nginx).
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .expect_at_least(1)
        .create_async()
        .await;
    // Mapping already exists, so the host is never (re)created.
    let create = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
        .expect(0)
        .create_async()
        .await;
    // It is fetched; stored state matches the payload, so no PUT is issued.
    let get = npm_server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(
            r#"{"id":50,"domain_names":["a.example.com"],"forward_scheme":"http",
                "forward_host":"10.0.0.1","forward_port":3000,"certificate_id":0,
                "ssl_forced":0,"http2_support":0,"hsts_enabled":0,
                "hsts_subdomains":0,"block_exploits":1,"allow_websocket_upgrade":1,
                "meta":{"docktornet":{"service_id":"m1/app","npm_index":0}}}"#,
        )
        .expect(1)
        .create_async()
        .await;
    let update = npm_server
        .mock("PUT", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
        .expect(0)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    let snap = Snapshot {
        machine_id: MachineId::new("m1").unwrap(),
        forward_host: "10.0.0.1".into(),
        taken_at: 1,
        agent_version: None,
        containers: vec![ContainerEntry {
            name: "app".into(),
            labels: npm_host_labels("a.example.com"),
        }],
    };
    // Seed the service row (npm_hosts has an FK to it) and the mapping so
    // reconcile takes the get/verify path rather than creating.
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

    reconciler.reconcile_snapshot(&snap).await.unwrap();

    create.assert_async().await;
    get.assert_async().await;
    update.assert_async().await;
}

#[tokio::test]
async fn npm_recreates_host_when_deleted_in_npm() {
    // A host is mapped (id 50) but has been deleted out-of-band in NPM, so
    // GET /proxy-hosts/50 returns 404.  reconcile_npm must recreate the host
    // (POST) and persist the new id, NOT PUT to the dead id 50.
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .expect_at_least(1)
        .create_async()
        .await;
    // The mapped host no longer exists in NPM.
    let get = npm_server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(404)
        .with_body(r#"{"error":{"message":"not found"}}"#)
        .expect(1)
        .create_async()
        .await;
    // PUTting to the dead id must never happen.
    let update = npm_server
        .mock("PUT", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
        .expect(0)
        .create_async()
        .await;
    // Instead the host is recreated with a fresh id.
    let create = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(r#"{"id":77,"domain_names":["a.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    let snap = Snapshot {
        machine_id: MachineId::new("m1").unwrap(),
        forward_host: "10.0.0.1".into(),
        taken_at: 1,
        agent_version: None,
        containers: vec![ContainerEntry {
            name: "app".into(),
            labels: npm_host_labels("a.example.com"),
        }],
    };
    // Seed the service row and a stale mapping so reconcile takes the
    // get/verify path against id 50.
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

    reconciler.reconcile_snapshot(&snap).await.unwrap();

    get.assert_async().await;
    update.assert_async().await;
    create.assert_async().await;
    // The mapping now points at the recreated host id.
    assert_eq!(
        storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
        Some(77)
    );
}

#[tokio::test]
async fn npm_adopts_existing_host_when_create_conflicts() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .expect_at_least(1)
        .create_async()
        .await;
    // Create conflicts because the host already exists in NPM.
    let create = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(400)
        .with_body(r#"{"error":{"message":"domain already in use"}}"#)
        .expect(1)
        .create_async()
        .await;
    // Adoption lists and finds id 61.
    let list = npm_server
        .mock("GET", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(r#"[{"id":61,"domain_names":["a.example.com"]}]"#)
        .expect(1)
        .create_async()
        .await;
    // The adopted host is then fetched; its stored state differs from the
    // payload (port 9999 vs 3000), so an update is issued.
    let get = npm_server
        .mock("GET", "/api/nginx/proxy-hosts/61")
        .with_status(200)
        .with_body(
            r#"{"id":61,"domain_names":["a.example.com"],"forward_scheme":"http",
                "forward_host":"10.0.0.1","forward_port":9999,"certificate_id":0}"#,
        )
        .expect(1)
        .create_async()
        .await;
    let update = npm_server
        .mock("PUT", "/api/nginx/proxy-hosts/61")
        .with_status(200)
        .with_body(r#"{"id":61,"domain_names":["a.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;

    let npm = crate::integrations::npm::NpmClient::new(crate::config::NpmConfig {
        url: url::Url::parse(&npm_server.url()).unwrap(),
        user: "u".into(),
        password: "p".into(),
    });
    let cf =
        crate::integrations::cloudflare::CloudflareClient::new(crate::config::CloudflareConfig {
            api_token: "t".into(),
            zone_id: "z".into(),
        });

    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: true,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    let mut labels = BTreeMap::new();
    labels.insert(
        "docktornet.npm.0.host".to_string(),
        "a.example.com".to_string(),
    );
    labels.insert("docktornet.npm.0.port".to_string(), "3000".to_string());
    labels.insert("docktornet.npm.0.scheme".to_string(), "http".to_string());
    labels.insert("docktornet.npm.0.ssl".to_string(), "false".to_string());

    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels,
            }],
        })
        .await
        .unwrap();

    create.assert_async().await;
    list.assert_async().await;
    get.assert_async().await;
    update.assert_async().await;

    let stored = storage::get_npm_host(&pool, "m1/app", 0).await.unwrap();
    assert_eq!(stored, Some(61));
}

#[tokio::test]
async fn npm_host_deferred_to_pending_when_service_absent_within_grace() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .create_async()
        .await;
    // A brief restart must NOT delete the proxy host.
    let delete = npm_server
        .mock("DELETE", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body("{}")
        .expect(0)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    let snap = Snapshot {
        machine_id: MachineId::new("m1").unwrap(),
        forward_host: "10.0.0.1".into(),
        taken_at: 100,
        agent_version: None,
        containers: vec![ContainerEntry {
            name: "app".into(),
            labels: npm_host_labels("a.example.com"),
        }],
    };
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

    // Service gone only 30s later — within the grace period.
    reconciler.set_now(130);
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 130,
            agent_version: None,
            containers: vec![],
        })
        .await
        .unwrap();

    delete.assert_async().await;
    // The host is recorded for a later sweep, carrying its original last_seen.
    assert_eq!(
        storage::list_pending_npm_removals(&pool).await.unwrap(),
        vec![("m1/app".to_string(), 0, 50, 100)]
    );
}

#[tokio::test]
async fn full_resync_prunes_pending_npm_removal_past_grace() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .create_async()
        .await;
    let delete = npm_server
        .mock("DELETE", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body("{}")
        .expect(1)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    // A pending removal whose last_seen is far in the past (well past grace).
    storage::add_pending_npm_removal(&pool, "m1/app", 0, 50, 1)
        .await
        .unwrap();

    reconciler.full_resync().await.unwrap();

    delete.assert_async().await;
    assert!(
        storage::list_pending_npm_removals(&pool)
            .await
            .unwrap()
            .is_empty()
    );
}

/// A reconciler with NPM and Cloudflare mocks that refuse any delete, for
/// tests where a removal must stay deferred.  The NPM mock still serves the
/// token and certificates an apply with NPM removals fetches.
async fn reconciler_refusing_deletes(
    dir: &std::path::Path,
    pool: Db,
) -> (
    Reconciler,
    mockito::ServerGuard,
    mockito::ServerGuard,
    Vec<mockito::Mock>,
) {
    let mut npm_server = mockito::Server::new_async().await;
    let mut cf_server = mockito::Server::new_async().await;
    let mocks = vec![
        npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .expect_at_least(1)
            .create_async()
            .await,
        npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .expect_at_least(1)
            .create_async()
            .await,
        npm_server
            .mock("DELETE", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body("{}")
            .expect(0)
            .create_async()
            .await,
        cf_server
            .mock("DELETE", "/zones/z/dns_records/rec-1")
            .with_status(200)
            .with_body(r#"{"success":true}"#)
            .expect(0)
            .create_async()
            .await,
    ];
    let (npm, cf) = test_clients_with_cf(&npm_server.url(), &cf_server.url());
    let reconciler = Reconciler::with_clients(
        pool,
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.join("gatus"),
        },
        npm,
        cf,
    );
    (reconciler, npm_server, cf_server, mocks)
}

/// Push `m1/app` with `taken_at`, give it NPM host 50 and Cloudflare record
/// `rec-1`, then push a snapshot without it at `left_at`.
async fn publish_then_drop(reconciler: &Reconciler, taken_at: u64, left_at: u64) {
    push_stamped(
        reconciler,
        taken_at,
        vec![ContainerEntry {
            name: "app".into(),
            labels: gatus_labels(),
        }],
    )
    .await;
    storage::set_npm_host(reconciler.pool(), "m1/app", 0, 50)
        .await
        .unwrap();
    storage::set_cf_dns_record(reconciler.pool(), "m1/app", "rec-1")
        .await
        .unwrap();
    push_stamped(reconciler, left_at, vec![]).await;
}

/// No delete reached NPM or Cloudflare, and both removals still wait.
async fn assert_removals_deferred(pool: &Db, mocks: &[mockito::Mock]) {
    for mock in mocks {
        mock.assert_async().await;
    }
    assert_eq!(
        storage::list_pending_npm_removals(pool)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        storage::list_pending_cf_removals(pool).await.unwrap().len(),
        1
    );
}

fn agent_now() -> u64 {
    unix_now() as u64
}

#[tokio::test]
async fn agent_clock_behind_the_server_keeps_the_grace_period() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let (reconciler, _npm, _cf, mocks) =
        reconciler_refusing_deletes(dir.path(), pool.clone()).await;

    // The agent's clock runs an hour behind; by it, the service left 10 s
    // after it was last seen.
    let agent_time = agent_now() - 3600;
    publish_then_drop(&reconciler, agent_time, agent_time + 10).await;
    reconciler.sweep_pending_removals().await.unwrap();

    assert_removals_deferred(&pool, &mocks).await;
}

#[tokio::test]
async fn snapshot_stamped_zero_keeps_the_grace_period() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let (reconciler, _npm, _cf, mocks) =
        reconciler_refusing_deletes(dir.path(), pool.clone()).await;

    // An agent that could not read its clock sends taken_at = 0.
    publish_then_drop(&reconciler, 0, agent_now()).await;
    reconciler.sweep_pending_removals().await.unwrap();

    assert_removals_deferred(&pool, &mocks).await;
}

#[tokio::test]
async fn npm_host_mapping_restored_when_service_reappears_within_grace() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .expect_at_least(1)
        .create_async()
        .await;
    // A returning service must UPDATE its existing proxy host, never create
    // a SECOND one for the same domain (which would orphan the original).
    let create = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(r#"{"id":99,"domain_names":["a.example.com"]}"#)
        .expect(0)
        .create_async()
        .await;
    // The now-live host must never be deleted.
    let delete = npm_server
        .mock("DELETE", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body("{}")
        .expect(0)
        .create_async()
        .await;
    // The restored mapping drives a fetch of the original host (id 50);
    // its stored port (9999) differs from the payload (3000), so it is
    // updated in place rather than recreated.
    let get = npm_server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(
            r#"{"id":50,"domain_names":["a.example.com"],"forward_scheme":"http",
                "forward_host":"10.0.0.1","forward_port":9999,"certificate_id":0}"#,
        )
        .expect(1)
        .create_async()
        .await;
    let update = npm_server
        .mock("PUT", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    // A pending removal left over from a brief disappearance.  The
    // npm_hosts mapping was cascade-deleted when the service left, so this
    // pending row is the only surviving record of host id 50.
    storage::add_pending_npm_removal(&pool, "m1/app", 0, 50, 100)
        .await
        .unwrap();

    // The service comes back in a fresh snapshot.
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 200,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels("a.example.com"),
            }],
        })
        .await
        .unwrap();

    create.assert_async().await;
    delete.assert_async().await;
    get.assert_async().await;
    update.assert_async().await;
    // The mapping is restored to the original host id and the pending row
    // is cleared.
    assert_eq!(
        storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
        Some(50)
    );
    assert!(
        storage::list_pending_npm_removals(&pool)
            .await
            .unwrap()
            .is_empty()
    );
}

fn npm_host_labels(host: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("docktornet.npm.0.host".to_string(), host.to_string());
    labels.insert("docktornet.npm.0.port".to_string(), "3000".to_string());
    labels.insert("docktornet.npm.0.scheme".to_string(), "http".to_string());
    labels.insert("docktornet.npm.0.ssl".to_string(), "false".to_string());
    labels
}

fn test_clients(
    npm_url: &str,
) -> (
    crate::integrations::npm::NpmClient,
    crate::integrations::cloudflare::CloudflareClient,
) {
    let npm = crate::integrations::npm::NpmClient::new(crate::config::NpmConfig {
        url: url::Url::parse(npm_url).unwrap(),
        user: "u".into(),
        password: "p".into(),
    });
    let cf =
        crate::integrations::cloudflare::CloudflareClient::new(crate::config::CloudflareConfig {
            api_token: "t".into(),
            zone_id: "z".into(),
        });
    (npm, cf)
}

/// Build NPM + Cloudflare clients where Cloudflare targets a mock server so
/// its DNS calls can be asserted (NPM points at `npm_url`, unused unless the
/// snapshot carries NPM labels).
fn test_clients_with_cf(
    npm_url: &str,
    cf_base: &str,
) -> (
    crate::integrations::npm::NpmClient,
    crate::integrations::cloudflare::CloudflareClient,
) {
    let npm = crate::integrations::npm::NpmClient::new(crate::config::NpmConfig {
        url: url::Url::parse(npm_url).unwrap(),
        user: "u".into(),
        password: "p".into(),
    });
    let cf = crate::integrations::cloudflare::CloudflareClient::with_base_url(
        crate::config::CloudflareConfig {
            api_token: "t".into(),
            zone_id: "z".into(),
        },
        cf_base,
    );
    (npm, cf)
}

fn cf_dns_labels(name: &str, target: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("docktornet.cf.dns.name".to_string(), name.to_string());
    labels.insert("docktornet.cf.dns.target".to_string(), target.to_string());
    labels.insert("docktornet.cf.dns.proxied".to_string(), "false".to_string());
    labels
}

#[tokio::test]
async fn cf_record_deleted_when_service_removed_past_grace() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut cf_server = mockito::Server::new_async().await;
    // The orphaned record must be deleted from Cloudflare.
    let delete = cf_server
        .mock("DELETE", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(r#"{"success":true}"#)
        .expect(1)
        .create_async()
        .await;

    let (npm, cf) = test_clients_with_cf(&cf_server.url(), &cf_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    // Seed a service that owns a Cloudflare DNS record.
    let snap = Snapshot {
        machine_id: MachineId::new("m1").unwrap(),
        forward_host: "10.0.0.1".into(),
        taken_at: 1,
        agent_version: None,
        containers: vec![ContainerEntry {
            name: "app".into(),
            labels: cf_dns_labels("app", "home.example.com"),
        }],
    };
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
        .await
        .unwrap();

    // The container is decommissioned: an empty snapshot, well past grace.
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: (1 + GATUS_REMOVAL_GRACE_SECS + 1) as u64,
            agent_version: None,
            containers: vec![],
        })
        .await
        .unwrap();

    delete.assert_async().await;
    assert_eq!(
        storage::get_cf_dns_record(&pool, "m1/app").await.unwrap(),
        None,
        "cf record mapping must be cleared after deletion"
    );
    assert!(
        storage::list_pending_cf_removals(&pool)
            .await
            .unwrap()
            .is_empty(),
        "no pending removal should remain once the record is deleted"
    );
}

#[tokio::test]
async fn cf_record_deferred_to_pending_when_service_absent_within_grace() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut cf_server = mockito::Server::new_async().await;
    // A brief restart must NOT delete the record.
    let delete = cf_server
        .mock("DELETE", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(r#"{"success":true}"#)
        .expect(0)
        .create_async()
        .await;

    let (npm, cf) = test_clients_with_cf(&cf_server.url(), &cf_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    let snap = Snapshot {
        machine_id: MachineId::new("m1").unwrap(),
        forward_host: "10.0.0.1".into(),
        taken_at: 100,
        agent_version: None,
        containers: vec![ContainerEntry {
            name: "app".into(),
            labels: cf_dns_labels("app", "home.example.com"),
        }],
    };
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
        .await
        .unwrap();

    // Gone only 30s later — within grace.
    reconciler.set_now(130);
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 130,
            agent_version: None,
            containers: vec![],
        })
        .await
        .unwrap();

    delete.assert_async().await;
    assert_eq!(
        storage::list_pending_cf_removals(&pool).await.unwrap(),
        vec![("m1/app".to_string(), "rec-1".to_string(), 100)],
        "the record must be recorded for a later sweep, carrying its last_seen"
    );
}

#[tokio::test]
async fn full_resync_prunes_pending_cf_removal_past_grace() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut cf_server = mockito::Server::new_async().await;
    let delete = cf_server
        .mock("DELETE", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(r#"{"success":true}"#)
        .expect(1)
        .create_async()
        .await;

    let (npm, cf) = test_clients_with_cf(&cf_server.url(), &cf_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    // A pending removal whose last_seen is far in the past (well past grace).
    storage::add_pending_cf_removal(&pool, "m1/app", "rec-1", 1)
        .await
        .unwrap();

    reconciler.full_resync().await.unwrap();

    delete.assert_async().await;
    assert!(
        storage::list_pending_cf_removals(&pool)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn cf_record_mapping_restored_when_service_reappears_within_grace() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut cf_server = mockito::Server::new_async().await;
    // Zone lookup for the returning service's reconcile.
    let _zone = cf_server
        .mock("GET", "/zones/z")
        .with_status(200)
        .with_body(r#"{"success":true,"result":{"id":"z","name":"example.com"}}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    // A returning service must UPDATE its existing DNS record, never create
    // a SECOND one for the same name (which would orphan the original).
    let create = cf_server
        .mock("POST", "/zones/z/dns_records")
        .with_status(200)
        .with_body(
            r#"{"success":true,"result":{"id":"rec-2","name":"app.example.com","content":"home.example.com","proxied":false}}"#,
        )
        .expect(0)
        .create_async()
        .await;
    // The now-live record must never be deleted.
    let delete = cf_server
        .mock("DELETE", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(r#"{"success":true}"#)
        .expect(0)
        .create_async()
        .await;
    // The reconcile fetches the existing record to drift-check it; it drifted
    // while the service was gone (stale content), so an update is warranted.
    let get = cf_server
        .mock("GET", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(
            r#"{"success":true,"result":{"id":"rec-1","name":"app.example.com","content":"stale.example.com","proxied":false}}"#,
        )
        .expect_at_least(1)
        .create_async()
        .await;
    // The restored mapping drives an update of the original record (rec-1).
    let update = cf_server
        .mock("PUT", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(
            r#"{"success":true,"result":{"id":"rec-1","name":"app.example.com","content":"home.example.com","proxied":false}}"#,
        )
        .expect(1)
        .create_async()
        .await;

    let (npm, cf) = test_clients_with_cf(&cf_server.url(), &cf_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    // A pending removal left over from a brief disappearance.  The
    // cf_dns_records mapping is already gone (cascade-deleted), so the
    // pending row is the only durable record of record id rec-1.
    storage::add_pending_cf_removal(&pool, "m1/app", "rec-1", 100)
        .await
        .unwrap();

    // The service comes back in a fresh snapshot.
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 200,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: cf_dns_labels("app", "home.example.com"),
            }],
        })
        .await
        .unwrap();

    create.assert_async().await;
    delete.assert_async().await;
    get.assert_async().await;
    update.assert_async().await;
    // The mapping is restored to the original record id and the pending row
    // is cleared.
    assert_eq!(
        storage::get_cf_dns_record(&pool, "m1/app").await.unwrap(),
        Some("rec-1".to_string())
    );
    assert!(
        storage::list_pending_cf_removals(&pool)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn cf_reconcile_skips_update_when_record_in_sync() {
    // A per-snapshot Cloudflare reconcile for a service whose DNS record is
    // already in its desired state must NOT issue a redundant update_record
    // PUT.  The snapshot intent hash is per-machine, so any unrelated label
    // change re-runs the intended loop; without a drift check every cf_dns
    // service on the machine gets a wasteful public-API write every time.
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut cf_server = mockito::Server::new_async().await;
    let _zone = cf_server
        .mock("GET", "/zones/z")
        .with_status(200)
        .with_body(r#"{"success":true,"result":{"id":"z","name":"example.com"}}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    // The existing record already matches the desired payload, so the
    // reconcile must fetch it to compare...
    let get = cf_server
        .mock("GET", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(
            r#"{"success":true,"result":{"id":"rec-1","name":"app.example.com","content":"home.example.com","proxied":false,"comment":"docktornet:m1/app"}}"#,
        )
        .expect_at_least(1)
        .create_async()
        .await;
    // ...and must NOT PUT an unchanged record.
    let update = cf_server
        .mock("PUT", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(
            r#"{"success":true,"result":{"id":"rec-1","name":"app.example.com","content":"home.example.com","proxied":false}}"#,
        )
        .expect(0)
        .create_async()
        .await;

    let (npm, cf) = test_clients_with_cf(&cf_server.url(), &cf_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    // Seed the service and its existing record mapping.
    let snap = Snapshot {
        machine_id: MachineId::new("m1").unwrap(),
        forward_host: "10.0.0.1".into(),
        taken_at: 1,
        agent_version: None,
        containers: vec![ContainerEntry {
            name: "app".into(),
            labels: cf_dns_labels("app", "home.example.com"),
        }],
    };
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
        .await
        .unwrap();

    reconciler.reconcile_snapshot(&snap).await.unwrap();

    get.assert_async().await;
    update.assert_async().await;
}

#[tokio::test]
async fn cf_record_deleted_when_a_running_container_drops_its_cf_labels() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut cf_server = mockito::Server::new_async().await;
    let delete = cf_server
        .mock("DELETE", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(r#"{"success":true}"#)
        .expect(1)
        .create_async()
        .await;

    let reconciler = reconciler_on(pool.clone(), &dir.path().join("gatus"), &cf_server);

    // The service owns a Cloudflare record and a Gatus monitor.
    let mut labels = gatus_labels();
    labels.extend(cf_dns_labels("app", "home.example.com"));
    storage::upsert_snapshot(
        &pool,
        &m1_snapshot(vec![ContainerEntry {
            name: "app".into(),
            labels,
        }]),
        1,
    )
    .await
    .unwrap();
    storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
        .await
        .unwrap();

    // The container keeps running with only its Gatus labels.
    push(&reconciler, 31, vec![gatus_entry("app")]).await;

    delete.assert_async().await;
    assert_eq!(
        storage::get_cf_dns_record(&pool, "m1/app").await.unwrap(),
        None,
        "the cf record mapping goes with the record"
    );
    assert!(
        storage::list_pending_cf_removals(&pool)
            .await
            .unwrap()
            .is_empty(),
        "a service still running owes no pending removal"
    );
    assert_eq!(
        storage::list_services_for_machine(&pool, "m1")
            .await
            .unwrap()
            .len(),
        1,
        "the service itself stays"
    );
}

#[tokio::test]
async fn npm_adoption_persists_mapping_even_if_update_fails() {
    use mockito::Matcher;

    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .expect_at_least(1)
        .create_async()
        .await;
    // Create conflicts: the host already exists in NPM.
    let _create = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(400)
        .with_body(r#"{"error":{"message":"domain already in use"}}"#)
        .expect(1)
        .create_async()
        .await;
    // Adoption finds the existing host id 61 ...
    let _list = npm_server
        .mock("GET", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(r#"[{"id":61,"domain_names":["a.example.com"]}]"#)
        .expect(1)
        .create_async()
        .await;
    // ... it is fetched and found to differ (port 9999 vs 3000) ...
    let _get = npm_server
        .mock("GET", "/api/nginx/proxy-hosts/61")
        .with_status(200)
        .with_body(
            r#"{"id":61,"domain_names":["a.example.com"],"forward_scheme":"http",
                "forward_host":"10.0.0.1","forward_port":9999,"certificate_id":0}"#,
        )
        .expect(1)
        .create_async()
        .await;
    // ... but the update to bring it in line fails (NPM overloaded).
    let _update = npm_server
        .mock("PUT", "/api/nginx/proxy-hosts/61")
        .match_body(Matcher::Any)
        .with_status(500)
        .with_body("boom")
        .expect(1)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: true,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    let result = reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels("a.example.com"),
            }],
        })
        .await;

    // The update failure is reported ...
    assert!(result.is_err(), "update failure should be surfaced");
    // ... but the adopted id is persisted so the next reconcile takes the
    // get/update path instead of re-adopting (and re-reloading nginx).
    assert_eq!(
        storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
        Some(61),
        "adopted npm_id must be stored before the update is attempted"
    );
}

#[tokio::test]
async fn npm_fetches_token_and_certs_once_per_snapshot() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    // Exactly one token fetch and one certificate list for the whole
    // snapshot, no matter how many NPM hosts it contains.
    let token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect(1)
        .create_async()
        .await;
    let certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .expect(1)
        .create_async()
        .await;
    // No host is marked for the service, so marker adoption finds nothing.
    let list = npm_server
        .mock("GET", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body("[]")
        .expect(1)
        .create_async()
        .await;
    let create = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(r#"{"id":1,"domain_names":["x"]}"#)
        .expect(2)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool,
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![
                ContainerEntry {
                    name: "app-a".into(),
                    labels: npm_host_labels("a.example.com"),
                },
                ContainerEntry {
                    name: "app-b".into(),
                    labels: npm_host_labels("b.example.com"),
                },
            ],
        })
        .await
        .unwrap();

    token.assert_async().await;
    certs.assert_async().await;
    // Both hosts are unmapped, but the host list is fetched once per pass.
    list.assert_async().await;
    create.assert_async().await;
}

#[tokio::test]
async fn npm_failure_for_one_service_does_not_block_others() {
    use mockito::Matcher;

    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .expect_at_least(1)
        .create_async()
        .await;
    // No host is marked for the service, so marker adoption finds nothing.
    let _list = npm_server
        .mock("GET", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body("[]")
        .create_async()
        .await;
    // The first service's host creation fails outright.
    let create_a = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .match_body(Matcher::PartialJson(
            serde_json::json!({ "domain_names": ["a.example.com"] }),
        ))
        .with_status(500)
        .with_body("boom")
        .expect(1)
        .create_async()
        .await;
    // The second service must still be attempted despite the first failing.
    let create_b = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .match_body(Matcher::PartialJson(
            serde_json::json!({ "domain_names": ["b.example.com"] }),
        ))
        .with_status(200)
        .with_body(r#"{"id":2,"domain_names":["b.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool,
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    let result = reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![
                ContainerEntry {
                    name: "app-a".into(),
                    labels: npm_host_labels("a.example.com"),
                },
                ContainerEntry {
                    name: "app-b".into(),
                    labels: npm_host_labels("b.example.com"),
                },
            ],
        })
        .await;

    create_a.assert_async().await;
    // The batch continued to the second service after the first failed.
    create_b.assert_async().await;
    // The failure is still surfaced (for logging), not silently swallowed.
    assert!(result.is_err(), "partial failure should be reported");
}

#[tokio::test]
async fn npm_ssl_without_matching_certificate_does_not_publish_plaintext() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    // NPM has no certificates, so ssl=true cannot resolve to a cert.
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .expect_at_least(1)
        .create_async()
        .await;
    // The proxy host must NOT be created over plain HTTP when TLS was
    // requested but no certificate matched the hostname.
    let create = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(r#"{"id":1,"domain_names":["app.example.com"]}"#)
        .expect(0)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool,
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    let mut labels = npm_host_labels("app.example.com");
    labels.insert("docktornet.npm.0.ssl".to_string(), "true".to_string());

    let result = reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels,
            }],
        })
        .await;

    // TLS was requested but no cert resolved: reconcile must fail loudly
    // rather than silently publishing an HTTP-only proxy host.
    assert!(
        result.is_err(),
        "ssl=true with no matching certificate must not silently downgrade to plaintext"
    );
    create.assert_async().await;
}

#[tokio::test]
async fn reconcile_skips_npm_calls_when_snapshot_unchanged() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    // Exactly one of each: the first push creates the host; the second
    // (identical) push must be short-circuited and touch NPM not at all.
    let token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect(1)
        .create_async()
        .await;
    let certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .expect(1)
        .create_async()
        .await;
    // No host is marked for the service, so marker adoption finds nothing.
    let _list = npm_server
        .mock("GET", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body("[]")
        .create_async()
        .await;
    let create = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(r#"{"id":1,"domain_names":["a.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool,
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    let snap = |taken_at| Snapshot {
        machine_id: MachineId::new("m1").unwrap(),
        forward_host: "10.0.0.1".into(),
        taken_at,
        containers: vec![ContainerEntry {
            name: "app".into(),
            labels: npm_host_labels("a.example.com"),
        }],
        agent_version: None,
    };

    // Two identical pushes (only taken_at differs, which is not part of the
    // intent hash).
    reconciler.reconcile_snapshot(&snap(1)).await.unwrap();
    reconciler.reconcile_snapshot(&snap(2)).await.unwrap();

    token.assert_async().await;
    certs.assert_async().await;
    create.assert_async().await;
}

#[tokio::test]
async fn npm_adopts_marked_host_without_create_or_flag() {
    // The database is empty and DOCKTORNET_ADOPT_EXISTING is off, but the
    // host carries this service's marker, so it is adopted with no create.
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .expect_at_least(1)
        .create_async()
        .await;
    let create = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(400)
        .with_body(r#"{"error":{"message":"domain already in use"}}"#)
        .expect(0)
        .create_async()
        .await;
    let _list = npm_server
        .mock("GET", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(
            r#"[{"id":90,"domain_names":["a.example.com"],
                 "meta":{"docktornet":{"service_id":"m1/app","npm_index":0}}}]"#,
        )
        .expect(1)
        .create_async()
        .await;
    // The adopted host already matches the payload, so it must NOT be
    // updated — avoiding a needless nginx reload.
    let _get = npm_server
        .mock("GET", "/api/nginx/proxy-hosts/90")
        .with_status(200)
        .with_body(
            r#"{"id":90,"domain_names":["a.example.com"],"forward_scheme":"http",
                "forward_host":"10.0.0.1","forward_port":3000,"certificate_id":0,
                "ssl_forced":0,"http2_support":0,"hsts_enabled":0,
                "hsts_subdomains":0,"block_exploits":1,"allow_websocket_upgrade":1,
                "meta":{"docktornet":{"service_id":"m1/app","npm_index":0}}}"#,
        )
        .expect(1)
        .create_async()
        .await;
    let update = npm_server
        .mock("PUT", "/api/nginx/proxy-hosts/90")
        .with_status(200)
        .with_body(r#"{"id":90,"domain_names":["a.example.com"]}"#)
        .expect(0)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels("a.example.com"),
            }],
        })
        .await
        .unwrap();

    // No create and no PUT was issued, but the mapping was still adopted.
    create.assert_async().await;
    update.assert_async().await;
    assert_eq!(
        storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
        Some(90)
    );
}

#[tokio::test]
async fn npm_adopt_refuses_to_drop_domains_of_matched_host() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .expect_at_least(1)
        .create_async()
        .await;
    let _create = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(400)
        .with_body(r#"{"error":{"message":"domain already in use"}}"#)
        .expect(1)
        .create_async()
        .await;
    // The pre-existing NPM host carries a second domain (b.example.com) that
    // our labels do not manage. It is matched by the single overlapping
    // domain a.example.com.
    let _list = npm_server
        .mock("GET", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(r#"[{"id":91,"domain_names":["a.example.com","b.example.com"]}]"#)
        .expect(1)
        .create_async()
        .await;
    // Provide the GET the (buggy) adoption path would follow, so the failing
    // signal is the clobbering PUT below rather than a missing mock.
    let _get = npm_server
        .mock("GET", "/api/nginx/proxy-hosts/91")
        .with_status(200)
        .with_body(
            r#"{"id":91,"domain_names":["a.example.com","b.example.com"],"forward_scheme":"http",
                "forward_host":"10.0.0.1","forward_port":3000,"certificate_id":0,
                "ssl_forced":0,"http2_support":0,"hsts_enabled":0,
                "hsts_subdomains":0,"block_exploits":1,"allow_websocket_upgrade":1}"#,
        )
        .create_async()
        .await;
    // A PUT here would overwrite domain_names with only [a.example.com],
    // silently dropping b.example.com. It must never be issued.
    let update = npm_server
        .mock("PUT", "/api/nginx/proxy-hosts/91")
        .with_status(200)
        .with_body(r#"{"id":91,"domain_names":["a.example.com"]}"#)
        .expect(0)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: true,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    // Adoption is refused (it would drop an unmanaged domain), so the
    // reconcile surfaces a failure.
    let result = reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels("a.example.com"),
            }],
        })
        .await;

    assert!(
        result.is_err(),
        "reconcile should surface the refused adoption"
    );
    // No clobbering PUT was issued...
    update.assert_async().await;
    // ...and the host that serves more than we manage was not taken over.
    assert_eq!(
        storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn npm_circuit_breaker_stops_calls_after_repeated_failures() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .expect_at_least(1)
        .create_async()
        .await;
    // No host is marked for the service, so marker adoption finds nothing.
    let _list = npm_server
        .mock("GET", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body("[]")
        .create_async()
        .await;
    // Every create fails.  The breaker (threshold 3) must open after 3
    // failing cycles and stop issuing creates thereafter.
    let create = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(500)
        .with_body("boom")
        .expect(NPM_BREAKER_THRESHOLD as usize)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool,
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    let snap = Snapshot {
        machine_id: MachineId::new("m1").unwrap(),
        forward_host: "10.0.0.1".into(),
        taken_at: 1,
        agent_version: None,
        containers: vec![ContainerEntry {
            name: "app".into(),
            labels: npm_host_labels("a.example.com"),
        }],
    };

    // Push more times than the threshold; failing cycles never cache the
    // intent hash, so each push re-runs until the breaker opens.
    for _ in 0..(NPM_BREAKER_THRESHOLD + 3) {
        let _ = reconciler.reconcile_snapshot(&snap).await;
    }

    // create was attempted exactly `threshold` times, then the breaker
    // suppressed further NPM calls.
    create.assert_async().await;
}

#[tokio::test]
async fn npm_host_deleted_when_service_absent_past_grace_period() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

    let mut npm_server = mockito::Server::new_async().await;
    let _token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .expect_at_least(1)
        .create_async()
        .await;
    let _certs = npm_server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .create_async()
        .await;
    // The proxy host for the departed service must be deleted exactly once.
    let delete = npm_server
        .mock("DELETE", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body("{}")
        .expect(1)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );

    // Seed a service with a mapped NPM host id.
    let snap = Snapshot {
        machine_id: MachineId::new("m1").unwrap(),
        forward_host: "10.0.0.1".into(),
        taken_at: 1,
        agent_version: None,
        containers: vec![ContainerEntry {
            name: "app".into(),
            labels: npm_host_labels("a.example.com"),
        }],
    };
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

    // Service leaves the snapshot, well past the grace period.
    reconciler
        .reconcile_snapshot(&Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: (1 + GATUS_REMOVAL_GRACE_SECS + 1) as u64,
            agent_version: None,
            containers: vec![],
        })
        .await
        .unwrap();

    delete.assert_async().await;
    // Pruned outright, so nothing is left pending.
    assert!(
        storage::list_pending_npm_removals(&pool)
            .await
            .unwrap()
            .is_empty()
    );
}

/// An NPM mock answering the token and certificate calls every pass makes.
async fn npm_server_with_auth() -> mockito::ServerGuard {
    let mut server = mockito::Server::new_async().await;
    server
        .mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .create_async()
        .await;
    server
        .mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .create_async()
        .await;
    server
}

fn reconciler_for(
    pool: Db,
    dir: &std::path::Path,
    npm_url: &str,
    cf_url: &str,
    dry_run: bool,
    adopt_existing: bool,
) -> Reconciler {
    let (npm, cf) = test_clients_with_cf(npm_url, cf_url);
    Reconciler::with_clients(
        pool,
        ReconcileOptions {
            dry_run,
            adopt_existing,
            gatus_config_dir: dir.join("gatus"),
        },
        npm,
        cf,
    )
}

/// Mocks on `server` that fail the test if any request reaches it.
async fn refuse_every_request(server: &mut mockito::ServerGuard) -> Vec<mockito::Mock> {
    let mut mocks = Vec::new();
    for method in ["GET", "POST", "PUT", "DELETE"] {
        mocks.push(
            server
                .mock(method, mockito::Matcher::Any)
                .with_status(200)
                .with_body("{}")
                .expect(0)
                .create_async()
                .await,
        );
    }
    mocks
}

fn snap_with(labels: BTreeMap<String, String>) -> Snapshot {
    Snapshot {
        machine_id: MachineId::new("m1").unwrap(),
        forward_host: "10.0.0.1".into(),
        taken_at: 1,
        agent_version: None,
        containers: vec![ContainerEntry {
            name: "app".into(),
            labels,
        }],
    }
}

#[tokio::test]
async fn npm_create_writes_ownership_marker() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut npm_server = npm_server_with_auth().await;
    npm_server
        .mock("GET", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body("[]")
        .create_async()
        .await;
    let create = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .match_body(mockito::Matcher::PartialJson(serde_json::json!({
            "meta": {"docktornet": {"service_id": "m1/app", "npm_index": 0}}
        })))
        .with_status(200)
        .with_body(r#"{"id":5,"domain_names":["a.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &npm_server.url(),
        &npm_server.url(),
        false,
        false,
    );
    reconciler
        .reconcile_snapshot(&snap_with(npm_host_labels("a.example.com")))
        .await
        .unwrap();

    create.assert_async().await;
    assert_eq!(
        storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
        Some(5)
    );
}

/// NPM's stored state for host 50 serving `a.example.com` exactly as
/// `npm_host_labels` asks, with `meta` as given. `certificate_id` 0 is how
/// NPM stores a host with no certificate.
fn stored_host_50(meta: serde_json::Value) -> String {
    serde_json::json!({
        "id": 50, "domain_names": ["a.example.com"], "forward_scheme": "http",
        "forward_host": "10.0.0.1", "forward_port": 3000, "certificate_id": 0,
        "ssl_forced": 0, "http2_support": 0, "hsts_enabled": 0,
        "hsts_subdomains": 0, "block_exploits": 1, "allow_websocket_upgrade": 1,
        "meta": meta
    })
    .to_string()
}

fn app_marker() -> serde_json::Value {
    serde_json::json!({"service_id": "m1/app", "npm_index": 0})
}

#[tokio::test]
async fn npm_update_keeps_every_other_meta_key() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut npm_server = npm_server_with_auth().await;
    // The port drifted, so the host is updated; its meta holds NPM's own
    // keys and one set in the UI.
    let mut stored: serde_json::Value = serde_json::from_str(&stored_host_50(serde_json::json!({
        "nginx_online": true,
        "nginx_err": null,
        "dns_challenge": true,
        "custom": "kept",
        "docktornet": app_marker()
    })))
    .unwrap();
    stored["forward_port"] = 9999.into();
    npm_server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(stored.to_string())
        .create_async()
        .await;
    let update = npm_server
        .mock("PUT", "/api/nginx/proxy-hosts/50")
        .match_body(mockito::Matcher::PartialJson(serde_json::json!({
            "forward_port": 3000,
            "meta": {
                "nginx_online": true,
                "nginx_err": null,
                "dns_challenge": true,
                "custom": "kept",
                "docktornet": app_marker()
            }
        })))
        .with_status(200)
        .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &npm_server.url(),
        &npm_server.url(),
        false,
        false,
    );
    let snap = snap_with(npm_host_labels("a.example.com"));
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

    reconciler.reconcile_snapshot(&snap).await.unwrap();

    update.assert_async().await;
}

#[tokio::test]
async fn npm_nginx_status_keys_alone_are_not_drift() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut npm_server = npm_server_with_auth().await;
    npm_server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(stored_host_50(serde_json::json!({
            "nginx_online": false,
            "nginx_err": "nginx: [emerg] something broke",
            "docktornet": app_marker()
        })))
        .create_async()
        .await;
    let update = npm_server
        .mock("PUT", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
        .expect(0)
        .create_async()
        .await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &npm_server.url(),
        &npm_server.url(),
        false,
        false,
    );
    let snap = snap_with(npm_host_labels("a.example.com"));
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

    reconciler.reconcile_snapshot(&snap).await.unwrap();
    reconciler.full_resync().await.unwrap();

    update.assert_async().await;
}

/// A reconciler over `m1/app` serving `host` as NPM host 50, with NPM
/// answering `stored` for that host and expecting `puts` updates.
async fn npm_resync_fixture(
    dir: &std::path::Path,
    host: &str,
    stored: String,
    puts: usize,
) -> (Reconciler, mockito::ServerGuard, mockito::Mock) {
    let pool = db::connect(&dir.join("t.db")).await.unwrap();
    let mut npm_server = npm_server_with_auth().await;
    npm_server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(stored)
        .create_async()
        .await;
    let update = npm_server
        .mock("PUT", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
        .expect(puts)
        .create_async()
        .await;
    let reconciler = reconciler_for(
        pool.clone(),
        dir,
        &npm_server.url(),
        &npm_server.url(),
        false,
        false,
    );
    let snap = snap_with(npm_host_labels(host));
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();
    (reconciler, npm_server, update)
}

#[tokio::test]
async fn full_resync_repairs_drifted_npm_forward_port() {
    let dir = tempdir().unwrap();
    let mut stored: serde_json::Value = serde_json::from_str(&stored_host_50(
        serde_json::json!({"docktornet": app_marker()}),
    ))
    .unwrap();
    stored["forward_port"] = 9999.into();
    let (reconciler, _npm_server, update) =
        npm_resync_fixture(dir.path(), "a.example.com", stored.to_string(), 1).await;

    let report = reconciler.full_resync().await.unwrap();

    update.assert_async().await;
    assert_eq!(
        report,
        ResyncReport {
            verified: 0,
            repaired: 1,
            failed: 0
        }
    );
}

#[tokio::test]
async fn full_resync_leaves_npm_host_in_sync_alone() {
    let dir = tempdir().unwrap();
    let stored = stored_host_50(serde_json::json!({"docktornet": app_marker()}));
    let (reconciler, _npm_server, update) =
        npm_resync_fixture(dir.path(), "a.example.com", stored, 0).await;

    let report = reconciler.full_resync().await.unwrap();

    update.assert_async().await;
    assert_eq!(
        report,
        ResyncReport {
            verified: 1,
            repaired: 0,
            failed: 0
        }
    );
}

#[tokio::test]
async fn full_resync_ignores_npm_sorting_the_domain_names() {
    let dir = tempdir().unwrap();
    // NPM sorts domain_names on every write, whatever order the labels use.
    let mut stored: serde_json::Value = serde_json::from_str(&stored_host_50(
        serde_json::json!({"docktornet": app_marker()}),
    ))
    .unwrap();
    stored["domain_names"] = serde_json::json!(["a.example.com", "b.example.com"]);
    let (reconciler, _npm_server, update) = npm_resync_fixture(
        dir.path(),
        "b.example.com,a.example.com",
        stored.to_string(),
        0,
    )
    .await;

    let report = reconciler.full_resync().await.unwrap();

    update.assert_async().await;
    assert_eq!(report.verified, 1);
}

#[tokio::test]
async fn full_resync_writes_missing_npm_marker_once() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut npm_server = npm_server_with_auth().await;
    // Before the upgrade the host carries no marker...
    let unmarked = npm_server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(stored_host_50(serde_json::json!({"nginx_online": true})))
        .expect(1)
        .create_async()
        .await;
    // ...so the first resync writes it, keeping NPM's own key.
    let update = npm_server
        .mock("PUT", "/api/nginx/proxy-hosts/50")
        .match_body(mockito::Matcher::PartialJson(serde_json::json!({
            "meta": {"nginx_online": true, "docktornet": app_marker()}
        })))
        .with_status(200)
        .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &npm_server.url(),
        &npm_server.url(),
        false,
        false,
    );
    let snap = snap_with(npm_host_labels("a.example.com"));
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

    let first = reconciler.full_resync().await.unwrap();
    unmarked.assert_async().await;
    update.assert_async().await;
    assert_eq!(
        first,
        ResyncReport {
            verified: 0,
            repaired: 1,
            failed: 0
        }
    );

    // NPM now holds the marker, and the second resync leaves it alone.
    unmarked.remove_async().await;
    npm_server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(stored_host_50(serde_json::json!({
            "nginx_online": true,
            "docktornet": app_marker()
        })))
        .create_async()
        .await;
    let second = reconciler.full_resync().await.unwrap();
    update.assert_async().await;
    assert_eq!(
        second,
        ResyncReport {
            verified: 1,
            repaired: 0,
            failed: 0
        }
    );
}

#[tokio::test]
async fn full_resync_counts_a_recreated_npm_host_as_repaired() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut npm_server = npm_server_with_auth().await;
    npm_server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(404)
        .create_async()
        .await;
    let create = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(r#"{"id":51,"domain_names":["a.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;
    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &npm_server.url(),
        &npm_server.url(),
        false,
        false,
    );
    let snap = snap_with(npm_host_labels("a.example.com"));
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

    let report = reconciler.full_resync().await.unwrap();

    create.assert_async().await;
    assert_eq!(
        report,
        ResyncReport {
            verified: 0,
            repaired: 1,
            failed: 0
        }
    );
    assert_eq!(
        storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
        Some(51)
    );
}

#[tokio::test]
async fn npm_marker_adoption_drops_domains_the_labels_no_longer_list() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut npm_server = npm_server_with_auth().await;
    let extra = serde_json::json!({
        "id": 50, "domain_names": ["a.example.com", "old.example.com"],
        "forward_scheme": "http", "forward_host": "10.0.0.1", "forward_port": 3000,
        "certificate_id": 0, "meta": {"docktornet": app_marker()}
    });
    npm_server
        .mock("GET", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(format!("[{extra}]"))
        .create_async()
        .await;
    npm_server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(extra.to_string())
        .create_async()
        .await;
    let create = npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(r#"{"id":51,"domain_names":["a.example.com"]}"#)
        .expect(0)
        .create_async()
        .await;
    let update = npm_server
        .mock("PUT", "/api/nginx/proxy-hosts/50")
        .match_body(mockito::Matcher::PartialJson(serde_json::json!({
            "domain_names": ["a.example.com"]
        })))
        .with_status(200)
        .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &npm_server.url(),
        &npm_server.url(),
        false,
        false,
    );
    reconciler
        .reconcile_snapshot(&snap_with(npm_host_labels("a.example.com")))
        .await
        .unwrap();

    create.assert_async().await;
    update.assert_async().await;
    assert_eq!(
        storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
        Some(50)
    );
}

#[tokio::test]
async fn npm_hostname_adoption_writes_the_marker() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut npm_server = npm_server_with_auth().await;
    npm_server
        .mock("GET", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(r#"[{"id":50,"domain_names":["a.example.com"],"meta":{}}]"#)
        .create_async()
        .await;
    npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(400)
        .with_body(r#"{"error":{"message":"domain already in use"}}"#)
        .create_async()
        .await;
    // In sync apart from the missing marker.
    npm_server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(stored_host_50(serde_json::json!({})))
        .create_async()
        .await;
    let update = npm_server
        .mock("PUT", "/api/nginx/proxy-hosts/50")
        .match_body(mockito::Matcher::PartialJson(serde_json::json!({
            "meta": {"docktornet": app_marker()}
        })))
        .with_status(200)
        .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &npm_server.url(),
        &npm_server.url(),
        false,
        true,
    );
    reconciler
        .reconcile_snapshot(&snap_with(npm_host_labels("a.example.com")))
        .await
        .unwrap();

    update.assert_async().await;
    assert_eq!(
        storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
        Some(50)
    );
}

#[tokio::test]
async fn npm_hostname_adoption_refuses_a_host_marked_for_another_service() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut npm_server = npm_server_with_auth().await;
    npm_server
        .mock("GET", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(
            r#"[{"id":50,"domain_names":["a.example.com"],
                 "meta":{"docktornet":{"service_id":"m2/other","npm_index":0}}}]"#,
        )
        .create_async()
        .await;
    npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(400)
        .with_body(r#"{"error":{"message":"domain already in use"}}"#)
        .create_async()
        .await;
    let get = npm_server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(stored_host_50(serde_json::json!({})))
        .expect(0)
        .create_async()
        .await;
    let update = npm_server
        .mock("PUT", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
        .expect(0)
        .create_async()
        .await;

    let (npm, cf) = test_clients(&npm_server.url());
    let reconciler = Reconciler::with_clients(
        pool.clone(),
        ReconcileOptions {
            dry_run: false,
            adopt_existing: true,
            gatus_config_dir: dir.path().join("gatus"),
        },
        npm,
        cf,
    );
    let snap = snap_with(npm_host_labels("a.example.com"));
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    let host = docktornet_core::labels::parse(&npm_host_labels("a.example.com"))
        .npm
        .remove(0);
    let (token, certs) = reconciler.fetch_npm_context().await.unwrap();

    let err = reconciler
        .reconcile_npm(
            "m1/app",
            &host,
            "10.0.0.1",
            &token,
            &certs,
            &NpmHostList::default(),
        )
        .await
        .unwrap_err();

    match err {
        ReconcileError::MarkedForOtherService {
            service_id, owner, ..
        } => {
            assert_eq!(service_id, "m1/app");
            assert_eq!(owner, "m2/other (index 0)");
        }
        other => panic!("expected MarkedForOtherService, got {other:?}"),
    }
    get.assert_async().await;
    update.assert_async().await;
    assert_eq!(
        storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn npm_host_adopted_earlier_in_the_pass_is_refused_to_the_next_service() {
    // Two containers label the same domain and one unmarked host serves
    // it. The first adopts it by hostname and marks it; the second must
    // see that marker, though the host list was fetched before it.
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut npm_server = npm_server_with_auth().await;
    let list = npm_server
        .mock("GET", "/api/nginx/proxy-hosts")
        .with_status(200)
        .with_body(r#"[{"id":50,"domain_names":["a.example.com"],"meta":{}}]"#)
        .expect(1)
        .create_async()
        .await;
    npm_server
        .mock("POST", "/api/nginx/proxy-hosts")
        .with_status(400)
        .with_body(r#"{"error":{"message":"domain already in use"}}"#)
        .create_async()
        .await;
    npm_server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(stored_host_50(serde_json::json!({})))
        .create_async()
        .await;
    let update = npm_server
        .mock("PUT", "/api/nginx/proxy-hosts/50")
        .match_body(mockito::Matcher::PartialJson(serde_json::json!({
            "meta": {"docktornet": app_marker()}
        })))
        .with_status(200)
        .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &npm_server.url(),
        &npm_server.url(),
        false,
        true,
    );
    let mut snap = snap_with(npm_host_labels("a.example.com"));
    snap.containers.push(ContainerEntry {
        name: "app2".into(),
        labels: npm_host_labels("a.example.com"),
    });

    let result = reconciler.reconcile_snapshot(&snap).await;

    assert!(result.is_err(), "the second service must be refused");
    list.assert_async().await;
    update.assert_async().await;
    assert_eq!(
        storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
        Some(50)
    );
    assert_eq!(
        storage::get_npm_host(&pool, "m1/app2", 0).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn dry_run_writes_no_npm_marker() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut npm_server = mockito::Server::new_async().await;
    // Dry-run reaches NPM for nothing: no token, no read, no write.
    let refused = refuse_every_request(&mut npm_server).await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &npm_server.url(),
        &npm_server.url(),
        true,
        true,
    );
    let snap = snap_with(npm_host_labels("a.example.com"));
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

    reconciler.reconcile_snapshot(&snap).await.unwrap();
    assert_eq!(reconciler.full_resync().await.unwrap().failed, 0);

    for mock in refused {
        mock.assert_async().await;
    }
}

/// A Cloudflare mock answering the zone call every pass makes.
async fn cf_server_with_zone() -> mockito::ServerGuard {
    let mut server = mockito::Server::new_async().await;
    server
        .mock("GET", "/zones/z")
        .with_status(200)
        .with_body(r#"{"success":true,"result":{"id":"z","name":"example.com"}}"#)
        .create_async()
        .await;
    server
}

fn cf_list(records: serde_json::Value) -> String {
    serde_json::json!({"success": true, "result": records}).to_string()
}

fn cf_record(id: &str, comment: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "id": id, "name": "app.example.com", "content": "home.example.com",
        "proxied": false, "comment": comment
    })
}

fn cf_one(record: serde_json::Value) -> String {
    serde_json::json!({"success": true, "result": record}).to_string()
}

#[tokio::test]
async fn cf_create_writes_ownership_marker() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut cf_server = cf_server_with_zone().await;
    let lookup = cf_server
        .mock("GET", "/zones/z/dns_records")
        .match_query(mockito::Matcher::UrlEncoded(
            "comment.exact".into(),
            "docktornet:m1/app".into(),
        ))
        .with_status(200)
        .with_body(cf_list(serde_json::json!([])))
        .expect(1)
        .create_async()
        .await;
    let create = cf_server
        .mock("POST", "/zones/z/dns_records")
        .match_body(mockito::Matcher::PartialJson(serde_json::json!({
            "name": "app.example.com",
            "comment": "docktornet:m1/app"
        })))
        .with_status(200)
        .with_body(cf_one(cf_record("rec-1", Some("docktornet:m1/app"))))
        .expect(1)
        .create_async()
        .await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &cf_server.url(),
        &cf_server.url(),
        false,
        false,
    );
    reconciler
        .reconcile_snapshot(&snap_with(cf_dns_labels("app", "home.example.com")))
        .await
        .unwrap();

    lookup.assert_async().await;
    create.assert_async().await;
    assert_eq!(
        storage::get_cf_dns_record(&pool, "m1/app").await.unwrap(),
        Some("rec-1".to_string())
    );
}

#[tokio::test]
async fn full_resync_writes_missing_cf_marker_once() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut cf_server = cf_server_with_zone().await;
    let unmarked = cf_server
        .mock("GET", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(cf_one(cf_record("rec-1", None)))
        .expect(1)
        .create_async()
        .await;
    let update = cf_server
        .mock("PUT", "/zones/z/dns_records/rec-1")
        .match_body(mockito::Matcher::PartialJson(serde_json::json!({
            "comment": "docktornet:m1/app"
        })))
        .with_status(200)
        .with_body(cf_one(cf_record("rec-1", Some("docktornet:m1/app"))))
        .expect(1)
        .create_async()
        .await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &cf_server.url(),
        &cf_server.url(),
        false,
        false,
    );
    let snap = snap_with(cf_dns_labels("app", "home.example.com"));
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
        .await
        .unwrap();

    let first = reconciler.full_resync().await.unwrap();
    unmarked.assert_async().await;
    update.assert_async().await;
    assert_eq!(
        first,
        ResyncReport {
            verified: 0,
            repaired: 1,
            failed: 0
        }
    );

    unmarked.remove_async().await;
    cf_server
        .mock("GET", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(cf_one(cf_record("rec-1", Some("docktornet:m1/app"))))
        .create_async()
        .await;
    let second = reconciler.full_resync().await.unwrap();
    update.assert_async().await;
    assert_eq!(
        second,
        ResyncReport {
            verified: 1,
            repaired: 0,
            failed: 0
        }
    );
}

#[tokio::test]
async fn full_resync_counts_a_recreated_cf_record_as_repaired() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut cf_server = cf_server_with_zone().await;
    cf_server
        .mock("GET", "/zones/z/dns_records/rec-1")
        .with_status(404)
        .create_async()
        .await;
    let create = cf_server
        .mock("POST", "/zones/z/dns_records")
        .with_status(200)
        .with_body(cf_one(cf_record("rec-2", Some("docktornet:m1/app"))))
        .expect(1)
        .create_async()
        .await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &cf_server.url(),
        &cf_server.url(),
        false,
        false,
    );
    let snap = snap_with(cf_dns_labels("app", "home.example.com"));
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
        .await
        .unwrap();

    let report = reconciler.full_resync().await.unwrap();

    create.assert_async().await;
    assert_eq!(
        report,
        ResyncReport {
            verified: 0,
            repaired: 1,
            failed: 0
        }
    );
}

/// A snapshot of `m1/app` and `m1/web`, each asking for a Cloudflare record.
fn two_cf_services_snap() -> Snapshot {
    let mut snap = snap_with(cf_dns_labels("app", "home.example.com"));
    snap.containers.push(ContainerEntry {
        name: "web".into(),
        labels: cf_dns_labels("web", "home.example.com"),
    });
    snap
}

/// The Cloudflare record `id` for service `m1/<name>`, as its labels want it.
fn cf_service_record(id: &str, name: &str) -> String {
    cf_one(serde_json::json!({
        "id": id, "name": format!("{name}.example.com"), "content": "home.example.com",
        "proxied": false, "comment": format!("docktornet:m1/{name}")
    }))
}

/// A Cloudflare mock holding `rec-app` and `rec-web` as
/// `two_cf_services_snap` wants them, and expecting one zone fetch.
async fn cf_server_with_two_records() -> (mockito::ServerGuard, mockito::Mock) {
    let mut cf_server = mockito::Server::new_async().await;
    let zone = cf_server
        .mock("GET", "/zones/z")
        .with_status(200)
        .with_body(r#"{"success":true,"result":{"id":"z","name":"example.com"}}"#)
        .expect(1)
        .create_async()
        .await;
    for name in ["app", "web"] {
        cf_server
            .mock("GET", format!("/zones/z/dns_records/rec-{name}").as_str())
            .with_status(200)
            .with_body(cf_service_record(&format!("rec-{name}"), name))
            .create_async()
            .await;
    }
    (cf_server, zone)
}

#[tokio::test]
async fn full_resync_fetches_the_cf_zone_once_for_two_services() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let (cf_server, zone) = cf_server_with_two_records().await;
    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &cf_server.url(),
        &cf_server.url(),
        false,
        false,
    );
    let snap = two_cf_services_snap();
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    for name in ["app", "web"] {
        storage::set_cf_dns_record(&pool, &format!("m1/{name}"), &format!("rec-{name}"))
            .await
            .unwrap();
    }

    let report = reconciler.full_resync().await.unwrap();

    zone.assert_async().await;
    assert_eq!(
        report,
        ResyncReport {
            verified: 2,
            repaired: 0,
            failed: 0
        }
    );
}

#[tokio::test]
async fn cf_apply_then_resync_fetches_the_zone_once() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let (mut cf_server, zone) = cf_server_with_two_records().await;
    cf_server
        .mock("GET", "/zones/z/dns_records")
        .match_query(mockito::Matcher::Any)
        .with_status(200)
        .with_body(cf_list(serde_json::json!([])))
        .create_async()
        .await;
    for name in ["app", "web"] {
        cf_server
            .mock("POST", "/zones/z/dns_records")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "name": format!("{name}.example.com")
            })))
            .with_status(200)
            .with_body(cf_service_record(&format!("rec-{name}"), name))
            .expect(1)
            .create_async()
            .await;
    }
    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &cf_server.url(),
        &cf_server.url(),
        false,
        false,
    );

    reconciler
        .reconcile_snapshot(&two_cf_services_snap())
        .await
        .unwrap();
    let report = reconciler.full_resync().await.unwrap();

    zone.assert_async().await;
    assert_eq!(report.verified, 2);
    assert_eq!(report.failed, 0);
}

/// `m1/app` with NPM host 50, Cloudflare record `rec-1` and a Gatus monitor.
async fn seed_published_app(pool: &Db) {
    let mut labels = npm_host_labels("a.example.com");
    labels.extend(cf_dns_labels("app", "home.example.com"));
    labels.extend(gatus_labels());
    storage::upsert_snapshot(pool, &snap_with(labels), 1)
        .await
        .unwrap();
    storage::set_npm_host(pool, "m1/app", 0, 50).await.unwrap();
    storage::set_cf_dns_record(pool, "m1/app", "rec-1")
        .await
        .unwrap();
}

/// A Cloudflare mock holding `rec-1` exactly as `seed_published_app` wants
/// it, expecting `reads` reads of the record.
async fn cf_server_with_app_record(reads: usize) -> (mockito::ServerGuard, mockito::Mock) {
    let mut cf_server = cf_server_with_zone().await;
    let get = cf_server
        .mock("GET", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(cf_one(cf_record("rec-1", Some("docktornet:m1/app"))))
        .expect(reads)
        .create_async()
        .await;
    (cf_server, get)
}

#[tokio::test]
async fn full_resync_npm_failures_open_the_breaker_and_then_skip_npm() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    seed_published_app(&pool).await;
    // A pending NPM removal past grace, so the sweep has NPM work too.
    storage::add_pending_npm_removal(&pool, "m1/old", 0, 60, 1)
        .await
        .unwrap();
    let mut npm_server = mockito::Server::new_async().await;
    let token = npm_server
        .mock("POST", "/api/tokens")
        .with_status(500)
        .expect(NPM_BREAKER_THRESHOLD as usize)
        .create_async()
        .await;
    let (cf_server, cf_get) = cf_server_with_app_record(NPM_BREAKER_THRESHOLD as usize + 1).await;
    let reconciler = reconciler_for(
        pool,
        dir.path(),
        &npm_server.url(),
        &cf_server.url(),
        false,
        false,
    );

    for _ in 0..NPM_BREAKER_THRESHOLD {
        reconciler.full_resync().await.unwrap();
    }
    // The breaker is open: this pass sends NPM nothing, while Gatus and
    // Cloudflare are still verified.
    let service_file = gatus::service_file_path(dir.path().join("gatus"), "m1/app");
    std::fs::remove_file(&service_file).unwrap();
    let report = reconciler.full_resync().await.unwrap();

    token.assert_async().await;
    cf_get.assert_async().await;
    assert!(service_file.exists(), "gatus resync must still run");
    assert_eq!(report.failed, 0, "skipped NPM hosts are not failures");
    assert_eq!(report.repaired, 1, "the rewritten gatus file");
    assert_eq!(
        report.verified, 1,
        "the cloudflare record, not the NPM host"
    );
}

#[tokio::test]
async fn clean_full_resync_resets_the_npm_breaker() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    seed_published_app(&pool).await;
    let mut npm_server = mockito::Server::new_async().await;
    let (cf_server, _cf_get) = cf_server_with_app_record(5).await;
    let reconciler = reconciler_for(
        pool,
        dir.path(),
        &npm_server.url(),
        &cf_server.url(),
        false,
        false,
    );
    let failing_token = |server: &mut mockito::ServerGuard| {
        server
            .mock("POST", "/api/tokens")
            .with_status(500)
            .expect(NPM_BREAKER_THRESHOLD as usize - 1)
            .create()
    };

    // One failure short of the threshold.
    let failing = failing_token(&mut npm_server);
    for _ in 1..NPM_BREAKER_THRESHOLD {
        reconciler.full_resync().await.unwrap();
    }
    failing.assert_async().await;
    failing.remove_async().await;

    // A clean pass: the host is in sync.
    let healthy = vec![
        npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .create_async()
            .await,
        npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .create_async()
            .await,
        npm_server
            .mock("GET", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(stored_host_50(
                serde_json::json!({ "docktornet": app_marker() }),
            ))
            .create_async()
            .await,
    ];
    assert_eq!(reconciler.full_resync().await.unwrap().failed, 0);
    for mock in healthy {
        mock.assert_async().await;
        mock.remove_async().await;
    }

    // The success cleared the count, so the same run of failures again
    // still leaves the breaker closed: every pass reaches NPM.
    let failing = failing_token(&mut npm_server);
    for _ in 1..NPM_BREAKER_THRESHOLD {
        reconciler.full_resync().await.unwrap();
    }
    failing.assert_async().await;
    assert!(!reconciler.npm_breaker.is_open(std::time::Instant::now()));
}

#[tokio::test]
async fn cf_adopts_marked_record_without_create_or_flag() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut cf_server = cf_server_with_zone().await;
    cf_server
        .mock("GET", "/zones/z/dns_records")
        .match_query(mockito::Matcher::UrlEncoded(
            "comment.exact".into(),
            "docktornet:m1/app".into(),
        ))
        .with_status(200)
        .with_body(cf_list(serde_json::json!([cf_record(
            "rec-9",
            Some("docktornet:m1/app")
        )])))
        .create_async()
        .await;
    let create = cf_server
        .mock("POST", "/zones/z/dns_records")
        .with_status(200)
        .with_body(cf_one(cf_record("rec-new", Some("docktornet:m1/app"))))
        .expect(0)
        .create_async()
        .await;
    let update = cf_server
        .mock("PUT", "/zones/z/dns_records/rec-9")
        .with_status(200)
        .with_body(cf_one(cf_record("rec-9", Some("docktornet:m1/app"))))
        .expect(0)
        .create_async()
        .await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &cf_server.url(),
        &cf_server.url(),
        false,
        false,
    );
    reconciler
        .reconcile_snapshot(&snap_with(cf_dns_labels("app", "home.example.com")))
        .await
        .unwrap();

    create.assert_async().await;
    update.assert_async().await;
    assert_eq!(
        storage::get_cf_dns_record(&pool, "m1/app").await.unwrap(),
        Some("rec-9".to_string())
    );
}

/// A Cloudflare mock where the marker lookup finds nothing, the create
/// conflicts, and the name lookup finds `existing`.
async fn cf_server_for_hostname_adoption(existing: serde_json::Value) -> mockito::ServerGuard {
    let mut cf_server = cf_server_with_zone().await;
    cf_server
        .mock("GET", "/zones/z/dns_records")
        .match_query(mockito::Matcher::UrlEncoded(
            "comment.exact".into(),
            "docktornet:m1/app".into(),
        ))
        .with_status(200)
        .with_body(cf_list(serde_json::json!([])))
        .create_async()
        .await;
    cf_server
        .mock("POST", "/zones/z/dns_records")
        .with_status(400)
        .with_body(
            r#"{"success":false,"errors":[{"code":81053,"message":"exists"}],"result":null}"#,
        )
        .create_async()
        .await;
    cf_server
        .mock("GET", "/zones/z/dns_records")
        .match_query(mockito::Matcher::UrlEncoded(
            "name".into(),
            "app.example.com".into(),
        ))
        .with_status(200)
        .with_body(cf_list(serde_json::json!([existing])))
        .create_async()
        .await;
    cf_server
}

#[tokio::test]
async fn cf_hostname_adoption_writes_the_marker() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut cf_server =
        cf_server_for_hostname_adoption(cf_record("rec-5", Some("set by hand"))).await;
    let update = cf_server
        .mock("PUT", "/zones/z/dns_records/rec-5")
        .match_body(mockito::Matcher::PartialJson(serde_json::json!({
            "comment": "docktornet:m1/app"
        })))
        .with_status(200)
        .with_body(cf_one(cf_record("rec-5", Some("docktornet:m1/app"))))
        .expect(1)
        .create_async()
        .await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &cf_server.url(),
        &cf_server.url(),
        false,
        true,
    );
    reconciler
        .reconcile_snapshot(&snap_with(cf_dns_labels("app", "home.example.com")))
        .await
        .unwrap();

    update.assert_async().await;
    assert_eq!(
        storage::get_cf_dns_record(&pool, "m1/app").await.unwrap(),
        Some("rec-5".to_string())
    );
}

#[tokio::test]
async fn cf_hostname_adoption_refuses_a_record_marked_for_another_service() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut cf_server =
        cf_server_for_hostname_adoption(cf_record("rec-5", Some("docktornet:m2/other"))).await;
    let update = cf_server
        .mock("PUT", "/zones/z/dns_records/rec-5")
        .with_status(200)
        .with_body(cf_one(cf_record("rec-5", Some("docktornet:m1/app"))))
        .expect(0)
        .create_async()
        .await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &cf_server.url(),
        &cf_server.url(),
        false,
        true,
    );
    let dns = docktornet_core::labels::parse(&cf_dns_labels("app", "home.example.com"))
        .cf_dns
        .unwrap();
    let snap = snap_with(cf_dns_labels("app", "home.example.com"));
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();

    let err = reconciler
        .reconcile_cloudflare("m1/app", &dns)
        .await
        .unwrap_err();

    match err {
        ReconcileError::MarkedForOtherService {
            service_id, owner, ..
        } => {
            assert_eq!(service_id, "m1/app");
            assert_eq!(owner, "m2/other");
        }
        other => panic!("expected MarkedForOtherService, got {other:?}"),
    }
    update.assert_async().await;
    assert_eq!(
        storage::get_cf_dns_record(&pool, "m1/app").await.unwrap(),
        None
    );
}

#[tokio::test]
async fn cf_record_for_a_too_long_service_id_is_created_without_comment() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut cf_server = cf_server_with_zone().await;
    // "docktornet:" + "m1/" + 87 characters is 101 characters.
    let container = "c".repeat(87);
    let lookup = cf_server
        .mock("GET", "/zones/z/dns_records")
        .match_query(mockito::Matcher::Any)
        .with_status(200)
        .with_body(cf_list(serde_json::json!([])))
        .expect(0)
        .create_async()
        .await;
    let create = cf_server
        .mock("POST", "/zones/z/dns_records")
        .match_body(mockito::Matcher::PartialJson(serde_json::json!({
            "name": "app.example.com",
            "comment": ""
        })))
        .with_status(200)
        .with_body(cf_one(cf_record("rec-1", None)))
        .expect(1)
        .create_async()
        .await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &cf_server.url(),
        &cf_server.url(),
        false,
        false,
    );
    let mut snap = snap_with(cf_dns_labels("app", "home.example.com"));
    snap.containers[0].name = container.clone();
    reconciler.reconcile_snapshot(&snap).await.unwrap();

    lookup.assert_async().await;
    create.assert_async().await;
    assert_eq!(
        storage::get_cf_dns_record(&pool, &format!("m1/{container}"))
            .await
            .unwrap(),
        Some("rec-1".to_string())
    );
}

#[tokio::test]
async fn dry_run_writes_no_cf_marker() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut cf_server = mockito::Server::new_async().await;
    let refused = refuse_every_request(&mut cf_server).await;

    let reconciler = reconciler_for(
        pool.clone(),
        dir.path(),
        &cf_server.url(),
        &cf_server.url(),
        true,
        true,
    );
    let snap = snap_with(cf_dns_labels("app", "home.example.com"));
    storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
        .await
        .unwrap();
    storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
        .await
        .unwrap();

    reconciler.reconcile_snapshot(&snap).await.unwrap();
    assert_eq!(reconciler.full_resync().await.unwrap().failed, 0);

    for mock in refused {
        mock.assert_async().await;
    }
}

/// Labels for `npm.0` on `a.example.com` and, when `with_second`, `npm.1` on
/// `b.example.com`.
fn two_npm_labels(with_second: bool) -> BTreeMap<String, String> {
    let mut labels = npm_host_labels("a.example.com");
    if with_second {
        for (key, value) in npm_host_labels("b.example.com") {
            labels.insert(key.replace(".npm.0.", ".npm.1."), value);
        }
    }
    labels
}

fn app_with_npm(with_second: bool) -> ContainerEntry {
    ContainerEntry {
        name: "app".into(),
        labels: two_npm_labels(with_second),
    }
}

/// An NPM mock answering auth and holding index 0's host 50 in sync.
async fn npm_server_holding_host_50() -> mockito::ServerGuard {
    let mut server = npm_server_with_auth().await;
    server
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(stored_host_50(
            serde_json::json!({ "docktornet": app_marker() }),
        ))
        .create_async()
        .await;
    server
}

/// Store `m1/app` at t=1 labelling `npm.0` (host 50) and `npm.1` (host 51).
async fn seed_two_npm_hosts(pool: &Db) {
    storage::upsert_snapshot(pool, &m1_snapshot(vec![app_with_npm(true)]), 1)
        .await
        .unwrap();
    storage::set_npm_host(pool, "m1/app", 0, 50).await.unwrap();
    storage::set_npm_host(pool, "m1/app", 1, 51).await.unwrap();
}

fn npm_reconciler(pool: Db, dir: &std::path::Path, npm_url: &str) -> Reconciler {
    reconciler_for(pool, dir, npm_url, npm_url, false, false)
}

#[tokio::test]
async fn npm_host_deleted_when_a_running_container_drops_its_npm_group() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut npm_server = npm_server_holding_host_50().await;
    let delete = npm_server
        .mock("DELETE", "/api/nginx/proxy-hosts/51")
        .with_status(200)
        .with_body("{}")
        .expect(1)
        .create_async()
        .await;
    let reconciler = npm_reconciler(pool.clone(), dir.path(), &npm_server.url());
    seed_two_npm_hosts(&pool).await;

    // The container keeps running with only its npm.0 group.
    push(&reconciler, 31, vec![app_with_npm(false)]).await;

    delete.assert_async().await;
    assert_eq!(
        storage::list_npm_hosts(&pool).await.unwrap(),
        vec![("m1/app".to_string(), 0, 50)],
        "the dropped index's mapping goes with its host"
    );
    assert!(
        storage::list_pending_npm_removals(&pool)
            .await
            .unwrap()
            .is_empty(),
        "a label edit owes no pending removal"
    );
}

#[tokio::test]
async fn a_dropped_npm_group_whose_delete_fails_is_swept_next() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut npm_server = npm_server_holding_host_50().await;
    let failing = npm_server
        .mock("DELETE", "/api/nginx/proxy-hosts/51")
        .with_status(500)
        .with_body("{}")
        .expect(1)
        .create_async()
        .await;
    let reconciler = npm_reconciler(pool.clone(), dir.path(), &npm_server.url());
    seed_two_npm_hosts(&pool).await;

    reconciler.set_now(31);
    let err = reconciler
        .reconcile_snapshot(&m1_snapshot(vec![app_with_npm(false)]))
        .await;

    assert!(err.is_err(), "a failed delete fails the apply");
    failing.assert_async().await;
    failing.remove_async().await;
    assert_eq!(
        storage::list_npm_hosts(&pool).await.unwrap(),
        vec![("m1/app".to_string(), 0, 50)]
    );
    assert_eq!(
        storage::list_pending_npm_removals(&pool).await.unwrap(),
        vec![("m1/app".to_string(), 1, 51, 31 - GATUS_REMOVAL_GRACE_SECS)],
        "the host is parked already past grace"
    );

    let delete = npm_server
        .mock("DELETE", "/api/nginx/proxy-hosts/51")
        .with_status(200)
        .with_body("{}")
        .expect(1)
        .create_async()
        .await;
    reconciler.sweep_pending_removals().await.unwrap();

    delete.assert_async().await;
    assert!(
        storage::list_pending_npm_removals(&pool)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_dropped_npm_group_is_parked_while_the_breaker_is_open() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut npm_server = npm_server_holding_host_50().await;
    let delete = npm_server
        .mock("DELETE", "/api/nginx/proxy-hosts/51")
        .with_status(200)
        .with_body("{}")
        .expect(1)
        .create_async()
        .await;
    let reconciler = npm_reconciler(pool.clone(), dir.path(), &npm_server.url());
    seed_two_npm_hosts(&pool).await;
    reconciler.trip_npm_breaker();

    push(&reconciler, 31, vec![app_with_npm(false)]).await;

    assert_eq!(
        storage::list_npm_hosts(&pool).await.unwrap(),
        vec![("m1/app".to_string(), 0, 50)]
    );
    assert_eq!(
        storage::list_pending_npm_removals(&pool).await.unwrap(),
        vec![("m1/app".to_string(), 1, 51, 31 - GATUS_REMOVAL_GRACE_SECS)],
        "the host is parked already past grace"
    );

    // A reconciler whose breaker is closed, as this one's will be once its
    // cooldown ends.
    let recovered = npm_reconciler(pool.clone(), dir.path(), &npm_server.url());
    recovered.set_now(31);
    recovered.sweep_pending_removals().await.unwrap();

    delete.assert_async().await;
    assert!(
        storage::list_pending_npm_removals(&pool)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_returning_service_gets_back_only_the_npm_groups_it_still_labels() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut npm_server = npm_server_holding_host_50().await;
    let delete = npm_server
        .mock("DELETE", "/api/nginx/proxy-hosts/51")
        .with_status(200)
        .with_body("{}")
        .expect(1)
        .create_async()
        .await;
    let reconciler = npm_reconciler(pool.clone(), dir.path(), &npm_server.url());
    seed_two_npm_hosts(&pool).await;

    // It leaves, then returns within grace labelling only npm.0.
    push(&reconciler, 31, vec![]).await;
    push(&reconciler, 61, vec![app_with_npm(false)]).await;

    assert_eq!(
        storage::list_npm_hosts(&pool).await.unwrap(),
        vec![("m1/app".to_string(), 0, 50)],
        "only the still-labelled index gets its mapping back"
    );
    assert_eq!(
        storage::list_pending_npm_removals(&pool).await.unwrap(),
        vec![("m1/app".to_string(), 1, 51, 1)],
        "the dropped index stays pending from its departure"
    );

    reconciler.set_now(1 + GATUS_REMOVAL_GRACE_SECS);
    reconciler.sweep_pending_removals().await.unwrap();

    delete.assert_async().await;
    assert!(
        storage::list_pending_npm_removals(&pool)
            .await
            .unwrap()
            .is_empty(),
        "the sweep deletes it though its service is stored again"
    );
    assert_eq!(
        storage::list_npm_hosts(&pool).await.unwrap(),
        vec![("m1/app".to_string(), 0, 50)]
    );
}

#[tokio::test]
async fn a_malformed_npm_group_keeps_its_host() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut npm_server = npm_server_holding_host_50().await;
    let delete = npm_server
        .mock("DELETE", "/api/nginx/proxy-hosts/51")
        .with_status(200)
        .with_body("{}")
        .expect(0)
        .create_async()
        .await;
    let reconciler = npm_reconciler(pool.clone(), dir.path(), &npm_server.url());
    seed_two_npm_hosts(&pool).await;

    // A typo in npm.1's port: the group is still there, just unparsable.
    let mut app = app_with_npm(true);
    app.labels
        .insert("docktornet.npm.1.port".to_string(), "80a".to_string());
    push(&reconciler, 31, vec![app]).await;
    reconciler.set_now(31 + GATUS_REMOVAL_GRACE_SECS);
    reconciler.sweep_pending_removals().await.unwrap();

    delete.assert_async().await;
    assert_eq!(
        storage::list_npm_hosts(&pool).await.unwrap(),
        vec![("m1/app".to_string(), 0, 50), ("m1/app".to_string(), 1, 51)],
        "a malformed group is not a dropped one"
    );
    assert!(
        storage::list_pending_npm_removals(&pool)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn sweep_keeps_the_pending_host_of_a_returning_service_with_a_malformed_npm_group() {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    let mut server = mockito::Server::new_async().await;
    let untouched = refuse_every_request(&mut server).await;
    let reconciler = npm_reconciler(pool.clone(), dir.path(), &server.url());
    // app is stored again with a typo in npm.0's port, before its apply runs.
    let mut labels = gatus_labels();
    labels.extend(npm_host_labels("a.example.com"));
    labels.insert("docktornet.npm.0.port".to_string(), "80a".to_string());
    storage::upsert_snapshot(
        &pool,
        &m1_snapshot(vec![ContainerEntry {
            name: "app".into(),
            labels,
        }]),
        1,
    )
    .await
    .unwrap();
    storage::add_pending_npm_removal(&pool, "m1/app", 0, 50, 1)
        .await
        .unwrap();
    reconciler.set_now(1 + GATUS_REMOVAL_GRACE_SECS + 60);

    reconciler.sweep_pending_removals().await.unwrap();

    for mock in untouched {
        mock.assert_async().await;
    }
    assert_eq!(
        storage::list_pending_npm_removals(&pool).await.unwrap(),
        vec![("m1/app".to_string(), 0, 50, 1)]
    );
}
