//! The Caddy provider driven through the reconciler against a mockito Caddy
//! admin API.

use super::*;
use crate::integrations::caddy::CaddyClient;
use crate::{db, storage};
use docktornet_core::{ContainerEntry, MachineId, caddy_route_marker};
use mockito::{Matcher, Mock, ServerGuard};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tempfile::{TempDir, tempdir};

const ANCHOR_ROUTES: &str = "/id/docktornet/handle/0/routes";

struct Fixture {
    dir: TempDir,
    pool: Db,
    caddy: ServerGuard,
}

async fn fixture() -> Fixture {
    let dir = tempdir().unwrap();
    let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
    Fixture {
        dir,
        pool,
        caddy: mockito::Server::new_async().await,
    }
}

impl Fixture {
    fn options(&self, dry_run: bool) -> ReconcileOptions {
        ReconcileOptions {
            dry_run,
            adopt_existing: false,
            gatus_config_dir: self.dir.path().join("gatus"),
        }
    }

    fn caddy_client(&self) -> CaddyClient {
        CaddyClient::new(crate::config::CaddyConfig {
            admin_url: url::Url::parse(&self.caddy.url()).unwrap(),
            anchor_id: "docktornet".into(),
        })
    }

    /// A reconciler with Caddy alone configured.  A fresh one has no cached
    /// intent hash, so it reconciles a snapshot the last one already did.
    fn reconciler(&self) -> Reconciler {
        Reconciler::new(self.pool.clone(), self.options(false)).with_caddy(self.caddy_client())
    }

    /// A reconciler with NPM and Cloudflare at `npm_url` and `cf_url`, and Caddy.
    fn reconciler_with(&self, npm_url: &str, cf_url: &str) -> Reconciler {
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
            cf_url,
        );
        Reconciler::with_clients(self.pool.clone(), self.options(false), npm, cf)
            .with_caddy(self.caddy_client())
    }

    /// Caddy's anchor holds `routes`.
    async fn anchor(&mut self, routes: Value) -> Mock {
        self.caddy
            .mock("GET", ANCHOR_ROUTES)
            .with_status(200)
            .with_body(routes.to_string())
            .create_async()
            .await
    }

    /// No route anywhere in Caddy's config carries a docktornet marker.
    async fn no_marked_route_elsewhere(&mut self) -> Mock {
        self.caddy
            .mock("GET", Matcher::Regex("^/id/docktornet--".into()))
            .with_status(404)
            .with_body(r#"{"error":"unknown object ID"}"#)
            .create_async()
            .await
    }

    /// Expect `route` appended to the anchor exactly once.
    async fn append(&mut self, route: &Value) -> Mock {
        self.caddy
            .mock("POST", ANCHOR_ROUTES)
            .match_body(Matcher::Json(route.clone()))
            .with_status(200)
            .expect(1)
            .create_async()
            .await
    }

    /// Expect no request with `method` at all.
    async fn never(&mut self, method: &str) -> Mock {
        self.caddy
            .mock(method, Matcher::Any)
            .with_status(200)
            .expect(0)
            .create_async()
            .await
    }

    async fn rows(&self, service_id: &str) -> Vec<storage::CaddyRouteRow> {
        storage::list_caddy_routes_for_service(&self.pool, service_id)
            .await
            .unwrap()
    }
}

/// `caddy.<index>` groups, each with `host` and `port`.
fn caddy_labels(groups: &[(u8, &str, u16)]) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    for (index, host, port) in groups {
        labels.insert(format!("docktornet.caddy.{index}.host"), host.to_string());
        labels.insert(format!("docktornet.caddy.{index}.port"), port.to_string());
    }
    labels
}

fn snap(
    machine: &str,
    forward_host: &str,
    containers: Vec<(&str, BTreeMap<String, String>)>,
) -> Snapshot {
    Snapshot {
        machine_id: MachineId::new(machine).unwrap(),
        forward_host: forward_host.into(),
        taken_at: 1,
        agent_version: None,
        containers: containers
            .into_iter()
            .map(|(name, labels)| ContainerEntry {
                name: name.into(),
                labels,
            })
            .collect(),
    }
}

/// The route the spec renders for a plain-HTTP group.
fn route(service_id: &str, index: u8, hosts: &[&str], dial: &str) -> Value {
    json!({
        "@id": caddy_route_marker(service_id, index),
        "match": [{ "host": hosts }],
        "handle": [{
            "handler": "reverse_proxy",
            "upstreams": [{ "dial": dial }]
        }],
        "terminal": true
    })
}

fn path(service_id: &str, index: u8) -> String {
    format!("/id/{}", caddy_route_marker(service_id, index))
}

#[tokio::test]
async fn a_caddy_route_is_created_in_the_anchor_and_mapped() {
    let mut f = fixture().await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let expected = route("m1/app", 0, &["app.example.com"], "10.0.0.1:3000");
    let create = f.append(&expected).await;

    f.reconciler()
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![("app", caddy_labels(&[(0, "App.Example.com", 3000)]))],
        ))
        .await
        .unwrap();

    create.assert_async().await;
    let rows = f.rows("m1/app").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].hosts, vec!["app.example.com"]);
    assert_eq!(rows[0].route, expected);
}

#[tokio::test]
async fn a_caddy_route_matching_caddys_sorted_copy_is_left_alone() {
    let mut f = fixture().await;
    let labels = || {
        snap(
            "m1",
            "10.0.0.1",
            vec![("app", caddy_labels(&[(0, "app.example.com", 3000)]))],
        )
    };
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    f.append(&route("m1/app", 0, &["app.example.com"], "10.0.0.1:3000"))
        .await;
    f.reconciler().reconcile_snapshot(&labels()).await.unwrap();
    f.caddy.reset();

    // Caddy hands the route back with its keys sorted.
    let marker = caddy_route_marker("m1/app", 0);
    let read = f
        .caddy
        .mock("GET", ANCHOR_ROUTES)
        .with_status(200)
        .with_body(format!(
            r#"[{{"@id":"{marker}","handle":[{{"handler":"reverse_proxy","upstreams":[{{"dial":"10.0.0.1:3000"}}]}}],"match":[{{"host":["app.example.com"]}}],"terminal":true}}]"#
        ))
        .expect(1)
        .create_async()
        .await;
    let post = f.never("POST").await;
    let patch = f.never("PATCH").await;

    f.reconciler().reconcile_snapshot(&labels()).await.unwrap();

    read.assert_async().await;
    post.assert_async().await;
    patch.assert_async().await;
}

#[tokio::test]
async fn a_changed_port_label_patches_the_route_in_place() {
    let mut f = fixture().await;
    let _anchor = f
        .anchor(json!([route(
            "m1/app",
            0,
            &["app.example.com"],
            "10.0.0.1:3000"
        )]))
        .await;
    let post = f.never("POST").await;
    let patch = f
        .caddy
        .mock("PATCH", path("m1/app", 0).as_str())
        .match_body(Matcher::Json(route(
            "m1/app",
            0,
            &["app.example.com"],
            "10.0.0.1:4000",
        )))
        .with_status(200)
        .expect(1)
        .create_async()
        .await;

    f.reconciler()
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![("app", caddy_labels(&[(0, "app.example.com", 4000)]))],
        ))
        .await
        .unwrap();

    patch.assert_async().await;
    post.assert_async().await;
}

#[tokio::test]
async fn a_marked_route_is_adopted_after_the_mapping_is_lost() {
    let mut f = fixture().await;
    // Caddy kept the route through a database loss: the fresh database has
    // no mapping for it.
    let _anchor = f
        .anchor(json!([route(
            "m1/app",
            0,
            &["app.example.com"],
            "10.0.0.1:3000"
        )]))
        .await;
    let post = f.never("POST").await;
    let patch = f.never("PATCH").await;

    f.reconciler()
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![("app", caddy_labels(&[(0, "app.example.com", 3000)]))],
        ))
        .await
        .unwrap();

    post.assert_async().await;
    patch.assert_async().await;
    assert_eq!(f.rows("m1/app").await[0].hosts, vec!["app.example.com"]);
}

#[tokio::test]
async fn caddy_forwards_to_each_machines_own_address() {
    let mut f = fixture().await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let create_a = f
        .append(&route("m1/app", 0, &["a.example.com"], "10.0.0.1:3000"))
        .await;
    let create_b = f
        .append(&route("m2/app", 0, &["b.example.com"], "[fd00::2]:3000"))
        .await;
    let reconciler = f.reconciler();

    reconciler
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![("app", caddy_labels(&[(0, "a.example.com", 3000)]))],
        ))
        .await
        .unwrap();
    reconciler
        .reconcile_snapshot(&snap(
            "m2",
            "fd00::2",
            vec![("app", caddy_labels(&[(0, "b.example.com", 3000)]))],
        ))
        .await
        .unwrap();

    create_a.assert_async().await;
    create_b.assert_async().await;
}

#[tokio::test]
async fn an_https_scheme_reaches_the_upstream_over_tls_without_verifying_it() {
    let mut f = fixture().await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let create = f
        .append(&json!({
            "@id": caddy_route_marker("m1/app", 0),
            "match": [{ "host": ["app.example.com"] }],
            "handle": [{
                "handler": "reverse_proxy",
                "transport": {
                    "protocol": "http",
                    "tls": { "insecure_skip_verify": true }
                },
                "upstreams": [{ "dial": "10.0.0.1:8443" }]
            }],
            "terminal": true
        }))
        .await;
    let mut labels = caddy_labels(&[(0, "app.example.com", 8443)]);
    labels.insert("docktornet.caddy.0.scheme".into(), "https".into());

    f.reconciler()
        .reconcile_snapshot(&snap("m1", "10.0.0.1", vec![("app", labels)]))
        .await
        .unwrap();

    create.assert_async().await;
}

#[tokio::test]
async fn a_host_clash_is_refused_and_retried_once_it_clears() {
    let mut f = fixture().await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let create_a = f
        .append(&route("m1/a", 0, &["x.example.com"], "10.0.0.1:3000"))
        .await;
    let reconciler = f.reconciler();
    reconciler
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![("a", caddy_labels(&[(0, "x.example.com", 3000)]))],
        ))
        .await
        .unwrap();
    create_a.assert_async().await;
    f.caddy.reset();

    // b asks for the host a holds: a keeps it, b gets nothing.
    let a_route = route("m1/a", 0, &["x.example.com"], "10.0.0.1:3000");
    let _anchor = f.anchor(json!([a_route])).await;
    let post = f.never("POST").await;
    let clash = snap(
        "m1",
        "10.0.0.1",
        vec![
            ("a", caddy_labels(&[(0, "x.example.com", 3000)])),
            ("b", caddy_labels(&[(0, "x.example.com", 4000)])),
        ],
    );
    assert!(reconciler.reconcile_snapshot(&clash).await.is_err());
    post.assert_async().await;
    assert_eq!(f.rows("m1/a").await[0].hosts, vec!["x.example.com"]);
    assert!(f.rows("m1/b").await.is_empty());
    f.caddy.reset();

    // a moves to another host, so b's claim goes through in the same apply.
    let _anchor = f.anchor(json!([a_route])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let patch_a = f
        .caddy
        .mock("PATCH", path("m1/a", 0).as_str())
        .match_body(Matcher::Json(route(
            "m1/a",
            0,
            &["y.example.com"],
            "10.0.0.1:3000",
        )))
        .with_status(200)
        .expect(1)
        .create_async()
        .await;
    let create_b = f
        .append(&route("m1/b", 0, &["x.example.com"], "10.0.0.1:4000"))
        .await;
    reconciler
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![
                ("a", caddy_labels(&[(0, "y.example.com", 3000)])),
                ("b", caddy_labels(&[(0, "x.example.com", 4000)])),
            ],
        ))
        .await
        .unwrap();
    patch_a.assert_async().await;
    create_b.assert_async().await;
}

#[tokio::test]
async fn hosts_differing_only_in_case_clash() {
    let mut f = fixture().await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let create_a = f
        .append(&route("m1/a", 0, &["app.example.com"], "10.0.0.1:3000"))
        .await;

    let result = f
        .reconciler()
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![
                ("a", caddy_labels(&[(0, "app.example.com", 3000)])),
                ("b", caddy_labels(&[(0, "APP.Example.COM", 4000)])),
            ],
        ))
        .await;

    assert!(result.is_err());
    create_a.assert_async().await;
    assert!(f.rows("m1/b").await.is_empty());
}

#[tokio::test]
async fn a_dropped_index_is_deleted_in_the_same_apply() {
    let mut f = fixture().await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    f.append(&route("m1/app", 0, &["a.example.com"], "10.0.0.1:3000"))
        .await;
    f.append(&route("m1/app", 1, &["b.example.com"], "10.0.0.1:3001"))
        .await;
    let reconciler = f.reconciler();
    reconciler
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![(
                "app",
                caddy_labels(&[(0, "a.example.com", 3000), (1, "b.example.com", 3001)]),
            )],
        ))
        .await
        .unwrap();
    f.caddy.reset();

    let _anchor = f
        .anchor(json!([route(
            "m1/app",
            0,
            &["a.example.com"],
            "10.0.0.1:3000"
        )]))
        .await;
    let delete = f
        .caddy
        .mock("DELETE", path("m1/app", 1).as_str())
        .with_status(200)
        .expect(1)
        .create_async()
        .await;
    reconciler
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![("app", caddy_labels(&[(0, "a.example.com", 3000)]))],
        ))
        .await
        .unwrap();

    delete.assert_async().await;
    let rows = f.rows("m1/app").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].caddy_index, 0);
    assert!(
        storage::list_pending_caddy_removals(&f.pool)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_dropped_index_whose_delete_fails_is_parked_past_grace_and_keeps_its_hosts() {
    let mut f = fixture().await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    f.append(&route("m1/a", 0, &["x.example.com"], "10.0.0.1:3000"))
        .await;
    f.append(&route("m1/a", 1, &["z.example.com"], "10.0.0.1:3001"))
        .await;
    let reconciler = f.reconciler();
    reconciler.set_now(10_000);
    reconciler
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![(
                "a",
                caddy_labels(&[(0, "x.example.com", 3000), (1, "z.example.com", 3001)]),
            )],
        ))
        .await
        .unwrap();
    f.caddy.reset();

    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let delete = f
        .caddy
        .mock("DELETE", path("m1/a", 0).as_str())
        .with_status(502)
        .expect(1)
        .create_async()
        .await;
    f.append(&route("m1/a", 1, &["z.example.com"], "10.0.0.1:3001"))
        .await;
    let result = reconciler
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![("a", caddy_labels(&[(1, "z.example.com", 3001)]))],
        ))
        .await;
    assert!(result.is_err());
    delete.assert_async().await;
    assert_eq!(
        storage::list_pending_caddy_removals(&f.pool).await.unwrap(),
        vec![storage::PendingCaddyRemoval {
            service_id: "m1/a".into(),
            caddy_index: 0,
            hosts: vec!["x.example.com".into()],
            last_seen: 10_000 - GATUS_REMOVAL_GRACE_SECS,
        }]
    );
    assert!(past_grace(10_000 - GATUS_REMOVAL_GRACE_SECS, 10_000));
    assert_eq!(f.rows("m1/a").await.len(), 1);
    f.caddy.reset();

    // The parked route still holds its host against another service.
    let _anchor = f.anchor(json!([])).await;
    let post = f.never("POST").await;
    let result = reconciler
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![("b", caddy_labels(&[(0, "x.example.com", 4000)]))],
        ))
        .await;
    assert!(result.is_err());
    post.assert_async().await;
    assert!(f.rows("m1/b").await.is_empty());
    f.caddy.reset();

    // Labelling the group again maps it again, so no sweep may delete it.
    let _anchor = f
        .anchor(json!([route(
            "m1/a",
            0,
            &["x.example.com"],
            "10.0.0.1:3000"
        )]))
        .await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    f.append(&route("m1/a", 1, &["z.example.com"], "10.0.0.1:3001"))
        .await;
    reconciler
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![(
                "a",
                caddy_labels(&[(0, "x.example.com", 3000), (1, "z.example.com", 3001)]),
            )],
        ))
        .await
        .unwrap();
    assert!(
        storage::list_pending_caddy_removals(&f.pool)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(f.rows("m1/a").await.len(), 2);
}

#[tokio::test]
async fn a_host_moves_between_two_indices_of_one_service_without_a_clash() {
    let mut f = fixture().await;
    let _anchor = f
        .anchor(json!([
            route("m1/app", 0, &["x.example.com"], "10.0.0.1:3000"),
            route("m1/app", 1, &["y.example.com"], "10.0.0.1:3001"),
        ]))
        .await;
    let reconciler = f.reconciler();
    reconciler
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![(
                "app",
                caddy_labels(&[(0, "x.example.com", 3000), (1, "y.example.com", 3001)]),
            )],
        ))
        .await
        .unwrap();

    let swap_0 = f
        .caddy
        .mock("PATCH", path("m1/app", 0).as_str())
        .match_body(Matcher::Json(route(
            "m1/app",
            0,
            &["y.example.com"],
            "10.0.0.1:3000",
        )))
        .with_status(200)
        .expect(1)
        .create_async()
        .await;
    let swap_1 = f
        .caddy
        .mock("PATCH", path("m1/app", 1).as_str())
        .match_body(Matcher::Json(route(
            "m1/app",
            1,
            &["x.example.com"],
            "10.0.0.1:3001",
        )))
        .with_status(200)
        .expect(1)
        .create_async()
        .await;
    reconciler
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![(
                "app",
                caddy_labels(&[(0, "y.example.com", 3000), (1, "x.example.com", 3001)]),
            )],
        ))
        .await
        .unwrap();

    swap_0.assert_async().await;
    swap_1.assert_async().await;
}

#[tokio::test]
async fn a_marked_route_outside_the_anchor_is_left_alone_without_opening_the_breaker() {
    let mut f = fixture().await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f
        .caddy
        .mock("GET", path("m1/app", 0).as_str())
        .with_status(200)
        .with_body(route("m1/app", 0, &["app.example.com"], "10.0.0.1:3000").to_string())
        .create_async()
        .await;
    let post = f.never("POST").await;
    let patch = f.never("PATCH").await;
    let reconciler = f.reconciler();
    let buf = LogBuf(Default::default());
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::ERROR)
            .finish(),
    );

    for _ in 0..(CADDY_BREAKER_THRESHOLD + 1) {
        let result = reconciler
            .reconcile_snapshot(&snap(
                "m1",
                "10.0.0.1",
                vec![("app", caddy_labels(&[(0, "app.example.com", 3000)]))],
            ))
            .await;
        assert!(result.is_err(), "the route outside the anchor is a failure");
    }

    post.assert_async().await;
    patch.assert_async().await;
    let logs = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    assert!(
        logs.contains("outside the anchor") && logs.contains(&caddy_route_marker("m1/app", 0)),
        "{logs}"
    );
    assert_eq!(reconciler.caddy_breaker.consecutive_failures(), 0);
    assert!(!reconciler.caddy_breaker.is_open(std::time::Instant::now()));
}

#[tokio::test]
async fn a_route_caddy_rejects_fails_alone_without_opening_the_breaker() {
    for status in [400, 500] {
        let mut f = fixture().await;
        let _anchor = f.anchor(json!([])).await;
        let _elsewhere = f.no_marked_route_elsewhere().await;
        let rejected = f
            .caddy
            .mock("POST", ANCHOR_ROUTES)
            .match_body(Matcher::PartialJson(
                json!({ "@id": caddy_route_marker("m1/bad", 0) }),
            ))
            .with_status(status)
            .with_body(r#"{"error":"loading new config: ..."}"#)
            .expect((CADDY_BREAKER_THRESHOLD + 1) as usize)
            .create_async()
            .await;
        let good = f
            .caddy
            .mock("POST", ANCHOR_ROUTES)
            .match_body(Matcher::PartialJson(
                json!({ "@id": caddy_route_marker("m1/good", 0) }),
            ))
            .with_status(200)
            .expect_at_least(1)
            .create_async()
            .await;
        let reconciler = f.reconciler();

        for _ in 0..(CADDY_BREAKER_THRESHOLD + 1) {
            let result = reconciler
                .reconcile_snapshot(&snap(
                    "m1",
                    "10.0.0.1",
                    vec![
                        ("bad", caddy_labels(&[(0, "bad.example.com", 3000)])),
                        ("good", caddy_labels(&[(0, "good.example.com", 3000)])),
                    ],
                ))
                .await;
            assert!(result.is_err(), "{status}");
        }

        rejected.assert_async().await;
        good.assert_async().await;
        assert!(
            !reconciler.caddy_breaker.is_open(std::time::Instant::now()),
            "{status}"
        );
    }
}

/// An NPM host and Cloudflare record mapped for `m1/app` and in sync, so each
/// apply reads them and writes nothing.  Returns the NPM read and Cloudflare
/// read, each expected `applies` times, with their servers.
async fn npm_and_cf_in_sync(f: &Fixture, applies: usize) -> (ServerGuard, Mock, ServerGuard, Mock) {
    let snap = snap("m1", "10.0.0.1", vec![("app", app_labels())]);
    storage::upsert_snapshot(&f.pool, &snap, 1).await.unwrap();
    storage::set_npm_host(&f.pool, "m1/app", 0, 50)
        .await
        .unwrap();
    storage::set_cf_dns_record(&f.pool, "m1/app", "rec-1")
        .await
        .unwrap();

    let mut npm = mockito::Server::new_async().await;
    npm.mock("POST", "/api/tokens")
        .with_status(200)
        .with_body(r#"{"token":"t"}"#)
        .create_async()
        .await;
    npm.mock("GET", "/api/nginx/certificates")
        .with_status(200)
        .with_body("[]")
        .create_async()
        .await;
    let npm_read = npm
        .mock("GET", "/api/nginx/proxy-hosts/50")
        .with_status(200)
        .with_body(
            r#"{"id":50,"domain_names":["app.example.com"],"forward_scheme":"http",
                "forward_host":"10.0.0.1","forward_port":3000,"certificate_id":0,
                "ssl_forced":0,"http2_support":0,"hsts_enabled":0,
                "hsts_subdomains":0,"block_exploits":1,"allow_websocket_upgrade":1,
                "meta":{"docktornet":{"service_id":"m1/app","npm_index":0}}}"#,
        )
        .expect(applies)
        .create_async()
        .await;

    let mut cf = mockito::Server::new_async().await;
    cf.mock("GET", "/zones/z")
        .with_status(200)
        .with_body(r#"{"success":true,"result":{"id":"z","name":"example.com"}}"#)
        .create_async()
        .await;
    let cf_read = cf
        .mock("GET", "/zones/z/dns_records/rec-1")
        .with_status(200)
        .with_body(
            json!({"success": true, "result": {
                "id": "rec-1", "name": "app.example.com", "content": "home.example.com",
                "proxied": false, "comment": "docktornet:m1/app"
            }})
            .to_string(),
        )
        .expect(applies)
        .create_async()
        .await;
    (npm, npm_read, cf, cf_read)
}

/// NPM, Cloudflare and Caddy labels for `app.example.com` on one container.
fn app_labels() -> BTreeMap<String, String> {
    let mut labels = caddy_labels(&[(0, "app.example.com", 3000)]);
    for (key, value) in [
        ("npm.0.host", "app.example.com"),
        ("npm.0.port", "3000"),
        ("npm.0.scheme", "http"),
        ("npm.0.ssl", "false"),
        ("cf.dns.name", "app"),
        ("cf.dns.target", "home.example.com"),
        ("cf.dns.proxied", "false"),
    ] {
        labels.insert(format!("docktornet.{key}"), value.into());
    }
    labels
}

#[tokio::test]
async fn an_open_caddy_breaker_skips_caddy_while_npm_and_cloudflare_still_run() {
    let mut f = fixture().await;
    let applies = CADDY_BREAKER_THRESHOLD as usize + 2;
    let (npm, npm_read, cf, cf_read) = npm_and_cf_in_sync(&f, applies).await;
    let unavailable = f
        .caddy
        .mock("GET", ANCHOR_ROUTES)
        .with_status(502)
        .expect(CADDY_BREAKER_THRESHOLD as usize)
        .create_async()
        .await;
    let reconciler = f.reconciler_with(&npm.url(), &cf.url());

    for apply in 0..applies {
        let result = reconciler
            .reconcile_snapshot(&snap("m1", "10.0.0.1", vec![("app", app_labels())]))
            .await;
        // Once the breaker is open, the apply skips Caddy instead of failing.
        assert_eq!(
            result.is_err(),
            apply < CADDY_BREAKER_THRESHOLD as usize,
            "apply {apply}"
        );
    }

    unavailable.assert_async().await;
    npm_read.assert_async().await;
    cf_read.assert_async().await;
    assert!(reconciler.caddy_breaker.is_open(std::time::Instant::now()));
    assert!(!reconciler.npm_breaker.is_open(std::time::Instant::now()));
}

#[tokio::test]
async fn one_service_routes_a_hostname_through_npm_and_caddy_while_npm_is_down() {
    let mut f = fixture().await;
    let mut npm = mockito::Server::new_async().await;
    let npm_down = npm
        .mock("POST", "/api/tokens")
        .with_status(503)
        .expect(1)
        .create_async()
        .await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let create = f
        .append(&route("m1/app", 0, &["app.example.com"], "10.0.0.1:3000"))
        .await;
    let mut labels = caddy_labels(&[(0, "app.example.com", 3000)]);
    for (key, value) in [
        ("npm.0.host", "app.example.com"),
        ("npm.0.port", "3000"),
        ("npm.0.scheme", "http"),
        ("npm.0.ssl", "false"),
    ] {
        labels.insert(format!("docktornet.{key}"), value.into());
    }

    let result = f
        .reconciler_with(&npm.url(), "http://127.0.0.1:9")
        .reconcile_snapshot(&snap("m1", "10.0.0.1", vec![("app", labels)]))
        .await;

    assert!(result.is_err(), "the NPM outage is a failure");
    npm_down.assert_async().await;
    create.assert_async().await;
    assert_eq!(f.rows("m1/app").await[0].hosts, vec!["app.example.com"]);
}

#[tokio::test]
async fn one_service_routes_a_hostname_through_npm_and_caddy_without_a_clash() {
    let mut f = fixture().await;
    let mut npm = mockito::Server::new_async().await;
    for (method, path, body) in [
        ("POST", "/api/tokens", r#"{"token":"t"}"#),
        ("GET", "/api/nginx/certificates", "[]"),
        ("GET", "/api/nginx/proxy-hosts", "[]"),
    ] {
        npm.mock(method, path)
            .with_status(200)
            .with_body(body)
            .create_async()
            .await;
    }
    let npm_create = npm
        .mock("POST", "/api/nginx/proxy-hosts")
        .match_body(Matcher::PartialJson(
            json!({ "domain_names": ["app.example.com"] }),
        ))
        .with_status(200)
        .with_body(r#"{"id":1,"domain_names":["app.example.com"]}"#)
        .expect(1)
        .create_async()
        .await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let caddy_create = f
        .append(&route("m1/app", 0, &["app.example.com"], "10.0.0.1:3000"))
        .await;
    let mut labels = caddy_labels(&[(0, "app.example.com", 3000)]);
    for (key, value) in [
        ("npm.0.host", "app.example.com"),
        ("npm.0.port", "3000"),
        ("npm.0.scheme", "http"),
        ("npm.0.ssl", "false"),
    ] {
        labels.insert(format!("docktornet.{key}"), value.to_string());
    }

    f.reconciler_with(&npm.url(), "http://127.0.0.1:9")
        .reconcile_snapshot(&snap("m1", "10.0.0.1", vec![("app", labels)]))
        .await
        .unwrap();

    npm_create.assert_async().await;
    caddy_create.assert_async().await;
}

#[tokio::test]
async fn a_refused_claim_is_retried_by_an_unchanged_snapshot() {
    let mut f = fixture().await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let create_a = f
        .append(&route("m2/a", 0, &["x.example.com"], "10.0.0.2:3000"))
        .await;
    let reconciler = f.reconciler();
    reconciler
        .reconcile_snapshot(&snap(
            "m2",
            "10.0.0.2",
            vec![("a", caddy_labels(&[(0, "x.example.com", 3000)]))],
        ))
        .await
        .unwrap();
    create_a.assert_async().await;

    // m1/b asks for the host m2/a holds, and is refused.
    let b = snap(
        "m1",
        "10.0.0.1",
        vec![("b", caddy_labels(&[(0, "x.example.com", 4000)]))],
    );
    assert!(reconciler.reconcile_snapshot(&b).await.is_err());

    // m2/a gives the host up on another machine's snapshot.
    let _delete = f
        .caddy
        .mock("DELETE", path("m2/a", 0).as_str())
        .with_status(200)
        .create_async()
        .await;
    f.append(&route("m2/a", 1, &["y.example.com"], "10.0.0.2:3001"))
        .await;
    reconciler
        .reconcile_snapshot(&snap(
            "m2",
            "10.0.0.2",
            vec![("a", caddy_labels(&[(1, "y.example.com", 3001)]))],
        ))
        .await
        .unwrap();

    // m1's snapshot is the one refused before, unchanged, and now goes through.
    let create_b = f
        .append(&route("m1/b", 0, &["x.example.com"], "10.0.0.1:4000"))
        .await;
    reconciler.reconcile_snapshot(&b).await.unwrap();
    create_b.assert_async().await;
}

#[tokio::test]
async fn dry_run_makes_no_caddy_requests() {
    let mut f = fixture().await;
    let mut never = Vec::new();
    for method in ["GET", "POST", "PATCH", "DELETE"] {
        never.push(f.never(method).await);
    }
    let reconciler = Reconciler::new(f.pool.clone(), f.options(true)).with_caddy(f.caddy_client());

    reconciler
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![("app", caddy_labels(&[(0, "app.example.com", 3000)]))],
        ))
        .await
        .unwrap();
    reconciler.full_resync().await.unwrap();
    reconciler.sweep_pending_removals().await.unwrap();

    for mock in never {
        mock.assert_async().await;
    }
}

#[derive(Clone)]
struct LogBuf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
    type Writer = LogBuf;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test]
async fn caddy_labels_without_caddy_configured_are_skipped_with_a_warning() {
    let f = fixture().await;
    let buf = LogBuf(Default::default());
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buf.clone())
        .with_max_level(tracing::Level::WARN)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    Reconciler::new(f.pool.clone(), f.options(false))
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![("app", caddy_labels(&[(0, "app.example.com", 3000)]))],
        ))
        .await
        .unwrap();

    let logs = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    assert!(
        logs.contains("Caddy is not configured") && logs.contains("m1/app"),
        "{logs}"
    );
    assert!(f.rows("m1/app").await.is_empty());
}

/// Publish `m1/app` with `groups` at `at` on a reconciler with Caddy alone,
/// then forget every Caddy mock.
async fn publish_app(f: &mut Fixture, at: i64, groups: &[(u8, &str, u16)]) -> Reconciler {
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let _append = f
        .caddy
        .mock("POST", ANCHOR_ROUTES)
        .with_status(200)
        .create_async()
        .await;
    let reconciler = f.reconciler();
    reconciler.set_now(at);
    reconciler
        .reconcile_snapshot(&snap("m1", "10.0.0.1", vec![("app", caddy_labels(groups))]))
        .await
        .unwrap();
    f.caddy.reset();
    reconciler
}

/// Push m1's snapshot with no containers at `at`.
async fn leave(reconciler: &Reconciler, at: i64) -> Result<(), ReconcileError> {
    reconciler.set_now(at);
    reconciler
        .reconcile_snapshot(&snap("m1", "10.0.0.1", vec![]))
        .await
}

async fn pending(f: &Fixture) -> Vec<storage::PendingCaddyRemoval> {
    storage::list_pending_caddy_removals(&f.pool).await.unwrap()
}

fn pending_app(index: i64, host: &str, last_seen: i64) -> storage::PendingCaddyRemoval {
    storage::PendingCaddyRemoval {
        service_id: "m1/app".into(),
        caddy_index: index,
        hosts: vec![host.into()],
        last_seen,
    }
}

#[tokio::test]
async fn a_route_whose_service_leaves_within_grace_is_kept_pending() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    let delete = f.never("DELETE").await;

    leave(&reconciler, 1_010).await.unwrap();

    delete.assert_async().await;
    assert_eq!(
        pending(&f).await,
        vec![pending_app(0, "app.example.com", 1_000)]
    );
}

/// Expect `service_id`'s route at `index` deleted `times` times, answered
/// with `status`.
async fn expect_delete(
    f: &mut Fixture,
    service_id: &str,
    index: u8,
    status: usize,
    times: usize,
) -> Mock {
    f.caddy
        .mock("DELETE", path(service_id, index).as_str())
        .with_status(status)
        .expect(times)
        .create_async()
        .await
}

#[tokio::test]
async fn the_sweep_deletes_a_pending_route_once_grace_ends_without_a_full_resync() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    leave(&reconciler, 1_010).await.unwrap();

    let early = f.never("DELETE").await;
    reconciler.set_now(1_000 + GATUS_REMOVAL_GRACE_SECS - 1);
    reconciler.sweep_pending_removals().await.unwrap();
    early.assert_async().await;
    assert_eq!(pending(&f).await.len(), 1);
    f.caddy.reset();

    let delete = expect_delete(&mut f, "m1/app", 0, 200, 1).await;
    reconciler.set_now(1_000 + GATUS_REMOVAL_GRACE_SECS);
    let report = reconciler.sweep_pending_removals().await.unwrap();

    delete.assert_async().await;
    assert_eq!(report.repaired, 1);
    assert!(pending(&f).await.is_empty());
}

#[tokio::test]
async fn a_route_whose_service_left_past_grace_is_deleted_in_the_apply() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    let delete = expect_delete(&mut f, "m1/app", 0, 200, 1).await;

    leave(&reconciler, 1_000 + GATUS_REMOVAL_GRACE_SECS)
        .await
        .unwrap();

    delete.assert_async().await;
    assert!(pending(&f).await.is_empty());
}

#[tokio::test]
async fn a_service_back_within_grace_keeps_its_route_and_gets_its_mapping_back() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    leave(&reconciler, 1_010).await.unwrap();

    let live = route("m1/app", 0, &["app.example.com"], "10.0.0.1:3000");
    let _anchor = f.anchor(json!([live.clone()])).await;
    let mut never = Vec::new();
    for method in ["POST", "PATCH", "DELETE"] {
        never.push(f.never(method).await);
    }
    let buf = LogBuf(Default::default());
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::INFO)
            .finish(),
    );
    reconciler.set_now(1_020);
    reconciler
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![("app", caddy_labels(&[(0, "app.example.com", 3000)]))],
        ))
        .await
        .unwrap();
    reconciler.set_now(1_000 + GATUS_REMOVAL_GRACE_SECS + 60);
    reconciler.sweep_pending_removals().await.unwrap();

    for mock in never {
        mock.assert_async().await;
    }
    let rows = f.rows("m1/app").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].route, live);
    assert!(pending(&f).await.is_empty());
    let logs = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    assert!(
        !logs.contains("adopted"),
        "a restored route is not an adoption: {logs}"
    );
}

/// Ingest m1's snapshot holding `app` or nothing with the clock at `at`,
/// returning the worker if the ingest started one.
async fn ingest_app(reconciler: &Reconciler, at: i64, present: bool) -> Option<ApplyWorker> {
    reconciler.set_now(at);
    let containers = if present {
        vec![("app", caddy_labels(&[(0, "app.example.com", 3000)]))]
    } else {
        vec![]
    };
    reconciler
        .ingest_snapshot(&snap("m1", "10.0.0.1", containers))
        .await
        .unwrap()
}

/// The route of `m1/app` is deleted once grace has run from `last_seen`,
/// not before.
async fn assert_deleted_once_grace_ends(f: &mut Fixture, reconciler: &Reconciler, last_seen: i64) {
    let early = f.never("DELETE").await;
    reconciler.set_now(last_seen + GATUS_REMOVAL_GRACE_SECS - 1);
    reconciler.sweep_pending_removals().await.unwrap();
    early.assert_async().await;
    f.caddy.reset();

    let delete = expect_delete(f, "m1/app", 0, 200, 1).await;
    reconciler.set_now(last_seen + GATUS_REMOVAL_GRACE_SECS);
    reconciler.sweep_pending_removals().await.unwrap();
    delete.assert_async().await;
    assert!(pending(f).await.is_empty());
}

#[tokio::test]
async fn a_route_captured_by_a_replaced_snapshot_keeps_its_grace() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1, &[(0, "app.example.com", 3000)]).await;

    // app leaves at t=10, is back at t=20 and gone at t=30, all behind one
    // apply.  Only the t=10 snapshot captured its route.
    let worker = ingest_app(&reconciler, 10, false).await.unwrap();
    assert!(ingest_app(&reconciler, 20, true).await.is_none());
    assert!(ingest_app(&reconciler, 30, false).await.is_none());
    worker.run().await;

    assert_eq!(
        pending(&f).await,
        vec![pending_app(0, "app.example.com", 20)]
    );
    assert_deleted_once_grace_ends(&mut f, &reconciler, 20).await;
}

#[tokio::test]
async fn a_service_that_returns_and_leaves_again_while_coalesced_keeps_its_grace() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1, &[(0, "app.example.com", 3000)]).await;
    // app leaves: its removal waits out the grace period from t=1.
    ingest_app(&reconciler, 10, false)
        .await
        .unwrap()
        .run()
        .await;

    // app is back at t=20 and gone again at t=30, both behind one apply.
    let worker = ingest_app(&reconciler, 20, true).await.unwrap();
    assert!(ingest_app(&reconciler, 30, false).await.is_none());
    worker.run().await;

    assert_eq!(
        pending(&f).await,
        vec![pending_app(0, "app.example.com", 20)]
    );
    assert_deleted_once_grace_ends(&mut f, &reconciler, 20).await;
}

#[tokio::test]
async fn a_returning_service_gets_back_only_the_routes_it_still_labels() {
    let mut f = fixture().await;
    let reconciler = publish_app(
        &mut f,
        1_000,
        &[(0, "a.example.com", 3000), (1, "b.example.com", 3001)],
    )
    .await;
    leave(&reconciler, 1_010).await.unwrap();

    let kept = route("m1/app", 0, &["a.example.com"], "10.0.0.1:3000");
    let _anchor = f
        .anchor(json!([
            kept.clone(),
            route("m1/app", 1, &["b.example.com"], "10.0.0.1:3001"),
        ]))
        .await;
    let mut never = Vec::new();
    for method in ["POST", "PATCH", "DELETE"] {
        never.push(f.never(method).await);
    }
    reconciler.set_now(1_020);
    reconciler
        .reconcile_snapshot(&snap(
            "m1",
            "10.0.0.1",
            vec![("app", caddy_labels(&[(0, "a.example.com", 3000)]))],
        ))
        .await
        .unwrap();
    for mock in never {
        mock.assert_async().await;
    }
    let rows = f.rows("m1/app").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].route, kept);
    assert_eq!(
        pending(&f).await,
        vec![pending_app(1, "b.example.com", 1_000)]
    );
    f.caddy.reset();

    // The unlabelled route goes once its grace period from t=1000 ends.
    let keep_0 = expect_delete(&mut f, "m1/app", 0, 200, 0).await;
    let delete_1 = expect_delete(&mut f, "m1/app", 1, 200, 1).await;
    reconciler.set_now(1_000 + GATUS_REMOVAL_GRACE_SECS);
    reconciler.sweep_pending_removals().await.unwrap();
    keep_0.assert_async().await;
    delete_1.assert_async().await;
    assert!(pending(&f).await.is_empty());
    assert_eq!(f.rows("m1/app").await.len(), 1);
}

#[tokio::test]
async fn a_failed_pending_delete_stays_for_the_next_sweep() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    leave(&reconciler, 1_010).await.unwrap();

    let failed = expect_delete(&mut f, "m1/app", 0, 502, 1).await;
    let _anchor = f.anchor(json!([])).await;
    reconciler.set_now(1_000 + GATUS_REMOVAL_GRACE_SECS);
    let report = reconciler.sweep_pending_removals().await.unwrap();
    failed.assert_async().await;
    assert_eq!(report.failed, 1);
    assert_eq!(
        pending(&f).await,
        vec![pending_app(0, "app.example.com", 1_000)]
    );
    assert_eq!(reconciler.caddy_breaker.consecutive_failures(), 1);
    f.caddy.reset();

    let retried = expect_delete(&mut f, "m1/app", 0, 200, 1).await;
    let _anchor = f.anchor(json!([])).await;
    reconciler.set_now(1_000 + GATUS_REMOVAL_GRACE_SECS + 60);
    reconciler.sweep_pending_removals().await.unwrap();
    retried.assert_async().await;
    assert!(pending(&f).await.is_empty());
    assert_eq!(reconciler.caddy_breaker.consecutive_failures(), 0);
}

#[tokio::test]
async fn a_pending_route_already_gone_is_cleared_without_feeding_the_breaker() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    leave(&reconciler, 1_010).await.unwrap();
    // Two failures short of the threshold: one more would open it.
    for _ in 1..CADDY_BREAKER_THRESHOLD {
        reconciler
            .caddy_breaker
            .record_failure(std::time::Instant::now());
    }

    let gone = expect_delete(&mut f, "m1/app", 0, 404, 1).await;
    let _anchor = f.anchor(json!([])).await;
    reconciler.set_now(1_000 + GATUS_REMOVAL_GRACE_SECS);
    let report = reconciler.sweep_pending_removals().await.unwrap();

    gone.assert_async().await;
    assert_eq!(report.failed, 0);
    assert!(pending(&f).await.is_empty());
    assert_eq!(reconciler.caddy_breaker.consecutive_failures(), 0);
    assert!(!reconciler.caddy_breaker.is_open(std::time::Instant::now()));
}

#[tokio::test]
async fn a_route_due_while_the_breaker_is_open_is_parked_pending() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    for _ in 0..CADDY_BREAKER_THRESHOLD {
        reconciler
            .caddy_breaker
            .record_failure(std::time::Instant::now());
    }
    let mut never = Vec::new();
    for method in ["GET", "DELETE"] {
        never.push(f.never(method).await);
    }

    leave(&reconciler, 1_000 + GATUS_REMOVAL_GRACE_SECS)
        .await
        .unwrap();
    reconciler.sweep_pending_removals().await.unwrap();

    for mock in never {
        mock.assert_async().await;
    }
    assert_eq!(
        pending(&f).await,
        vec![pending_app(0, "app.example.com", 1_000)]
    );
}

#[tokio::test]
async fn a_route_due_with_caddy_unconfigured_is_parked_pending() {
    let mut f = fixture().await;
    publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    let unconfigured = Reconciler::new(f.pool.clone(), f.options(false));

    leave(&unconfigured, 1_000 + GATUS_REMOVAL_GRACE_SECS)
        .await
        .unwrap();
    unconfigured.sweep_pending_removals().await.unwrap();

    assert_eq!(
        pending(&f).await,
        vec![pending_app(0, "app.example.com", 1_000)]
    );
}

#[tokio::test]
async fn dry_run_sweeps_delete_no_pending_route() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    leave(&reconciler, 1_010).await.unwrap();
    let mut never = Vec::new();
    for method in ["GET", "POST", "PATCH", "DELETE"] {
        never.push(f.never(method).await);
    }
    let dry = Reconciler::new(f.pool.clone(), f.options(true)).with_caddy(f.caddy_client());
    dry.set_now(1_000 + GATUS_REMOVAL_GRACE_SECS + 60);

    dry.sweep_pending_removals().await.unwrap();
    dry.full_resync().await.unwrap();

    for mock in never {
        mock.assert_async().await;
    }
    assert_eq!(
        pending(&f).await,
        vec![pending_app(0, "app.example.com", 1_000)]
    );
}

#[tokio::test]
async fn a_departed_route_holds_its_hosts_before_its_apply_runs() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    // m1's departure is persisted, and its apply has not run yet.
    let _worker = ingest_app(&reconciler, 1_010, false).await.unwrap();

    let _anchor = f.anchor(json!([])).await;
    let post = f.never("POST").await;
    let result = reconciler
        .reconcile_snapshot(&snap(
            "m2",
            "10.0.0.2",
            vec![("x", caddy_labels(&[(0, "app.example.com", 4000)]))],
        ))
        .await;

    assert!(result.is_err(), "m2/x must not take m1/app's host");
    post.assert_async().await;
    assert!(f.rows("m2/x").await.is_empty());
}

#[tokio::test]
async fn a_route_whose_delete_fails_in_the_apply_is_kept_pending() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    let failed = expect_delete(&mut f, "m1/app", 0, 502, 1).await;

    let result = leave(&reconciler, 1_000 + GATUS_REMOVAL_GRACE_SECS).await;

    assert!(result.is_err());
    failed.assert_async().await;
    assert_eq!(
        pending(&f).await,
        vec![pending_app(0, "app.example.com", 1_000)]
    );
    assert_eq!(reconciler.caddy_breaker.consecutive_failures(), 1);
}

#[tokio::test]
async fn an_apply_that_never_reaches_caddy_leaves_the_breaker_alone() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    for _ in 1..CADDY_BREAKER_THRESHOLD {
        reconciler
            .caddy_breaker
            .record_failure(std::time::Instant::now());
    }

    leave(&reconciler, 1_010).await.unwrap();

    assert_eq!(
        reconciler.caddy_breaker.consecutive_failures(),
        CADDY_BREAKER_THRESHOLD - 1
    );
}

#[tokio::test]
async fn a_route_captured_by_a_replaced_snapshot_is_deleted_in_the_apply_past_grace() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1, &[(0, "app.example.com", 3000)]).await;
    let delete = expect_delete(&mut f, "m1/app", 0, 200, 1).await;

    // app leaves at t=10, is back at t=20 and gone past grace from there,
    // all behind one apply.  Only the t=10 snapshot captured its route.
    let gone_at = 20 + GATUS_REMOVAL_GRACE_SECS + 10;
    let worker = ingest_app(&reconciler, 10, false).await.unwrap();
    assert!(ingest_app(&reconciler, 20, true).await.is_none());
    assert!(ingest_app(&reconciler, gone_at, false).await.is_none());
    worker.run().await;

    delete.assert_async().await;
    assert!(pending(&f).await.is_empty());
}

/// m1/app's published route, as its mapping row holds it.
fn app_route(hosts: &[&str], port: u16) -> Value {
    route("m1/app", 0, hosts, &format!("10.0.0.1:{port}"))
}

#[tokio::test]
async fn the_drift_check_recreates_a_deleted_route_without_a_snapshot() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let create = f.append(&app_route(&["app.example.com"], 3000)).await;

    let report = reconciler.sweep_pending_removals().await.unwrap();

    create.assert_async().await;
    assert_eq!(report.repaired, 1);
    assert_eq!(reconciler.caddy_anchor(), CaddyAnchor::Ok);
}

#[tokio::test]
async fn the_full_resync_runs_the_drift_check_too() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let create = f.append(&app_route(&["app.example.com"], 3000)).await;

    reconciler.full_resync().await.unwrap();

    create.assert_async().await;
}

#[tokio::test]
async fn the_drift_check_reverts_a_hand_edited_route() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    let _anchor = f
        .anchor(json!([app_route(
            &["app.example.com", "extra.example.com"],
            9999
        )]))
        .await;
    let post = f.never("POST").await;
    let revert = f
        .caddy
        .mock("PATCH", path("m1/app", 0).as_str())
        .match_body(Matcher::Json(app_route(&["app.example.com"], 3000)))
        .with_status(200)
        .expect(1)
        .create_async()
        .await;

    let report = reconciler.sweep_pending_removals().await.unwrap();

    revert.assert_async().await;
    post.assert_async().await;
    assert_eq!(report.repaired, 1);
}

#[tokio::test]
async fn the_drift_check_restores_the_last_claimed_route_not_a_refused_label_edit() {
    let mut f = fixture().await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let _append = f
        .caddy
        .mock("POST", ANCHOR_ROUTES)
        .with_status(200)
        .create_async()
        .await;
    let reconciler = f.reconciler();
    let apps = |b_host| {
        snap(
            "m1",
            "10.0.0.1",
            vec![
                ("a", caddy_labels(&[(0, "x.example.com", 3000)])),
                ("b", caddy_labels(&[(0, b_host, 4000)])),
            ],
        )
    };
    reconciler
        .reconcile_snapshot(&apps("y.example.com"))
        .await
        .unwrap();
    f.caddy.reset();
    // b's edit asks for the host a holds, and is refused.
    let a_route = route("m1/a", 0, &["x.example.com"], "10.0.0.1:3000");
    let b_route = route("m1/b", 0, &["y.example.com"], "10.0.0.1:4000");
    let _anchor = f.anchor(json!([a_route, b_route])).await;
    assert!(
        reconciler
            .reconcile_snapshot(&apps("x.example.com"))
            .await
            .is_err()
    );
    f.caddy.reset();

    // Someone deletes b's route by hand.  The drift check puts back the
    // route b last claimed.
    let _anchor = f.anchor(json!([a_route])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let restore = f.append(&b_route).await;
    let patch = f.never("PATCH").await;

    reconciler.sweep_pending_removals().await.unwrap();

    restore.assert_async().await;
    patch.assert_async().await;
}

#[tokio::test]
async fn the_drift_check_leaves_a_marked_route_outside_the_anchor_alone() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f
        .caddy
        .mock("GET", path("m1/app", 0).as_str())
        .with_status(200)
        .with_body(app_route(&["app.example.com"], 3000).to_string())
        .create_async()
        .await;
    let post = f.never("POST").await;
    let patch = f.never("PATCH").await;
    let buf = LogBuf(Default::default());
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::ERROR)
            .finish(),
    );

    let report = reconciler.sweep_pending_removals().await.unwrap();

    post.assert_async().await;
    patch.assert_async().await;
    assert_eq!(report.failed, 1);
    let logs = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    assert!(
        logs.contains("outside the anchor") && logs.contains(&caddy_route_marker("m1/app", 0)),
        "{logs}"
    );
    assert_eq!(reconciler.caddy_breaker.consecutive_failures(), 0);
}

#[tokio::test]
async fn the_drift_check_writes_nothing_when_caddy_matches() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    let read = f
        .caddy
        .mock("GET", ANCHOR_ROUTES)
        .with_status(200)
        .with_body(json!([app_route(&["app.example.com"], 3000)]).to_string())
        .expect(1)
        .create_async()
        .await;
    let mut never = Vec::new();
    for method in ["POST", "PATCH", "DELETE"] {
        never.push(f.never(method).await);
    }

    let report = reconciler.sweep_pending_removals().await.unwrap();

    read.assert_async().await;
    for mock in never {
        mock.assert_async().await;
    }
    assert_eq!(report.verified, 1);
    assert_eq!(report.repaired, 0);
    assert_eq!(reconciler.caddy_anchor(), CaddyAnchor::Ok);
}

#[tokio::test]
async fn the_drift_check_leaves_unmapped_and_pending_routes_alone() {
    let mut f = fixture().await;
    // m1/app leaves, so its route is a pending removal within grace.
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    leave(&reconciler, 1_010).await.unwrap();
    // m2/x is mapped and in sync.
    let x_route = route("m2/x", 0, &["x.example.com"], "10.0.0.2:4000");
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    f.append(&x_route).await;
    reconciler
        .reconcile_snapshot(&snap(
            "m2",
            "10.0.0.2",
            vec![("x", caddy_labels(&[(0, "x.example.com", 4000)]))],
        ))
        .await
        .unwrap();
    f.caddy.reset();

    // The anchor lacks the pending route and holds a marked route nothing
    // maps.
    let ghost = route("m9/ghost", 0, &["ghost.example.com"], "10.0.0.9:80");
    let _anchor = f.anchor(json!([x_route, ghost])).await;
    let mut never = Vec::new();
    for method in ["POST", "PATCH", "DELETE"] {
        never.push(f.never(method).await);
    }

    reconciler.set_now(1_020);
    let report = reconciler.sweep_pending_removals().await.unwrap();

    for mock in never {
        mock.assert_async().await;
    }
    // Only m2/x was checked: trying to restore the pending route would have
    // looked it up outside the anchor first, which no mock answers.
    assert_eq!(report.verified, 1);
    assert_eq!(report.failed, 0);
}

#[tokio::test]
async fn the_drift_check_reports_a_missing_anchor_and_never_creates_it() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    let missing = f
        .caddy
        .mock("GET", ANCHOR_ROUTES)
        .with_status(404)
        .with_body(r#"{"error":"unknown object ID 'docktornet'"}"#)
        .expect(1)
        .create_async()
        .await;
    let mut never = Vec::new();
    for method in ["POST", "PUT", "PATCH", "DELETE"] {
        never.push(f.never(method).await);
    }
    let buf = LogBuf(Default::default());
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::ERROR)
            .finish(),
    );

    let report = reconciler.sweep_pending_removals().await.unwrap();

    missing.assert_async().await;
    for mock in never {
        mock.assert_async().await;
    }
    assert_eq!(report.failed, 1);
    assert_eq!(reconciler.caddy_anchor(), CaddyAnchor::Missing);
    let logs = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("anchor route not found"), "{logs}");
}

#[tokio::test]
async fn the_drift_check_is_skipped_while_the_breaker_is_open() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    let _anchor = f
        .anchor(json!([app_route(&["app.example.com"], 3000)]))
        .await;
    reconciler.sweep_pending_removals().await.unwrap();
    assert_eq!(reconciler.caddy_anchor(), CaddyAnchor::Ok);
    f.caddy.reset();
    for _ in 0..CADDY_BREAKER_THRESHOLD {
        reconciler
            .caddy_breaker
            .record_failure(std::time::Instant::now());
    }
    let never = f.never("GET").await;

    reconciler.sweep_pending_removals().await.unwrap();
    reconciler.full_resync().await.unwrap();

    never.assert_async().await;
    assert_eq!(reconciler.caddy_anchor(), CaddyAnchor::Unknown);
}

#[tokio::test]
async fn the_drift_check_feeds_the_breaker_when_caddy_is_unreachable() {
    let mut f = fixture().await;
    let reconciler = publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    let _down = f
        .caddy
        .mock("GET", ANCHOR_ROUTES)
        .with_status(502)
        .create_async()
        .await;

    for _ in 0..CADDY_BREAKER_THRESHOLD {
        assert_eq!(reconciler.sweep_pending_removals().await.unwrap().failed, 1);
    }

    assert!(reconciler.caddy_breaker.is_open(std::time::Instant::now()));
    assert_eq!(reconciler.caddy_anchor(), CaddyAnchor::Unknown);
}

#[tokio::test]
async fn a_dry_run_drift_check_sends_no_writes() {
    let mut f = fixture().await;
    publish_app(&mut f, 1_000, &[(0, "app.example.com", 3000)]).await;
    let _anchor = f.anchor(json!([])).await;
    let _elsewhere = f.no_marked_route_elsewhere().await;
    let mut never = Vec::new();
    for method in ["POST", "PATCH", "DELETE"] {
        never.push(f.never(method).await);
    }
    let dry = Reconciler::new(f.pool.clone(), f.options(true)).with_caddy(f.caddy_client());

    dry.sweep_pending_removals().await.unwrap();
    dry.full_resync().await.unwrap();

    for mock in never {
        mock.assert_async().await;
    }
}
