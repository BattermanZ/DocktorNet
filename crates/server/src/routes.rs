use axum::Router;
use axum::routing::{get, post};

use crate::auth::{AuthState, require_bearer};
use crate::handlers::{health, snapshot::{self, AppState}};

pub fn build(state: AppState) -> Router {
    let auth_state = AuthState {
        secret: state.secret.clone(),
    };
    let authed = Router::new()
        .route("/api/v1/snapshot", post(snapshot::ingest))
        .layer(axum::middleware::from_fn_with_state(
            auth_state,
            require_bearer,
        ));
    Router::new()
        .route("/healthz", get(health::healthz))
        .merge(authed)
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use docktornet_core::{ContainerEntry, MachineId, Snapshot};
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;
    use tower::ServiceExt;

    use crate::db;
    use crate::flapping::FlappingTracker;
    use crate::reconcile::{ReconcileOptions, Reconciler};
    use crate::storage::list_services_for_machine;

    async fn make_app() -> (Router, crate::db::Db, tempfile::TempDir) {
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
        let state = AppState {
            reconciler,
            secret: Arc::new("s3cret".into()),
            flapping: Arc::new(FlappingTracker::new(Duration::from_secs(300))),
        };
        (build(state), pool, dir)
    }

    fn body(snap: &Snapshot) -> Body {
        Body::from(serde_json::to_vec(snap).unwrap())
    }

    fn snap() -> Snapshot {
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "u".into());
        labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
        Snapshot {
            machine_id: MachineId::new("m1"),
            forward_host: "10.0.0.10".into(),
            taken_at: 1,
            containers: vec![ContainerEntry {
                name: "a".into(),
                labels,
            }],
        }
    }

    fn req(method: &str, token: Option<&str>, snap: &Snapshot) -> Request<Body> {
        let mut b = Request::builder()
            .method(method)
            .uri("/api/v1/snapshot")
            .header("content-type", "application/json");
        if let Some(t) = token {
            b = b.header("authorization", format!("Bearer {t}"));
        }
        let mut request = b.body(body(snap)).unwrap();
        request
            .extensions_mut()
            .insert(axum::extract::ConnectInfo::<SocketAddr>(
                "127.0.0.1:9999".parse().unwrap(),
            ));
        request
    }

    #[tokio::test]
    async fn ingest_rejects_wrong_token() {
        let (app, _, _dir) = make_app().await;
        let resp = app
            .oneshot(req("POST", Some("wrongtoken"), &snap()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn ingest_requires_bearer() {
        let (app, _, _dir) = make_app().await;
        let resp = app.oneshot(req("POST", None, &snap())).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn ingest_persists_snapshot() {
        let (app, pool, _dir) = make_app().await;
        let resp = app
            .oneshot(req("POST", Some("s3cret"), &snap()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        let rows = list_services_for_machine(&pool, "m1").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "m1/a");
    }

    #[tokio::test]
    async fn ingest_acks_before_external_reconcile_and_survives_its_failure() {
        // A reconciler wired to an NPM server that fails every proxy-host
        // create. Because reconcile is deferred to a background task, the push
        // must still be acknowledged (204) and the snapshot persisted; the
        // external failure only affects the background task.
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut npm_server = mockito::Server::new_async().await;
        let _token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .expect_at_least(0)
            .create_async()
            .await;
        let _certs = npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .expect_at_least(0)
            .create_async()
            .await;
        let _create = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(500)
            .with_body("boom")
            .expect_at_least(0)
            .create_async()
            .await;

        let npm = crate::integrations::npm::NpmClient::new(crate::config::NpmConfig {
            url: url::Url::parse(&npm_server.url()).unwrap(),
            user: "u".into(),
            password: "p".into(),
        });
        let cf = crate::integrations::cloudflare::CloudflareClient::new(
            crate::config::CloudflareConfig {
                api_token: "t".into(),
                zone_id: "z".into(),
            },
        );
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
        let state = AppState {
            reconciler,
            secret: Arc::new("s3cret".into()),
            flapping: Arc::new(FlappingTracker::new(Duration::from_secs(300))),
        };
        let app = build(state);

        let mut labels = BTreeMap::new();
        labels.insert("docktornet.npm.0.host".into(), "a.example.com".into());
        labels.insert("docktornet.npm.0.port".into(), "3000".into());
        labels.insert("docktornet.npm.0.scheme".into(), "http".into());
        labels.insert("docktornet.npm.0.ssl".into(), "false".into());
        let snap = Snapshot {
            machine_id: MachineId::new("m1"),
            forward_host: "10.0.0.10".into(),
            taken_at: 1,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels,
            }],
        };

        let resp = app.oneshot(req("POST", Some("s3cret"), &snap)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // Snapshot is durably persisted before the ack, even though the
        // background NPM reconcile will fail.
        let rows = list_services_for_machine(&pool, "m1").await.unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn healthz_is_public_and_returns_200() {
        let (app, _pool, _dir) = make_app().await;
        let request = Request::builder()
            .method("GET")
            .uri("/healthz")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(request).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn healthz_lists_agents_after_snapshot_push() {
        // Use an app that has seen the snapshot so we can verify "m1" appears in the response.
        let (app, _pool, _dir) = make_app().await;

        // Push a snapshot for machine "m1".
        let push = req("POST", Some("s3cret"), &snap());
        app.clone().oneshot(push).await.unwrap();

        // Now fetch /healthz and assert the body contains "m1".
        let get_req = Request::builder()
            .method("GET")
            .uri("/healthz")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(get_req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = std::str::from_utf8(&body_bytes).unwrap();
        assert!(body_str.contains("m1"), "body should contain machine id m1: {body_str}");
    }
}
