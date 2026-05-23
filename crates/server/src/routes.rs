use axum::Router;
use axum::routing::post;

use crate::auth::{AuthState, require_bearer};
use crate::handlers::snapshot::{self, AppState};

pub fn build(state: AppState) -> Router {
    let auth_state = AuthState {
        secret: state.secret.clone(),
    };
    Router::new()
        .route("/api/v1/snapshot", post(snapshot::ingest))
        .layer(axum::middleware::from_fn_with_state(
            auth_state,
            require_bearer,
        ))
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
                npm_forward_host: "127.0.0.1".to_string(),
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
}
