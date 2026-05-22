use std::sync::Arc;

use axum::Router;
use axum::routing::post;

use crate::auth::{AuthState, require_bearer};
use crate::db::Db;
use crate::handlers::snapshot::{AppState, ingest};

pub fn build(db: Db, secret: String) -> Router {
    let auth_state = AuthState {
        secret: Arc::new(secret.clone()),
    };
    let app_state = AppState {
        db,
        secret: auth_state.secret.clone(),
    };

    Router::new()
        .route("/api/v1/snapshot", post(ingest))
        .layer(axum::middleware::from_fn_with_state(
            auth_state.clone(),
            require_bearer,
        ))
        .with_state(app_state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use docktornet_core::{ContainerEntry, MachineId, Snapshot};
    use std::collections::BTreeMap;
    use tempfile::tempdir;
    use tower::ServiceExt;

    use crate::db;
    use crate::storage::list_services_for_machine;

    async fn make_app() -> (Router, Db) {
        let dir = Box::leak(Box::new(tempdir().unwrap()));
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let router = build(pool.clone(), "s3cret".into());
        (router, pool)
    }

    fn body(snap: &Snapshot) -> Body {
        Body::from(serde_json::to_vec(snap).unwrap())
    }

    fn snap() -> Snapshot {
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "u".into());
        Snapshot {
            machine_id: MachineId::new("m1"),
            taken_at: 1,
            containers: vec![ContainerEntry {
                name: "a".into(),
                labels,
            }],
        }
    }

    #[tokio::test]
    async fn ingest_requires_bearer() {
        let (app, _) = make_app().await;
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/snapshot")
            .header("content-type", "application/json")
            .body(body(&snap()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn ingest_persists_snapshot() {
        let (app, pool) = make_app().await;
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/snapshot")
            .header("authorization", "Bearer s3cret")
            .header("content-type", "application/json")
            .body(body(&snap()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        let rows = list_services_for_machine(&pool, "m1").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "m1/a");
    }
}
