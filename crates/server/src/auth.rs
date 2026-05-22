use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use tracing::warn;

/// Shared state carried by the bearer-auth middleware.
#[derive(Clone)]
pub struct AuthState {
    pub secret: Arc<String>,
}

#[allow(dead_code)]
pub async fn require_bearer(
    State(state): State<AuthState>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let header = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let provided = header.and_then(|h| h.strip_prefix("Bearer "));
    if provided != Some(state.secret.as_str()) {
        warn!("rejected request with invalid or missing bearer token");
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::{Router, routing::get};
    use tower::ServiceExt;

    fn app() -> Router {
        let state = AuthState {
            secret: Arc::new("s3cret".into()),
        };
        Router::new()
            .route("/ping", get(|| async { "pong" }))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                require_bearer,
            ))
            .with_state(state)
    }

    #[tokio::test]
    async fn missing_header_returns_401() {
        let resp = app()
            .oneshot(Request::builder().uri("/ping").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn correct_token_returns_200() {
        let req = Request::builder()
            .uri("/ping")
            .header("authorization", "Bearer s3cret")
            .body(Body::empty())
            .unwrap();
        let resp = app().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn wrong_token_returns_401() {
        let req = Request::builder()
            .uri("/ping")
            .header("authorization", "Bearer nope")
            .body(Body::empty())
            .unwrap();
        let resp = app().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
