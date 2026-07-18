use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use subtle::ConstantTimeEq;
use tracing::warn;

/// Shared state carried by the bearer-auth middleware.
#[derive(Clone)]
pub struct AuthState {
    pub secret: Arc<String>,
}

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
    if !token_matches(provided, state.secret.as_str()) {
        warn!("rejected request with invalid or missing bearer token");
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(req).await)
}

/// Compares a caller-supplied bearer token against the configured secret in
/// constant time, avoiding the early-exit timing side-channel of `==`/`!=`.
///
/// A `None` token (missing/invalid header) never matches. `subtle`'s slice
/// comparison already folds the length check into a constant-time result, so
/// mismatched lengths are rejected without leaking timing information about
/// the secret's contents.
fn token_matches(provided: Option<&str>, secret: &str) -> bool {
    match provided {
        Some(provided) => provided.as_bytes().ct_eq(secret.as_bytes()).into(),
        None => false,
    }
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

    #[test]
    fn token_matches_is_constant_time_and_correct() {
        // The comparison must be performed with a constant-time primitive
        // (no early-exit `!=` on the secret) to avoid a timing side-channel.
        // These cases pin the correctness contract of that primitive.
        assert!(token_matches(Some("s3cret"), "s3cret"));
        assert!(!token_matches(Some("nope"), "s3cret"));
        // differs only in the final byte (would early-exit late with `!=`)
        assert!(!token_matches(Some("s3creT"), "s3cret"));
        // length mismatch must be rejected without panicking
        assert!(!token_matches(Some("s3cre"), "s3cret"));
        assert!(!token_matches(Some("s3cret-extra"), "s3cret"));
        assert!(!token_matches(None, "s3cret"));
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
