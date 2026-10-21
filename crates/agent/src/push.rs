use std::time::Duration;

use docktornet_core::Snapshot;
use reqwest::Client;
use tracing::{info, warn};

const MAX_ERROR_BODY_LEN: usize = 4096;

#[derive(Clone)]
pub struct PushClient {
    http: Client,
    base: String,
    secret: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PushError {
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("server returned {status}")]
    Status { status: u16, body: String },
    #[error("push attempts must be greater than zero")]
    NoAttempts,
}

impl PushClient {
    pub fn new(base: impl Into<String>, secret: impl Into<String>) -> anyhow::Result<Self> {
        let http = Client::builder().timeout(Duration::from_secs(10)).build()?;

        Ok(Self {
            http,
            base: base.into(),
            secret: secret.into(),
        })
    }

    pub async fn push(&self, snap: &Snapshot) -> Result<(), PushError> {
        let url = format!("{}/api/v1/snapshot", self.base.trim_end_matches('/'));
        let resp = self
            .http
            .post(url)
            .bearer_auth(&self.secret)
            .json(snap)
            .send()
            .await?;
        let status = resp.status();

        if status.is_success() {
            return Ok(());
        }

        let body = read_error_body(resp).await;
        Err(PushError::Status {
            status: status.as_u16(),
            body,
        })
    }

    pub async fn push_with_retry(&self, snap: &Snapshot, attempts: u32) -> Result<(), PushError> {
        if attempts == 0 {
            return Err(PushError::NoAttempts);
        }

        let mut delay = Duration::from_secs(1);
        let mut last_err = None;

        for attempt in 1..=attempts {
            match self.push(snap).await {
                Ok(()) => {
                    if attempt > 1 {
                        info!(attempt, "snapshot push recovered");
                    }
                    return Ok(());
                }
                Err(error) => {
                    warn!(attempt, max = attempts, error = %error, "snapshot push failed");
                    let should_retry = error.is_retryable();
                    last_err = Some(error);

                    if should_retry && attempt < attempts {
                        tokio::time::sleep(delay).await;
                        delay = (delay * 4).min(Duration::from_secs(60));
                    } else {
                        break;
                    }
                }
            }
        }

        Err(last_err.expect("at least one push attempt"))
    }
}

impl PushError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Status { status, .. } => *status == 429 || (500..=599).contains(status),
            Self::NoAttempts => false,
        }
    }
}

fn truncate_body(body: String) -> String {
    if body.len() <= MAX_ERROR_BODY_LEN {
        return body;
    }

    let mut end = MAX_ERROR_BODY_LEN;
    while !body.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…<truncated>", &body[..end])
}

async fn read_error_body(resp: reqwest::Response) -> String {
    if resp
        .content_length()
        .is_some_and(|len| len > MAX_ERROR_BODY_LEN as u64)
    {
        return "<response body too large>".to_string();
    }

    truncate_body(resp.text().await.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use docktornet_core::{ContainerEntry, MachineId};
    use mockito::Server;

    use super::*;

    fn snap() -> Snapshot {
        Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.10".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "a".into(),
                labels: BTreeMap::new(),
            }],
        }
    }

    #[tokio::test]
    async fn push_sends_bearer_token_and_json_body() {
        let mut server = Server::new_async().await;
        let m = server
            .mock("POST", "/api/v1/snapshot")
            .match_header("authorization", "Bearer s3cret")
            .match_header("content-type", "application/json")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "machine_id": "m1"
            })))
            .with_status(204)
            .create_async()
            .await;

        let client = PushClient::new(server.url(), "s3cret").unwrap();
        client.push(&snap()).await.unwrap();

        m.assert_async().await;
    }

    #[tokio::test]
    async fn push_returns_status_error_on_4xx() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/api/v1/snapshot")
            .with_status(401)
            .with_body("nope")
            .create_async()
            .await;

        let client = PushClient::new(server.url(), "wrong").unwrap();
        let err = client.push(&snap()).await.unwrap_err();

        match err {
            PushError::Status { status, body } => {
                assert_eq!(status, 401);
                assert_eq!(body, "nope");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn push_rejects_declared_large_error_body() {
        let body = "x".repeat(MAX_ERROR_BODY_LEN + 100);
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/api/v1/snapshot")
            .with_status(500)
            .with_body(body)
            .create_async()
            .await;

        let client = PushClient::new(server.url(), "s").unwrap();
        let err = client.push(&snap()).await.unwrap_err();

        match err {
            PushError::Status { body, .. } => {
                assert_eq!(body, "<response body too large>");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn status_error_display_does_not_include_body() {
        let err = PushError::Status {
            status: 500,
            body: "secret diagnostic".into(),
        };

        let rendered = err.to_string();
        assert_eq!(rendered, "server returned 500");
        assert!(!rendered.contains("secret diagnostic"));
    }

    #[test]
    fn truncate_body_preserves_utf8_boundaries() {
        let body = format!("{}é", "x".repeat(MAX_ERROR_BODY_LEN));
        let truncated = truncate_body(body);

        assert!(truncated.ends_with("…<truncated>"));
        assert!(std::str::from_utf8(truncated.as_bytes()).is_ok());
    }

    #[tokio::test]
    async fn push_with_retry_rejects_zero_attempts() {
        let client = PushClient::new("http://127.0.0.1:1", "s").unwrap();
        let err = client.push_with_retry(&snap(), 0).await.unwrap_err();
        assert!(matches!(err, PushError::NoAttempts));
    }

    #[tokio::test]
    async fn push_with_retry_recovers_after_one_failure() {
        let mut server = Server::new_async().await;
        let m1 = server
            .mock("POST", "/api/v1/snapshot")
            .with_status(503)
            .expect(1)
            .create_async()
            .await;
        let m2 = server
            .mock("POST", "/api/v1/snapshot")
            .with_status(204)
            .expect(1)
            .create_async()
            .await;

        let client = PushClient::new(server.url(), "s").unwrap();
        client.push_with_retry(&snap(), 2).await.unwrap();

        m1.assert_async().await;
        m2.assert_async().await;
    }

    #[tokio::test]
    async fn push_with_retry_does_not_retry_auth_failures() {
        let mut server = Server::new_async().await;
        let m = server
            .mock("POST", "/api/v1/snapshot")
            .with_status(401)
            .expect(1)
            .create_async()
            .await;

        let client = PushClient::new(server.url(), "wrong").unwrap();
        let err = client.push_with_retry(&snap(), 3).await.unwrap_err();

        assert!(matches!(err, PushError::Status { status: 401, .. }));
        m.assert_async().await;
    }
}
