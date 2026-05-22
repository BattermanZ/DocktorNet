use std::time::Duration;

use docktornet_core::Snapshot;
use reqwest::Client;
use tracing::{info, warn};

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
    #[error("server returned {status}: {body}")]
    Status { status: u16, body: String },
}

impl PushClient {
    pub fn new(base: impl Into<String>, secret: impl Into<String>) -> anyhow::Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

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

        let body = resp.text().await.unwrap_or_default();
        Err(PushError::Status {
            status: status.as_u16(),
            body,
        })
    }

    pub async fn push_with_retry(
        &self,
        snap: &Snapshot,
        attempts: u32,
    ) -> Result<(), PushError> {
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
                    last_err = Some(error);

                    if attempt < attempts {
                        tokio::time::sleep(delay).await;
                        delay = (delay * 4).min(Duration::from_secs(60));
                    }
                }
            }
        }

        Err(last_err.expect("at least one push attempt"))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use docktornet_core::{ContainerEntry, MachineId};
    use mockito::Server;

    use super::*;

    fn snap() -> Snapshot {
        Snapshot {
            machine_id: MachineId::new("m1"),
            taken_at: 1,
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
}
