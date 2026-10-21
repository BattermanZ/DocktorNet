use docktornet_core::labels::CloudflareDns;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::CloudflareConfig;

/// Per-request timeout for Cloudflare API calls.  Without it a hung Cloudflare
/// edge (as opposed to a refused connection) would block a reconcile
/// indefinitely, holding the global reconcile lock and stalling every machine's
/// updates.  Mirrors the protection already applied to the NPM client.
const CF_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Fallback back-off after a 429 that carries no (or an unparseable) Retry-After
/// header, so we still stop hammering the API for a sensible interval.
const DEFAULT_CF_RATE_LIMIT_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DnsRecordPayload {
    #[serde(rename = "type")]
    pub record_type: String,
    pub name: String,
    pub content: String,
    pub proxied: bool,
    pub ttl: u32,
}

#[derive(Clone)]
pub struct CloudflareClient {
    api_token: String,
    zone_id: String,
    base_url: String,
    http: Client,
    /// Instant until which further calls are suppressed after a 429.  Shared
    /// across clones so every reconcile honours the same back-off window.
    rate_limited_until: Arc<Mutex<Option<Instant>>>,
}

#[derive(Debug, thiserror::Error)]
pub enum CloudflareError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("cloudflare api error")]
    Api,
    #[error("cloudflare rate limited")]
    RateLimited { retry_after: Option<Duration> },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ZoneResponse {
    pub success: bool,
    pub result: Zone,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Zone {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DnsRecordResponse {
    pub success: bool,
    pub result: DnsRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DnsRecord {
    pub id: String,
    pub name: String,
    pub content: String,
    pub proxied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DnsRecordListResponse {
    pub success: bool,
    pub result: Vec<DnsRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct DeleteResponse {
    pub success: bool,
}

impl CloudflareClient {
    pub fn new(config: CloudflareConfig) -> Self {
        Self::with_base_url(config, crate::config::CLOUDFLARE_API_URL)
    }

    pub fn with_base_url(config: CloudflareConfig, base_url: impl Into<String>) -> Self {
        Self::with_base_url_and_timeout(config, base_url, CF_REQUEST_TIMEOUT)
    }

    pub fn with_base_url_and_timeout(
        config: CloudflareConfig,
        base_url: impl Into<String>,
        timeout: Duration,
    ) -> Self {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let http = Client::builder()
            .timeout(timeout)
            .build()
            .expect("failed to build Cloudflare HTTP client");

        Self {
            api_token: config.api_token,
            zone_id: config.zone_id,
            base_url,
            http,
            rate_limited_until: Arc::new(Mutex::new(None)),
        }
    }

    /// Reject a call locally if we are still inside a 429 back-off window, so a
    /// rate-limited state does not turn every push and resync into another
    /// request fired straight into the limit.
    fn check_rate_limit(&self) -> Result<(), CloudflareError> {
        let guard = self
            .rate_limited_until
            .lock()
            .expect("cloudflare rate-limit mutex poisoned");
        if let Some(until) = *guard
            && let Some(remaining) = until.checked_duration_since(Instant::now())
        {
            return Err(CloudflareError::RateLimited {
                retry_after: Some(remaining),
            });
        }
        Ok(())
    }

    /// Open the back-off window after a 429, honouring Retry-After when present.
    fn trip_rate_limit(&self, retry_after: Option<Duration>) {
        let backoff = retry_after.unwrap_or(DEFAULT_CF_RATE_LIMIT_BACKOFF);
        let mut guard = self
            .rate_limited_until
            .lock()
            .expect("cloudflare rate-limit mutex poisoned");
        *guard = Some(Instant::now() + backoff);
    }

    /// Convert a 429 response into a typed RateLimited error and trip the
    /// back-off gate; pass every other response through untouched.
    fn guard_rate_limit(
        &self,
        resp: reqwest::Response,
    ) -> Result<reqwest::Response, CloudflareError> {
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = parse_retry_after(resp.headers());
            self.trip_rate_limit(retry_after);
            return Err(CloudflareError::RateLimited { retry_after });
        }
        Ok(resp)
    }

    pub async fn zone(&self) -> Result<Zone, CloudflareError> {
        self.check_rate_limit()?;
        let resp = self
            .http
            .get(self.endpoint(&format!("zones/{}", self.zone_id)))
            .bearer_auth(&self.api_token)
            .send()
            .await?;
        let resp = self.guard_rate_limit(resp)?.error_for_status()?;

        let response = resp.json::<ZoneResponse>().await?;
        response.result_if_success()
    }

    pub async fn find_record_by_name(
        &self,
        name: &str,
    ) -> Result<Option<DnsRecord>, CloudflareError> {
        self.check_rate_limit()?;
        let resp = self
            .http
            .get(self.endpoint(&format!("zones/{}/dns_records", self.zone_id)))
            .query(&[("type", "CNAME"), ("name", name)])
            .bearer_auth(&self.api_token)
            .send()
            .await?;
        let resp = self.guard_rate_limit(resp)?.error_for_status()?;

        let response = resp.json::<DnsRecordListResponse>().await?;
        if !response.success {
            return Err(CloudflareError::Api);
        }

        Ok(response.result.into_iter().next())
    }

    pub async fn create_record(
        &self,
        payload: &DnsRecordPayload,
    ) -> Result<DnsRecord, CloudflareError> {
        self.check_rate_limit()?;
        let resp = self
            .http
            .post(self.endpoint(&format!("zones/{}/dns_records", self.zone_id)))
            .bearer_auth(&self.api_token)
            .json(payload)
            .send()
            .await?;
        let resp = self.guard_rate_limit(resp)?.error_for_status()?;

        let response = resp.json::<DnsRecordResponse>().await?;
        response.result_if_success()
    }

    pub async fn update_record(
        &self,
        id: &str,
        payload: &DnsRecordPayload,
    ) -> Result<DnsRecord, CloudflareError> {
        self.check_rate_limit()?;
        let resp = self
            .http
            .put(self.endpoint(&format!("zones/{}/dns_records/{id}", self.zone_id)))
            .bearer_auth(&self.api_token)
            .json(payload)
            .send()
            .await?;
        let resp = self.guard_rate_limit(resp)?.error_for_status()?;

        let response = resp.json::<DnsRecordResponse>().await?;
        response.result_if_success()
    }

    pub async fn get_record(&self, id: &str) -> Result<Option<DnsRecord>, CloudflareError> {
        self.check_rate_limit()?;
        let resp = self
            .http
            .get(self.endpoint(&format!("zones/{}/dns_records/{id}", self.zone_id)))
            .bearer_auth(&self.api_token)
            .send()
            .await?;
        let resp = self.guard_rate_limit(resp)?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp = resp.error_for_status()?;
        let response = resp.json::<DnsRecordResponse>().await?;
        Ok(Some(response.result_if_success()?))
    }

    pub async fn delete_record(&self, id: &str) -> Result<(), CloudflareError> {
        self.check_rate_limit()?;
        let resp = self
            .http
            .delete(self.endpoint(&format!("zones/{}/dns_records/{id}", self.zone_id)))
            .bearer_auth(&self.api_token)
            .send()
            .await?;
        let resp = self.guard_rate_limit(resp)?;
        // An already-absent record is the desired end state: treat 404 as a
        // successful delete so the caller can clear its DB pointer and cache the
        // intent hash instead of retrying forever.
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        let resp = resp.error_for_status()?;

        let response = resp.json::<DeleteResponse>().await?;
        if response.success {
            Ok(())
        } else {
            Err(CloudflareError::Api)
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}/{}", self.base_url, path)
    }
}

impl ZoneResponse {
    fn result_if_success(self) -> Result<Zone, CloudflareError> {
        if self.success {
            Ok(self.result)
        } else {
            Err(CloudflareError::Api)
        }
    }
}

impl DnsRecordResponse {
    fn result_if_success(self) -> Result<DnsRecord, CloudflareError> {
        if self.success {
            Ok(self.result)
        } else {
            Err(CloudflareError::Api)
        }
    }
}

/// Parse a Retry-After header expressed as delta-seconds (the form Cloudflare
/// uses).  The HTTP-date form is not honoured and falls back to the default
/// back-off.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let secs: u64 = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(secs))
}

pub fn dns_payload(dns: &CloudflareDns, zone_name: &str) -> DnsRecordPayload {
    let name = format!(
        "{}.{}",
        dns.name.trim_end_matches('.'),
        zone_name.trim_end_matches('.')
    );

    DnsRecordPayload {
        record_type: "CNAME".to_string(),
        name,
        content: dns.target.clone(),
        proxied: dns.proxied,
        ttl: 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::{Matcher, Server};

    #[test]
    fn builds_cname_payload() {
        let dns = CloudflareDns {
            name: "app".to_string(),
            target: "home.example.com".to_string(),
            proxied: true,
        };

        let payload = dns_payload(&dns, "example.com");

        assert_eq!(
            payload,
            DnsRecordPayload {
                record_type: "CNAME".to_string(),
                name: "app.example.com".to_string(),
                content: "home.example.com".to_string(),
                proxied: true,
                ttl: 1,
            }
        );
    }

    #[tokio::test]
    async fn find_record_by_name_uses_injected_base_url_and_auth_headers() {
        let mut server = Server::new_async().await;
        let _mock = server
            .mock("GET", "/client/v4/zones/zone-123/dns_records")
            .match_header("authorization", "Bearer token-abc")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("type".into(), "CNAME".into()),
                Matcher::UrlEncoded("name".into(), "app.example.com".into()),
            ]))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "success": true,
                    "result": [{
                        "id": "rec-1",
                        "name": "app.example.com",
                        "content": "target.example.com",
                        "proxied": true
                    }]
                })
                .to_string(),
            )
            .create_async()
            .await;

        let client = CloudflareClient::with_base_url(
            CloudflareConfig {
                api_token: "token-abc".to_string(),
                zone_id: "zone-123".to_string(),
            },
            format!("{}/client/v4", server.url()),
        );

        let record = client.find_record_by_name("app.example.com").await.unwrap();

        assert_eq!(
            record,
            Some(DnsRecord {
                id: "rec-1".to_string(),
                name: "app.example.com".to_string(),
                content: "target.example.com".to_string(),
                proxied: true,
            })
        );
    }

    #[tokio::test]
    async fn delete_record_returns_api_error_when_success_is_false() {
        let mut server = Server::new_async().await;
        let _mock = server
            .mock("DELETE", "/client/v4/zones/zone-123/dns_records/rec-42")
            .match_header("authorization", "Bearer token-abc")
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "success": false,
                    "errors": [],
                    "messages": [],
                    "result": null
                })
                .to_string(),
            )
            .create_async()
            .await;

        let client = CloudflareClient::with_base_url(
            CloudflareConfig {
                api_token: "token-abc".to_string(),
                zone_id: "zone-123".to_string(),
            },
            format!("{}/client/v4", server.url()),
        );

        let err = client.delete_record("rec-42").await.unwrap_err();

        assert!(matches!(err, CloudflareError::Api));
    }

    #[tokio::test]
    async fn requests_time_out_against_an_unresponsive_server() {
        use std::time::{Duration, Instant};

        // A listener that accepts connections but never sends a response, so
        // the request hangs until the client's timeout fires (as opposed to a
        // fast connection-refused when nothing is listening). Without a request
        // timeout this would hang forever while the global reconcile lock is
        // held, stalling ingestion for every machine.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });

        let client = CloudflareClient::with_base_url_and_timeout(
            CloudflareConfig {
                api_token: "t".to_string(),
                zone_id: "zone-id".to_string(),
            },
            format!("http://{addr}/client/v4"),
            Duration::from_millis(150),
        );

        let start = Instant::now();
        let err = client.zone().await.unwrap_err();
        let elapsed = start.elapsed();

        match err {
            CloudflareError::Http(e) => {
                assert!(e.is_timeout(), "expected a timeout error, got: {e}")
            }
            other => panic!("expected http timeout error, got: {other:?}"),
        }
        assert!(
            elapsed < Duration::from_secs(5),
            "should time out promptly, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn delete_record_treats_404_as_success() {
        // Cloudflare returns 404 when the record is already gone.  Deleting an
        // already-absent record must be a no-op success, otherwise the caller
        // leaves a stale DB pointer and blocks intent-hash caching, retrying
        // forever.
        let mut server = Server::new_async().await;
        let m = server
            .mock("DELETE", "/client/v4/zones/zone-id/dns_records/gone")
            .with_status(404)
            .with_body(
                serde_json::json!({
                    "success": false,
                    "errors": [{ "code": 81044, "message": "Record does not exist." }],
                    "messages": [],
                    "result": null
                })
                .to_string(),
            )
            .create_async()
            .await;

        let client = CloudflareClient::with_base_url(
            CloudflareConfig {
                api_token: "t".to_string(),
                zone_id: "zone-id".to_string(),
            },
            format!("{}/client/v4", server.url()),
        );

        client.delete_record("gone").await.unwrap();
        m.assert_async().await;
    }

    #[tokio::test]
    async fn rate_limited_call_backs_off_without_hammering() {
        // Cloudflare returns 429 with a Retry-After when rate limited.  Without a
        // gate every subsequent snapshot push and resync would immediately fire
        // another request straight into the limit, never healing.  After a 429
        // the client must short-circuit further calls locally until the
        // Retry-After window elapses, and surface a typed RateLimited error.
        let mut server = Server::new_async().await;
        // Exactly one request may reach Cloudflare: the gate must block the
        // second call locally while the (300s) window is open.
        let m = server
            .mock("GET", "/client/v4/zones/zone-id")
            .with_status(429)
            .with_header("retry-after", "300")
            .with_body(r#"{"success":false,"errors":[{"code":10000,"message":"rate limited"}]}"#)
            .expect(1)
            .create_async()
            .await;

        let client = CloudflareClient::with_base_url(
            CloudflareConfig {
                api_token: "t".to_string(),
                zone_id: "zone-id".to_string(),
            },
            format!("{}/client/v4", server.url()),
        );

        let first = client.zone().await.unwrap_err();
        assert!(
            matches!(first, CloudflareError::RateLimited { .. }),
            "a 429 must surface as RateLimited, got: {first:?}"
        );

        // Within the Retry-After window the next call must NOT hit the network.
        let second = client.zone().await.unwrap_err();
        assert!(
            matches!(second, CloudflareError::RateLimited { .. }),
            "a call inside the backoff window must short-circuit as RateLimited, got: {second:?}"
        );

        m.assert_async().await;
    }

    #[tokio::test]
    async fn get_record_returns_none_on_404() {
        let mut server = Server::new_async().await;
        let m = server
            .mock("GET", "/client/v4/zones/zone-id/dns_records/recX")
            .with_status(404)
            .create_async()
            .await;

        let client = CloudflareClient::with_base_url(
            CloudflareConfig {
                api_token: "t".to_string(),
                zone_id: "zone-id".to_string(),
            },
            format!("{}/client/v4", server.url()),
        );
        let got = client.get_record("recX").await.unwrap();
        m.assert_async().await;
        assert!(got.is_none());
    }
}
