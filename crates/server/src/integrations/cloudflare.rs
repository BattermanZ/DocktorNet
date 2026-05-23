use docktornet_core::labels::CloudflareDns;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::config::CloudflareConfig;

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
}

#[derive(Debug, thiserror::Error)]
pub enum CloudflareError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("cloudflare api error")]
    Api,
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
        Self::with_base_url(config, "https://api.cloudflare.com/client/v4")
    }

    pub fn with_base(config: CloudflareConfig, base_url: impl Into<String>) -> Self {
        Self::with_base_url(config, base_url)
    }

    fn with_base_url(config: CloudflareConfig, base_url: impl Into<String>) -> Self {
        let base_url = base_url.into().trim_end_matches('/').to_string();

        Self {
            api_token: config.api_token,
            zone_id: config.zone_id,
            base_url,
            http: Client::new(),
        }
    }

    pub async fn zone(&self) -> Result<Zone, CloudflareError> {
        let resp = self
            .http
            .get(self.endpoint(&format!("zones/{}", self.zone_id)))
            .bearer_auth(&self.api_token)
            .send()
            .await?
            .error_for_status()?;

        let response = resp.json::<ZoneResponse>().await?;
        response.result_if_success()
    }

    pub async fn find_record_by_name(
        &self,
        name: &str,
    ) -> Result<Option<DnsRecord>, CloudflareError> {
        let resp = self
            .http
            .get(self.endpoint(&format!("zones/{}/dns_records", self.zone_id)))
            .query(&[("type", "CNAME"), ("name", name)])
            .bearer_auth(&self.api_token)
            .send()
            .await?
            .error_for_status()?;

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
        let resp = self
            .http
            .post(self.endpoint(&format!("zones/{}/dns_records", self.zone_id)))
            .bearer_auth(&self.api_token)
            .json(payload)
            .send()
            .await?
            .error_for_status()?;

        let response = resp.json::<DnsRecordResponse>().await?;
        response.result_if_success()
    }

    pub async fn update_record(
        &self,
        id: &str,
        payload: &DnsRecordPayload,
    ) -> Result<DnsRecord, CloudflareError> {
        let resp = self
            .http
            .put(self.endpoint(&format!("zones/{}/dns_records/{id}", self.zone_id)))
            .bearer_auth(&self.api_token)
            .json(payload)
            .send()
            .await?
            .error_for_status()?;

        let response = resp.json::<DnsRecordResponse>().await?;
        response.result_if_success()
    }

    pub async fn get_record(&self, id: &str) -> Result<Option<DnsRecord>, CloudflareError> {
        let resp = self
            .http
            .get(self.endpoint(&format!("zones/{}/dns_records/{id}", self.zone_id)))
            .bearer_auth(&self.api_token)
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp = resp.error_for_status()?;
        let response = resp.json::<DnsRecordResponse>().await?;
        Ok(Some(response.result_if_success()?))
    }

    pub async fn delete_record(&self, id: &str) -> Result<(), CloudflareError> {
        let resp = self
            .http
            .delete(self.endpoint(&format!("zones/{}/dns_records/{id}", self.zone_id)))
            .bearer_auth(&self.api_token)
            .send()
            .await?
            .error_for_status()?;

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
    async fn get_record_returns_none_on_404() {
        let mut server = Server::new_async().await;
        let m = server
            .mock("GET", "/client/v4/zones/zone-id/dns_records/recX")
            .with_status(404)
            .create_async()
            .await;

        let client = CloudflareClient::with_base(
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
