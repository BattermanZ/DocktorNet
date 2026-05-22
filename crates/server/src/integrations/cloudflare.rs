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

impl CloudflareClient {
    pub fn new(config: CloudflareConfig) -> Self {
        Self {
            api_token: config.api_token,
            zone_id: config.zone_id,
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

    pub async fn delete_record(&self, id: &str) -> Result<(), CloudflareError> {
        self.http
            .delete(self.endpoint(&format!("zones/{}/dns_records/{id}", self.zone_id)))
            .bearer_auth(&self.api_token)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    fn endpoint(&self, path: &str) -> String {
        format!("https://api.cloudflare.com/client/v4/{path}")
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
}
