use docktornet_core::labels::{NpmHost, NpmScheme};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use crate::config::NpmConfig;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProxyHostPayload {
    pub domain_names: Vec<String>,
    pub forward_scheme: String,
    pub forward_host: String,
    pub forward_port: u16,
    pub access_list_id: i64,
    pub certificate_id: i64,
    pub ssl_forced: bool,
    pub caching_enabled: bool,
    pub block_exploits: bool,
    pub allow_websocket_upgrade: bool,
    pub http2_support: bool,
    pub hsts_enabled: bool,
    pub hsts_subdomains: bool,
    pub advanced_config: String,
    pub locations: Vec<Value>,
    pub meta: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Certificate {
    pub id: i64,
    pub domain_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ProxyHostResponse {
    pub id: i64,
    pub domain_names: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenResponse {
    pub token: Option<String>,
}

#[derive(Clone)]
pub struct NpmClient {
    base_url: Url,
    user: String,
    password: String,
    http: Client,
}

#[derive(Debug, thiserror::Error)]
pub enum NpmError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("NPM response missing token")]
    MissingToken,
}

impl NpmClient {
    pub fn new(config: NpmConfig) -> Self {
        Self {
            base_url: config.url,
            user: config.user,
            password: config.password,
            http: Client::new(),
        }
    }

    pub async fn token(&self) -> Result<String, NpmError> {
        let resp = self
            .http
            .post(self.endpoint("api/tokens"))
            .json(&serde_json::json!({
                "identity": self.user,
                "secret": self.password,
            }))
            .send()
            .await?
            .error_for_status()?;

        let token = resp.json::<TokenResponse>().await?;
        token.token.ok_or(NpmError::MissingToken)
    }

    pub async fn certificates(&self, token: &str) -> Result<Vec<Certificate>, NpmError> {
        let resp = self
            .http
            .get(self.endpoint("api/nginx/certificates"))
            .bearer_auth(token)
            .send()
            .await?
            .error_for_status()?;

        Ok(resp.json::<Vec<Certificate>>().await?)
    }

    pub async fn create_proxy_host(
        &self,
        token: &str,
        payload: &ProxyHostPayload,
    ) -> Result<ProxyHostResponse, NpmError> {
        let resp = self
            .http
            .post(self.endpoint("api/nginx/proxy-hosts"))
            .bearer_auth(token)
            .json(payload)
            .send()
            .await?
            .error_for_status()?;

        Ok(resp.json::<ProxyHostResponse>().await?)
    }

    pub async fn update_proxy_host(
        &self,
        token: &str,
        id: i64,
        payload: &ProxyHostPayload,
    ) -> Result<ProxyHostResponse, NpmError> {
        let resp = self
            .http
            .put(self.endpoint(&format!("api/nginx/proxy-hosts/{id}")))
            .bearer_auth(token)
            .json(payload)
            .send()
            .await?
            .error_for_status()?;

        Ok(resp.json::<ProxyHostResponse>().await?)
    }

    pub async fn delete_proxy_host(&self, token: &str, id: i64) -> Result<(), NpmError> {
        self.http
            .delete(self.endpoint(&format!("api/nginx/proxy-hosts/{id}")))
            .bearer_auth(token)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    fn endpoint(&self, path: &str) -> String {
        self.base_url
            .join(path)
            .expect("valid NPM API path")
            .to_string()
    }
}

pub fn proxy_payload(
    host: &NpmHost,
    forward_host: impl Into<String>,
    cert_id: Option<i64>,
) -> ProxyHostPayload {
    let cert_id = cert_id.unwrap_or_default();
    let ssl_enabled = host.ssl && cert_id > 0;
    let hsts_enabled = ssl_enabled && host.hsts.unwrap_or(true);
    let http2_support = ssl_enabled && host.http2.unwrap_or(true);

    ProxyHostPayload {
        domain_names: vec![host.host.clone()],
        forward_scheme: match host.scheme {
            NpmScheme::Http => "http".to_string(),
            NpmScheme::Https => "https".to_string(),
        },
        forward_host: forward_host.into(),
        forward_port: host.port,
        access_list_id: 0,
        certificate_id: cert_id,
        ssl_forced: ssl_enabled,
        caching_enabled: false,
        block_exploits: false,
        allow_websocket_upgrade: false,
        http2_support,
        hsts_enabled,
        hsts_subdomains: false,
        advanced_config: String::new(),
        locations: vec![],
        meta: serde_json::json!({
            "letsencrypt_agree": false,
            "dns_challenge": false,
        }),
    }
}

pub fn matching_cert_id(host: &str, certs: &[Certificate]) -> Option<i64> {
    certs
        .iter()
        .find(|cert| cert.domain_names.iter().any(|domain| domain == host))
        .map(|cert| cert.id)
        .or_else(|| {
            certs
                .iter()
                .find(|cert| {
                    cert.domain_names
                        .iter()
                        .any(|domain| wildcard_matches(domain, host))
                })
                .map(|cert| cert.id)
        })
}

fn wildcard_matches(pattern: &str, host: &str) -> bool {
    let Some(suffix) = pattern.strip_prefix("*.") else {
        return false;
    };

    let Some(prefix) = host.strip_suffix(suffix) else {
        return false;
    };

    let Some(label) = prefix.strip_suffix('.') else {
        return false;
    };

    !label.is_empty()
        && !label.contains('.')
        && host.split('.').count() == suffix.split('.').count() + 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use docktornet_core::labels::{NpmHost, NpmScheme};
    use mockito::Server;
    use url::Url;

    fn host() -> NpmHost {
        NpmHost {
            index: 0,
            host: "app.example.com".to_string(),
            port: 3000,
            scheme: NpmScheme::Http,
            ssl: true,
            hsts: Some(true),
            http2: Some(true),
            cert_id: Some(42),
        }
    }

    #[test]
    fn ssl_payload_enables_expected_flags() {
        let payload = proxy_payload(&host(), "web.internal", Some(42));

        assert_eq!(
            payload,
            ProxyHostPayload {
                domain_names: vec!["app.example.com".to_string()],
                forward_scheme: "http".to_string(),
                forward_host: "web.internal".to_string(),
                forward_port: 3000,
                access_list_id: 0,
                certificate_id: 42,
                ssl_forced: true,
                caching_enabled: false,
                block_exploits: false,
                allow_websocket_upgrade: false,
                http2_support: true,
                hsts_enabled: true,
                hsts_subdomains: false,
                advanced_config: String::new(),
                locations: vec![],
                meta: serde_json::json!({
                    "letsencrypt_agree": false,
                    "dns_challenge": false
                }),
            }
        );
    }

    #[test]
    fn wildcard_certificate_matches_subdomain() {
        let certs = vec![Certificate {
            id: 1,
            domain_names: vec!["*.example.com".to_string()],
        }];

        assert_eq!(matching_cert_id("app.example.com", &certs), Some(1));
    }

    #[test]
    fn wildcard_certificate_does_not_match_different_suffix() {
        let certs = vec![Certificate {
            id: 1,
            domain_names: vec!["*.example.com".to_string()],
        }];

        assert_eq!(matching_cert_id("foo.badexample.com", &certs), None);
    }

    #[test]
    fn exact_certificate_beats_wildcard() {
        let certs = vec![
            Certificate {
                id: 1,
                domain_names: vec!["*.example.com".to_string()],
            },
            Certificate {
                id: 2,
                domain_names: vec!["app.example.com".to_string()],
            },
        ];

        assert_eq!(matching_cert_id("app.example.com", &certs), Some(2));
    }

    #[tokio::test]
    async fn token_posts_identity_and_secret_and_parses_token() {
        let mut server = Server::new_async().await;
        let _mock = server
            .mock("POST", "/api/tokens")
            .match_header("content-type", "application/json")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "identity": "admin@example.com",
                "secret": "pw",
            })))
            .with_status(200)
            .with_body(r#"{"token":"abc123"}"#)
            .create_async()
            .await;

        let client = NpmClient::new(crate::config::NpmConfig {
            url: Url::parse(&server.url()).unwrap(),
            user: "admin@example.com".to_string(),
            password: "pw".to_string(),
        });

        assert_eq!(client.token().await.unwrap(), "abc123");
    }

    #[tokio::test]
    async fn create_proxy_host_posts_payload_and_returns_response() {
        let mut server = Server::new_async().await;
        let _mock = server
            .mock("POST", "/api/nginx/proxy-hosts")
            .match_header("authorization", "Bearer abc123")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "domain_names": ["app.example.com"],
                "forward_scheme": "http",
                "forward_host": "web.internal",
                "forward_port": 3000,
                "certificate_id": 42,
                "ssl_forced": true,
                "http2_support": true,
                "hsts_enabled": true
            })))
            .with_status(200)
            .with_body(r#"{"id":7,"domain_names":["app.example.com"]}"#)
            .create_async()
            .await;

        let client = NpmClient::new(crate::config::NpmConfig {
            url: Url::parse(&server.url()).unwrap(),
            user: "admin@example.com".to_string(),
            password: "pw".to_string(),
        });
        let payload = proxy_payload(&host(), "web.internal", Some(42));

        let response = client.create_proxy_host("abc123", &payload).await.unwrap();

        assert_eq!(response.id, 7);
        assert_eq!(response.domain_names, vec!["app.example.com".to_string()]);
    }
}
