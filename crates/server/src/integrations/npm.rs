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
pub struct ProxyHost {
    pub id: i64,
    #[serde(default)]
    pub certificate_id: Option<i64>,
    #[serde(default)]
    pub domain_names: Vec<String>,
    #[serde(default)]
    pub forward_scheme: String,
    #[serde(default)]
    pub forward_host: String,
    #[serde(default)]
    pub forward_port: u16,
    #[serde(default, deserialize_with = "de_bool")]
    pub ssl_forced: bool,
    #[serde(default, deserialize_with = "de_bool")]
    pub http2_support: bool,
    #[serde(default, deserialize_with = "de_bool")]
    pub hsts_enabled: bool,
    #[serde(default, deserialize_with = "de_bool")]
    pub hsts_subdomains: bool,
    #[serde(default, deserialize_with = "de_bool")]
    pub block_exploits: bool,
    #[serde(default, deserialize_with = "de_bool")]
    pub allow_websocket_upgrade: bool,
}

impl ProxyHost {
    /// True when NPM's stored state already matches the payload we would push.
    /// Only the fields we derive from labels are compared; flags we always set
    /// to a constant (caching) can't drift, so skipping them avoids false
    /// mismatches from NPM normalizing its response.
    pub fn matches_payload(&self, payload: &ProxyHostPayload) -> bool {
        self.certificate_id.unwrap_or_default() == payload.certificate_id
            && self.domain_names == payload.domain_names
            && self.forward_scheme == payload.forward_scheme
            && self.forward_host == payload.forward_host
            && self.forward_port == payload.forward_port
            && self.ssl_forced == payload.ssl_forced
            && self.http2_support == payload.http2_support
            && self.hsts_enabled == payload.hsts_enabled
            && self.hsts_subdomains == payload.hsts_subdomains
            && self.block_exploits == payload.block_exploits
            && self.allow_websocket_upgrade == payload.allow_websocket_upgrade
    }
}

/// NPM serializes boolean columns as integers (0/1) in some versions and as
/// JSON booleans in others; accept either.
fn de_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match Value::deserialize(deserializer)? {
        Value::Bool(b) => Ok(b),
        Value::Number(n) => Ok(n.as_i64().unwrap_or(0) != 0),
        _ => Ok(false),
    }
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

/// Per-request timeout for NPM API calls.  Without it a hung NPM (as opposed to
/// a refused connection) would block a reconcile indefinitely, holding the
/// reconcile lock and stalling every machine's updates.
const NPM_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

impl NpmClient {
    pub fn new(config: NpmConfig) -> Self {
        Self::with_timeout(config, NPM_REQUEST_TIMEOUT)
    }

    pub fn with_timeout(config: NpmConfig, timeout: std::time::Duration) -> Self {
        let http = Client::builder()
            .timeout(timeout)
            .build()
            .expect("failed to build NPM HTTP client");
        Self {
            base_url: normalize_base_url(config.url),
            user: config.user,
            password: config.password,
            http,
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

    /// Find an existing proxy host whose `domain_names` include `domain`
    /// (case-insensitive). NPM has no server-side filter, so list and match
    /// locally. Used to adopt a host that already exists when our DB has no
    /// mapping for it.
    pub async fn find_proxy_host_by_domain(
        &self,
        token: &str,
        domain: &str,
    ) -> Result<Option<ProxyHostResponse>, NpmError> {
        self.find_proxy_host_by_any_domain(token, std::slice::from_ref(&domain.to_string()))
            .await
    }

    /// Like `find_proxy_host_by_domain` but matches if *any* of `domains` is
    /// present on an existing host. A proxy host with several domain names is
    /// one NPM entry, so finding it by any of its domains lets us adopt it.
    pub async fn find_proxy_host_by_any_domain(
        &self,
        token: &str,
        domains: &[String],
    ) -> Result<Option<ProxyHostResponse>, NpmError> {
        let resp = self
            .http
            .get(self.endpoint("api/nginx/proxy-hosts"))
            .bearer_auth(token)
            .send()
            .await?
            .error_for_status()?;

        let hosts = resp.json::<Vec<ProxyHostResponse>>().await?;
        Ok(hosts.into_iter().find(|host| {
            host.domain_names.iter().any(|name| {
                domains
                    .iter()
                    .any(|domain| name.eq_ignore_ascii_case(domain))
            })
        }))
    }

    pub async fn get_proxy_host(
        &self,
        token: &str,
        id: i64,
    ) -> Result<Option<ProxyHost>, NpmError> {
        let resp = self
            .http
            .get(self.endpoint(&format!("api/nginx/proxy-hosts/{id}")))
            .bearer_auth(token)
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp = resp.error_for_status()?;
        Ok(Some(resp.json::<ProxyHost>().await?))
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
    let hsts_subdomains = hsts_enabled && host.hsts_subdomains.unwrap_or(true);
    let http2_support = ssl_enabled && host.http2.unwrap_or(true);

    ProxyHostPayload {
        domain_names: host.hosts.clone(),
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
        block_exploits: host.block_exploits.unwrap_or(true),
        allow_websocket_upgrade: host.websockets.unwrap_or(true),
        http2_support,
        hsts_enabled,
        hsts_subdomains,
        advanced_config: String::new(),
        locations: vec![],
        meta: serde_json::json!({
            "letsencrypt_agree": false,
            "dns_challenge": false,
        }),
    }
}

/// Resolve a certificate for a proxy host that may carry several domains. NPM
/// stores one certificate per proxy host, so we only pick a cert that covers
/// *every* domain (exact or wildcard). Selecting one that matches a subset
/// would serve the remaining domains a mismatched cert while `ssl_forced`
/// redirects them to HTTPS, breaking them.
pub fn matching_cert_id_for_hosts(hosts: &[String], certs: &[Certificate]) -> Option<i64> {
    if hosts.is_empty() {
        return None;
    }
    certs
        .iter()
        .find(|cert| hosts.iter().all(|host| cert_covers(cert, host)))
        .map(|cert| cert.id)
}

/// Whether `cert` covers `host` by an exact (case-insensitive) or wildcard
/// domain entry.
fn cert_covers(cert: &Certificate, host: &str) -> bool {
    cert.domain_names
        .iter()
        .any(|domain| domain.eq_ignore_ascii_case(host) || wildcard_matches(domain, host))
}

pub fn matching_cert_id(host: &str, certs: &[Certificate]) -> Option<i64> {
    certs
        .iter()
        .find(|cert| {
            cert.domain_names
                .iter()
                .any(|domain| domain.eq_ignore_ascii_case(host))
        })
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
    let pattern = pattern.to_ascii_lowercase();
    let host = host.to_ascii_lowercase();
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

fn normalize_base_url(mut url: Url) -> Url {
    if !url.path().ends_with('/') {
        let normalized_path = format!("{}/", url.path());
        url.set_path(&normalized_path);
    }

    url
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
            hosts: vec!["app.example.com".to_string()],
            port: 3000,
            scheme: NpmScheme::Http,
            ssl: true,
            hsts: Some(true),
            hsts_subdomains: Some(true),
            http2: Some(true),
            block_exploits: Some(true),
            websockets: Some(true),
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
                block_exploits: true,
                allow_websocket_upgrade: true,
                http2_support: true,
                hsts_enabled: true,
                hsts_subdomains: true,
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
    fn payload_carries_every_domain_on_one_entry() {
        let mut multi = host();
        multi.hosts = vec![
            "app.example.com".to_string(),
            "app.mydomain.org".to_string(),
        ];
        let payload = proxy_payload(&multi, "web.internal", Some(42));

        assert_eq!(
            payload.domain_names,
            vec![
                "app.example.com".to_string(),
                "app.mydomain.org".to_string()
            ]
        );
    }

    #[test]
    fn cert_for_hosts_requires_coverage_of_every_domain() {
        // A single cert whose SANs cover both domains is selected even when the
        // matching entry for the second domain comes later in its list.
        let certs = vec![Certificate {
            id: 9,
            domain_names: vec![
                "app.example.com".to_string(),
                "app.mydomain.org".to_string(),
            ],
        }];
        let hosts = vec![
            "app.example.com".to_string(),
            "app.mydomain.org".to_string(),
        ];

        assert_eq!(matching_cert_id_for_hosts(&hosts, &certs), Some(9));
    }

    #[test]
    fn cert_covering_only_one_domain_is_not_selected() {
        // A cert that matches only one of the host's domains must NOT be
        // selected: NPM stores one cert per proxy host, so the other domain
        // would be served a mismatched cert while ssl_forced redirects it to
        // HTTPS, breaking it.
        let certs = vec![Certificate {
            id: 9,
            domain_names: vec!["app.mydomain.org".to_string()],
        }];
        let hosts = vec![
            "app.example.com".to_string(),
            "app.mydomain.org".to_string(),
        ];

        assert_eq!(matching_cert_id_for_hosts(&hosts, &certs), None);
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
    fn certificate_exact_match_is_case_insensitive() {
        let certs = vec![Certificate {
            id: 1,
            domain_names: vec!["APP.EXAMPLE.COM".to_string()],
        }];

        assert_eq!(matching_cert_id("app.example.com", &certs), Some(1));
    }

    #[tokio::test]
    async fn token_preserves_configured_base_path() {
        let mut server = Server::new_async().await;
        let _mock = server
            .mock("POST", "/npm/api/tokens")
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
            url: Url::parse(&format!("{}/npm", server.url())).unwrap(),
            user: "admin@example.com".to_string(),
            password: "pw".to_string(),
        });

        assert_eq!(client.token().await.unwrap(), "abc123");
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
    async fn requests_time_out_against_an_unresponsive_server() {
        use std::time::{Duration, Instant};

        // A listener that accepts connections but never sends a response, so
        // the request hangs until the client's timeout fires (as opposed to a
        // fast connection-refused when nothing is listening).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });

        let client = NpmClient::with_timeout(
            crate::config::NpmConfig {
                url: Url::parse(&format!("http://{addr}")).unwrap(),
                user: "u".into(),
                password: "p".into(),
            },
            Duration::from_millis(150),
        );

        let start = Instant::now();
        let err = client.token().await.unwrap_err();
        let elapsed = start.elapsed();

        match err {
            NpmError::Http(e) => assert!(e.is_timeout(), "expected a timeout error, got: {e}"),
            other => panic!("expected http timeout error, got: {other:?}"),
        }
        assert!(
            elapsed < Duration::from_secs(5),
            "should time out promptly, took {elapsed:?}"
        );
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
                "hsts_enabled": true,
                "hsts_subdomains": true,
                "block_exploits": true,
                "allow_websocket_upgrade": true
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

    #[tokio::test]
    async fn find_proxy_host_by_domain_matches_case_insensitively() {
        let mut server = Server::new_async().await;
        let m = server
            .mock("GET", "/api/nginx/proxy-hosts")
            .match_header("authorization", "Bearer tok")
            .with_status(200)
            .with_body(
                r#"[{"id":3,"domain_names":["other.example.com"]},
                    {"id":61,"domain_names":["APP.EXAMPLE.COM"]}]"#,
            )
            .create_async()
            .await;

        let client = NpmClient::new(crate::config::NpmConfig {
            url: Url::parse(&server.url()).unwrap(),
            user: "u".into(),
            password: "p".into(),
        });
        let got = client
            .find_proxy_host_by_domain("tok", "app.example.com")
            .await
            .unwrap()
            .unwrap();
        m.assert_async().await;
        assert_eq!(got.id, 61);
    }

    #[tokio::test]
    async fn find_proxy_host_by_domain_returns_none_when_absent() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(r#"[{"id":3,"domain_names":["other.example.com"]}]"#)
            .create_async()
            .await;

        let client = NpmClient::new(crate::config::NpmConfig {
            url: Url::parse(&server.url()).unwrap(),
            user: "u".into(),
            password: "p".into(),
        });
        let got = client
            .find_proxy_host_by_domain("tok", "missing.example.com")
            .await
            .unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn get_proxy_host_returns_none_on_404() {
        let mut server = Server::new_async().await;
        let m = server
            .mock("GET", "/api/nginx/proxy-hosts/42")
            .with_status(404)
            .create_async()
            .await;

        let client = NpmClient::new(crate::config::NpmConfig {
            url: Url::parse(&server.url()).unwrap(),
            user: "u".into(),
            password: "p".into(),
        });
        let got = client.get_proxy_host("tok", 42).await.unwrap();
        m.assert_async().await;
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn get_proxy_host_returns_some_with_cert_id() {
        let mut server = Server::new_async().await;
        let m = server
            .mock("GET", "/api/nginx/proxy-hosts/7")
            .with_status(200)
            .with_body(r#"{"id":7,"certificate_id":12}"#)
            .create_async()
            .await;

        let client = NpmClient::new(crate::config::NpmConfig {
            url: Url::parse(&server.url()).unwrap(),
            user: "u".into(),
            password: "p".into(),
        });
        let got = client.get_proxy_host("tok", 7).await.unwrap().unwrap();
        m.assert_async().await;
        assert_eq!(got.id, 7);
        assert_eq!(got.certificate_id, Some(12));
    }
}
