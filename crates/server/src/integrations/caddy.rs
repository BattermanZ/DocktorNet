use docktornet_core::labels::{CaddyRoute, CaddyScheme};
use reqwest::{Client, Method, StatusCode};
use serde_json::{Value, json};
use url::Url;

use crate::config::CaddyConfig;

/// A client for Caddy's admin API.  Docktornet reads and writes routes only
/// inside the anchor route's subroute, at `/id/<anchor>/handle/0/routes`, and
/// addresses each route it owns by its ownership marker, at `/id/<marker>`.
/// It sends no credentials: the admin API is protected by network isolation.
#[derive(Clone)]
pub struct CaddyClient {
    base_url: Url,
    anchor_id: String,
    http: Client,
}

#[derive(Debug, thiserror::Error)]
pub enum CaddyError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    /// Caddy refused a route: a 400 for a bad or duplicate route, or a 500
    /// for a config it could not load.  It is that route's failure.
    #[error("caddy rejected the route ({status}): {body}")]
    Rejected { status: StatusCode, body: String },
    #[error("unexpected caddy response ({status}): {body}")]
    Status { status: StatusCode, body: String },
}

impl CaddyError {
    /// Whether the failure says Caddy itself is struggling, so it counts
    /// toward the Caddy breaker.  A rejected route doesn't: one bad route
    /// would otherwise stall every other route.
    pub fn feeds_breaker(&self) -> bool {
        !matches!(self, Self::Rejected { .. })
    }
}

impl CaddyClient {
    pub fn new(config: CaddyConfig) -> Self {
        let http = Client::builder()
            .timeout(crate::integrations::npm::NPM_REQUEST_TIMEOUT)
            .build()
            .expect("failed to build Caddy HTTP client");
        Self {
            base_url: config.admin_url,
            anchor_id: config.anchor_id,
            http,
        }
    }

    pub fn anchor_id(&self) -> &str {
        &self.anchor_id
    }

    /// The routes inside the anchor route, or `None` when Caddy has no anchor.
    pub async fn anchor_routes(&self) -> Result<Option<Vec<Value>>, CaddyError> {
        self.get_optional(&self.anchor_routes_path()).await
    }

    /// The route with `marker` anywhere in Caddy's config, or `None`.
    pub async fn get_route(&self, marker: &str) -> Result<Option<Value>, CaddyError> {
        self.get_optional(&["id", marker]).await
    }

    /// The JSON at `segments`, or `None` when Caddy has no such object.
    async fn get_optional<T: serde::de::DeserializeOwned>(
        &self,
        segments: &[&str],
    ) -> Result<Option<T>, CaddyError> {
        let resp = self.request(Method::GET, segments).send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(read_ok(resp).await?.json().await?))
    }

    /// Append `route` to the anchor's routes.
    pub async fn append_route(&self, route: &Value) -> Result<(), CaddyError> {
        let resp = self
            .request(Method::POST, &self.anchor_routes_path())
            .json(route)
            .send()
            .await?;
        write_ok(resp).await
    }

    /// Replace the route with `marker` in place.
    pub async fn patch_route(&self, marker: &str, route: &Value) -> Result<(), CaddyError> {
        let resp = self
            .request(Method::PATCH, &["id", marker])
            .json(route)
            .send()
            .await?;
        write_ok(resp).await
    }

    /// Delete the route with `marker`.  A route already gone counts as deleted.
    pub async fn delete_route(&self, marker: &str) -> Result<(), CaddyError> {
        let resp = self.request(Method::DELETE, &["id", marker]).send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        write_ok(resp).await
    }

    fn anchor_routes_path(&self) -> [&str; 5] {
        ["id", &self.anchor_id, "handle", "0", "routes"]
    }

    /// A request to the admin API path made of `segments`, each
    /// percent-encoded as one path segment.
    fn request(&self, method: Method, segments: &[&str]) -> reqwest::RequestBuilder {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .expect("an http(s) URL has path segments")
            .pop_if_empty()
            .extend(segments);
        self.http.request(method, url)
    }
}

async fn read_ok(resp: reqwest::Response) -> Result<reqwest::Response, CaddyError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    Err(CaddyError::Status {
        status,
        body: resp.text().await.unwrap_or_default(),
    })
}

/// Check the response to a config change.  A 400 or 500 rejects the route.
async fn write_ok(resp: reqwest::Response) -> Result<(), CaddyError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let body = resp.text().await.unwrap_or_default();
    if status == StatusCode::BAD_REQUEST || status == StatusCode::INTERNAL_SERVER_ERROR {
        Err(CaddyError::Rejected { status, body })
    } else {
        Err(CaddyError::Status { status, body })
    }
}

/// The route docktornet writes for `route`, a service's `caddy.N` group,
/// marked with `marker` and forwarding to the machine's `forward_host`.
pub fn render_route(marker: &str, route: &CaddyRoute, forward_host: &str) -> Value {
    // Only an IPv6 address holds a ':', and Caddy needs it bracketed.
    let dial = if forward_host.contains(':') && !forward_host.starts_with('[') {
        format!("[{forward_host}]:{}", route.port)
    } else {
        format!("{forward_host}:{}", route.port)
    };
    let mut handler = json!({
        "handler": "reverse_proxy",
        "upstreams": [{ "dial": dial }],
    });
    if route.scheme == CaddyScheme::Https {
        // NPM doesn't verify upstream certificates either, and no upstream
        // certificate names the dial address.
        handler["transport"] = json!({
            "protocol": "http",
            "tls": { "insecure_skip_verify": true },
        });
    }
    json!({
        "@id": marker,
        "match": [{ "host": route.hosts }],
        "handle": [handler],
        "terminal": true,
    })
}

/// The position in `routes` of the route whose `@id` is `marker`.
pub fn position_of(routes: &[Value], marker: &str) -> Option<usize> {
    routes
        .iter()
        .position(|route| route.get("@id").and_then(Value::as_str) == Some(marker))
}
