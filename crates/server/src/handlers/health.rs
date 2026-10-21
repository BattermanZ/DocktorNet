use axum::Json;
use axum::extract::State;
use axum::http::HeaderMap;
use serde::Serialize;

use crate::handlers::snapshot::AppState;

#[derive(Serialize)]
pub struct AgentStatus {
    pub machine_id: String,
    pub forward_host: String,
    pub last_seen: String,
    /// The release the agent last reported; `null` for an agent that
    /// predates version reporting.
    pub agent_version: Option<String>,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub server_time: String,
    /// The server's release.  Like `agents`, only shown to authenticated
    /// callers, so the exact release cannot be fingerprinted from outside.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Per-agent inventory (machine id, internal forward address, last-seen).
    /// Only populated for callers presenting a valid bearer token; omitted
    /// entirely for unauthenticated liveness probes so those details are not
    /// leaked to anyone who can reach the listen socket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agents: Option<Vec<AgentStatus>>,
}

/// Returns true when the request carries the expected bearer token.
fn is_authenticated(headers: &HeaderMap, secret: &str) -> bool {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        == Some(secret)
}

pub async fn healthz(State(state): State<AppState>, headers: HeaderMap) -> Json<HealthResponse> {
    let server_time = humantime::format_rfc3339_seconds(std::time::SystemTime::now()).to_string();

    // Unauthenticated callers get a bare liveness probe only. The per-agent
    // inventory (which discloses internal LAN forward addresses) is gated
    // behind the same bearer token that guards snapshot ingest.
    if !is_authenticated(&headers, state.secret.as_str()) {
        return Json(HealthResponse {
            status: "ok",
            server_time,
            version: None,
            agents: None,
        });
    }

    let (status, agents) = match crate::storage::list_machines(state.reconciler.pool()).await {
        Ok(rows) => {
            let agents = rows
                .into_iter()
                .map(|machine| AgentStatus {
                    machine_id: machine.machine_id,
                    forward_host: machine.forward_host,
                    last_seen: humantime::format_rfc3339_seconds(
                        std::time::UNIX_EPOCH
                            + std::time::Duration::from_secs(machine.last_seen as u64),
                    )
                    .to_string(),
                    agent_version: machine.agent_version,
                })
                .collect();
            ("ok", Some(agents))
        }
        Err(error) => {
            tracing::warn!(%error, "healthz: failed to list machines");
            ("degraded", None)
        }
    };

    Json(HealthResponse {
        status,
        server_time,
        version: Some(state.agent_versions.server_version().to_string()),
        agents,
    })
}
