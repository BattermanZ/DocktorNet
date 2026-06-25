use axum::Json;
use axum::extract::State;
use serde::Serialize;

use crate::handlers::snapshot::AppState;

#[derive(Serialize)]
pub struct AgentStatus {
    pub machine_id: String,
    pub forward_host: String,
    pub last_seen: String,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub server_time: String,
    pub agents: Vec<AgentStatus>,
}

pub async fn healthz(State(state): State<AppState>) -> Json<HealthResponse> {
    let server_time = humantime::format_rfc3339_seconds(std::time::SystemTime::now()).to_string();

    let (status, agents) = match crate::storage::list_machines(state.reconciler.pool()).await {
        Ok(rows) => {
            let agents = rows
                .into_iter()
                .map(|(machine_id, forward_host, last_seen)| AgentStatus {
                    machine_id,
                    forward_host,
                    last_seen: humantime::format_rfc3339_seconds(
                        std::time::UNIX_EPOCH + std::time::Duration::from_secs(last_seen as u64),
                    )
                    .to_string(),
                })
                .collect();
            ("ok", agents)
        }
        Err(error) => {
            tracing::warn!(%error, "healthz: failed to list machines");
            ("degraded", vec![])
        }
    };

    Json(HealthResponse {
        status,
        server_time,
        agents,
    })
}
