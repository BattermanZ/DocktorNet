use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{ConnectInfo, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use docktornet_core::Snapshot;
use tracing::{info, warn};

use crate::agent_versions::{AgentVersionTracker, VersionNote};
use crate::flapping::{FlappingTracker, Verdict};
use crate::reconcile::Reconciler;

#[derive(Clone)]
pub struct AppState {
    pub reconciler: Reconciler,
    pub secret: Arc<String>,
    pub flapping: Arc<FlappingTracker>,
    pub agent_versions: Arc<AgentVersionTracker>,
}

pub async fn ingest(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    payload: Result<Json<Snapshot>, JsonRejection>,
) -> Response {
    match payload {
        Ok(Json(snap)) => ingest_snapshot(state, addr, snap).await.into_response(),
        // Valid JSON that breaks a snapshot rule, such as an invalid machine
        // id, is a bad request.  Axum would answer 422.
        Err(JsonRejection::JsonDataError(err)) => {
            warn!(ip = %addr.ip(), error = %err.body_text(), "rejecting invalid snapshot");
            (StatusCode::BAD_REQUEST, err.body_text()).into_response()
        }
        Err(rejection) => rejection.into_response(),
    }
}

async fn ingest_snapshot(state: AppState, addr: SocketAddr, snap: Snapshot) -> StatusCode {
    match state.flapping.observe(
        &snap.machine_id,
        addr.ip(),
        &snap.forward_host,
        Instant::now(),
    ) {
        Verdict::Reject {
            owner_ip,
            owner_forward_host,
        } => {
            warn!(
                machine_id = %snap.machine_id.as_str(),
                owner_ip = %owner_ip,
                owner_forward_host,
                ip = %addr.ip(),
                forward_host = snap.forward_host,
                "machine id is owned by another agent with a different forward host; rejecting push"
            );
            return StatusCode::CONFLICT;
        }
        Verdict::Accept {
            prev_ip: Some(prev),
        } => warn!(
            machine_id = %snap.machine_id.as_str(),
            prev_ip = %prev,
            new_ip = %addr.ip(),
            "machine id seen from multiple source IPs within window"
        ),
        Verdict::Accept { prev_ip: None } => {}
    }
    match state
        .agent_versions
        .observe(&snap.machine_id, snap.agent_version.as_deref())
    {
        Some(VersionNote::Mismatch { agent_version }) => warn!(
            machine_id = %snap.machine_id.as_str(),
            agent_version,
            server_version = state.agent_versions.server_version(),
            "agent and server versions differ"
        ),
        Some(VersionNote::Predates) => info!(
            machine_id = %snap.machine_id.as_str(),
            "agent predates version reporting"
        ),
        None => {}
    }
    tracing::debug!(machine = %snap.machine_id.as_str(), containers = snap.containers.len(), "snapshot received");

    // Persist the snapshot synchronously, then ack.  The external reconcile
    // (NPM / Cloudflare / Gatus) runs in the background so a bulk update of
    // many services can't outlast the agent's push timeout.
    match state.reconciler.ingest_snapshot(&snap).await {
        Ok(Some(worker)) => {
            tokio::spawn(worker.run());
        }
        // A worker is already applying for this machine; it takes this
        // snapshot next.
        Ok(None) => {}
        Err(e) => {
            warn!(error = %e, "failed to persist snapshot");
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    }

    StatusCode::NO_CONTENT
}
