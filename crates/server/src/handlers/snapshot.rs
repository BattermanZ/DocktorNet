use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::extract::{ConnectInfo, State};
use axum::http::StatusCode;
use docktornet_core::Snapshot;
use tracing::{info, warn};

use crate::agent_versions::{AgentVersionTracker, VersionNote};
use crate::flapping::FlappingTracker;
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
    Json(snap): Json<Snapshot>,
) -> StatusCode {
    if let Some(prev) = state
        .flapping
        .observe(&snap.machine_id, addr.ip(), Instant::now())
    {
        warn!(
            machine_id = %snap.machine_id.as_str(),
            prev_ip = %prev,
            new_ip = %addr.ip(),
            "machine id seen from multiple source IPs within window"
        );
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
    let ticket = match state.reconciler.ingest_snapshot(&snap).await {
        Ok(ticket) => ticket,
        Err(e) => {
            warn!(error = %e, "failed to persist snapshot");
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    let reconciler = state.reconciler.clone();
    tokio::spawn(async move {
        if let Err(e) = reconciler.apply_reconcile(&snap, ticket).await {
            warn!(error = %e, "background reconcile reported failures");
        }
    });

    StatusCode::NO_CONTENT
}
