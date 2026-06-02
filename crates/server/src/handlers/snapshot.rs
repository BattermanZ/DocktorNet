use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::extract::{ConnectInfo, State};
use axum::http::StatusCode;
use docktornet_core::Snapshot;
use tracing::warn;

use crate::flapping::FlappingTracker;
use crate::reconcile::Reconciler;

#[derive(Clone)]
pub struct AppState {
    pub reconciler: Reconciler,
    pub secret: Arc<String>,
    pub flapping: Arc<FlappingTracker>,
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
    tracing::debug!(machine = %snap.machine_id.as_str(), containers = snap.containers.len(), "snapshot received");

    // Persist the snapshot synchronously, then ack.  The external reconcile
    // (NPM / Cloudflare / Gatus) runs in the background so a bulk update of
    // many services can't outlast the agent's push timeout.
    let removed_last_seen = match state.reconciler.ingest_snapshot(&snap).await {
        Ok(removed) => removed,
        Err(e) => {
            warn!(error = %e, "failed to persist snapshot");
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    let reconciler = state.reconciler.clone();
    tokio::spawn(async move {
        if let Err(e) = reconciler.apply_reconcile(&snap, removed_last_seen).await {
            warn!(error = %e, "background reconcile reported failures");
        }
    });

    StatusCode::NO_CONTENT
}
