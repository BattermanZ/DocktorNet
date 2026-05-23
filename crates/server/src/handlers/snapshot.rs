use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::extract::{ConnectInfo, State};
use axum::http::StatusCode;
use docktornet_core::Snapshot;
use tracing::{info, warn};

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
    info!(machine = %snap.machine_id.as_str(), containers = snap.containers.len(), "snapshot received");
    match state.reconciler.reconcile_snapshot(&snap).await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            warn!(error = %e, "failed to reconcile snapshot");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
