use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use docktornet_core::Snapshot;
use tracing::{info, warn};

use crate::reconcile::Reconciler;

#[derive(Clone)]
pub struct AppState {
    pub reconciler: Reconciler,
    pub secret: Arc<String>,
}

pub async fn ingest(State(state): State<AppState>, Json(snap): Json<Snapshot>) -> StatusCode {
    info!(machine = %snap.machine_id.as_str(), containers = snap.containers.len(), "snapshot received");
    match state.reconciler.reconcile_snapshot(&snap).await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            warn!(error = %e, "failed to reconcile snapshot");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
