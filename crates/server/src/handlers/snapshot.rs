use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use docktornet_core::Snapshot;
use tracing::{info, warn};

use crate::db::Db;
use crate::storage;

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    #[allow(dead_code)]
    pub secret: Arc<String>,
}

pub async fn ingest(State(state): State<AppState>, Json(snap): Json<Snapshot>) -> StatusCode {
    info!(machine = %snap.machine_id.as_str(), containers = snap.containers.len(), "snapshot received");
    match storage::upsert_snapshot(&state.db, &snap).await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            warn!(error = %e, "failed to persist snapshot");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
