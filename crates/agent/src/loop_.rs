use std::time::Duration;

use docktornet_core::MachineId;
use tokio::time::{Instant, sleep_until};
use tracing::{error, info};

use crate::push::PushClient;
use crate::snapshot;
use crate::sockets::ContainerSource;

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    tokio::select! {
        () = ctrl_c => {},
        _ = sigterm => {},
    }
}

/// Run the poll/push loop until a shutdown signal (SIGINT or SIGTERM) is
/// received. Each cycle:
///   1. lists containers from every source
///   2. builds a Snapshot
///   3. pushes it with retry
///
/// Errors after retry exhaust are logged and the loop continues — we never
/// abort because of a transient server outage. An in-flight push is always
/// allowed to finish; the signal is only checked at the sleep boundary.
pub async fn run(
    machine: MachineId,
    forward_host: String,
    sources: Vec<Box<dyn ContainerSource>>,
    client: PushClient,
    interval: Duration,
) {
    let mut next = Instant::now();

    loop {
        let raw = crate::sockets::list_all(&sources).await;
        let snap = snapshot::build(&machine, &forward_host, &raw);

        match client.push_with_retry(&snap, 3).await {
            Ok(()) => tracing::debug!(containers = snap.containers.len(), "snapshot pushed"),
            Err(error) => {
                error!(%error, "snapshot push failed after retries; will try again next tick");
            }
        }

        next += interval;
        tokio::select! {
            () = sleep_until(next) => {},
            () = shutdown_signal() => {
                info!("shutdown signal received");
                return;
            },
        }
    }
}
