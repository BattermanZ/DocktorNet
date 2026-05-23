use std::time::Duration;

use docktornet_core::MachineId;
use tokio::time::{Instant, sleep_until};
use tracing::{error, info};

use crate::push::PushClient;
use crate::snapshot;
use crate::sockets::ContainerSource;

/// Run the poll/push loop forever. Each cycle:
///   1. lists containers from every source
///   2. builds a Snapshot
///   3. pushes it with retry
///
/// Errors after retry exhaust are logged and the loop continues — we never
/// abort because of a transient server outage.
pub async fn run(
    machine: MachineId,
    sources: Vec<Box<dyn ContainerSource>>,
    client: PushClient,
    interval: Duration,
) -> ! {
    let mut next = Instant::now();

    loop {
        let raw = crate::sockets::list_all(&sources).await;
        let snap = snapshot::build(&machine, &raw);

        match client.push_with_retry(&snap, 3).await {
            Ok(()) => info!(containers = snap.containers.len(), "snapshot pushed"),
            Err(error) => {
                error!(%error, "snapshot push failed after retries; will try again next tick");
            }
        }

        next += interval;
        sleep_until(next).await;
    }
}
