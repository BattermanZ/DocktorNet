use std::time::Duration;

use docktornet_core::MachineId;
use tokio::time::{Instant, sleep_until};
use tracing::{error, info};

use crate::push::PushClient;
use crate::snapshot;
use crate::sockets::ContainerSource;

/// Run the poll/push loop forever.
pub async fn run(
    machine: MachineId,
    sources: Vec<Box<dyn ContainerSource>>,
    client: PushClient,
    interval: Duration,
) {
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use docktornet_core::MachineId;

    use crate::push::PushClient;
    use crate::sockets::ContainerSource;

    fn _signature_check(
        m: MachineId,
        s: Vec<Box<dyn ContainerSource>>,
        c: PushClient,
        d: Duration,
    ) -> impl std::future::Future<Output = ()> {
        super::run(m, s, c, d)
    }
}
