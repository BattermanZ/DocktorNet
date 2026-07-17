use std::time::Duration;

use docktornet_core::{MachineId, Snapshot};
use tokio::time::{Instant, sleep_until};
use tracing::{error, info, warn};

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
        match poll_once(&machine, &forward_host, &sources, interval).await {
            Some(snap) => match client.push_with_retry(&snap, 3).await {
                Ok(()) => tracing::debug!(containers = snap.containers.len(), "snapshot pushed"),
                Err(error) => {
                    error!(%error, "snapshot push failed after retries; will try again next tick");
                }
            },
            None => {
                // A source failed to enumerate this tick, so the snapshot would
                // be a partial view of the machine. Pushing it would let the
                // server treat the missing containers as removed and tear down
                // their DNS/proxy resources. Skip the push and retry next tick.
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

/// Enumerate every source and build a snapshot for this tick.
///
/// Returns `None` when any configured source failed to enumerate: the resulting
/// container list would be a partial view of the machine, and the server treats
/// every pushed snapshot as the authoritative complete state. Encoding a
/// failed enumeration as "these containers are gone" would prune healthy
/// services' DNS records and reverse-proxy hosts, so a degraded tick must be
/// skipped rather than pushed.
async fn poll_once(
    machine: &MachineId,
    forward_host: &str,
    sources: &[Box<dyn ContainerSource>],
    per_source_timeout: Duration,
) -> Option<Snapshot> {
    let outcome = crate::sockets::list_all(sources, per_source_timeout).await;
    if outcome.any_failed() {
        warn!(
            failed_sources = ?outcome.failed_sources,
            "skipping snapshot push: a container source failed to enumerate; \
             refusing to push a partial machine state"
        );
        return None;
    }

    Some(snapshot::build(machine, forward_host, &outcome.containers))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use async_trait::async_trait;

    use super::*;
    use crate::sockets::RawContainer;

    struct Fake {
        items: Vec<RawContainer>,
    }

    #[async_trait]
    impl ContainerSource for Fake {
        async fn list(&self) -> anyhow::Result<Vec<RawContainer>> {
            Ok(self.items.clone())
        }

        fn label(&self) -> &str {
            "Fake"
        }
    }

    struct Boom;

    #[async_trait]
    impl ContainerSource for Boom {
        async fn list(&self) -> anyhow::Result<Vec<RawContainer>> {
            anyhow::bail!("socket unreadable")
        }

        fn label(&self) -> &str {
            "Boom"
        }
    }

    fn labelled(name: &str) -> RawContainer {
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".to_string(), "u".to_string());
        RawContainer {
            name: name.into(),
            labels,
        }
    }

    #[tokio::test]
    async fn poll_once_builds_snapshot_when_all_sources_succeed() {
        let sources: Vec<Box<dyn ContainerSource>> = vec![
            Box::new(Fake {
                items: vec![labelled("a")],
            }),
            Box::new(Fake {
                items: vec![labelled("b")],
            }),
        ];

        let snap = poll_once(
            &MachineId::new("m1"),
            "10.0.0.10",
            &sources,
            Duration::from_secs(30),
        )
        .await
        .expect("snapshot should be built when every source succeeds");

        assert_eq!(snap.containers.len(), 2);
    }

    #[tokio::test]
    async fn poll_once_skips_push_when_a_source_fails() {
        // A healthy source keeps returning its running containers while another
        // socket is unreadable. The tick must be skipped so the server never
        // sees a partial snapshot and prunes the still-running containers.
        let sources: Vec<Box<dyn ContainerSource>> = vec![
            Box::new(Fake {
                items: vec![labelled("still-running")],
            }),
            Box::new(Boom),
        ];

        let snap = poll_once(
            &MachineId::new("m1"),
            "10.0.0.10",
            &sources,
            Duration::from_secs(30),
        )
        .await;

        assert!(
            snap.is_none(),
            "a failed source must abort the push, not yield a partial snapshot"
        );
    }
}
