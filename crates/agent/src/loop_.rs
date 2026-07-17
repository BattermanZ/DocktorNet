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
    run_until(
        machine,
        forward_host,
        sources,
        client,
        interval,
        shutdown_signal,
    )
    .await;
}

/// Core poll/push loop, parameterised over how the shutdown signal is awaited so
/// it can be driven deterministically in tests.
///
/// `make_shutdown` yields the future that resolves when shutdown is requested.
async fn run_until<Sig, Fut>(
    machine: MachineId,
    forward_host: String,
    sources: Vec<Box<dyn ContainerSource>>,
    client: PushClient,
    interval: Duration,
    make_shutdown: Sig,
) where
    Sig: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    // Register the shutdown listener ONCE, up front. tokio installs a
    // process-wide signal handler on first use and retains any signal that
    // arrives while the future is parked, so building this future a single time
    // means a SIGTERM/SIGINT that lands mid-push is still observed at the next
    // sleep boundary. Rebuilding it each iteration would drop the pending edge
    // and silently swallow the shutdown request.
    let shutdown = make_shutdown();
    tokio::pin!(shutdown);

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
            () = &mut shutdown => {
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

    /// Models tokio's process-signal semantics as an *edge*: a listener only
    /// observes a signal that is raised **after** the listener is created.
    /// `listen` captures the current generation; the returned future resolves
    /// only once `fire` bumps the generation past that captured value — exactly
    /// like a freshly-constructed `tokio::signal::Signal` that never observes an
    /// edge delivered before it existed.
    #[derive(Clone)]
    struct EdgeSignal {
        generation: std::sync::Arc<std::sync::atomic::AtomicU64>,
    }

    impl EdgeSignal {
        fn new() -> Self {
            Self {
                generation: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            }
        }

        fn fire(&self) {
            self.generation
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }

        fn listen(self) -> impl std::future::Future<Output = ()> + 'static {
            let generation = self.generation;
            let start = generation.load(std::sync::atomic::Ordering::SeqCst);
            async move {
                while generation.load(std::sync::atomic::Ordering::SeqCst) == start {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }

    /// Raises a shutdown signal on its first enumeration, i.e. while the tick's
    /// poll/push is in flight.
    struct FireOnList {
        signal: EdgeSignal,
        fired: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    #[async_trait]
    impl ContainerSource for FireOnList {
        async fn list(&self) -> anyhow::Result<Vec<RawContainer>> {
            if !self.fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                self.signal.fire();
            }
            Ok(vec![labelled("a")])
        }

        fn label(&self) -> &str {
            "FireOnList"
        }
    }

    #[tokio::test]
    async fn shutdown_raised_during_in_flight_push_is_honored() {
        // The shutdown signal is raised while the first tick's poll/push is in
        // flight — no listener is registered at that instant. A correct loop
        // registers its shutdown listener ONCE up front, so the pending signal
        // is still observed at the next sleep boundary and the agent exits. The
        // buggy loop rebuilds the listener every iteration, so the edge is lost
        // and the agent spins forever.
        let signal = EdgeSignal::new();

        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/api/v1/snapshot")
            .with_status(204)
            .create_async()
            .await;
        let client = crate::push::PushClient::new(server.url(), "s").unwrap();

        let sources: Vec<Box<dyn ContainerSource>> = vec![Box::new(FireOnList {
            signal: signal.clone(),
            fired: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })];

        let make_shutdown = {
            let signal = signal.clone();
            move || signal.clone().listen()
        };

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            run_until(
                MachineId::new("m1"),
                "10.0.0.10".into(),
                sources,
                client,
                Duration::from_millis(10),
                make_shutdown,
            ),
        )
        .await;

        assert!(
            result.is_ok(),
            "agent must exit when a shutdown signal is raised during an in-flight push"
        );
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
