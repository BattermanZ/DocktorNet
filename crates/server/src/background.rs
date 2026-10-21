//! The server's timed reconcile work: the full resync and the short
//! pending-removal sweep.

use std::time::Duration;

use tokio::time::MissedTickBehavior;

use crate::reconcile::Reconciler;

/// How often removals whose grace period has ended are swept.
const PENDING_REMOVAL_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Start both loops.  The first full resync runs at once, then one every
/// `full_resync_interval`; the pending-removal sweep runs every minute after.
pub fn spawn(reconciler: Reconciler, full_resync_interval: Duration) {
    let resync = reconciler.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(full_resync_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            // The first tick completes immediately: the startup resync.
            ticker.tick().await;
            tracing::info!("full resync starting");
            if let Err(err) = resync.full_resync().await {
                tracing::warn!(error = %err, "full resync failed; will retry next cycle");
            }
        }
    });

    tokio::spawn(async move {
        let start = tokio::time::Instant::now() + PENDING_REMOVAL_SWEEP_INTERVAL;
        let mut ticker = tokio::time::interval_at(start, PENDING_REMOVAL_SWEEP_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(err) = reconciler.sweep_pending_removals().await {
                tracing::warn!(error = %err, "pending removal sweep failed; will retry next cycle");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::integrations::gatus;
    use crate::reconcile::{ReconcileOptions, Reconciler};
    use docktornet_core::{ContainerEntry, MachineId, Snapshot};
    use std::collections::BTreeMap;
    use std::time::Duration;
    use tempfile::tempdir;

    #[tokio::test]
    async fn first_full_resync_runs_at_startup() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = Reconciler::new(
            pool,
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: gatus_dir.clone(),
            },
        );
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
        labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "127.0.0.1".into(),
                taken_at: 1,
                agent_version: None,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels,
                }],
            })
            .await
            .unwrap();
        // Only a full resync rewrites a missing file for a live service.
        let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
        std::fs::remove_file(&service_file).unwrap();

        spawn(reconciler, Duration::from_secs(3600));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !service_file.exists() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the first full resync must not wait a full interval"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
