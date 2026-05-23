use std::path::PathBuf;

use docktornet_core::Snapshot;

use crate::db::Db;

#[derive(Clone)]
pub struct Reconciler {
    db: Db,
    options: ReconcileOptions,
}

#[derive(Debug, Clone)]
pub struct ReconcileOptions {
    pub dry_run: bool,
    pub adopt_existing: bool,
    pub gatus_config_dir: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("external integration error: {0}")]
    External(#[from] anyhow::Error),
}

impl Reconciler {
    pub fn new(db: Db, options: ReconcileOptions) -> Self {
        Self { db, options }
    }

    pub async fn reconcile_snapshot(&self, snap: &Snapshot) -> Result<(), ReconcileError> {
        let intended = crate::integrations::intended::intended_services(&snap.machine_id, &snap.containers);
        let current_ids = intended
            .iter()
            .map(|service| service.service_id.clone())
            .collect::<Vec<_>>();
        let removed = crate::storage::removed_service_ids(
            &self.db,
            snap.machine_id.as_str(),
            &current_ids,
        )
        .await?;

        crate::storage::upsert_snapshot(&self.db, snap).await?;

        if self.options.dry_run {
            tracing::info!(
                removed = removed.len(),
                current = current_ids.len(),
                "dry-run: would reconcile snapshot"
            );
            return Ok(());
        }

        for service_id in removed {
            crate::integrations::gatus::remove_service_file(
                &self.options.gatus_config_dir,
                &service_id,
            )
            .await?;
        }

        for service in intended {
            if !service.labels.gatus.is_empty() {
                crate::integrations::gatus::write_service_file(
                    &self.options.gatus_config_dir,
                    &service.service_id,
                    &service.labels.gatus,
                )
                .await?;
            } else {
                crate::integrations::gatus::remove_service_file(
                    &self.options.gatus_config_dir,
                    &service.service_id,
                )
                .await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::integrations::gatus;
    use crate::{db, storage};
    use docktornet_core::{ContainerEntry, MachineId};
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    #[tokio::test]
    async fn dry_run_persists_snapshot_without_writing_gatus_file() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = Reconciler::new(
            pool.clone(),
            ReconcileOptions {
                dry_run: true,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
        );

        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".to_string(), "https://example.com".to_string());
        labels.insert("docktornet.gatus.0.interval".to_string(), "30s".to_string());
        let snap = Snapshot {
            machine_id: MachineId::new("m1"),
            taken_at: 1,
            containers: vec![ContainerEntry { name: "app".to_string(), labels }],
        };

        reconciler.reconcile_snapshot(&snap).await.unwrap();

        let rows = storage::list_services_for_machine(&pool, "m1").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert!(!dir.path().join("gatus").exists());
    }

    #[tokio::test]
    async fn removes_gatus_file_when_service_disappears() {
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
        labels.insert(
            "docktornet.gatus.0.url".to_string(),
            "https://example.com".to_string(),
        );
        labels.insert("docktornet.gatus.0.interval".to_string(), "30s".to_string());

        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                taken_at: 1,
                containers: vec![ContainerEntry {
                    name: "app".to_string(),
                    labels,
                }],
            })
            .await
            .unwrap();
        let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
        assert!(service_file.exists());

        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                taken_at: 2,
                containers: vec![],
            })
            .await
            .unwrap();
        assert!(!service_file.exists());
    }
}
