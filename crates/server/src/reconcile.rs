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
    #[error("gatus file error: {0}")]
    Gatus(#[from] anyhow::Error),
}

impl Reconciler {
    pub fn new(db: Db, options: ReconcileOptions) -> Self {
        Self { db, options }
    }

    pub async fn reconcile_snapshot(&self, snap: &Snapshot) -> Result<(), ReconcileError> {
        crate::storage::upsert_snapshot(&self.db, snap).await?;
        let intended = crate::integrations::intended::intended_services(&snap.machine_id, &snap.containers);

        for service in intended {
            if self.options.dry_run {
                tracing::info!(
                    service_id = %service.service_id,
                    npm = service.labels.npm.len(),
                    cf_dns = service.labels.cf_dns.is_some(),
                    gatus = service.labels.gatus.len(),
                    "dry-run: would reconcile service"
                );
                continue;
            }

            if !service.labels.gatus.is_empty() {
                crate::integrations::gatus::write_service_file(
                    &self.options.gatus_config_dir,
                    &service.service_id,
                    &service.labels.gatus,
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
        assert!(!dir.path().join("gatus/m1__app.yaml").exists());
    }
}
