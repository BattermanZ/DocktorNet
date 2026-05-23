use std::path::PathBuf;
use std::sync::Arc;

use docktornet_core::Snapshot;
use tokio::sync::Mutex;

use crate::db::Db;

#[derive(Clone)]
pub struct Reconciler {
    db: Db,
    options: ReconcileOptions,
    npm: Option<crate::integrations::npm::NpmClient>,
    cloudflare: Option<crate::integrations::cloudflare::CloudflareClient>,
    lock: Arc<Mutex<()>>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ResyncReport {
    pub verified: usize,
    pub repaired: usize,
    pub failed: usize,
}

#[derive(Debug, Clone)]
pub struct ReconcileOptions {
    pub dry_run: bool,
    pub adopt_existing: bool,
    pub gatus_config_dir: PathBuf,
    pub npm_forward_host: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("external integration error: {0}")]
    External(#[from] anyhow::Error),
}

fn cf_record_matches(
    found: &crate::integrations::cloudflare::DnsRecord,
    intended: &crate::integrations::cloudflare::DnsRecordPayload,
) -> bool {
    found.name == intended.name
        && found.content == intended.content
        && found.proxied == intended.proxied
}

impl Reconciler {
    pub fn new(db: Db, options: ReconcileOptions) -> Self {
        Self {
            db,
            options,
            npm: None,
            cloudflare: None,
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn with_clients(
        db: Db,
        options: ReconcileOptions,
        npm: crate::integrations::npm::NpmClient,
        cloudflare: crate::integrations::cloudflare::CloudflareClient,
    ) -> Self {
        Self {
            db,
            options,
            npm: Some(npm),
            cloudflare: Some(cloudflare),
            lock: Arc::new(Mutex::new(())),
        }
    }

    async fn reconcile_npm(
        &self,
        service_id: &str,
        host: &docktornet_core::labels::NpmHost,
        forward_host: &str,
    ) -> Result<(), ReconcileError> {
        let Some(client) = &self.npm else {
            tracing::warn!(
                service_id,
                index = host.index,
                "NPM labels present but NPM client is not configured"
            );
            return Ok(());
        };
        let token = client
            .token()
            .await
            .map_err(|err| ReconcileError::External(err.into()))?;
        let certs = client
            .certificates(&token)
            .await
            .map_err(|err| ReconcileError::External(err.into()))?;
        let cert_id = host
            .cert_id
            .map(i64::from)
            .or_else(|| crate::integrations::npm::matching_cert_id(&host.host, &certs));
        let payload = crate::integrations::npm::proxy_payload(host, forward_host, cert_id);

        if let Some(id) =
            crate::storage::get_npm_host(&self.db, service_id, host.index.into()).await?
        {
            client
                .update_proxy_host(&token, id, &payload)
                .await
                .map_err(|err| ReconcileError::External(err.into()))?;
            return Ok(());
        }

        let created = client
            .create_proxy_host(&token, &payload)
            .await
            .map_err(|err| ReconcileError::External(err.into()))?;
        crate::storage::set_npm_host(&self.db, service_id, host.index.into(), created.id).await?;
        Ok(())
    }

    async fn reconcile_cloudflare(
        &self,
        service_id: &str,
        dns: &docktornet_core::labels::CloudflareDns,
    ) -> Result<(), ReconcileError> {
        let Some(client) = &self.cloudflare else {
            tracing::warn!(
                service_id,
                "Cloudflare DNS labels present but Cloudflare client is not configured"
            );
            return Ok(());
        };
        let zone = client
            .zone()
            .await
            .map_err(|err| ReconcileError::External(err.into()))?;
        let payload = crate::integrations::cloudflare::dns_payload(dns, &zone.name);
        let existing_id = crate::storage::get_cf_dns_record(&self.db, service_id).await?;

        if let Some(id) = existing_id {
            client
                .update_record(&id, &payload)
                .await
                .map_err(|err| ReconcileError::External(err.into()))?;
            return Ok(());
        }

        match client.create_record(&payload).await {
            Ok(record) => {
                crate::storage::set_cf_dns_record(&self.db, service_id, &record.id).await?;
                Ok(())
            }
            Err(err) if self.options.adopt_existing => {
                let record = client
                    .find_record_by_name(&payload.name)
                    .await
                    .map_err(|e| ReconcileError::External(e.into()))?
                    .ok_or_else(|| ReconcileError::External(anyhow::anyhow!(err)))?;
                client
                    .update_record(&record.id, &payload)
                    .await
                    .map_err(|e| ReconcileError::External(e.into()))?;
                crate::storage::set_cf_dns_record(&self.db, service_id, &record.id).await?;
                Ok(())
            }
            Err(err) => Err(ReconcileError::External(err.into())),
        }
    }

    pub async fn full_resync(&self) -> Result<ResyncReport, ReconcileError> {
        let _guard = self.lock.lock().await;
        let mut report = ResyncReport::default();

        let services = crate::storage::list_all_services(&self.db).await?;
        for (service_id, labels_json) in services {
            let raw: std::collections::BTreeMap<String, String> = match serde_json::from_str(
                &labels_json,
            ) {
                Ok(map) => map,
                Err(err) => {
                    tracing::warn!(service_id, error = %err, "skipping service with malformed labels_json");
                    report.failed += 1;
                    continue;
                }
            };
            let parsed = docktornet_core::labels::parse(&raw);

            for host in &parsed.npm {
                if let Err(err) = self.resync_npm_host(&service_id, host).await {
                    tracing::warn!(service_id, index = host.index, error = %err, "npm resync failed");
                    report.failed += 1;
                } else {
                    report.verified += 1;
                }
            }

            if let Some(dns) = &parsed.cf_dns {
                if let Err(err) = self.resync_cloudflare(&service_id, dns).await {
                    tracing::warn!(service_id, error = %err, "cloudflare resync failed");
                    report.failed += 1;
                } else {
                    report.verified += 1;
                }
            }

            if !parsed.gatus.is_empty() {
                match self.resync_gatus(&service_id, &parsed.gatus).await {
                    Ok(rewrote) => {
                        if rewrote {
                            report.repaired += 1
                        } else {
                            report.verified += 1
                        }
                    }
                    Err(err) => {
                        tracing::warn!(service_id, error = %err, "gatus resync failed");
                        report.failed += 1;
                    }
                }
            }
        }

        tracing::info!(
            verified = report.verified,
            repaired = report.repaired,
            failed = report.failed,
            "full resync complete"
        );
        Ok(report)
    }

    async fn resync_npm_host(
        &self,
        service_id: &str,
        host: &docktornet_core::labels::NpmHost,
    ) -> Result<(), ReconcileError> {
        let Some(client) = &self.npm else {
            return Ok(());
        };
        let Some(npm_id) =
            crate::storage::get_npm_host(&self.db, service_id, host.index.into()).await?
        else {
            return Ok(());
        };
        if self.options.dry_run {
            tracing::info!(service_id, npm_id, "dry-run: would verify npm host");
            return Ok(());
        }

        let token = client
            .token()
            .await
            .map_err(|e| ReconcileError::External(e.into()))?;
        let existing = client
            .get_proxy_host(&token, npm_id)
            .await
            .map_err(|e| ReconcileError::External(e.into()))?;
        let certs = client
            .certificates(&token)
            .await
            .map_err(|e| ReconcileError::External(e.into()))?;
        let desired_cert = host
            .cert_id
            .map(i64::from)
            .or_else(|| crate::integrations::npm::matching_cert_id(&host.host, &certs));
        let payload = crate::integrations::npm::proxy_payload(
            host,
            &self.options.npm_forward_host,
            desired_cert,
        );

        match existing {
            None => {
                tracing::info!(service_id, npm_id, "npm host missing, recreating");
                let created = client
                    .create_proxy_host(&token, &payload)
                    .await
                    .map_err(|e| ReconcileError::External(e.into()))?;
                crate::storage::set_npm_host(&self.db, service_id, host.index.into(), created.id)
                    .await?;
            }
            Some(found) if found.certificate_id != desired_cert => {
                tracing::info!(
                    service_id,
                    npm_id,
                    ?desired_cert,
                    "npm cert drifted, updating"
                );
                client
                    .update_proxy_host(&token, npm_id, &payload)
                    .await
                    .map_err(|e| ReconcileError::External(e.into()))?;
            }
            Some(_) => {}
        }
        Ok(())
    }

    async fn resync_cloudflare(
        &self,
        service_id: &str,
        dns: &docktornet_core::labels::CloudflareDns,
    ) -> Result<(), ReconcileError> {
        let Some(client) = &self.cloudflare else {
            return Ok(());
        };
        let Some(record_id) = crate::storage::get_cf_dns_record(&self.db, service_id).await? else {
            return Ok(());
        };
        if self.options.dry_run {
            tracing::info!(service_id, record_id = %record_id, "dry-run: would verify cf record");
            return Ok(());
        }

        let zone = client
            .zone()
            .await
            .map_err(|e| ReconcileError::External(e.into()))?;
        let payload = crate::integrations::cloudflare::dns_payload(dns, &zone.name);
        let existing = client
            .get_record(&record_id)
            .await
            .map_err(|e| ReconcileError::External(e.into()))?;

        match existing {
            None => {
                tracing::info!(service_id, "cf record missing, recreating");
                let created = client
                    .create_record(&payload)
                    .await
                    .map_err(|e| ReconcileError::External(e.into()))?;
                crate::storage::set_cf_dns_record(&self.db, service_id, &created.id).await?;
            }
            Some(found) if !cf_record_matches(&found, &payload) => {
                tracing::info!(service_id, "cf record drifted, updating");
                client
                    .update_record(&record_id, &payload)
                    .await
                    .map_err(|e| ReconcileError::External(e.into()))?;
            }
            Some(_) => {}
        }
        Ok(())
    }

    async fn resync_gatus(
        &self,
        service_id: &str,
        monitors: &[docktornet_core::labels::GatusMonitor],
    ) -> Result<bool, ReconcileError> {
        let target = crate::integrations::gatus::service_file_path(
            &self.options.gatus_config_dir,
            service_id,
        );
        let intended = crate::integrations::gatus::render_gatus_yaml(service_id, monitors)
            .map_err(|e| ReconcileError::External(e.into()))?;
        let current = tokio::fs::read_to_string(&target).await.ok();
        if current.as_deref() == Some(intended.as_str()) {
            return Ok(false);
        }
        if self.options.dry_run {
            tracing::info!(service_id, path = %target.display(), "dry-run: would rewrite gatus file");
            return Ok(false);
        }
        tracing::info!(service_id, path = %target.display(), "gatus file drifted, rewriting");
        crate::integrations::gatus::write_service_file(
            &self.options.gatus_config_dir,
            service_id,
            monitors,
        )
        .await
        .map_err(ReconcileError::External)?;
        Ok(true)
    }

    pub async fn reconcile_snapshot(&self, snap: &Snapshot) -> Result<(), ReconcileError> {
        let _guard = self.lock.lock().await;
        let intended =
            crate::integrations::intended::intended_services(&snap.machine_id, &snap.containers);
        let current_ids = intended
            .iter()
            .map(|service| service.service_id.clone())
            .collect::<Vec<_>>();
        let removed =
            crate::storage::removed_service_ids(&self.db, snap.machine_id.as_str(), &current_ids)
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

            for npm_host in &service.labels.npm {
                self.reconcile_npm(
                    &service.service_id,
                    npm_host,
                    &self.options.npm_forward_host,
                )
                .await?;
            }

            if let Some(dns) = &service.labels.cf_dns {
                self.reconcile_cloudflare(&service.service_id, dns).await?;
            } else if let Some(record_id) =
                crate::storage::get_cf_dns_record(&self.db, &service.service_id).await?
            {
                if let Some(client) = &self.cloudflare {
                    client
                        .delete_record(&record_id)
                        .await
                        .map_err(|err| ReconcileError::External(err.into()))?;
                }
                crate::storage::delete_cf_dns_record(&self.db, &service.service_id).await?;
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
                npm_forward_host: "127.0.0.1".to_string(),
            },
        );

        let mut labels = BTreeMap::new();
        labels.insert(
            "docktornet.gatus.0.url".to_string(),
            "https://example.com".to_string(),
        );
        labels.insert("docktornet.gatus.0.interval".to_string(), "30s".to_string());
        let snap = Snapshot {
            machine_id: MachineId::new("m1"),
            taken_at: 1,
            containers: vec![ContainerEntry {
                name: "app".to_string(),
                labels,
            }],
        };

        reconciler.reconcile_snapshot(&snap).await.unwrap();

        let rows = storage::list_services_for_machine(&pool, "m1")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(!dir.path().join("gatus").exists());
    }

    #[tokio::test]
    async fn full_resync_rewrites_missing_gatus_file() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = Reconciler::new(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: gatus_dir.clone(),
                npm_forward_host: "127.0.0.1".to_string(),
            },
        );

        // Seed via a snapshot ingest so SQLite + file are in a consistent starting state.
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
        labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                taken_at: 1,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels,
                }],
            })
            .await
            .unwrap();

        // Delete the file out from under us.
        let path = gatus::service_file_path(&gatus_dir, "m1/app");
        assert!(path.exists());
        std::fs::remove_file(&path).unwrap();

        let report = reconciler.full_resync().await.unwrap();
        assert_eq!(report.repaired, 1);
        assert_eq!(report.failed, 0);
        assert!(path.exists());
    }

    #[tokio::test]
    async fn full_resync_is_a_noop_when_state_is_already_correct() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = Reconciler::new(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: gatus_dir.clone(),
                npm_forward_host: "127.0.0.1".to_string(),
            },
        );

        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
        labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                taken_at: 1,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels,
                }],
            })
            .await
            .unwrap();

        let report = reconciler.full_resync().await.unwrap();
        assert_eq!(report.repaired, 0);
        assert_eq!(report.failed, 0);
        assert_eq!(report.verified, 1);
    }

    #[tokio::test]
    async fn full_resync_dry_run_does_not_rewrite_drifted_file() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        // Build state via a non-dry-run reconciler first, then resync with dry_run = true.
        let writer = Reconciler::new(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: gatus_dir.clone(),
                npm_forward_host: "127.0.0.1".to_string(),
            },
        );
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
        labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
        writer
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                taken_at: 1,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels,
                }],
            })
            .await
            .unwrap();

        let path = gatus::service_file_path(&gatus_dir, "m1/app");
        std::fs::write(&path, "drifted").unwrap();

        let dryrun = Reconciler::new(
            pool,
            ReconcileOptions {
                dry_run: true,
                adopt_existing: false,
                gatus_config_dir: gatus_dir,
                npm_forward_host: "127.0.0.1".to_string(),
            },
        );
        let report = dryrun.full_resync().await.unwrap();
        assert_eq!(report.repaired, 0);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "drifted");
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
                npm_forward_host: "127.0.0.1".to_string(),
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
