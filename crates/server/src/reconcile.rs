use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use docktornet_core::Snapshot;
use tokio::sync::Mutex;

use crate::breaker::CircuitBreaker;
use crate::db::Db;

/// Consecutive NPM failures before the breaker opens.
const NPM_BREAKER_THRESHOLD: u32 = 3;
/// How long the NPM breaker stays open before probing again.
const NPM_BREAKER_COOLDOWN: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct Reconciler {
    db: Db,
    options: ReconcileOptions,
    npm: Option<crate::integrations::npm::NpmClient>,
    cloudflare: Option<crate::integrations::cloudflare::CloudflareClient>,
    lock: Arc<Mutex<()>>,
    npm_breaker: Arc<CircuitBreaker>,
    /// Hash of the last successfully-reconciled intent per machine.  Lets an
    /// unchanged snapshot skip the external reconcile entirely (drift is still
    /// caught by the periodic `full_resync`).
    last_reconciled: Arc<Mutex<HashMap<String, u64>>>,
}

/// A service that has just left a machine's snapshot, along with the data a
/// deferred external reconcile needs.  `npm_hosts` is captured *before*
/// `upsert_snapshot` runs, because its cascade deletes the `npm_hosts` rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemovedService {
    pub service_id: String,
    pub last_seen: i64,
    /// `(npm_index, npm_id)` for every NPM proxy host the service owned.
    pub npm_hosts: Vec<(i64, i64)>,
    /// The Cloudflare DNS `record_id` the service owned, captured before the
    /// `cf_dns_records` cascade wipes it, so the public record can still be
    /// deleted after the service row is gone.
    pub cf_record: Option<String>,
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
}

#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("external integration error: {0}")]
    External(#[from] anyhow::Error),
}

/// Gatus files for containers that temporarily disappear from a snapshot (e.g.
/// during a restart) are kept for this many seconds before being deleted.  This
/// prevents Gatus from resetting alert state and skipping recovery notifications.
/// Truly-removed services that fall within the grace window are cleaned up by the
/// periodic orphan sweep in `full_resync`.
const GATUS_REMOVAL_GRACE_SECS: i64 = 300;

fn cf_record_matches(
    found: &crate::integrations::cloudflare::DnsRecord,
    intended: &crate::integrations::cloudflare::DnsRecordPayload,
) -> bool {
    found.name == intended.name
        && found.content == intended.content
        && found.proxied == intended.proxied
}

/// Stable hash of the parts of a snapshot that drive the external reconcile:
/// the machine's forward host and every opted-in container's docktornet labels.
/// Used to skip re-reconciling a snapshot whose intent is unchanged.
fn snapshot_intent_hash(forward_host: &str, containers: &[docktornet_core::ContainerEntry]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    forward_host.hash(&mut hasher);
    for container in containers {
        if !container
            .labels
            .keys()
            .any(|k| k.starts_with("docktornet."))
        {
            continue;
        }
        container.name.hash(&mut hasher);
        // BTreeMap iterates in sorted key order, so the hash is stable.
        for (key, value) in &container.labels {
            if key.starts_with("docktornet.") {
                key.hash(&mut hasher);
                value.hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}

impl Reconciler {
    pub fn pool(&self) -> &Db {
        &self.db
    }

    pub fn new(db: Db, options: ReconcileOptions) -> Self {
        Self {
            db,
            options,
            npm: None,
            cloudflare: None,
            lock: Arc::new(Mutex::new(())),
            npm_breaker: Arc::new(CircuitBreaker::new(
                NPM_BREAKER_THRESHOLD,
                NPM_BREAKER_COOLDOWN,
            )),
            last_reconciled: Arc::new(Mutex::new(HashMap::new())),
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
            npm_breaker: Arc::new(CircuitBreaker::new(
                NPM_BREAKER_THRESHOLD,
                NPM_BREAKER_COOLDOWN,
            )),
            last_reconciled: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Fetch an NPM auth token and the certificate list once, to be shared
    /// across every host in a single reconcile pass.  Re-fetching these per
    /// host turns a bulk update into a burst of redundant calls against NPM.
    async fn fetch_npm_context(
        &self,
    ) -> Result<(String, Vec<crate::integrations::npm::Certificate>), ReconcileError> {
        let client = self
            .npm
            .as_ref()
            .expect("fetch_npm_context called without an NPM client");
        let token = client
            .token()
            .await
            .map_err(|err| ReconcileError::External(err.into()))?;
        let certs = client
            .certificates(&token)
            .await
            .map_err(|err| ReconcileError::External(err.into()))?;
        Ok((token, certs))
    }

    async fn reconcile_npm(
        &self,
        service_id: &str,
        host: &docktornet_core::labels::NpmHost,
        forward_host: &str,
        token: &str,
        certs: &[crate::integrations::npm::Certificate],
    ) -> Result<(), ReconcileError> {
        let Some(client) = &self.npm else {
            tracing::warn!(
                service_id,
                index = host.index,
                "NPM labels present but NPM client is not configured"
            );
            return Ok(());
        };
        let cert_id = host
            .cert_id
            .map(i64::from)
            .or_else(|| crate::integrations::npm::matching_cert_id_for_hosts(&host.hosts, certs));
        let payload = crate::integrations::npm::proxy_payload(host, forward_host, cert_id);

        // Resolve the NPM host id: an existing mapping, a fresh create, or an
        // adoption of a host that already exists in NPM.
        let npm_id =
            match crate::storage::get_npm_host(&self.db, service_id, host.index.into()).await? {
                Some(id) => id,
                None => match client.create_proxy_host(token, &payload).await {
                    Ok(created) => {
                        tracing::info!(service_id, npm_id = created.id, "npm host created");
                        crate::storage::set_npm_host(
                            &self.db,
                            service_id,
                            host.index.into(),
                            created.id,
                        )
                        .await?;
                        // A freshly created host already matches the payload.
                        return Ok(());
                    }
                    Err(err) if self.options.adopt_existing => {
                        let existing = client
                            .find_proxy_host_by_any_domain(token, &host.hosts)
                            .await
                            .map_err(|e| ReconcileError::External(e.into()))?
                            .ok_or_else(|| ReconcileError::External(err.into()))?;
                        tracing::info!(service_id, npm_id = existing.id, "npm host adopted");
                        // Persist the mapping before any further call.  If a later
                        // step fails (e.g. NPM overloaded), the next reconcile takes
                        // the get/update path instead of re-adopting — which would
                        // otherwise loop forever, each adoption triggering an nginx
                        // reload and keeping NPM pegged.
                        crate::storage::set_npm_host(
                            &self.db,
                            service_id,
                            host.index.into(),
                            existing.id,
                        )
                        .await?;
                        existing.id
                    }
                    Err(err) => return Err(ReconcileError::External(err.into())),
                },
            };

        // Only update — which makes NPM regenerate its config and reload nginx —
        // when the stored host has actually drifted from the desired payload.
        let existing = client
            .get_proxy_host(token, npm_id)
            .await
            .map_err(|err| ReconcileError::External(err.into()))?;
        match existing {
            Some(found) if found.matches_payload(&payload) => {
                tracing::debug!(service_id, npm_id, "npm host already in sync");
            }
            _ => {
                tracing::info!(service_id, npm_id, "npm host updated");
                client
                    .update_proxy_host(token, npm_id, &payload)
                    .await
                    .map_err(|err| ReconcileError::External(err.into()))?;
            }
        }
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
            tracing::info!(service_id, record_id = %id, "cf record updated");
            client
                .update_record(&id, &payload)
                .await
                .map_err(|err| ReconcileError::External(err.into()))?;
            return Ok(());
        }

        match client.create_record(&payload).await {
            Ok(record) => {
                tracing::info!(service_id, record_id = %record.id, "cf record created");
                crate::storage::set_cf_dns_record(&self.db, service_id, &record.id).await?;
                Ok(())
            }
            Err(err) if self.options.adopt_existing => {
                let record = client
                    .find_record_by_name(&payload.name)
                    .await
                    .map_err(|e| ReconcileError::External(e.into()))?
                    .ok_or_else(|| ReconcileError::External(anyhow::anyhow!(err)))?;
                tracing::info!(service_id, record_id = %record.id, "cf record adopted");
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
        let mut expected_gatus_files = std::collections::HashSet::new();

        let services = crate::storage::list_all_services(&self.db).await?;

        // Fetch the NPM token + certificate list once for the whole resync
        // pass rather than once per host.
        let npm_ctx = if self.npm.is_some() && !self.options.dry_run {
            match self.fetch_npm_context().await {
                Ok(ctx) => Some(ctx),
                Err(err) => {
                    tracing::warn!(error = %err, "npm resync skipped: token/cert fetch failed");
                    None
                }
            }
        } else {
            None
        };

        for (service_id, labels_json, forward_host) in services {
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
            if !parsed.warnings.is_empty() {
                tracing::warn!(
                    service_id,
                    warnings = ?parsed.warnings,
                    "malformed docktornet labels during resync; affected resources will not be reconciled"
                );
            }

            for host in &parsed.npm {
                if let Err(err) = self
                    .resync_npm_host(&service_id, host, &forward_host, npm_ctx.as_ref())
                    .await
                {
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
                expected_gatus_files.insert(crate::integrations::gatus::service_file_path(
                    &self.options.gatus_config_dir,
                    &service_id,
                ));
            }
        }

        // Remove gatus files that are no longer backed by a DB service.  These
        // are left behind when reconcile_snapshot skips deletion due to the
        // grace period (see GATUS_REMOVAL_GRACE_SECS).
        if !self.options.dry_run {
            self.sweep_orphan_gatus_files(&expected_gatus_files).await;
            self.sweep_pending_npm_removals(npm_ctx.as_ref(), &mut report)
                .await;
            self.sweep_pending_cf_removals(&mut report).await;
        }

        tracing::info!(
            verified = report.verified,
            repaired = report.repaired,
            failed = report.failed,
            "full resync complete"
        );
        Ok(report)
    }

    async fn sweep_orphan_gatus_files(
        &self,
        expected: &std::collections::HashSet<std::path::PathBuf>,
    ) {
        let mut entries = match tokio::fs::read_dir(&self.options.gatus_config_dir).await {
            Ok(e) => e,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return,
            Err(err) => {
                tracing::warn!(error = %err, "could not read gatus config dir for orphan sweep");
                return;
            }
        };
        loop {
            match entries.next_entry().await {
                Ok(Some(entry)) => {
                    let path = entry.path();
                    if !crate::integrations::gatus::is_managed_service_file(&path) {
                        continue;
                    }
                    if expected.contains(&path) {
                        continue;
                    }
                    tracing::info!(path = %path.display(), "removing orphan gatus file");
                    match tokio::fs::remove_file(&path).await {
                        Ok(()) => {}
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                        Err(err) => {
                            tracing::warn!(path = %path.display(), error = %err, "failed to remove orphan gatus file");
                        }
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    tracing::warn!(error = %err, "error reading gatus config dir entry");
                    break;
                }
            }
        }
    }

    /// Prune NPM proxy hosts recorded in `pending_npm_removals` once they are
    /// past the grace period.  This is the backstop for removals deferred by
    /// `apply_inner` (within-grace, breaker open, or a failed delete) and for
    /// machines that went offline entirely without a final empty snapshot.
    async fn sweep_pending_npm_removals(
        &self,
        npm_ctx: Option<&(String, Vec<crate::integrations::npm::Certificate>)>,
        report: &mut ResyncReport,
    ) {
        let pending = match crate::storage::list_pending_npm_removals(&self.db).await {
            Ok(p) => p,
            Err(err) => {
                tracing::warn!(error = %err, "could not list pending npm removals");
                return;
            }
        };
        if pending.is_empty() {
            return;
        }
        let Some((token, _)) = npm_ctx else {
            // No NPM access this pass; leave the entries for the next sweep.
            return;
        };
        let client = self.npm.as_ref().expect("npm_ctx implies npm client");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(i64::MAX);

        for (service_id, npm_index, npm_id, last_seen) in pending {
            if now.saturating_sub(last_seen) < GATUS_REMOVAL_GRACE_SECS {
                // Still within grace (a removal that landed just before this
                // sweep); a later sweep will pick it up.
                continue;
            }
            match client.delete_proxy_host(token, npm_id).await {
                Ok(()) => {
                    tracing::info!(service_id, npm_id, "pending npm host pruned");
                    if let Err(err) =
                        crate::storage::delete_pending_npm_removal(&self.db, &service_id, npm_index)
                            .await
                    {
                        tracing::warn!(service_id, error = %err, "failed to clear pending npm removal");
                        report.failed += 1;
                    } else {
                        report.repaired += 1;
                    }
                }
                Err(err) => {
                    // Leave the pending row in place so the next sweep retries.
                    tracing::warn!(service_id, npm_id, error = %err, "pending npm host prune failed");
                    report.failed += 1;
                }
            }
        }
    }

    /// Delete Cloudflare DNS records recorded in `pending_cf_removals` once they
    /// are past the grace period.  This is the backstop for removals deferred by
    /// `apply_inner` (within-grace, no client, or a failed delete) and for
    /// machines that went offline entirely without a final empty snapshot.
    async fn sweep_pending_cf_removals(&self, report: &mut ResyncReport) {
        let pending = match crate::storage::list_pending_cf_removals(&self.db).await {
            Ok(p) => p,
            Err(err) => {
                tracing::warn!(error = %err, "could not list pending cf removals");
                return;
            }
        };
        if pending.is_empty() {
            return;
        }
        let Some(client) = &self.cloudflare else {
            // No Cloudflare access this pass; leave the entries for the next sweep.
            return;
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(i64::MAX);

        for (service_id, record_id, last_seen) in pending {
            if now.saturating_sub(last_seen) < GATUS_REMOVAL_GRACE_SECS {
                // Still within grace (a removal that landed just before this
                // sweep); a later sweep will pick it up.
                continue;
            }
            match client.delete_record(&record_id).await {
                Ok(()) => {
                    tracing::info!(service_id, record_id = %record_id, "pending cf record deleted");
                    if let Err(err) =
                        crate::storage::delete_pending_cf_removal(&self.db, &service_id).await
                    {
                        tracing::warn!(service_id, error = %err, "failed to clear pending cf removal");
                        report.failed += 1;
                    } else {
                        report.repaired += 1;
                    }
                }
                Err(err) => {
                    // Leave the pending row in place so the next sweep retries.
                    tracing::warn!(service_id, record_id = %record_id, error = %err, "pending cf record delete failed");
                    report.failed += 1;
                }
            }
        }
    }

    async fn resync_npm_host(
        &self,
        service_id: &str,
        host: &docktornet_core::labels::NpmHost,
        forward_host: &str,
        npm_ctx: Option<&(String, Vec<crate::integrations::npm::Certificate>)>,
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
            tracing::debug!(service_id, npm_id, "dry-run: would verify npm host");
            return Ok(());
        }
        let Some((token, certs)) = npm_ctx else {
            return Err(ReconcileError::External(anyhow::anyhow!(
                "NPM token/certificates unavailable"
            )));
        };

        let existing = client
            .get_proxy_host(token, npm_id)
            .await
            .map_err(|e| ReconcileError::External(e.into()))?;
        let desired_cert = host
            .cert_id
            .map(i64::from)
            .or_else(|| crate::integrations::npm::matching_cert_id_for_hosts(&host.hosts, certs));
        let payload = crate::integrations::npm::proxy_payload(host, forward_host, desired_cert);

        match existing {
            None => {
                tracing::info!(service_id, npm_id, "npm host missing, recreating");
                let created = client
                    .create_proxy_host(token, &payload)
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
                    .update_proxy_host(token, npm_id, &payload)
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
            tracing::debug!(service_id, record_id = %record_id, "dry-run: would verify cf record");
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
            tracing::debug!(service_id, path = %target.display(), "dry-run: would rewrite gatus file");
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

    /// Persist the snapshot to SQLite and return the `(service_id, last_seen)`
    /// pairs for services that have just left this machine's snapshot.  This is
    /// the fast, local-only half of reconciliation and must run before a push
    /// is acked so the snapshot is durable even when the external reconcile is
    /// deferred.  `removed_last_seen` is captured here because `upsert_snapshot`
    /// deletes those rows.
    async fn persist_inner(&self, snap: &Snapshot) -> Result<Vec<RemovedService>, ReconcileError> {
        let current_ids =
            crate::integrations::intended::intended_services(&snap.machine_id, &snap.containers)
                .iter()
                .map(|service| service.service_id.clone())
                .collect::<Vec<_>>();
        let removed =
            crate::storage::removed_service_ids(&self.db, snap.machine_id.as_str(), &current_ids)
                .await?;
        let removed_last_seen = crate::storage::get_services_last_seen(&self.db, &removed).await?;
        // Capture NPM host + Cloudflare record mappings before upsert_snapshot's
        // cascade wipes them.
        let npm_hosts = crate::storage::get_npm_hosts_for_services(&self.db, &removed).await?;
        let cf_records = crate::storage::get_cf_records_for_services(&self.db, &removed).await?;
        crate::storage::upsert_snapshot(&self.db, snap).await?;

        let mut hosts_by_service: HashMap<String, Vec<(i64, i64)>> = HashMap::new();
        for (service_id, npm_index, npm_id) in npm_hosts {
            hosts_by_service
                .entry(service_id)
                .or_default()
                .push((npm_index, npm_id));
        }
        let mut cf_by_service: HashMap<String, String> = HashMap::new();
        for (service_id, record_id) in cf_records {
            cf_by_service.insert(service_id, record_id);
        }
        Ok(removed_last_seen
            .into_iter()
            .map(|(service_id, last_seen)| RemovedService {
                npm_hosts: hosts_by_service.remove(&service_id).unwrap_or_default(),
                cf_record: cf_by_service.remove(&service_id),
                service_id,
                last_seen,
            })
            .collect())
    }

    /// Apply the external side effects of a snapshot: Gatus config files, NPM
    /// proxy hosts and Cloudflare DNS records.  Each service is reconciled
    /// independently — a failure for one is logged and counted but never aborts
    /// work for the others (the next push and the periodic full resync retry).
    /// Returns an error summarizing how many services failed, for the caller to
    /// log.
    async fn apply_inner(
        &self,
        snap: &Snapshot,
        removed: Vec<RemovedService>,
    ) -> Result<(), ReconcileError> {
        let intended =
            crate::integrations::intended::intended_services(&snap.machine_id, &snap.containers);
        let machine = snap.machine_id.as_str().to_string();
        let intent_hash = snapshot_intent_hash(&snap.forward_host, &snap.containers);

        // Skip the whole external reconcile when nothing relevant changed since
        // the last successful run.  A polling agent re-pushes an identical
        // snapshot every tick; re-running issues NPM calls (and risks reloads)
        // for nothing.  Only safe when there are no pending removals to process;
        // genuine drift is still corrected by the periodic full_resync.
        if removed.is_empty()
            && self.last_reconciled.lock().await.get(&machine) == Some(&intent_hash)
        {
            tracing::debug!(machine = %machine, "snapshot unchanged since last successful reconcile; skipping external reconcile");
            return Ok(());
        }

        let mut failures = 0usize;

        // Write gatus files for all current services before removing any stale
        // ones.  This ensures Gatus never reloads with an active service missing
        // from the config directory.
        for service in &intended {
            if let Err(err) = self.apply_gatus(service).await {
                tracing::warn!(service_id = %service.service_id, error = %err, "gatus write failed");
                failures += 1;
            }
        }

        // Remove gatus files for services that have left this machine's
        // snapshot, but only after a grace period.  Containers that are
        // restarting briefly disappear from the snapshot; deleting their
        // config file causes Gatus to reset alert state so recovery
        // notifications are never sent.  Files for truly-removed services
        // that are still within the grace period are cleaned up by
        // full_resync's orphan sweep.
        let snap_time = snap.taken_at as i64;
        for rs in &removed {
            let absent_secs = snap_time.saturating_sub(rs.last_seen);
            if absent_secs >= GATUS_REMOVAL_GRACE_SECS {
                tracing::info!(service_id = %rs.service_id, absent_secs, "service removed");
                if let Err(err) = crate::integrations::gatus::remove_service_file(
                    &self.options.gatus_config_dir,
                    &rs.service_id,
                )
                .await
                {
                    tracing::warn!(service_id = %rs.service_id, error = %err, "gatus removal failed");
                    failures += 1;
                }
            } else {
                tracing::debug!(
                    service_id = %rs.service_id,
                    absent_secs,
                    grace_secs = GATUS_REMOVAL_GRACE_SECS,
                    "service absent but within grace period, keeping gatus file"
                );
            }
        }

        // Fetch the NPM token + certificate list once for the whole snapshot,
        // shared across every host below.  If the circuit breaker is open
        // (NPM has been failing), skip NPM entirely this cycle so we stop
        // adding load to a struggling dependency.
        let has_npm_removals = removed.iter().any(|r| !r.npm_hosts.is_empty());
        let needs_npm = self.npm.is_some()
            && (has_npm_removals || intended.iter().any(|s| !s.labels.npm.is_empty()));
        let now = std::time::Instant::now();
        let skip_npm = needs_npm && self.npm_breaker.is_open(now);
        if skip_npm {
            tracing::warn!(
                "npm circuit breaker open; skipping NPM reconcile this cycle to let NPM recover"
            );
        }
        let mut npm_failed = false;
        let npm_ctx = if needs_npm && !skip_npm {
            match self.fetch_npm_context().await {
                Ok(ctx) => Some(ctx),
                Err(err) => {
                    tracing::warn!(error = %err, "npm reconcile skipped: token/cert fetch failed");
                    npm_failed = true;
                    failures += 1;
                    None
                }
            }
        } else {
            None
        };

        // Prune NPM proxy hosts for services that left the snapshot.  Past the
        // grace period we delete immediately; within it we defer (a brief
        // container restart should not tear down and recreate the host, which
        // reloads nginx).  Anything we can't delete right now — within grace,
        // breaker open, token unavailable, or a failed API call — is recorded
        // in pending_npm_removals so the periodic resync sweep retries it.  The
        // npm_hosts mapping has already been cascade-deleted, so that table is
        // the only durable record of what to prune.
        for rs in &removed {
            let absent_secs = snap_time.saturating_sub(rs.last_seen);
            let prune_now = absent_secs >= GATUS_REMOVAL_GRACE_SECS && !skip_npm;
            for &(npm_index, npm_id) in &rs.npm_hosts {
                let deleted = if prune_now {
                    match &npm_ctx {
                        Some((token, _)) => {
                            let client = self.npm.as_ref().expect("npm_ctx implies npm client");
                            match client.delete_proxy_host(token, npm_id).await {
                                Ok(()) => {
                                    tracing::info!(service_id = %rs.service_id, npm_id, "npm host pruned");
                                    true
                                }
                                Err(err) => {
                                    tracing::warn!(service_id = %rs.service_id, npm_id, error = %err, "npm host prune failed; deferring");
                                    npm_failed = true;
                                    failures += 1;
                                    false
                                }
                            }
                        }
                        None => false,
                    }
                } else {
                    false
                };
                if !deleted {
                    crate::storage::add_pending_npm_removal(
                        &self.db,
                        &rs.service_id,
                        npm_index,
                        npm_id,
                        rs.last_seen,
                    )
                    .await?;
                }
            }
        }

        // Delete Cloudflare DNS records for services that left the snapshot.
        // Past the grace period we delete immediately; within it we defer (a
        // brief container restart should not tear down a live public record).
        // Anything we can't delete right now — within grace, no client, or a
        // failed API call — is recorded in pending_cf_removals so the periodic
        // resync sweep retries it.  The cf_dns_records mapping has already been
        // cascade-deleted, so that table is the only durable record of what to
        // delete.
        for rs in &removed {
            let Some(record_id) = &rs.cf_record else {
                continue;
            };
            let absent_secs = snap_time.saturating_sub(rs.last_seen);
            let delete_now = absent_secs >= GATUS_REMOVAL_GRACE_SECS;
            let deleted = if delete_now {
                match &self.cloudflare {
                    Some(client) => match client.delete_record(record_id).await {
                        Ok(()) => {
                            tracing::info!(service_id = %rs.service_id, record_id = %record_id, "cf record deleted");
                            true
                        }
                        Err(err) => {
                            tracing::warn!(service_id = %rs.service_id, record_id = %record_id, error = %err, "cf record delete failed; deferring");
                            failures += 1;
                            false
                        }
                    },
                    None => false,
                }
            } else {
                false
            };
            if !deleted {
                crate::storage::add_pending_cf_removal(
                    &self.db,
                    &rs.service_id,
                    record_id,
                    rs.last_seen,
                )
                .await?;
            }
        }

        for service in &intended {
            // A service present in the snapshot is live, so any pending NPM
            // removal for it is stale (it briefly disappeared and came back).
            //
            // But the pending row is also the only surviving record of the
            // external host id: upsert_snapshot's cascade wiped the npm_hosts
            // mapping the moment the container left a snapshot.  If we simply
            // dropped the pending row, reconcile_npm's get_npm_host would return
            // None and create a SECOND proxy host for the same domain, orphaning
            // the original.  So restore the mapping first, then clear the
            // pending row — reconcile then updates the existing host in place.
            for (npm_index, npm_id) in
                crate::storage::get_pending_npm_removals_for_service(&self.db, &service.service_id)
                    .await?
            {
                crate::storage::set_npm_host(&self.db, &service.service_id, npm_index, npm_id)
                    .await?;
            }
            crate::storage::clear_pending_npm_removals_for_service(&self.db, &service.service_id)
                .await?;
            // Likewise for a pending Cloudflare removal: a returning service's
            // record must never be deleted, and its record_id must be restored
            // (the cf_dns_records mapping was cascade-deleted too) so reconcile
            // updates the existing DNS record instead of creating a duplicate.
            if let Some(record_id) =
                crate::storage::get_pending_cf_removal(&self.db, &service.service_id).await?
            {
                crate::storage::set_cf_dns_record(&self.db, &service.service_id, &record_id)
                    .await?;
            }
            crate::storage::delete_pending_cf_removal(&self.db, &service.service_id).await?;

            if !skip_npm {
                for npm_host in &service.labels.npm {
                    match &npm_ctx {
                        Some((token, certs)) => {
                            if let Err(err) = self
                                .reconcile_npm(
                                    &service.service_id,
                                    npm_host,
                                    &snap.forward_host,
                                    token,
                                    certs,
                                )
                                .await
                            {
                                tracing::warn!(service_id = %service.service_id, index = npm_host.index, error = %err, "npm reconcile failed");
                                npm_failed = true;
                                failures += 1;
                            }
                        }
                        // No NPM client configured at all: warn (as before) but
                        // do not count it as a reconcile failure.
                        None if self.npm.is_none() => {
                            tracing::warn!(
                                service_id = %service.service_id,
                                index = npm_host.index,
                                "NPM labels present but NPM client is not configured"
                            );
                        }
                        // Client present but the shared token/cert fetch failed
                        // above (already counted once); skip without double-counting.
                        None => {}
                    }
                }
            }

            if let Err(err) = self.reconcile_cf_for_service(service).await {
                tracing::warn!(service_id = %service.service_id, error = %err, "cloudflare reconcile failed");
                failures += 1;
            }
        }

        // Feed the breaker only when we actually exercised NPM this cycle.
        if needs_npm && !skip_npm {
            if npm_failed {
                self.npm_breaker.record_failure(now);
            } else {
                self.npm_breaker.record_success();
            }
        }

        if failures > 0 {
            // Don't cache the intent hash on failure, so the next push retries.
            return Err(ReconcileError::External(anyhow::anyhow!(
                "{failures} service(s) failed to reconcile"
            )));
        }
        // Record success only when NPM actually ran (not when skipped by the
        // breaker), so a breaker-skipped cycle still retries once it closes.
        if !skip_npm {
            self.last_reconciled
                .lock()
                .await
                .insert(machine, intent_hash);
        }
        Ok(())
    }

    /// Write (or remove) the Gatus config file for a single current service.
    async fn apply_gatus(
        &self,
        service: &crate::integrations::intended::IntendedService,
    ) -> Result<(), ReconcileError> {
        if service.labels.gatus.is_empty() {
            // Service still exists but gatus labels were intentionally removed.
            return crate::integrations::gatus::remove_service_file(
                &self.options.gatus_config_dir,
                &service.service_id,
            )
            .await
            .map_err(ReconcileError::External);
        }

        let target = crate::integrations::gatus::service_file_path(
            &self.options.gatus_config_dir,
            &service.service_id,
        );
        let intended_yaml = crate::integrations::gatus::render_gatus_yaml(
            &service.service_id,
            &service.labels.gatus,
        )
        .map_err(|e| ReconcileError::External(e.into()))?;
        let current = tokio::fs::read_to_string(&target).await.ok();
        if current.as_deref() != Some(intended_yaml.as_str()) {
            tracing::info!(service_id = %service.service_id, path = %target.display(), "gatus file written");
            crate::integrations::gatus::write_service_file(
                &self.options.gatus_config_dir,
                &service.service_id,
                &service.labels.gatus,
            )
            .await?;
        }
        Ok(())
    }

    /// Reconcile (or delete) the Cloudflare DNS record for a single service.
    async fn reconcile_cf_for_service(
        &self,
        service: &crate::integrations::intended::IntendedService,
    ) -> Result<(), ReconcileError> {
        if let Some(dns) = &service.labels.cf_dns {
            return self.reconcile_cloudflare(&service.service_id, dns).await;
        }
        if let Some(record_id) =
            crate::storage::get_cf_dns_record(&self.db, &service.service_id).await?
        {
            tracing::info!(service_id = %service.service_id, record_id = %record_id, "cf record deleted");
            if let Some(client) = &self.cloudflare {
                client
                    .delete_record(&record_id)
                    .await
                    .map_err(|err| ReconcileError::External(err.into()))?;
            }
            crate::storage::delete_cf_dns_record(&self.db, &service.service_id).await?;
        }
        Ok(())
    }

    pub async fn reconcile_snapshot(&self, snap: &Snapshot) -> Result<(), ReconcileError> {
        let _guard = self.lock.lock().await;
        let removed = self.persist_inner(snap).await?;
        if self.options.dry_run {
            tracing::debug!(removed = removed.len(), "dry-run: would reconcile snapshot");
            return Ok(());
        }
        self.apply_inner(snap, removed).await
    }

    /// Persist the snapshot and return the data a deferred external reconcile
    /// needs.  The ingest handler calls this so a push is acked as soon as the
    /// snapshot is durable, without blocking on slow NPM/Cloudflare calls, then
    /// runs `apply_reconcile` in the background.
    pub async fn ingest_snapshot(
        &self,
        snap: &Snapshot,
    ) -> Result<Vec<RemovedService>, ReconcileError> {
        let _guard = self.lock.lock().await;
        self.persist_inner(snap).await
    }

    /// Apply the external side effects for an already-persisted snapshot.  Runs
    /// after `ingest_snapshot`, off the request path.
    pub async fn apply_reconcile(
        &self,
        snap: &Snapshot,
        removed: Vec<RemovedService>,
    ) -> Result<(), ReconcileError> {
        if self.options.dry_run {
            return Ok(());
        }
        let _guard = self.lock.lock().await;
        self.apply_inner(snap, removed).await
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
        labels.insert(
            "docktornet.gatus.0.url".to_string(),
            "https://example.com".to_string(),
        );
        labels.insert("docktornet.gatus.0.interval".to_string(), "30s".to_string());
        let snap = Snapshot {
            machine_id: MachineId::new("m1"),
            forward_host: "127.0.0.1".into(),
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
            },
        );

        // Seed via a snapshot ingest so SQLite + file are in a consistent starting state.
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
        labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "127.0.0.1".into(),
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
            },
        );

        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
        labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "127.0.0.1".into(),
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
            },
        );
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
        labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
        writer
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "127.0.0.1".into(),
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
            },
        );
        let report = dryrun.full_resync().await.unwrap();
        assert_eq!(report.repaired, 0);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "drifted");
    }

    #[tokio::test]
    async fn gatus_file_survives_when_service_temporarily_absent() {
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
                forward_host: "127.0.0.1".into(),
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

        // Snapshot taken only 30 seconds after the service was last seen:
        // still within the 5-minute grace period, so the file must survive.
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "127.0.0.1".into(),
                taken_at: 31,
                containers: vec![],
            })
            .await
            .unwrap();
        assert!(
            service_file.exists(),
            "gatus file must not be deleted within the grace period"
        );
    }

    #[tokio::test]
    async fn gatus_file_removed_when_service_absent_past_grace_period() {
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
                forward_host: "127.0.0.1".into(),
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

        // Snapshot taken well past the 5-minute grace period.
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "127.0.0.1".into(),
                taken_at: (1 + GATUS_REMOVAL_GRACE_SECS + 1) as u64,
                containers: vec![],
            })
            .await
            .unwrap();
        assert!(
            !service_file.exists(),
            "gatus file must be deleted after the grace period"
        );
    }

    #[tokio::test]
    async fn full_resync_removes_orphan_gatus_file() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = Reconciler::new(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: gatus_dir.clone(),
            },
        );

        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
        labels.insert("docktornet.gatus.0.interval".into(), "30s".into());

        // Seed the service.
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "127.0.0.1".into(),
                taken_at: 1,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels,
                }],
            })
            .await
            .unwrap();
        let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
        assert!(service_file.exists());

        // Remove the service within the grace period so the file is kept.
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "127.0.0.1".into(),
                taken_at: 31,
                containers: vec![],
            })
            .await
            .unwrap();
        assert!(service_file.exists(), "file should survive grace period");

        // full_resync should sweep the orphan file (service is no longer in DB).
        let report = reconciler.full_resync().await.unwrap();
        assert_eq!(report.failed, 0);
        assert!(
            !service_file.exists(),
            "full_resync must remove the orphan gatus file"
        );
    }

    #[tokio::test]
    async fn npm_forwards_to_each_machines_own_address() {
        use mockito::Matcher;

        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut npm_server = mockito::Server::new_async().await;
        let _token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .expect_at_least(1)
            .create_async()
            .await;
        let _certs = npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .expect_at_least(1)
            .create_async()
            .await;
        // One expectation per machine, matching its own forward_host.
        let create_a = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .match_body(Matcher::PartialJson(
                serde_json::json!({ "forward_host": "10.0.0.1" }),
            ))
            .with_status(200)
            .with_body(r#"{"id":1,"domain_names":["a.example.com"]}"#)
            .expect(1)
            .create_async()
            .await;
        let create_b = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .match_body(Matcher::PartialJson(
                serde_json::json!({ "forward_host": "10.0.0.2" }),
            ))
            .with_status(200)
            .with_body(r#"{"id":2,"domain_names":["b.example.com"]}"#)
            .expect(1)
            .create_async()
            .await;

        let npm = crate::integrations::npm::NpmClient::new(crate::config::NpmConfig {
            url: url::Url::parse(&npm_server.url()).unwrap(),
            user: "u".into(),
            password: "p".into(),
        });
        let cf = crate::integrations::cloudflare::CloudflareClient::new(
            crate::config::CloudflareConfig {
                api_token: "t".into(),
                zone_id: "z".into(),
            },
        );

        let reconciler = Reconciler::with_clients(
            pool,
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        let npm_labels = |host: &str| {
            let mut labels = BTreeMap::new();
            labels.insert("docktornet.npm.0.host".to_string(), host.to_string());
            labels.insert("docktornet.npm.0.port".to_string(), "3000".to_string());
            labels.insert("docktornet.npm.0.scheme".to_string(), "http".to_string());
            labels.insert("docktornet.npm.0.ssl".to_string(), "false".to_string());
            labels
        };

        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels: npm_labels("a.example.com"),
                }],
            })
            .await
            .unwrap();

        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m2"),
                forward_host: "10.0.0.2".into(),
                taken_at: 1,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels: npm_labels("b.example.com"),
                }],
            })
            .await
            .unwrap();

        create_a.assert_async().await;
        create_b.assert_async().await;
    }

    #[tokio::test]
    async fn npm_skips_update_when_host_already_in_sync() {
        // A host that is already mapped and whose NPM state matches the payload
        // must be fetched and left untouched — no PUT (which would reload nginx).
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut npm_server = mockito::Server::new_async().await;
        let _token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .expect_at_least(1)
            .create_async()
            .await;
        let _certs = npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .expect_at_least(1)
            .create_async()
            .await;
        // Mapping already exists, so the host is never (re)created.
        let create = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
            .expect(0)
            .create_async()
            .await;
        // It is fetched; stored state matches the payload, so no PUT is issued.
        let get = npm_server
            .mock("GET", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(
                r#"{"id":50,"domain_names":["a.example.com"],"forward_scheme":"http",
                    "forward_host":"10.0.0.1","forward_port":3000,"certificate_id":0,
                    "ssl_forced":0,"http2_support":0,"hsts_enabled":0,
                    "hsts_subdomains":0,"block_exploits":1,"allow_websocket_upgrade":1}"#,
            )
            .expect(1)
            .create_async()
            .await;
        let update = npm_server
            .mock("PUT", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
            .expect(0)
            .create_async()
            .await;

        let (npm, cf) = test_clients(&npm_server.url());
        let reconciler = Reconciler::with_clients(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        let snap = Snapshot {
            machine_id: MachineId::new("m1"),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels("a.example.com"),
            }],
        };
        // Seed the service row (npm_hosts has an FK to it) and the mapping so
        // reconcile takes the get/verify path rather than creating.
        storage::upsert_snapshot(&pool, &snap).await.unwrap();
        storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

        reconciler.reconcile_snapshot(&snap).await.unwrap();

        create.assert_async().await;
        get.assert_async().await;
        update.assert_async().await;
    }

    #[tokio::test]
    async fn npm_adopts_existing_host_when_create_conflicts() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut npm_server = mockito::Server::new_async().await;
        let _token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .expect_at_least(1)
            .create_async()
            .await;
        let _certs = npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .expect_at_least(1)
            .create_async()
            .await;
        // Create conflicts because the host already exists in NPM.
        let create = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(400)
            .with_body(r#"{"error":{"message":"domain already in use"}}"#)
            .expect(1)
            .create_async()
            .await;
        // Adoption lists and finds id 61.
        let list = npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(r#"[{"id":61,"domain_names":["a.example.com"]}]"#)
            .expect(1)
            .create_async()
            .await;
        // The adopted host is then fetched; its stored state differs from the
        // payload (port 9999 vs 3000), so an update is issued.
        let get = npm_server
            .mock("GET", "/api/nginx/proxy-hosts/61")
            .with_status(200)
            .with_body(
                r#"{"id":61,"domain_names":["a.example.com"],"forward_scheme":"http",
                    "forward_host":"10.0.0.1","forward_port":9999,"certificate_id":0}"#,
            )
            .expect(1)
            .create_async()
            .await;
        let update = npm_server
            .mock("PUT", "/api/nginx/proxy-hosts/61")
            .with_status(200)
            .with_body(r#"{"id":61,"domain_names":["a.example.com"]}"#)
            .expect(1)
            .create_async()
            .await;

        let npm = crate::integrations::npm::NpmClient::new(crate::config::NpmConfig {
            url: url::Url::parse(&npm_server.url()).unwrap(),
            user: "u".into(),
            password: "p".into(),
        });
        let cf = crate::integrations::cloudflare::CloudflareClient::new(
            crate::config::CloudflareConfig {
                api_token: "t".into(),
                zone_id: "z".into(),
            },
        );

        let reconciler = Reconciler::with_clients(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: true,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        let mut labels = BTreeMap::new();
        labels.insert(
            "docktornet.npm.0.host".to_string(),
            "a.example.com".to_string(),
        );
        labels.insert("docktornet.npm.0.port".to_string(), "3000".to_string());
        labels.insert("docktornet.npm.0.scheme".to_string(), "http".to_string());
        labels.insert("docktornet.npm.0.ssl".to_string(), "false".to_string());

        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels,
                }],
            })
            .await
            .unwrap();

        create.assert_async().await;
        list.assert_async().await;
        get.assert_async().await;
        update.assert_async().await;

        let stored = storage::get_npm_host(&pool, "m1/app", 0).await.unwrap();
        assert_eq!(stored, Some(61));
    }

    #[tokio::test]
    async fn npm_host_deferred_to_pending_when_service_absent_within_grace() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut npm_server = mockito::Server::new_async().await;
        let _token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .create_async()
            .await;
        let _certs = npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .create_async()
            .await;
        // A brief restart must NOT delete the proxy host.
        let delete = npm_server
            .mock("DELETE", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body("{}")
            .expect(0)
            .create_async()
            .await;

        let (npm, cf) = test_clients(&npm_server.url());
        let reconciler = Reconciler::with_clients(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        let snap = Snapshot {
            machine_id: MachineId::new("m1"),
            forward_host: "10.0.0.1".into(),
            taken_at: 100,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels("a.example.com"),
            }],
        };
        storage::upsert_snapshot(&pool, &snap).await.unwrap();
        storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

        // Service gone only 30s later — within the grace period.
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 130,
                containers: vec![],
            })
            .await
            .unwrap();

        delete.assert_async().await;
        // The host is recorded for a later sweep, carrying its original last_seen.
        assert_eq!(
            storage::list_pending_npm_removals(&pool).await.unwrap(),
            vec![("m1/app".to_string(), 0, 50, 100)]
        );
    }

    #[tokio::test]
    async fn full_resync_prunes_pending_npm_removal_past_grace() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut npm_server = mockito::Server::new_async().await;
        let _token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .create_async()
            .await;
        let _certs = npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .create_async()
            .await;
        let delete = npm_server
            .mock("DELETE", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body("{}")
            .expect(1)
            .create_async()
            .await;

        let (npm, cf) = test_clients(&npm_server.url());
        let reconciler = Reconciler::with_clients(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        // A pending removal whose last_seen is far in the past (well past grace).
        storage::add_pending_npm_removal(&pool, "m1/app", 0, 50, 1)
            .await
            .unwrap();

        reconciler.full_resync().await.unwrap();

        delete.assert_async().await;
        assert!(
            storage::list_pending_npm_removals(&pool)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn npm_host_mapping_restored_when_service_reappears_within_grace() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut npm_server = mockito::Server::new_async().await;
        let _token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .expect_at_least(1)
            .create_async()
            .await;
        let _certs = npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .expect_at_least(1)
            .create_async()
            .await;
        // A returning service must UPDATE its existing proxy host, never create
        // a SECOND one for the same domain (which would orphan the original).
        let create = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(r#"{"id":99,"domain_names":["a.example.com"]}"#)
            .expect(0)
            .create_async()
            .await;
        // The now-live host must never be deleted.
        let delete = npm_server
            .mock("DELETE", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body("{}")
            .expect(0)
            .create_async()
            .await;
        // The restored mapping drives a fetch of the original host (id 50);
        // its stored port (9999) differs from the payload (3000), so it is
        // updated in place rather than recreated.
        let get = npm_server
            .mock("GET", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(
                r#"{"id":50,"domain_names":["a.example.com"],"forward_scheme":"http",
                    "forward_host":"10.0.0.1","forward_port":9999,"certificate_id":0}"#,
            )
            .expect(1)
            .create_async()
            .await;
        let update = npm_server
            .mock("PUT", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
            .expect(1)
            .create_async()
            .await;

        let (npm, cf) = test_clients(&npm_server.url());
        let reconciler = Reconciler::with_clients(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        // A pending removal left over from a brief disappearance.  The
        // npm_hosts mapping was cascade-deleted when the service left, so this
        // pending row is the only surviving record of host id 50.
        storage::add_pending_npm_removal(&pool, "m1/app", 0, 50, 100)
            .await
            .unwrap();

        // The service comes back in a fresh snapshot.
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 200,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels: npm_host_labels("a.example.com"),
                }],
            })
            .await
            .unwrap();

        create.assert_async().await;
        delete.assert_async().await;
        get.assert_async().await;
        update.assert_async().await;
        // The mapping is restored to the original host id and the pending row
        // is cleared.
        assert_eq!(
            storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
            Some(50)
        );
        assert!(
            storage::list_pending_npm_removals(&pool)
                .await
                .unwrap()
                .is_empty()
        );
    }

    fn npm_host_labels(host: &str) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.npm.0.host".to_string(), host.to_string());
        labels.insert("docktornet.npm.0.port".to_string(), "3000".to_string());
        labels.insert("docktornet.npm.0.scheme".to_string(), "http".to_string());
        labels.insert("docktornet.npm.0.ssl".to_string(), "false".to_string());
        labels
    }

    fn test_clients(
        npm_url: &str,
    ) -> (
        crate::integrations::npm::NpmClient,
        crate::integrations::cloudflare::CloudflareClient,
    ) {
        let npm = crate::integrations::npm::NpmClient::new(crate::config::NpmConfig {
            url: url::Url::parse(npm_url).unwrap(),
            user: "u".into(),
            password: "p".into(),
        });
        let cf = crate::integrations::cloudflare::CloudflareClient::new(
            crate::config::CloudflareConfig {
                api_token: "t".into(),
                zone_id: "z".into(),
            },
        );
        (npm, cf)
    }

    /// Build NPM + Cloudflare clients where Cloudflare targets a mock server so
    /// its DNS calls can be asserted (NPM points at `npm_url`, unused unless the
    /// snapshot carries NPM labels).
    fn test_clients_with_cf(
        npm_url: &str,
        cf_base: &str,
    ) -> (
        crate::integrations::npm::NpmClient,
        crate::integrations::cloudflare::CloudflareClient,
    ) {
        let npm = crate::integrations::npm::NpmClient::new(crate::config::NpmConfig {
            url: url::Url::parse(npm_url).unwrap(),
            user: "u".into(),
            password: "p".into(),
        });
        let cf = crate::integrations::cloudflare::CloudflareClient::with_base_url(
            crate::config::CloudflareConfig {
                api_token: "t".into(),
                zone_id: "z".into(),
            },
            cf_base,
        );
        (npm, cf)
    }

    fn cf_dns_labels(name: &str, target: &str) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.cf.dns.name".to_string(), name.to_string());
        labels.insert("docktornet.cf.dns.target".to_string(), target.to_string());
        labels.insert("docktornet.cf.dns.proxied".to_string(), "false".to_string());
        labels
    }

    #[tokio::test]
    async fn cf_record_deleted_when_service_removed_past_grace() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut cf_server = mockito::Server::new_async().await;
        // The orphaned record must be deleted from Cloudflare.
        let delete = cf_server
            .mock("DELETE", "/zones/z/dns_records/rec-1")
            .with_status(200)
            .with_body(r#"{"success":true}"#)
            .expect(1)
            .create_async()
            .await;

        let (npm, cf) = test_clients_with_cf(&cf_server.url(), &cf_server.url());
        let reconciler = Reconciler::with_clients(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        // Seed a service that owns a Cloudflare DNS record.
        let snap = Snapshot {
            machine_id: MachineId::new("m1"),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: cf_dns_labels("app", "home.example.com"),
            }],
        };
        storage::upsert_snapshot(&pool, &snap).await.unwrap();
        storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
            .await
            .unwrap();

        // The container is decommissioned: an empty snapshot, well past grace.
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: (1 + GATUS_REMOVAL_GRACE_SECS + 1) as u64,
                containers: vec![],
            })
            .await
            .unwrap();

        delete.assert_async().await;
        assert_eq!(
            storage::get_cf_dns_record(&pool, "m1/app").await.unwrap(),
            None,
            "cf record mapping must be cleared after deletion"
        );
        assert!(
            storage::list_pending_cf_removals(&pool)
                .await
                .unwrap()
                .is_empty(),
            "no pending removal should remain once the record is deleted"
        );
    }

    #[tokio::test]
    async fn cf_record_deferred_to_pending_when_service_absent_within_grace() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut cf_server = mockito::Server::new_async().await;
        // A brief restart must NOT delete the record.
        let delete = cf_server
            .mock("DELETE", "/zones/z/dns_records/rec-1")
            .with_status(200)
            .with_body(r#"{"success":true}"#)
            .expect(0)
            .create_async()
            .await;

        let (npm, cf) = test_clients_with_cf(&cf_server.url(), &cf_server.url());
        let reconciler = Reconciler::with_clients(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        let snap = Snapshot {
            machine_id: MachineId::new("m1"),
            forward_host: "10.0.0.1".into(),
            taken_at: 100,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: cf_dns_labels("app", "home.example.com"),
            }],
        };
        storage::upsert_snapshot(&pool, &snap).await.unwrap();
        storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
            .await
            .unwrap();

        // Gone only 30s later — within grace.
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 130,
                containers: vec![],
            })
            .await
            .unwrap();

        delete.assert_async().await;
        assert_eq!(
            storage::list_pending_cf_removals(&pool).await.unwrap(),
            vec![("m1/app".to_string(), "rec-1".to_string(), 100)],
            "the record must be recorded for a later sweep, carrying its last_seen"
        );
    }

    #[tokio::test]
    async fn full_resync_prunes_pending_cf_removal_past_grace() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut cf_server = mockito::Server::new_async().await;
        let delete = cf_server
            .mock("DELETE", "/zones/z/dns_records/rec-1")
            .with_status(200)
            .with_body(r#"{"success":true}"#)
            .expect(1)
            .create_async()
            .await;

        let (npm, cf) = test_clients_with_cf(&cf_server.url(), &cf_server.url());
        let reconciler = Reconciler::with_clients(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        // A pending removal whose last_seen is far in the past (well past grace).
        storage::add_pending_cf_removal(&pool, "m1/app", "rec-1", 1)
            .await
            .unwrap();

        reconciler.full_resync().await.unwrap();

        delete.assert_async().await;
        assert!(
            storage::list_pending_cf_removals(&pool)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn cf_record_mapping_restored_when_service_reappears_within_grace() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut cf_server = mockito::Server::new_async().await;
        // Zone lookup for the returning service's reconcile.
        let _zone = cf_server
            .mock("GET", "/zones/z")
            .with_status(200)
            .with_body(r#"{"success":true,"result":{"id":"z","name":"example.com"}}"#)
            .expect_at_least(1)
            .create_async()
            .await;
        // A returning service must UPDATE its existing DNS record, never create
        // a SECOND one for the same name (which would orphan the original).
        let create = cf_server
            .mock("POST", "/zones/z/dns_records")
            .with_status(200)
            .with_body(
                r#"{"success":true,"result":{"id":"rec-2","name":"app.example.com","content":"home.example.com","proxied":false}}"#,
            )
            .expect(0)
            .create_async()
            .await;
        // The now-live record must never be deleted.
        let delete = cf_server
            .mock("DELETE", "/zones/z/dns_records/rec-1")
            .with_status(200)
            .with_body(r#"{"success":true}"#)
            .expect(0)
            .create_async()
            .await;
        // The restored mapping drives an update of the original record (rec-1).
        let update = cf_server
            .mock("PUT", "/zones/z/dns_records/rec-1")
            .with_status(200)
            .with_body(
                r#"{"success":true,"result":{"id":"rec-1","name":"app.example.com","content":"home.example.com","proxied":false}}"#,
            )
            .expect(1)
            .create_async()
            .await;

        let (npm, cf) = test_clients_with_cf(&cf_server.url(), &cf_server.url());
        let reconciler = Reconciler::with_clients(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        // A pending removal left over from a brief disappearance.  The
        // cf_dns_records mapping is already gone (cascade-deleted), so the
        // pending row is the only durable record of record id rec-1.
        storage::add_pending_cf_removal(&pool, "m1/app", "rec-1", 100)
            .await
            .unwrap();

        // The service comes back in a fresh snapshot.
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 200,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels: cf_dns_labels("app", "home.example.com"),
                }],
            })
            .await
            .unwrap();

        create.assert_async().await;
        delete.assert_async().await;
        update.assert_async().await;
        // The mapping is restored to the original record id and the pending row
        // is cleared.
        assert_eq!(
            storage::get_cf_dns_record(&pool, "m1/app").await.unwrap(),
            Some("rec-1".to_string())
        );
        assert!(
            storage::list_pending_cf_removals(&pool)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn npm_adoption_persists_mapping_even_if_update_fails() {
        use mockito::Matcher;

        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut npm_server = mockito::Server::new_async().await;
        let _token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .expect_at_least(1)
            .create_async()
            .await;
        let _certs = npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .expect_at_least(1)
            .create_async()
            .await;
        // Create conflicts: the host already exists in NPM.
        let _create = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(400)
            .with_body(r#"{"error":{"message":"domain already in use"}}"#)
            .expect(1)
            .create_async()
            .await;
        // Adoption finds the existing host id 61 ...
        let _list = npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(r#"[{"id":61,"domain_names":["a.example.com"]}]"#)
            .expect(1)
            .create_async()
            .await;
        // ... it is fetched and found to differ (port 9999 vs 3000) ...
        let _get = npm_server
            .mock("GET", "/api/nginx/proxy-hosts/61")
            .with_status(200)
            .with_body(
                r#"{"id":61,"domain_names":["a.example.com"],"forward_scheme":"http",
                    "forward_host":"10.0.0.1","forward_port":9999,"certificate_id":0}"#,
            )
            .expect(1)
            .create_async()
            .await;
        // ... but the update to bring it in line fails (NPM overloaded).
        let _update = npm_server
            .mock("PUT", "/api/nginx/proxy-hosts/61")
            .match_body(Matcher::Any)
            .with_status(500)
            .with_body("boom")
            .expect(1)
            .create_async()
            .await;

        let (npm, cf) = test_clients(&npm_server.url());
        let reconciler = Reconciler::with_clients(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: true,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        let result = reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels: npm_host_labels("a.example.com"),
                }],
            })
            .await;

        // The update failure is reported ...
        assert!(result.is_err(), "update failure should be surfaced");
        // ... but the adopted id is persisted so the next reconcile takes the
        // get/update path instead of re-adopting (and re-reloading nginx).
        assert_eq!(
            storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
            Some(61),
            "adopted npm_id must be stored before the update is attempted"
        );
    }

    #[tokio::test]
    async fn npm_fetches_token_and_certs_once_per_snapshot() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut npm_server = mockito::Server::new_async().await;
        // Exactly one token fetch and one certificate list for the whole
        // snapshot, no matter how many NPM hosts it contains.
        let token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .expect(1)
            .create_async()
            .await;
        let certs = npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .expect(1)
            .create_async()
            .await;
        let create = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(r#"{"id":1,"domain_names":["x"]}"#)
            .expect(2)
            .create_async()
            .await;

        let (npm, cf) = test_clients(&npm_server.url());
        let reconciler = Reconciler::with_clients(
            pool,
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                containers: vec![
                    ContainerEntry {
                        name: "app-a".into(),
                        labels: npm_host_labels("a.example.com"),
                    },
                    ContainerEntry {
                        name: "app-b".into(),
                        labels: npm_host_labels("b.example.com"),
                    },
                ],
            })
            .await
            .unwrap();

        token.assert_async().await;
        certs.assert_async().await;
        create.assert_async().await;
    }

    #[tokio::test]
    async fn npm_failure_for_one_service_does_not_block_others() {
        use mockito::Matcher;

        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut npm_server = mockito::Server::new_async().await;
        let _token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .expect_at_least(1)
            .create_async()
            .await;
        let _certs = npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .expect_at_least(1)
            .create_async()
            .await;
        // The first service's host creation fails outright.
        let create_a = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .match_body(Matcher::PartialJson(
                serde_json::json!({ "domain_names": ["a.example.com"] }),
            ))
            .with_status(500)
            .with_body("boom")
            .expect(1)
            .create_async()
            .await;
        // The second service must still be attempted despite the first failing.
        let create_b = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .match_body(Matcher::PartialJson(
                serde_json::json!({ "domain_names": ["b.example.com"] }),
            ))
            .with_status(200)
            .with_body(r#"{"id":2,"domain_names":["b.example.com"]}"#)
            .expect(1)
            .create_async()
            .await;

        let (npm, cf) = test_clients(&npm_server.url());
        let reconciler = Reconciler::with_clients(
            pool,
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        let result = reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                containers: vec![
                    ContainerEntry {
                        name: "app-a".into(),
                        labels: npm_host_labels("a.example.com"),
                    },
                    ContainerEntry {
                        name: "app-b".into(),
                        labels: npm_host_labels("b.example.com"),
                    },
                ],
            })
            .await;

        create_a.assert_async().await;
        // The batch continued to the second service after the first failed.
        create_b.assert_async().await;
        // The failure is still surfaced (for logging), not silently swallowed.
        assert!(result.is_err(), "partial failure should be reported");
    }

    #[tokio::test]
    async fn reconcile_skips_npm_calls_when_snapshot_unchanged() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut npm_server = mockito::Server::new_async().await;
        // Exactly one of each: the first push creates the host; the second
        // (identical) push must be short-circuited and touch NPM not at all.
        let token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .expect(1)
            .create_async()
            .await;
        let certs = npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .expect(1)
            .create_async()
            .await;
        let create = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(r#"{"id":1,"domain_names":["a.example.com"]}"#)
            .expect(1)
            .create_async()
            .await;

        let (npm, cf) = test_clients(&npm_server.url());
        let reconciler = Reconciler::with_clients(
            pool,
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        let snap = |taken_at| Snapshot {
            machine_id: MachineId::new("m1"),
            forward_host: "10.0.0.1".into(),
            taken_at,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels("a.example.com"),
            }],
        };

        // Two identical pushes (only taken_at differs, which is not part of the
        // intent hash).
        reconciler.reconcile_snapshot(&snap(1)).await.unwrap();
        reconciler.reconcile_snapshot(&snap(2)).await.unwrap();

        token.assert_async().await;
        certs.assert_async().await;
        create.assert_async().await;
    }

    #[tokio::test]
    async fn npm_adopt_skips_update_when_already_in_sync() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut npm_server = mockito::Server::new_async().await;
        let _token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .expect_at_least(1)
            .create_async()
            .await;
        let _certs = npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .expect_at_least(1)
            .create_async()
            .await;
        let _create = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(400)
            .with_body(r#"{"error":{"message":"domain already in use"}}"#)
            .expect(1)
            .create_async()
            .await;
        let _list = npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(r#"[{"id":90,"domain_names":["a.example.com"]}]"#)
            .expect(1)
            .create_async()
            .await;
        // The adopted host already matches the payload, so it must NOT be
        // updated — avoiding a needless nginx reload.
        let _get = npm_server
            .mock("GET", "/api/nginx/proxy-hosts/90")
            .with_status(200)
            .with_body(
                r#"{"id":90,"domain_names":["a.example.com"],"forward_scheme":"http",
                    "forward_host":"10.0.0.1","forward_port":3000,"certificate_id":0,
                    "ssl_forced":0,"http2_support":0,"hsts_enabled":0,
                    "hsts_subdomains":0,"block_exploits":1,"allow_websocket_upgrade":1}"#,
            )
            .expect(1)
            .create_async()
            .await;
        let update = npm_server
            .mock("PUT", "/api/nginx/proxy-hosts/90")
            .with_status(200)
            .with_body(r#"{"id":90,"domain_names":["a.example.com"]}"#)
            .expect(0)
            .create_async()
            .await;

        let (npm, cf) = test_clients(&npm_server.url());
        let reconciler = Reconciler::with_clients(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: true,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels: npm_host_labels("a.example.com"),
                }],
            })
            .await
            .unwrap();

        // No PUT was issued, but the mapping was still adopted.
        update.assert_async().await;
        assert_eq!(
            storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
            Some(90)
        );
    }

    #[tokio::test]
    async fn npm_circuit_breaker_stops_calls_after_repeated_failures() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut npm_server = mockito::Server::new_async().await;
        let _token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .expect_at_least(1)
            .create_async()
            .await;
        let _certs = npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .expect_at_least(1)
            .create_async()
            .await;
        // Every create fails.  The breaker (threshold 3) must open after 3
        // failing cycles and stop issuing creates thereafter.
        let create = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(500)
            .with_body("boom")
            .expect(NPM_BREAKER_THRESHOLD as usize)
            .create_async()
            .await;

        let (npm, cf) = test_clients(&npm_server.url());
        let reconciler = Reconciler::with_clients(
            pool,
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        let snap = Snapshot {
            machine_id: MachineId::new("m1"),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels("a.example.com"),
            }],
        };

        // Push more times than the threshold; failing cycles never cache the
        // intent hash, so each push re-runs until the breaker opens.
        for _ in 0..(NPM_BREAKER_THRESHOLD + 3) {
            let _ = reconciler.reconcile_snapshot(&snap).await;
        }

        // create was attempted exactly `threshold` times, then the breaker
        // suppressed further NPM calls.
        create.assert_async().await;
    }

    #[tokio::test]
    async fn npm_host_deleted_when_service_absent_past_grace_period() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut npm_server = mockito::Server::new_async().await;
        let _token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .expect_at_least(1)
            .create_async()
            .await;
        let _certs = npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .create_async()
            .await;
        // The proxy host for the departed service must be deleted exactly once.
        let delete = npm_server
            .mock("DELETE", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body("{}")
            .expect(1)
            .create_async()
            .await;

        let (npm, cf) = test_clients(&npm_server.url());
        let reconciler = Reconciler::with_clients(
            pool.clone(),
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        // Seed a service with a mapped NPM host id.
        let snap = Snapshot {
            machine_id: MachineId::new("m1"),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels("a.example.com"),
            }],
        };
        storage::upsert_snapshot(&pool, &snap).await.unwrap();
        storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

        // Service leaves the snapshot, well past the grace period.
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: (1 + GATUS_REMOVAL_GRACE_SECS + 1) as u64,
                containers: vec![],
            })
            .await
            .unwrap();

        delete.assert_async().await;
        // Pruned outright, so nothing is left pending.
        assert!(
            storage::list_pending_npm_removals(&pool)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
