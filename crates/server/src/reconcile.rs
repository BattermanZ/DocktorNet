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
    /// Per-machine tail of the apply chain: the receiver that resolves when the
    /// most recently ingested snapshot's background apply finishes.  Ingest
    /// hands each new apply the previous tail to wait on, so a machine's applies
    /// run strictly in ingest order and a stale snapshot can never be applied to
    /// public DNS / proxy state after a newer one.
    apply_chain: Arc<Mutex<HashMap<String, tokio::sync::oneshot::Receiver<()>>>>,
}

/// Everything a background apply needs, handed back by [`Reconciler::ingest_snapshot`].
/// It carries the ingest-order sequencing token so applies for a machine are
/// serialized in the order they were persisted, never reordered by the scheduler.
pub struct ApplyTicket {
    removed: Vec<RemovedService>,
    /// Resolves when the previous snapshot's apply for this machine finished.
    /// `None` for the first apply after startup.  The apply awaits this before
    /// touching any external state.
    wait_on: Option<tokio::sync::oneshot::Receiver<()>>,
    /// Signals the next queued apply for this machine that this one is done.
    done: tokio::sync::oneshot::Sender<()>,
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
    #[error(
        "NPM host {service_id} (index {index}) requested ssl=true but no certificate resolved for {hosts:?}; refusing to publish over plain HTTP"
    )]
    SslWithoutCertificate {
        service_id: String,
        index: u8,
        hosts: Vec<String>,
    },
    #[error(
        "NPM host {service_id} (index {index}) matched an existing host serving {existing:?} but labels only manage {wanted:?}; refusing to adopt and drop the unmanaged domains"
    )]
    AdoptWouldDropDomains {
        service_id: String,
        index: u8,
        existing: Vec<String>,
        wanted: Vec<String>,
    },
}

/// Gatus files for containers that temporarily disappear from a snapshot (e.g.
/// during a restart) are kept for this many seconds before being deleted.  This
/// prevents Gatus from resetting alert state and skipping recovery notifications.
/// A file kept through the grace window is a pending removal, deleted by
/// `sweep_pending_removals` (every minute) once the window ends.
const GATUS_REMOVAL_GRACE_SECS: i64 = 300;

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(i64::MAX)
}

/// Whether a pending removal last seen at `last_seen` has outlived the grace period.
fn past_grace(last_seen: i64, now: i64) -> bool {
    now.saturating_sub(last_seen) >= GATUS_REMOVAL_GRACE_SECS
}

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
            apply_chain: Arc::new(Mutex::new(HashMap::new())),
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
            apply_chain: Arc::new(Mutex::new(HashMap::new())),
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

        // ssl=true but no certificate resolved would silently publish the
        // hostname over plain HTTP (ssl_forced/hsts/http2 all disabled). Fail
        // this host loudly instead of inverting the operator's TLS intent.
        if host.ssl && cert_id.unwrap_or_default() <= 0 {
            tracing::error!(
                service_id,
                index = host.index,
                hosts = ?host.hosts,
                "NPM host requested ssl=true but no certificate resolved; refusing to publish over plain HTTP"
            );
            return Err(ReconcileError::SslWithoutCertificate {
                service_id: service_id.to_string(),
                index: host.index,
                hosts: host.hosts.clone(),
            });
        }

        let payload = crate::integrations::npm::proxy_payload(host, forward_host, cert_id);

        // Resolve the NPM host id: an existing mapping, a fresh create, or an
        // adoption of a host that already exists in NPM.
        let npm_id = match crate::storage::get_npm_host(&self.db, service_id, host.index.into())
            .await?
        {
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

                    // The match only requires a single overlapping domain,
                    // so the existing host may carry others we do not
                    // manage. Overwriting it with our payload would silently
                    // drop those domains, so refuse to adopt (and clobber) a
                    // host that serves more than our labels intend.
                    let unmanaged: Vec<String> = existing
                        .domain_names
                        .iter()
                        .filter(|name| {
                            !host
                                .hosts
                                .iter()
                                .any(|wanted| wanted.eq_ignore_ascii_case(name))
                        })
                        .cloned()
                        .collect();
                    if !unmanaged.is_empty() {
                        tracing::error!(
                            service_id,
                            index = host.index,
                            npm_id = existing.id,
                            unmanaged = ?unmanaged,
                            "refusing to adopt NPM host: it serves domains we do not manage; adopting would drop them"
                        );
                        return Err(ReconcileError::AdoptWouldDropDomains {
                            service_id: service_id.to_string(),
                            index: host.index,
                            existing: existing.domain_names.clone(),
                            wanted: host.hosts.clone(),
                        });
                    }

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
            // The host was deleted out-of-band in NPM, so the stored id is dead.
            // PUTting to it would 404; recreate the host and remap to the new id.
            None => {
                tracing::info!(service_id, npm_id, "npm host missing, recreating");
                let created = client
                    .create_proxy_host(token, &payload)
                    .await
                    .map_err(|err| ReconcileError::External(err.into()))?;
                crate::storage::set_npm_host(&self.db, service_id, host.index.into(), created.id)
                    .await?;
            }
            Some(found) if found.matches_payload(&payload) => {
                tracing::debug!(service_id, npm_id, "npm host already in sync");
            }
            Some(_) => {
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
            // Only PUT when the record has actually drifted.  The intent hash is
            // per-machine, so any unrelated label change re-runs this loop for
            // every cf_dns service; an unconditional update would issue a
            // redundant public-API write each time.  Mirrors resync_cloudflare.
            let existing = client
                .get_record(&id)
                .await
                .map_err(|err| ReconcileError::External(err.into()))?;
            match existing {
                // The record was deleted out-of-band, so the stored id is dead;
                // recreate it and remap to the new id rather than PUT to a 404.
                None => {
                    tracing::info!(service_id, "cf record missing, recreating");
                    let created = client
                        .create_record(&payload)
                        .await
                        .map_err(|err| ReconcileError::External(err.into()))?;
                    crate::storage::set_cf_dns_record(&self.db, service_id, &created.id).await?;
                }
                Some(found) if !cf_record_matches(&found, &payload) => {
                    tracing::info!(service_id, record_id = %id, "cf record drifted, updating");
                    client
                        .update_record(&id, &payload)
                        .await
                        .map_err(|err| ReconcileError::External(err.into()))?;
                }
                Some(_) => {
                    tracing::debug!(service_id, record_id = %id, "cf record already in sync");
                }
            }
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
            }
        }

        if !self.options.dry_run {
            let expected = self.expected_gatus_files().await?;
            self.sweep_removals(&expected, npm_ctx.as_ref(), &mut report)
                .await;
        }

        tracing::info!(
            verified = report.verified,
            repaired = report.repaired,
            failed = report.failed,
            "full resync complete"
        );
        Ok(report)
    }

    /// Delete what services that left their snapshot still own once their grace
    /// period is over: Gatus files kept through it, then pending NPM and
    /// Cloudflare removals.  The server runs this on a short timer so a
    /// removal lands soon after its grace period ends, rather than on the next
    /// `full_resync`.  Unlike `full_resync`, it never re-verifies live services.
    pub async fn sweep_pending_removals(&self) -> Result<ResyncReport, ReconcileError> {
        let _guard = self.lock.lock().await;
        let mut report = ResyncReport::default();
        if self.options.dry_run {
            return Ok(report);
        }
        let expected = self.expected_gatus_files().await?;

        // Only reach NPM when a pending removal is due, and never while the
        // breaker is open.  Most sweeps have nothing to do.
        let now = unix_now();
        let npm_due = self.npm.is_some()
            && crate::storage::list_pending_npm_removals(&self.db)
                .await?
                .iter()
                .any(|&(_, _, _, last_seen)| past_grace(last_seen, now));
        let npm_ctx = if npm_due {
            if self.npm_breaker.is_open(std::time::Instant::now()) {
                tracing::warn!("npm circuit breaker open; deferring pending npm removals");
                None
            } else {
                match self.fetch_npm_context().await {
                    Ok(ctx) => Some(ctx),
                    Err(err) => {
                        tracing::warn!(error = %err, "pending npm removals skipped: token/cert fetch failed");
                        self.npm_breaker.record_failure(std::time::Instant::now());
                        None
                    }
                }
            }
        } else {
            None
        };

        let npm_failed = self
            .sweep_removals(&expected, npm_ctx.as_ref(), &mut report)
            .await;
        if npm_ctx.is_some() {
            if npm_failed {
                self.npm_breaker.record_failure(std::time::Instant::now());
            } else {
                self.npm_breaker.record_success();
            }
        }

        if report != ResyncReport::default() {
            tracing::info!(
                removed = report.repaired,
                failed = report.failed,
                "pending removal sweep complete"
            );
        }
        Ok(report)
    }

    /// The removal sweeps shared by `full_resync` and `sweep_pending_removals`.
    /// Returns whether an NPM delete failed, for the caller's breaker.
    async fn sweep_removals(
        &self,
        expected_gatus_files: &std::collections::HashSet<PathBuf>,
        npm_ctx: Option<&(String, Vec<crate::integrations::npm::Certificate>)>,
        report: &mut ResyncReport,
    ) -> bool {
        // Gatus files no longer backed by a DB service, kept by apply_inner
        // through the grace period.
        self.sweep_orphan_gatus_files(expected_gatus_files).await;
        let npm_failed = self.sweep_pending_npm_removals(npm_ctx, report).await;
        self.sweep_pending_cf_removals(report).await;
        npm_failed
    }

    /// The Gatus file paths of every stored service that has Gatus labels.  A
    /// managed file outside this set is an orphan.
    async fn expected_gatus_files(
        &self,
    ) -> Result<std::collections::HashSet<PathBuf>, ReconcileError> {
        let mut expected = std::collections::HashSet::new();
        for (service_id, labels_json, _) in crate::storage::list_all_services(&self.db).await? {
            // A row with malformed labels has no intent to keep, as in full_resync.
            let Ok(raw) =
                serde_json::from_str::<std::collections::BTreeMap<String, String>>(&labels_json)
            else {
                continue;
            };
            if !docktornet_core::labels::parse(&raw).gatus.is_empty() {
                expected.insert(crate::integrations::gatus::service_file_path(
                    &self.options.gatus_config_dir,
                    &service_id,
                ));
            }
        }
        Ok(expected)
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
        // Respect the removal grace period.  A container that briefly leaves a
        // snapshot (e.g. a restart) has its service row cascade-deleted, so it
        // is no longer "expected", yet apply_inner deliberately keeps its file
        // and records when it left.  Deleting a still-in-grace file here would
        // reset alert state — exactly what the grace period exists to prevent.
        let pending = match crate::storage::list_pending_gatus_removals(&self.db).await {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(error = %err, "could not list pending gatus removals; skipping orphan sweep");
                return;
            }
        };
        let mut left_at: HashMap<PathBuf, (String, i64)> = pending
            .into_iter()
            .map(|(service_id, left_at)| {
                (
                    crate::integrations::gatus::service_file_path(
                        &self.options.gatus_config_dir,
                        &service_id,
                    ),
                    (service_id, left_at),
                )
            })
            .collect();
        let now = unix_now();
        let mut removed_any = false;
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
                    let pending = left_at.remove(&path);
                    let within_grace = match &pending {
                        Some((_, left_at)) => !past_grace(*left_at, now),
                        // No record of the departure (a file from before
                        // pending_gatus_removals existed): fall back to the
                        // mtime, the last time the file's monitors changed.
                        None => match entry.metadata().await {
                            Ok(meta) => meta
                                .modified()
                                .ok()
                                .and_then(|m| m.elapsed().ok())
                                .is_some_and(|age| age.as_secs() < GATUS_REMOVAL_GRACE_SECS as u64),
                            Err(_) => false,
                        },
                    };
                    if within_grace {
                        tracing::debug!(path = %path.display(), "orphan gatus file still within grace period, keeping");
                        continue;
                    }
                    tracing::info!(path = %path.display(), "removing orphan gatus file");
                    let gone = match tokio::fs::remove_file(&path).await {
                        Ok(()) => {
                            removed_any = true;
                            true
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => true,
                        Err(err) => {
                            tracing::warn!(path = %path.display(), error = %err, "failed to remove orphan gatus file");
                            false
                        }
                    };
                    if gone && let Some((service_id, _)) = pending {
                        self.clear_pending_gatus_removal(&service_id).await;
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    tracing::warn!(error = %err, "error reading gatus config dir entry");
                    break;
                }
            }
        }
        if removed_any && let Err(err) = self.touch_gatus_reload_trigger().await {
            tracing::warn!(error = %err, "gatus reload trigger failed");
        }
        // Records whose file is already gone, or which belong to a live
        // service again, have nothing left to wait for once grace has passed.
        for (_, (service_id, left_at)) in left_at {
            if past_grace(left_at, now) {
                self.clear_pending_gatus_removal(&service_id).await;
            }
        }
    }

    async fn clear_pending_gatus_removal(&self, service_id: &str) {
        if let Err(err) = crate::storage::delete_pending_gatus_removal(&self.db, service_id).await {
            tracing::warn!(service_id, error = %err, "failed to clear pending gatus removal");
        }
    }

    /// Make Gatus reload after this pass deleted one or more service files.
    async fn touch_gatus_reload_trigger(&self) -> anyhow::Result<()> {
        crate::integrations::gatus::touch_reload_trigger(&self.options.gatus_config_dir).await?;
        tracing::info!("gatus service files deleted; reload trigger touched");
        Ok(())
    }

    /// Prune NPM proxy hosts recorded in `pending_npm_removals` once they are
    /// past the grace period.  This is the backstop for removals deferred by
    /// `apply_inner` (within-grace, breaker open, or a failed delete) and for
    /// machines that went offline entirely without a final empty snapshot.
    /// Returns whether an NPM delete failed.
    async fn sweep_pending_npm_removals(
        &self,
        npm_ctx: Option<&(String, Vec<crate::integrations::npm::Certificate>)>,
        report: &mut ResyncReport,
    ) -> bool {
        let pending = match crate::storage::list_pending_npm_removals(&self.db).await {
            Ok(p) => p,
            Err(err) => {
                tracing::warn!(error = %err, "could not list pending npm removals");
                return false;
            }
        };
        if pending.is_empty() {
            return false;
        }
        let Some((token, _)) = npm_ctx else {
            // No NPM access this pass; leave the entries for the next sweep.
            return false;
        };
        let client = self.npm.as_ref().expect("npm_ctx implies npm client");
        let now = unix_now();
        let mut npm_failed = false;

        for (service_id, npm_index, npm_id, last_seen) in pending {
            if !past_grace(last_seen, now) {
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
                    npm_failed = true;
                }
            }
        }
        npm_failed
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
        let now = unix_now();

        for (service_id, record_id, last_seen) in pending {
            if !past_grace(last_seen, now) {
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

        // Same guard as reconcile_npm: never silently republish an ssl=true host
        // over plain HTTP because its certificate no longer resolves.
        if host.ssl && desired_cert.unwrap_or_default() <= 0 {
            tracing::error!(
                service_id,
                index = host.index,
                hosts = ?host.hosts,
                "NPM host requested ssl=true but no certificate resolved; refusing to publish over plain HTTP"
            );
            return Err(ReconcileError::SslWithoutCertificate {
                service_id: service_id.to_string(),
                index: host.index,
                hosts: host.hosts.clone(),
            });
        }

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
        self.record_gatus_departures(snap, &current_ids, &removed_last_seen)
            .await?;
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

    /// Keep `pending_gatus_removals` in step with the snapshot: record when each
    /// service that just left within the grace period did so, and forget the
    /// departure of any service that is back.  This runs at persist time, before
    /// `upsert_snapshot` deletes the service rows, so no sweep can ever see a
    /// kept Gatus file without its departure; the apply runs later, off the lock.
    async fn record_gatus_departures(
        &self,
        snap: &Snapshot,
        current_ids: &[String],
        removed_last_seen: &[(String, i64)],
    ) -> Result<(), ReconcileError> {
        let now = unix_now();
        for (service_id, last_seen) in removed_last_seen {
            let absent_secs = (snap.taken_at as i64).saturating_sub(*last_seen).max(0);
            if absent_secs < GATUS_REMOVAL_GRACE_SECS {
                // In server time, backdated by how long the agent saw it gone.
                crate::storage::add_pending_gatus_removal(
                    &self.db,
                    service_id,
                    now.saturating_sub(absent_secs),
                )
                .await?;
            }
        }
        let returned: Vec<String> = crate::storage::list_pending_gatus_removals(&self.db)
            .await?
            .into_iter()
            .map(|(service_id, _)| service_id)
            .filter(|service_id| current_ids.contains(service_id))
            .collect();
        for service_id in returned {
            crate::storage::delete_pending_gatus_removal(&self.db, &service_id).await?;
        }
        Ok(())
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
        // The cached hash describes the machine before these removals.  Drop it
        // now, so that if this apply fails, a later snapshot bringing the
        // services back is not skipped as unchanged with their pending
        // removals still queued for the sweep.
        if !removed.is_empty() {
            self.last_reconciled.lock().await.remove(&machine);
        }

        let mut failures = 0usize;
        let mut gatus_removed = false;

        // Write gatus files for all current services before removing any stale
        // ones.  This ensures Gatus never reloads with an active service missing
        // from the config directory.
        for service in &intended {
            match self.apply_gatus(service).await {
                Ok(removed) => gatus_removed |= removed,
                Err(err) => {
                    tracing::warn!(service_id = %service.service_id, error = %err, "gatus write failed");
                    failures += 1;
                }
            }
        }

        // Remove gatus files for services that have left this machine's
        // snapshot, but only after a grace period.  Containers that are
        // restarting briefly disappear from the snapshot; deleting their
        // config file causes Gatus to reset alert state so recovery
        // notifications are never sent.  A file kept through the grace period
        // is a pending removal: sweep_pending_removals deletes it once the
        // grace period that persist_inner recorded ends.
        let snap_time = snap.taken_at as i64;
        for rs in &removed {
            let absent_secs = snap_time.saturating_sub(rs.last_seen);
            if absent_secs >= GATUS_REMOVAL_GRACE_SECS {
                tracing::info!(service_id = %rs.service_id, absent_secs, "service removed");
                match crate::integrations::gatus::remove_service_file(
                    &self.options.gatus_config_dir,
                    &rs.service_id,
                )
                .await
                {
                    Ok(removed) => gatus_removed |= removed,
                    Err(err) => {
                        tracing::warn!(service_id = %rs.service_id, error = %err, "gatus removal failed");
                        failures += 1;
                    }
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
        if gatus_removed && let Err(err) = self.touch_gatus_reload_trigger().await {
            tracing::warn!(error = %err, "gatus reload trigger failed");
            failures += 1;
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
        // in pending_npm_removals so the pending-removal sweep retries it.  The
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
        // failed API call — is recorded in pending_cf_removals so the
        // pending-removal sweep retries it.  The cf_dns_records mapping has
        // already been cascade-deleted, so that table is the only durable
        // record of what to delete.
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
    /// Returns whether it deleted the file.
    async fn apply_gatus(
        &self,
        service: &crate::integrations::intended::IntendedService,
    ) -> Result<bool, ReconcileError> {
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
        Ok(false)
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
    pub async fn ingest_snapshot(&self, snap: &Snapshot) -> Result<ApplyTicket, ReconcileError> {
        let _guard = self.lock.lock().await;
        let removed = self.persist_inner(snap).await?;
        // Link this apply into the machine's ordered chain: it waits on the
        // previous tail and installs its own completion channel as the new
        // tail.  `insert` returns the prior tail (the receiver this apply must
        // wait for), or `None` if this is the first snapshot for the machine.
        let (done, done_rx) = tokio::sync::oneshot::channel();
        let wait_on = self
            .apply_chain
            .lock()
            .await
            .insert(snap.machine_id.as_str().to_string(), done_rx);
        Ok(ApplyTicket {
            removed,
            wait_on,
            done,
        })
    }

    /// Apply the external side effects for an already-persisted snapshot.  Runs
    /// after `ingest_snapshot`, off the request path.  Applies for a single
    /// machine are serialized in ingest order via the ticket's chain, so a
    /// snapshot can never be applied to public DNS / proxy state after a newer
    /// one has already been applied.
    pub async fn apply_reconcile(
        &self,
        snap: &Snapshot,
        ticket: ApplyTicket,
    ) -> Result<(), ReconcileError> {
        let ApplyTicket {
            removed,
            wait_on,
            done,
        } = ticket;
        // Block until the previous snapshot's apply for this machine has
        // finished.  A dropped sender (predecessor task cancelled/panicked)
        // resolves the receiver with an error, which we treat as "go ahead".
        if let Some(rx) = wait_on {
            let _ = rx.await;
        }
        let result = if self.options.dry_run {
            Ok(())
        } else {
            let _guard = self.lock.lock().await;
            self.apply_inner(snap, removed).await
        };
        // Release the next queued apply for this machine, whatever the outcome.
        let _ = done.send(());
        result
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
    async fn out_of_order_background_applies_do_not_resurrect_removed_service() {
        // Two pushes arrive back-to-back: A with the container present, then B
        // with it gone (past the grace period).  The ingest path persists each
        // synchronously and fires a background apply.  The scheduler may run
        // B's apply before A's; without per-machine sequencing the stale apply
        // A re-creates the just-removed service's public state.  The gatus file
        // stands in here for the NPM proxy host / Cloudflare record: the final
        // state must reflect B (file gone), never the resurrected A.
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

        let snap_present = Snapshot {
            machine_id: MachineId::new("m1"),
            forward_host: "127.0.0.1".into(),
            taken_at: 1,
            containers: vec![ContainerEntry {
                name: "app".to_string(),
                labels,
            }],
        };
        // B is taken well past the grace period, so its apply must delete the
        // service's gatus file rather than defer it.
        let snap_gone = Snapshot {
            machine_id: MachineId::new("m1"),
            forward_host: "127.0.0.1".into(),
            taken_at: (1 + GATUS_REMOVAL_GRACE_SECS + 1) as u64,
            containers: vec![],
        };

        // Persist both synchronously in order, exactly as the ingest handler does.
        let ticket_present = reconciler.ingest_snapshot(&snap_present).await.unwrap();
        let ticket_gone = reconciler.ingest_snapshot(&snap_gone).await.unwrap();

        // Simulate the scheduler favouring B's apply: spawn it first and give
        // it a generous head start.  Per-machine ordering must hold it until
        // A's apply has completed.
        let reconciler_b = reconciler.clone();
        let snap_gone_bg = snap_gone.clone();
        let handle_b = tokio::spawn(async move {
            reconciler_b
                .apply_reconcile(&snap_gone_bg, ticket_gone)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        reconciler
            .apply_reconcile(&snap_present, ticket_present)
            .await
            .unwrap();
        handle_b.await.unwrap().unwrap();

        let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
        assert!(
            !service_file.exists(),
            "a stale background apply resurrected a removed service's public state"
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

        // Let the grace period recorded for its departure run out.
        end_grace(&reconciler, "m1/app").await;

        // full_resync should sweep the orphan file (service is no longer in DB).
        let report = reconciler.full_resync().await.unwrap();
        assert_eq!(report.failed, 0);
        assert!(
            !service_file.exists(),
            "full_resync must remove the orphan gatus file"
        );
    }

    #[tokio::test]
    async fn full_resync_keeps_orphan_gatus_file_within_grace() {
        // A container that briefly leaves the snapshot (e.g. a restart) has its
        // service row cascade-deleted from the DB, so it is no longer "expected"
        // by the orphan sweep — yet apply_inner deliberately keeps its gatus file
        // during the grace period to preserve Gatus alert state.  A full_resync
        // that coincides with the grace window must NOT delete the file.
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

        // Remove the service within the grace period so apply_inner keeps the
        // file (its mtime stays recent, standing in for last-seen).
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

        // A coinciding full_resync must respect the grace period and leave the
        // still-in-grace file in place.
        let report = reconciler.full_resync().await.unwrap();
        assert_eq!(report.failed, 0);
        assert!(
            service_file.exists(),
            "full_resync must not delete a gatus file that is still within the grace period"
        );
    }

    /// Push a snapshot of machine `m1` holding `containers` at `taken_at`.
    async fn push(reconciler: &Reconciler, taken_at: u64, containers: Vec<ContainerEntry>) {
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at,
                containers,
            })
            .await
            .unwrap();
    }

    fn gatus_labels() -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "https://x".into());
        labels.insert("docktornet.gatus.0.interval".into(), "30s".into());
        labels
    }

    fn set_mtime(path: &std::path::Path, mtime: std::time::SystemTime) {
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(mtime)
            .unwrap();
    }

    /// Backdate a departure that apply_inner recorded so its grace period is over.
    async fn end_grace(reconciler: &Reconciler, service_id: &str) {
        storage::add_pending_gatus_removal(
            reconciler.pool(),
            service_id,
            unix_now() - GATUS_REMOVAL_GRACE_SECS - 60,
        )
        .await
        .unwrap();
    }

    fn aged_past_grace() -> std::time::SystemTime {
        std::time::SystemTime::now()
            - std::time::Duration::from_secs((GATUS_REMOVAL_GRACE_SECS + 60) as u64)
    }

    #[tokio::test]
    async fn pending_removal_sweep_removes_gatus_file_without_full_resync() {
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
        let app = || ContainerEntry {
            name: "app".into(),
            labels: gatus_labels(),
        };

        // Present at t=1, gone at t=31 (within grace), and still gone after it.
        push(&reconciler, 1, vec![app()]).await;
        let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
        assert!(service_file.exists());
        push(&reconciler, 31, vec![]).await;
        assert!(service_file.exists(), "file should survive grace period");
        push(
            &reconciler,
            (31 + GATUS_REMOVAL_GRACE_SECS + 30) as u64,
            vec![],
        )
        .await;
        assert!(
            service_file.exists(),
            "a later snapshot no longer names the service, so it cannot remove the file"
        );

        // Grace has passed since the service left.
        end_grace(&reconciler, "m1/app").await;
        reconciler.sweep_pending_removals().await.unwrap();

        assert!(
            !service_file.exists(),
            "the short sweep must remove the gatus file once grace has passed"
        );
    }

    /// Create the reload trigger with an old mtime and return that mtime.
    fn age_reload_trigger(gatus_dir: &std::path::Path) -> std::time::SystemTime {
        let trigger = gatus::reload_trigger_path(gatus_dir);
        std::fs::create_dir_all(gatus_dir).unwrap();
        std::fs::write(&trigger, "{}\n").unwrap();
        let old = aged_past_grace();
        set_mtime(&trigger, old);
        old
    }

    fn reload_trigger_mtime(gatus_dir: &std::path::Path) -> std::time::SystemTime {
        std::fs::metadata(gatus::reload_trigger_path(gatus_dir))
            .unwrap()
            .modified()
            .unwrap()
    }

    fn gatus_reconciler(pool: Db, gatus_dir: &std::path::Path) -> Reconciler {
        Reconciler::new(
            pool,
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: gatus_dir.to_path_buf(),
            },
        )
    }

    #[tokio::test]
    async fn pending_removal_sweep_keeps_long_unchanged_gatus_file_within_grace() {
        // A stable service's file is only rewritten when its monitors change,
        // so its mtime can be far older than the grace period.  A restart that
        // drops it from one snapshot must not cost it its monitor.
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = gatus_reconciler(pool, &gatus_dir);
        let app = || ContainerEntry {
            name: "app".into(),
            labels: gatus_labels(),
        };

        push(&reconciler, 1, vec![app()]).await;
        let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
        set_mtime(&service_file, aged_past_grace());
        push(&reconciler, 31, vec![]).await;

        reconciler.sweep_pending_removals().await.unwrap();
        reconciler.full_resync().await.unwrap();

        assert!(
            service_file.exists(),
            "a service absent for less than the grace period keeps its gatus file"
        );
        assert!(!gatus::reload_trigger_path(&gatus_dir).exists());
    }

    #[tokio::test]
    async fn sweep_between_ingest_and_apply_keeps_gatus_file_within_grace() {
        // The ingest handler persists a snapshot, then applies it later in a
        // background task.  A sweep that runs in between must already know the
        // service only just left.
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = gatus_reconciler(pool, &gatus_dir);
        push(
            &reconciler,
            1,
            vec![ContainerEntry {
                name: "app".into(),
                labels: gatus_labels(),
            }],
        )
        .await;
        let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
        set_mtime(&service_file, aged_past_grace());

        let gone = Snapshot {
            machine_id: MachineId::new("m1"),
            forward_host: "10.0.0.1".into(),
            taken_at: 31,
            containers: vec![],
        };
        let ticket = reconciler.ingest_snapshot(&gone).await.unwrap();
        reconciler.sweep_pending_removals().await.unwrap();
        reconciler.apply_reconcile(&gone, ticket).await.unwrap();

        assert!(
            service_file.exists(),
            "a sweep between ingest and apply must respect the grace period"
        );
    }

    #[tokio::test]
    async fn service_returning_after_a_failed_removal_apply_is_reconciled() {
        // The apply that records a removal fails (NPM is down).  When the
        // service comes back with the same labels it had, that snapshot must
        // not be skipped as unchanged, or its pending NPM removal survives and
        // the sweep deletes a live proxy host.
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut npm_server = mockito::Server::new_async().await;
        let token_ok = npm_server
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
        let _get = npm_server
            .mock("GET", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(
                r#"{"id":50,"domain_names":["a.example.com"],"forward_scheme":"http","forward_host":"10.0.0.1","forward_port":3000,"certificate_id":0,"ssl_forced":false,"http2_support":false,"hsts_enabled":false,"block_exploits":true,"allow_websocket_upgrade":true,"enabled":true}"#,
            )
            .create_async()
            .await;
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
        let app = || ContainerEntry {
            name: "app".into(),
            labels: npm_host_labels("a.example.com"),
        };

        // Present and reconciled, so its intent hash is cached.
        storage::upsert_snapshot(
            &pool,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                containers: vec![app()],
            },
        )
        .await
        .unwrap();
        storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();
        push(&reconciler, 2, vec![app()]).await;

        // Gone within grace while NPM fails: the apply errors.
        token_ok.remove_async().await;
        let token_down = npm_server
            .mock("POST", "/api/tokens")
            .with_status(500)
            .create_async()
            .await;
        let result = reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 32,
                containers: vec![],
            })
            .await;
        assert!(result.is_err());
        assert_eq!(
            storage::list_pending_npm_removals(&pool)
                .await
                .unwrap()
                .len(),
            1
        );

        // Back with the same labels, NPM healthy again.
        token_down.remove_async().await;
        let _token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .create_async()
            .await;
        push(&reconciler, 62, vec![app()]).await;

        assert!(
            storage::list_pending_npm_removals(&pool)
                .await
                .unwrap()
                .is_empty(),
            "the returning service's pending removal must be cleared"
        );
        assert_eq!(
            storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
            Some(50)
        );
        reconciler.sweep_pending_removals().await.unwrap();
        delete.assert_async().await;
    }

    #[tokio::test]
    async fn removal_past_grace_touches_gatus_reload_trigger() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = gatus_reconciler(pool, &gatus_dir);
        let app = ContainerEntry {
            name: "app".into(),
            labels: gatus_labels(),
        };

        push(&reconciler, 1, vec![app]).await;
        let before = age_reload_trigger(&gatus_dir);
        push(
            &reconciler,
            (1 + GATUS_REMOVAL_GRACE_SECS + 1) as u64,
            vec![],
        )
        .await;

        assert!(!gatus::service_file_path(&gatus_dir, "m1/app").exists());
        assert!(reload_trigger_mtime(&gatus_dir) > before);
    }

    #[tokio::test]
    async fn orphan_sweep_touches_gatus_reload_trigger() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = gatus_reconciler(pool, &gatus_dir);
        let app = ContainerEntry {
            name: "app".into(),
            labels: gatus_labels(),
        };

        push(&reconciler, 1, vec![app]).await;
        push(&reconciler, 31, vec![]).await;
        let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
        end_grace(&reconciler, "m1/app").await;
        let before = age_reload_trigger(&gatus_dir);

        reconciler.sweep_pending_removals().await.unwrap();

        assert!(!service_file.exists());
        assert!(reload_trigger_mtime(&gatus_dir) > before);
        assert!(
            gatus::reload_trigger_path(&gatus_dir).exists(),
            "the orphan sweep must never delete the trigger"
        );
    }

    #[tokio::test]
    async fn dropping_gatus_labels_touches_gatus_reload_trigger() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = gatus_reconciler(pool, &gatus_dir);

        let mut labels = gatus_labels();
        labels.extend(cf_dns_labels("app", "home.example.com"));
        push(
            &reconciler,
            1,
            vec![ContainerEntry {
                name: "app".into(),
                labels,
            }],
        )
        .await;
        let before = age_reload_trigger(&gatus_dir);

        // Still running, but its Gatus labels are gone.
        push(
            &reconciler,
            31,
            vec![ContainerEntry {
                name: "app".into(),
                labels: cf_dns_labels("app", "home.example.com"),
            }],
        )
        .await;

        assert!(!gatus::service_file_path(&gatus_dir, "m1/app").exists());
        assert!(reload_trigger_mtime(&gatus_dir) > before);
    }

    #[tokio::test]
    async fn pass_without_gatus_deletion_leaves_reload_trigger_alone() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = gatus_reconciler(pool, &gatus_dir);
        let app = || ContainerEntry {
            name: "app".into(),
            labels: gatus_labels(),
        };

        push(&reconciler, 1, vec![app()]).await;
        // A restart within grace keeps the file, so Gatus has nothing to reload.
        push(&reconciler, 31, vec![]).await;
        assert_eq!(
            storage::list_pending_gatus_removals(reconciler.pool())
                .await
                .unwrap()
                .len(),
            1
        );
        push(&reconciler, 61, vec![app()]).await;
        reconciler.sweep_pending_removals().await.unwrap();

        assert!(!gatus::reload_trigger_path(&gatus_dir).exists());
        assert!(
            storage::list_pending_gatus_removals(reconciler.pool())
                .await
                .unwrap()
                .is_empty(),
            "a returning service's departure is forgotten"
        );
    }

    #[tokio::test]
    async fn dry_run_pending_removal_sweep_writes_nothing() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = gatus_reconciler(pool.clone(), &gatus_dir);
        push(
            &reconciler,
            1,
            vec![ContainerEntry {
                name: "app".into(),
                labels: gatus_labels(),
            }],
        )
        .await;
        push(&reconciler, 31, vec![]).await;
        let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
        end_grace(&reconciler, "m1/app").await;

        let dryrun = Reconciler::new(
            pool,
            ReconcileOptions {
                dry_run: true,
                adopt_existing: false,
                gatus_config_dir: gatus_dir.clone(),
            },
        );
        dryrun.sweep_pending_removals().await.unwrap();

        assert!(service_file.exists());
        assert!(!gatus::reload_trigger_path(&gatus_dir).exists());
    }

    #[tokio::test]
    async fn pending_removal_sweep_deletes_cf_record_without_full_resync() {
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
        let app = || ContainerEntry {
            name: "app".into(),
            labels: cf_dns_labels("app", "home.example.com"),
        };

        // Present at t=1, owning a Cloudflare record.
        storage::upsert_snapshot(
            &pool,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                containers: vec![app()],
            },
        )
        .await
        .unwrap();
        storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
            .await
            .unwrap();
        push(&reconciler, 31, vec![]).await;
        push(
            &reconciler,
            (31 + GATUS_REMOVAL_GRACE_SECS + 30) as u64,
            vec![],
        )
        .await;
        assert_eq!(
            storage::list_pending_cf_removals(&pool).await.unwrap(),
            vec![("m1/app".to_string(), "rec-1".to_string(), 1)],
            "no later snapshot names the service, so the record waits in pending"
        );

        reconciler.sweep_pending_removals().await.unwrap();

        delete.assert_async().await;
        assert!(
            storage::list_pending_cf_removals(&pool)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn pending_removal_sweep_prunes_npm_host_without_full_resync() {
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
        let app = || ContainerEntry {
            name: "app".into(),
            labels: npm_host_labels("a.example.com"),
        };

        // Present at t=1, owning an NPM proxy host.
        storage::upsert_snapshot(
            &pool,
            &Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                containers: vec![app()],
            },
        )
        .await
        .unwrap();
        storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();
        push(&reconciler, 31, vec![]).await;
        push(
            &reconciler,
            (31 + GATUS_REMOVAL_GRACE_SECS + 30) as u64,
            vec![],
        )
        .await;
        assert_eq!(
            storage::list_pending_npm_removals(&pool).await.unwrap(),
            vec![("m1/app".to_string(), 0, 50, 1)],
            "no later snapshot names the service, so the host waits in pending"
        );

        reconciler.sweep_pending_removals().await.unwrap();

        delete.assert_async().await;
        assert!(
            storage::list_pending_npm_removals(&pool)
                .await
                .unwrap()
                .is_empty()
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
    async fn npm_recreates_host_when_deleted_in_npm() {
        // A host is mapped (id 50) but has been deleted out-of-band in NPM, so
        // GET /proxy-hosts/50 returns 404.  reconcile_npm must recreate the host
        // (POST) and persist the new id, NOT PUT to the dead id 50.
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
        // The mapped host no longer exists in NPM.
        let get = npm_server
            .mock("GET", "/api/nginx/proxy-hosts/50")
            .with_status(404)
            .with_body(r#"{"error":{"message":"not found"}}"#)
            .expect(1)
            .create_async()
            .await;
        // PUTting to the dead id must never happen.
        let update = npm_server
            .mock("PUT", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
            .expect(0)
            .create_async()
            .await;
        // Instead the host is recreated with a fresh id.
        let create = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(r#"{"id":77,"domain_names":["a.example.com"]}"#)
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

        let snap = Snapshot {
            machine_id: MachineId::new("m1"),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels("a.example.com"),
            }],
        };
        // Seed the service row and a stale mapping so reconcile takes the
        // get/verify path against id 50.
        storage::upsert_snapshot(&pool, &snap).await.unwrap();
        storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

        reconciler.reconcile_snapshot(&snap).await.unwrap();

        get.assert_async().await;
        update.assert_async().await;
        create.assert_async().await;
        // The mapping now points at the recreated host id.
        assert_eq!(
            storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
            Some(77)
        );
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
        // The reconcile fetches the existing record to drift-check it; it drifted
        // while the service was gone (stale content), so an update is warranted.
        let get = cf_server
            .mock("GET", "/zones/z/dns_records/rec-1")
            .with_status(200)
            .with_body(
                r#"{"success":true,"result":{"id":"rec-1","name":"app.example.com","content":"stale.example.com","proxied":false}}"#,
            )
            .expect_at_least(1)
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
        get.assert_async().await;
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
    async fn cf_reconcile_skips_update_when_record_in_sync() {
        // A per-snapshot Cloudflare reconcile for a service whose DNS record is
        // already in its desired state must NOT issue a redundant update_record
        // PUT.  The snapshot intent hash is per-machine, so any unrelated label
        // change re-runs the intended loop; without a drift check every cf_dns
        // service on the machine gets a wasteful public-API write every time.
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();

        let mut cf_server = mockito::Server::new_async().await;
        let _zone = cf_server
            .mock("GET", "/zones/z")
            .with_status(200)
            .with_body(r#"{"success":true,"result":{"id":"z","name":"example.com"}}"#)
            .expect_at_least(1)
            .create_async()
            .await;
        // The existing record already matches the desired payload, so the
        // reconcile must fetch it to compare...
        let get = cf_server
            .mock("GET", "/zones/z/dns_records/rec-1")
            .with_status(200)
            .with_body(
                r#"{"success":true,"result":{"id":"rec-1","name":"app.example.com","content":"home.example.com","proxied":false}}"#,
            )
            .expect_at_least(1)
            .create_async()
            .await;
        // ...and must NOT PUT an unchanged record.
        let update = cf_server
            .mock("PUT", "/zones/z/dns_records/rec-1")
            .with_status(200)
            .with_body(
                r#"{"success":true,"result":{"id":"rec-1","name":"app.example.com","content":"home.example.com","proxied":false}}"#,
            )
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

        // Seed the service and its existing record mapping.
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

        reconciler.reconcile_snapshot(&snap).await.unwrap();

        get.assert_async().await;
        update.assert_async().await;
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
    async fn npm_ssl_without_matching_certificate_does_not_publish_plaintext() {
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
        // NPM has no certificates, so ssl=true cannot resolve to a cert.
        let _certs = npm_server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .expect_at_least(1)
            .create_async()
            .await;
        // The proxy host must NOT be created over plain HTTP when TLS was
        // requested but no certificate matched the hostname.
        let create = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(r#"{"id":1,"domain_names":["app.example.com"]}"#)
            .expect(0)
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

        let mut labels = npm_host_labels("app.example.com");
        labels.insert("docktornet.npm.0.ssl".to_string(), "true".to_string());

        let result = reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1"),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels,
                }],
            })
            .await;

        // TLS was requested but no cert resolved: reconcile must fail loudly
        // rather than silently publishing an HTTP-only proxy host.
        assert!(
            result.is_err(),
            "ssl=true with no matching certificate must not silently downgrade to plaintext"
        );
        create.assert_async().await;
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
    async fn npm_adopt_refuses_to_drop_domains_of_matched_host() {
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
        // The pre-existing NPM host carries a second domain (b.example.com) that
        // our labels do not manage. It is matched by the single overlapping
        // domain a.example.com.
        let _list = npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(r#"[{"id":91,"domain_names":["a.example.com","b.example.com"]}]"#)
            .expect(1)
            .create_async()
            .await;
        // Provide the GET the (buggy) adoption path would follow, so the failing
        // signal is the clobbering PUT below rather than a missing mock.
        let _get = npm_server
            .mock("GET", "/api/nginx/proxy-hosts/91")
            .with_status(200)
            .with_body(
                r#"{"id":91,"domain_names":["a.example.com","b.example.com"],"forward_scheme":"http",
                    "forward_host":"10.0.0.1","forward_port":3000,"certificate_id":0,
                    "ssl_forced":0,"http2_support":0,"hsts_enabled":0,
                    "hsts_subdomains":0,"block_exploits":1,"allow_websocket_upgrade":1}"#,
            )
            .create_async()
            .await;
        // A PUT here would overwrite domain_names with only [a.example.com],
        // silently dropping b.example.com. It must never be issued.
        let update = npm_server
            .mock("PUT", "/api/nginx/proxy-hosts/91")
            .with_status(200)
            .with_body(r#"{"id":91,"domain_names":["a.example.com"]}"#)
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

        // Adoption is refused (it would drop an unmanaged domain), so the
        // reconcile surfaces a failure.
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

        assert!(
            result.is_err(),
            "reconcile should surface the refused adoption"
        );
        // No clobbering PUT was issued...
        update.assert_async().await;
        // ...and the host that serves more than we manage was not taken over.
        assert_eq!(
            storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
            None
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
