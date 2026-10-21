use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

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
    /// Per-machine apply state.  Each machine has at most one worker applying
    /// and at most one apply pending behind it.  A newer push replaces the
    /// pending snapshot, so applies never queue up behind a slow NPM or
    /// Cloudflare, and a stale snapshot is never applied after a newer one.
    applies: Arc<std::sync::Mutex<HashMap<String, MachineApplies>>>,
    /// How the last full resync ended, for the health endpoint.
    last_resync: Arc<std::sync::Mutex<Option<LastResync>>>,
    clock: Clock,
}

/// The server's clock, in Unix seconds.  Every grace-period decision reads it,
/// never an agent's `taken_at`: agent clocks can be skewed, or read as zero.
/// Tests set it by hand to step through a grace period.
#[derive(Clone, Default)]
struct Clock {
    #[cfg(test)]
    fixed: Arc<std::sync::Mutex<Option<i64>>>,
}

impl Clock {
    fn now(&self) -> i64 {
        #[cfg(test)]
        if let Some(now) = *self.fixed.lock().expect("clock mutex poisoned") {
            return now;
        }
        unix_now()
    }
}

/// A machine's apply state: whether a worker is applying, and the apply
/// waiting for it.  A machine with neither has no entry.
#[derive(Default)]
struct MachineApplies {
    running: bool,
    pending: Option<PendingApply>,
}

/// A persisted snapshot waiting for its external apply.
struct PendingApply {
    snap: Snapshot,
    /// Every service that left the machine in this snapshot or in a snapshot
    /// it replaced.  Persisting cascade-deleted their mappings, so this is the
    /// only copy of the NPM host and Cloudflare record ids they owned.
    removed: Vec<RemovedService>,
    /// When the server persisted the snapshot.  The apply judges each removal
    /// against this, so waiting for a worker never shortens a grace period.
    ingested_at: i64,
}

impl PendingApply {
    /// Replace this pending apply with `newer`, keeping every removal either
    /// carries.  A service removed twice keeps its most recent sighting and
    /// every resource either removal captured.
    fn replace_with(self, mut newer: PendingApply) -> PendingApply {
        for older in self.removed {
            match newer
                .removed
                .iter_mut()
                .find(|rs| rs.service_id == older.service_id)
            {
                Some(rs) => {
                    rs.last_seen = rs.last_seen.max(older.last_seen);
                    for host in older.npm_hosts {
                        if !rs.npm_hosts.contains(&host) {
                            rs.npm_hosts.push(host);
                        }
                    }
                    if rs.cf_record.is_none() {
                        rs.cf_record = older.cf_record;
                    }
                }
                None => newer.removed.push(older),
            }
        }
        newer
    }
}

/// Runs a machine's applies, handed out by [`Reconciler::ingest_snapshot`] when
/// no worker is running for the machine.  It applies the pending snapshot until
/// none is left.
pub struct ApplyWorker {
    reconciler: Reconciler,
    machine: String,
    finished: bool,
}

impl ApplyWorker {
    pub async fn run(mut self) {
        loop {
            // Take the pending apply under the reconciler lock that persisting
            // holds, so no push lands between taking a snapshot and applying
            // it: the apply is always of the newest persisted snapshot.
            let _guard = self.reconciler.lock.lock().await;
            let Some(next) = self.reconciler.take_pending(&self.machine) else {
                break;
            };
            if let Err(e) = self.reconciler.apply_pending(next).await {
                tracing::warn!(machine = %self.machine, error = %e, "background reconcile reported failures");
            }
        }
        self.finished = true;
    }
}

impl Drop for ApplyWorker {
    /// A worker dropped before it finished, by a panic in an apply or by never
    /// being run, hands its machine back so the next push starts a new worker
    /// for whatever is pending.
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let Ok(mut applies) = self.reconciler.applies.lock() else {
            return;
        };
        if let Some(state) = applies.get_mut(&self.machine) {
            state.running = false;
            if state.pending.is_none() {
                applies.remove(&self.machine);
            }
        }
    }
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

/// The NPM proxy-host list for one reconcile pass, fetched on first need and
/// shared by every unmapped host in the pass.  A hostname adoption writes its
/// marker into the list, so a later service in the same pass sees the host as
/// taken.
#[derive(Default)]
struct NpmHostList(Mutex<Option<Vec<crate::integrations::npm::ProxyHostResponse>>>);

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ResyncReport {
    pub verified: usize,
    pub repaired: usize,
    pub failed: usize,
}

impl ResyncReport {
    /// Count one resource the resync reached without error: `repaired` when
    /// it wrote to it, `verified` when it left it alone.
    fn count(&mut self, repaired: bool) {
        if repaired {
            self.repaired += 1;
        } else {
            self.verified += 1;
        }
    }
}

/// How a full resync ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastResync {
    pub finished_at: SystemTime,
    /// The counts the pass reached, kept when it returned an error.
    pub report: ResyncReport,
    /// The error the pass returned; `None` when it finished.
    pub error: Option<String>,
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
    #[error(
        "{resource} matches {service_id} by hostname but its ownership marker names {owner}; refusing to adopt it"
    )]
    MarkedForOtherService {
        service_id: String,
        resource: String,
        owner: String,
    },
}

/// Gatus files for containers that temporarily disappear from a snapshot (e.g.
/// during a restart) are kept for this many seconds before being deleted.  This
/// prevents Gatus from resetting alert state and skipping recovery notifications.
/// A file kept through the grace window is a pending removal, deleted by
/// `sweep_pending_removals` (every minute) once the window ends.
const GATUS_REMOVAL_GRACE_SECS: i64 = 300;

/// How old a temporary file from an atomic Gatus write must be before the
/// sweep deletes it.  A write finishes in well under this, so a file this old
/// was left behind by a failed or interrupted write.
const STALE_TEMP_FILE_SECS: u64 = 300;

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
        && found.comment() == intended.comment.as_deref()
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

    /// How the last full resync ended, or `None` before the first one does.
    pub fn last_resync(&self) -> Option<LastResync> {
        self.last_resync
            .lock()
            .expect("last resync mutex poisoned")
            .clone()
    }

    /// The wall-clock time the NPM breaker's cooldown ends while it is open,
    /// or `None` when it is closed.
    pub fn npm_breaker_open_until(&self) -> Option<SystemTime> {
        let now = std::time::Instant::now();
        let until = self.npm_breaker.open_until(now)?;
        Some(SystemTime::now() + (until - now))
    }

    /// Open the NPM breaker as a run of failed NPM calls would.
    #[cfg(test)]
    pub(crate) fn trip_npm_breaker(&self) {
        let now = std::time::Instant::now();
        for _ in 0..NPM_BREAKER_THRESHOLD {
            self.npm_breaker.record_failure(now);
        }
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
            applies: Arc::default(),
            last_resync: Arc::default(),
            clock: Clock::default(),
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
            applies: Arc::default(),
            last_resync: Arc::default(),
            clock: Clock::default(),
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
        host_list: &NpmHostList,
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

        let payload =
            crate::integrations::npm::proxy_payload(host, forward_host, cert_id, service_id);

        // Resolve the NPM host id: an existing mapping, a host marked for this
        // service, a fresh create, or a hostname adoption.
        let npm_id = match crate::storage::get_npm_host(&self.db, service_id, host.index.into())
            .await?
        {
            Some(id) => id,
            None => {
                let mut cached = host_list.0.lock().await;
                if cached.is_none() {
                    *cached = Some(
                        client
                            .list_proxy_hosts(token)
                            .await
                            .map_err(|err| ReconcileError::External(err.into()))?,
                    );
                }
                let hosts = cached.as_mut().expect("host list fetched above");
                let marker = crate::integrations::npm::OwnershipMarker::new(service_id, host.index);
                if let Some(marked) = crate::integrations::npm::find_by_marker(hosts, &marker) {
                    // The marker proves ownership, so this needs no flag and
                    // skips the extra-domains guard: the update below drops
                    // any domain the labels no longer list, as a label edit
                    // would.
                    tracing::info!(service_id, npm_id = marked.id, "npm host adopted by marker");
                    crate::storage::set_npm_host(
                        &self.db,
                        service_id,
                        host.index.into(),
                        marked.id,
                    )
                    .await?;
                    marked.id
                } else {
                    match client.create_proxy_host(token, &payload).await {
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
                            let existing =
                                crate::integrations::npm::find_by_any_domain(hosts, &host.hosts)
                                    .ok_or_else(|| ReconcileError::External(err.into()))?;
                            self.check_npm_hostname_adoption(service_id, host, existing)?;
                            let npm_id = existing.id;

                            tracing::info!(service_id, npm_id, "npm host adopted by hostname");
                            if let Some(adopted) = hosts.iter_mut().find(|h| h.id == npm_id) {
                                adopted.set_marker(&marker);
                            }
                            // Persist the mapping before any further call.  If a later
                            // step fails (e.g. NPM overloaded), the next reconcile takes
                            // the get/update path instead of re-adopting, which would
                            // otherwise loop forever, each adoption triggering an nginx
                            // reload and keeping NPM pegged.
                            crate::storage::set_npm_host(
                                &self.db,
                                service_id,
                                host.index.into(),
                                npm_id,
                            )
                            .await?;
                            npm_id
                        }
                        Err(err) => return Err(ReconcileError::External(err.into())),
                    }
                }
            }
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
            Some(found) => {
                tracing::info!(service_id, npm_id, "npm host updated");
                client
                    .update_proxy_host(token, npm_id, &payload.merged_with_meta(&found.meta))
                    .await
                    .map_err(|err| ReconcileError::External(err.into()))?;
            }
        }
        Ok(())
    }

    /// Refuse a hostname adoption of `existing`, an NPM host found by one of
    /// `host`'s domains, when it is marked for another service or when
    /// adopting it would drop domains the labels do not manage.
    fn check_npm_hostname_adoption(
        &self,
        service_id: &str,
        host: &docktornet_core::labels::NpmHost,
        existing: &crate::integrations::npm::ProxyHostResponse,
    ) -> Result<(), ReconcileError> {
        // A host marked for this service and index was adopted by marker
        // before the create, so any marker here names another owner: another
        // service, or this service's host at another index.
        if let Some(owner) = existing.marker() {
            let owner = format!("{} (index {})", owner.service_id, owner.npm_index);
            tracing::error!(
                service_id,
                index = host.index,
                npm_id = existing.id,
                owner,
                "refusing to adopt NPM host: its ownership marker names another owner"
            );
            return Err(ReconcileError::MarkedForOtherService {
                service_id: service_id.to_string(),
                resource: format!("NPM host {}", existing.id),
                owner,
            });
        }

        // The match only requires a single overlapping domain, so the
        // existing host may carry others we do not manage. Overwriting it
        // with our payload would silently drop those domains, so refuse to
        // adopt (and clobber) a host that serves more than our labels intend.
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
        let payload = crate::integrations::cloudflare::dns_payload(dns, &zone.name, service_id);
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

        // No mapping: a record marked for this service is adopted before any
        // create, with no flag needed.
        if let Some(comment) = &payload.comment
            && let Some(record) = client
                .find_record_by_marker(comment)
                .await
                .map_err(|e| ReconcileError::External(e.into()))?
        {
            tracing::info!(service_id, record_id = %record.id, "cf record adopted by marker");
            crate::storage::set_cf_dns_record(&self.db, service_id, &record.id).await?;
            if !cf_record_matches(&record, &payload) {
                tracing::info!(service_id, record_id = %record.id, "cf record drifted, updating");
                client
                    .update_record(&record.id, &payload)
                    .await
                    .map_err(|e| ReconcileError::External(e.into()))?;
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
                if let Some(owner) = record.marker_owner()
                    && owner != service_id
                {
                    tracing::error!(
                        service_id,
                        record_id = %record.id,
                        owner,
                        "refusing to adopt cf record: its ownership marker names another service"
                    );
                    return Err(ReconcileError::MarkedForOtherService {
                        service_id: service_id.to_string(),
                        resource: format!("Cloudflare record {} ({})", record.id, record.name),
                        owner: owner.to_string(),
                    });
                }
                if let Some(old) = record.comment()
                    && record.marker_owner().is_none()
                {
                    tracing::warn!(
                        service_id,
                        record_id = %record.id,
                        old_comment = old,
                        "cf record adoption replaces a comment docktornet did not write"
                    );
                }
                tracing::info!(service_id, record_id = %record.id, "cf record adopted by hostname");
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

    /// Verify and repair every live service, then sweep removals.  Records how
    /// the pass ended, error or not, for `last_resync`.
    pub async fn full_resync(&self) -> Result<ResyncReport, ReconcileError> {
        let mut report = ResyncReport::default();
        let result = self.full_resync_pass(&mut report).await;
        *self.last_resync.lock().expect("last resync mutex poisoned") = Some(LastResync {
            finished_at: SystemTime::now(),
            report: report.clone(),
            error: result.as_ref().err().map(ToString::to_string),
        });
        result.map(|()| report)
    }

    async fn full_resync_pass(&self, report: &mut ResyncReport) -> Result<(), ReconcileError> {
        let _guard = self.lock.lock().await;

        let services = crate::storage::list_all_services(&self.db).await?;
        // Read before NPM is touched, so a database error can never cut a pass
        // short after it reached NPM but before it fed the breaker.
        let expected_gatus_files = if self.options.dry_run {
            None
        } else {
            Some(self.expected_gatus_files().await?)
        };

        // Fetch the NPM token + certificate list once for the whole resync
        // pass rather than once per host.  While the circuit breaker is open,
        // leave NPM alone entirely: no fetch, no host checks, no sweep.
        let use_npm = self.npm.is_some() && !self.options.dry_run;
        let skip_npm = use_npm && self.npm_breaker.is_open(std::time::Instant::now());
        if skip_npm {
            tracing::warn!("npm circuit breaker open; skipping NPM resync this pass");
        }
        let mut npm_failed = false;
        let npm_ctx = if use_npm && !skip_npm {
            match self.fetch_npm_context().await {
                Ok(ctx) => Some(ctx),
                Err(err) => {
                    tracing::warn!(error = %err, "npm resync skipped: token/cert fetch failed");
                    npm_failed = true;
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

            if !skip_npm {
                for host in &parsed.npm {
                    match self
                        .resync_npm_host(&service_id, host, &forward_host, npm_ctx.as_ref())
                        .await
                    {
                        Ok(repaired) => report.count(repaired),
                        Err(err) => {
                            tracing::warn!(service_id, index = host.index, error = %err, "npm resync failed");
                            npm_failed = true;
                            report.failed += 1;
                        }
                    }
                }
            }

            if let Some(dns) = &parsed.cf_dns {
                match self.resync_cloudflare(&service_id, dns).await {
                    Ok(repaired) => report.count(repaired),
                    Err(err) => {
                        tracing::warn!(service_id, error = %err, "cloudflare resync failed");
                        report.failed += 1;
                    }
                }
            }

            if !parsed.gatus.is_empty() {
                match self.resync_gatus(&service_id, &parsed.gatus).await {
                    Ok(repaired) => report.count(repaired),
                    Err(err) => {
                        tracing::warn!(service_id, error = %err, "gatus resync failed");
                        report.failed += 1;
                    }
                }
            }
        }

        if let Some(expected) = &expected_gatus_files {
            npm_failed |= self
                .sweep_removals(expected, npm_ctx.as_ref(), report)
                .await;
            if let Err(err) = self.refresh_classification_map().await {
                tracing::warn!(error = %err, "gatus classification map refresh failed");
                report.failed += 1;
            }
        }
        // Feed the breaker only when this pass actually reached NPM.
        if use_npm && !skip_npm {
            if npm_failed {
                self.npm_breaker.record_failure(std::time::Instant::now());
            } else {
                self.npm_breaker.record_success();
            }
        }

        tracing::info!(
            verified = report.verified,
            repaired = report.repaired,
            failed = report.failed,
            "full resync complete"
        );
        Ok(())
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
        let now = self.clock.now();
        let npm_due = self.npm.is_some() && {
            let mut due = false;
            for (service_id, _, _, last_seen) in
                crate::storage::list_pending_npm_removals(&self.db).await?
            {
                if past_grace(last_seen, now) && !self.stored_again(&service_id).await {
                    due = true;
                    break;
                }
            }
            due
        };
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
        if let Err(err) = self.refresh_classification_map().await {
            tracing::warn!(error = %err, "gatus classification map refresh failed");
            report.failed += 1;
        }
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
        let now = self.clock.now();
        let mut removed_any = false;
        loop {
            match entries.next_entry().await {
                Ok(Some(entry)) => {
                    let path = entry.path();
                    if crate::integrations::gatus::is_temp_file(&path) {
                        Self::remove_stale_temp_file(&entry).await;
                        continue;
                    }
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

    /// Delete a temporary file a failed or interrupted Gatus write left behind,
    /// once it is stale, so no write still in flight can be using it.  Gatus
    /// never loads these, so no reload is needed.
    async fn remove_stale_temp_file(entry: &tokio::fs::DirEntry) {
        let path = entry.path();
        let stale = match entry.metadata().await {
            Ok(meta) => meta
                .modified()
                .ok()
                .and_then(|m| m.elapsed().ok())
                .is_some_and(|age| age.as_secs() >= STALE_TEMP_FILE_SECS),
            Err(_) => false,
        };
        if !stale {
            return;
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => tracing::info!(path = %path.display(), "removed stale gatus temp file"),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(path = %path.display(), error = %err, "failed to remove stale gatus temp file");
            }
        }
    }

    /// Make Gatus reload after this pass deleted one or more service files.
    async fn touch_gatus_reload_trigger(&self) -> anyhow::Result<()> {
        crate::integrations::gatus::touch_reload_trigger(&self.options.gatus_config_dir).await?;
        tracing::info!("gatus service files deleted; reload trigger touched");
        Ok(())
    }

    /// Store a service's classification map entries once its Gatus file holds
    /// these monitors.
    async fn record_gatus_classification(
        &self,
        service_id: &str,
        monitors: &[docktornet_core::labels::GatusMonitor],
    ) -> Result<(), ReconcileError> {
        let endpoints = crate::integrations::gatus::classify_endpoints(service_id, monitors);
        let json =
            serde_json::to_string(&endpoints).map_err(|e| ReconcileError::External(e.into()))?;
        crate::storage::set_gatus_classification(&self.db, service_id, &json).await?;
        Ok(())
    }

    /// Rewrite the classification map from the stored entries of every service
    /// whose Gatus file is on disk, so the map lists exactly the endpoints Gatus
    /// loads, a departed service's included until its grace period ends.  Rows
    /// whose file is gone are dropped.
    async fn refresh_classification_map(&self) -> anyhow::Result<()> {
        let dir = &self.options.gatus_config_dir;
        let mut endpoints = Vec::new();
        for (service_id, endpoints_json) in
            crate::storage::list_gatus_classifications(&self.db).await?
        {
            let path = crate::integrations::gatus::service_file_path(dir, &service_id);
            match tokio::fs::try_exists(&path).await {
                Ok(true) => {}
                Ok(false) => {
                    crate::storage::delete_gatus_classification(&self.db, &service_id).await?;
                    continue;
                }
                // Keep an entry whose file cannot be checked: dropping it would
                // unlabel a monitor Gatus may still run.
                Err(err) => {
                    tracing::warn!(service_id, path = %path.display(), error = %err, "could not check gatus file; keeping its classification");
                }
            }
            match serde_json::from_str::<Vec<crate::integrations::gatus::ClassifiedEndpoint>>(
                &endpoints_json,
            ) {
                Ok(stored) => endpoints.extend(stored),
                Err(err) => {
                    tracing::warn!(service_id, error = %err, "dropping unreadable gatus classification");
                    crate::storage::delete_gatus_classification(&self.db, &service_id).await?;
                }
            }
        }
        let thresholds = crate::integrations::gatus::alert_thresholds(dir).await;
        if crate::integrations::gatus::write_classification_map(dir, &endpoints, thresholds).await?
        {
            tracing::info!(
                endpoints = endpoints.len(),
                "gatus classification map written"
            );
        }
        Ok(())
    }

    /// Whether a service with a pending removal is back: stored for its
    /// machine again, with labels that ask for resources.  It came back in a
    /// snapshot whose apply has not run yet, and that apply restores the
    /// removal's resources, so no sweep may delete them.  When the store
    /// cannot tell, the removal is kept for a later sweep.
    async fn stored_again(&self, service_id: &str) -> bool {
        match crate::storage::get_service_labels(&self.db, service_id).await {
            Ok(labels) => {
                let back = labels.is_some_and(|labels| {
                    crate::integrations::intended::has_intent(&docktornet_core::labels::parse(
                        &labels,
                    ))
                });
                if back {
                    tracing::debug!(
                        service_id,
                        "service is back; leaving its pending removal to the next apply"
                    );
                }
                back
            }
            Err(err) => {
                tracing::warn!(service_id, error = %err, "could not check whether the service is back; keeping its pending removal");
                true
            }
        }
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
        let now = self.clock.now();
        let mut npm_failed = false;

        for (service_id, npm_index, npm_id, last_seen) in pending {
            if !past_grace(last_seen, now) {
                // Still within grace (a removal that landed just before this
                // sweep); a later sweep will pick it up.
                continue;
            }
            if self.stored_again(&service_id).await {
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
        let now = self.clock.now();

        for (service_id, record_id, last_seen) in pending {
            if !past_grace(last_seen, now) {
                // Still within grace (a removal that landed just before this
                // sweep); a later sweep will pick it up.
                continue;
            }
            if self.stored_again(&service_id).await {
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
    ) -> Result<bool, ReconcileError> {
        let Some(client) = &self.npm else {
            return Ok(false);
        };
        let Some(npm_id) =
            crate::storage::get_npm_host(&self.db, service_id, host.index.into()).await?
        else {
            return Ok(false);
        };
        if self.options.dry_run {
            tracing::debug!(service_id, npm_id, "dry-run: would verify npm host");
            return Ok(false);
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

        let payload =
            crate::integrations::npm::proxy_payload(host, forward_host, desired_cert, service_id);

        match existing {
            None => {
                tracing::info!(service_id, npm_id, "npm host missing, recreating");
                let created = client
                    .create_proxy_host(token, &payload)
                    .await
                    .map_err(|e| ReconcileError::External(e.into()))?;
                crate::storage::set_npm_host(&self.db, service_id, host.index.into(), created.id)
                    .await?;
                Ok(true)
            }
            Some(found) if !found.matches_payload(&payload) => {
                tracing::info!(
                    service_id,
                    npm_id,
                    ?desired_cert,
                    "npm host drifted, updating"
                );
                client
                    .update_proxy_host(token, npm_id, &payload.merged_with_meta(&found.meta))
                    .await
                    .map_err(|e| ReconcileError::External(e.into()))?;
                Ok(true)
            }
            Some(_) => Ok(false),
        }
    }

    async fn resync_cloudflare(
        &self,
        service_id: &str,
        dns: &docktornet_core::labels::CloudflareDns,
    ) -> Result<bool, ReconcileError> {
        let Some(client) = &self.cloudflare else {
            return Ok(false);
        };
        let Some(record_id) = crate::storage::get_cf_dns_record(&self.db, service_id).await? else {
            return Ok(false);
        };
        if self.options.dry_run {
            tracing::debug!(service_id, record_id = %record_id, "dry-run: would verify cf record");
            return Ok(false);
        }

        let zone = client
            .zone()
            .await
            .map_err(|e| ReconcileError::External(e.into()))?;
        let payload = crate::integrations::cloudflare::dns_payload(dns, &zone.name, service_id);
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
                Ok(true)
            }
            Some(found) if !cf_record_matches(&found, &payload) => {
                tracing::info!(service_id, "cf record drifted, updating");
                client
                    .update_record(&record_id, &payload)
                    .await
                    .map_err(|e| ReconcileError::External(e.into()))?;
                Ok(true)
            }
            Some(_) => Ok(false),
        }
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
        if self.options.dry_run {
            if current.as_deref() != Some(intended.as_str()) {
                tracing::debug!(service_id, path = %target.display(), "dry-run: would rewrite gatus file");
            }
            return Ok(false);
        }
        let rewrote = current.as_deref() != Some(intended.as_str());
        if rewrote {
            tracing::info!(service_id, path = %target.display(), "gatus file drifted, rewriting");
            crate::integrations::gatus::write_service_file(
                &self.options.gatus_config_dir,
                service_id,
                monitors,
            )
            .await
            .map_err(ReconcileError::External)?;
        }
        self.record_gatus_classification(service_id, monitors)
            .await?;
        Ok(rewrote)
    }

    /// Persist the snapshot to SQLite and return the `(service_id, last_seen)`
    /// pairs for services that have just left this machine's snapshot.  This is
    /// the fast, local-only half of reconciliation and must run before a push
    /// is acked so the snapshot is durable even when the external reconcile is
    /// deferred.  `removed_last_seen` is captured here because `upsert_snapshot`
    /// deletes those rows.
    async fn persist_inner(
        &self,
        snap: &Snapshot,
        now: i64,
    ) -> Result<Vec<RemovedService>, ReconcileError> {
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
        self.record_gatus_departures(now, &current_ids, &removed_last_seen)
            .await?;
        for (service_id, last_seen) in &removed_last_seen {
            crate::storage::refresh_pending_removals(&self.db, service_id, *last_seen).await?;
        }
        crate::storage::upsert_snapshot(&self.db, snap, now).await?;

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
        now: i64,
        current_ids: &[String],
        removed_last_seen: &[(String, i64)],
    ) -> Result<(), ReconcileError> {
        for (service_id, last_seen) in removed_last_seen {
            if !past_grace(*last_seen, now) {
                crate::storage::add_pending_gatus_removal(&self.db, service_id, *last_seen).await?;
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
    /// log.  `ingested_at` is when the snapshot was persisted.
    async fn apply_inner(
        &self,
        snap: &Snapshot,
        removed: Vec<RemovedService>,
        ingested_at: i64,
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

        // A service removed by a snapshot this one replaced may be back in it.
        // Its removal is deferred like one within its grace period, so the
        // pending rows it writes are restored below rather than its live
        // resources deleted.
        let back = |rs: &RemovedService| intended.iter().any(|s| s.service_id == rs.service_id);
        // Whether a removal is over its grace period and its service still gone.
        let due = |rs: &RemovedService| {
            ingested_at.saturating_sub(rs.last_seen) >= GATUS_REMOVAL_GRACE_SECS && !back(rs)
        };

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
        for rs in &removed {
            let absent_secs = ingested_at.saturating_sub(rs.last_seen);
            if due(rs) {
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
        if let Err(err) = self.refresh_classification_map().await {
            tracing::warn!(error = %err, "gatus classification map refresh failed");
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
            let prune_now = due(rs) && !skip_npm;
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
            let delete_now = due(rs);
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

        // The NPM host list for marker and hostname adoption, fetched at most
        // once this pass and only if some host has no mapping.
        let npm_host_list = NpmHostList::default();
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
                                    &npm_host_list,
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
        self.record_gatus_classification(&service.service_id, &service.labels.gatus)
            .await?;
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
        let ingested_at = self.clock.now();
        let removed = self.persist_inner(snap, ingested_at).await?;
        if self.options.dry_run {
            tracing::debug!(removed = removed.len(), "dry-run: would reconcile snapshot");
            return Ok(());
        }
        self.apply_inner(snap, removed, ingested_at).await
    }

    /// Persist the snapshot and queue its external apply.  The ingest handler
    /// calls this so a push is acked as soon as the snapshot is durable,
    /// without blocking on slow NPM/Cloudflare calls.  The snapshot replaces
    /// any apply already pending for its machine.  Returns a worker to run in
    /// the background when none is running for the machine.
    pub async fn ingest_snapshot(
        &self,
        snap: &Snapshot,
    ) -> Result<Option<ApplyWorker>, ReconcileError> {
        let _guard = self.lock.lock().await;
        let ingested_at = self.clock.now();
        let removed = self.persist_inner(snap, ingested_at).await?;
        // Queue under the persist lock, so pending snapshots replace each
        // other in the order they were persisted.
        let machine = snap.machine_id.as_str().to_string();
        let mut applies = self.applies.lock().expect("applies mutex poisoned");
        let state = applies.entry(machine.clone()).or_default();
        let queued = PendingApply {
            snap: snap.clone(),
            removed,
            ingested_at,
        };
        state.pending = Some(match state.pending.take() {
            Some(older) => older.replace_with(queued),
            None => queued,
        });
        if state.running {
            return Ok(None);
        }
        state.running = true;
        Ok(Some(ApplyWorker {
            reconciler: self.clone(),
            machine,
            finished: false,
        }))
    }

    /// Take the machine's pending apply.  When there is none, the worker is
    /// done and the machine's entry goes, under the same locks `ingest_snapshot`
    /// queues under: a push landing as the worker finishes either is taken
    /// here or starts a new worker.
    fn take_pending(&self, machine: &str) -> Option<PendingApply> {
        let mut applies = self.applies.lock().expect("applies mutex poisoned");
        let pending = applies.get_mut(machine)?.pending.take();
        if pending.is_none() {
            applies.remove(machine);
        }
        pending
    }

    /// Apply the external side effects for an already-persisted snapshot.  The
    /// caller holds the reconciler lock.
    async fn apply_pending(&self, pending: PendingApply) -> Result<(), ReconcileError> {
        if self.options.dry_run {
            return Ok(());
        }
        self.apply_inner(&pending.snap, pending.removed, pending.ingested_at)
            .await
    }
}

#[cfg(test)]
impl Reconciler {
    /// Stop this reconciler's clock, shared with its clones, at `now`.
    fn set_now(&self, now: i64) {
        *self.clock.fixed.lock().expect("clock mutex poisoned") = Some(now);
    }

    /// Wait until no worker is applying for any machine.
    pub(crate) async fn wait_for_applies(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !self.applies.lock().unwrap().is_empty() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "applies did not finish"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "127.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
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
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "127.0.0.1".into(),
                taken_at: 1,
                agent_version: None,
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
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "127.0.0.1".into(),
                taken_at: 31,
                agent_version: None,
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

        reconciler.set_now(1);
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "127.0.0.1".into(),
                taken_at: 1,
                agent_version: None,
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
        reconciler.set_now(1 + GATUS_REMOVAL_GRACE_SECS + 1);
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "127.0.0.1".into(),
                taken_at: (1 + GATUS_REMOVAL_GRACE_SECS + 1) as u64,
                agent_version: None,
                containers: vec![],
            })
            .await
            .unwrap();
        assert!(
            !service_file.exists(),
            "gatus file must be deleted after the grace period"
        );
    }

    /// A snapshot of machine `m1`, forwarding to 10.0.0.1, holding `containers`.
    fn m1_snapshot(containers: Vec<ContainerEntry>) -> Snapshot {
        Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers,
        }
    }

    /// Ingest `snap` with the server's clock at `at`, returning the worker if
    /// the ingest started one.
    async fn ingest_at(reconciler: &Reconciler, at: i64, snap: &Snapshot) -> Option<ApplyWorker> {
        reconciler.set_now(at);
        reconciler.ingest_snapshot(snap).await.unwrap()
    }

    fn gatus_entry(name: &str) -> ContainerEntry {
        ContainerEntry {
            name: name.into(),
            labels: gatus_labels(),
        }
    }

    /// An NPM stand-in that counts token requests, the first call of every
    /// apply that reaches NPM, and holds every request until `release` is sent,
    /// then fails it.
    async fn blocking_npm() -> (
        String,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::sync::watch::Sender<bool>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let tokens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (release, released) = tokio::sync::watch::channel(false);
        let counted = tokens.clone();
        tokio::spawn(async move {
            loop {
                let (mut conn, _) = listener.accept().await.unwrap();
                let counted = counted.clone();
                let mut released = released.clone();
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                        match conn.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => head.extend_from_slice(&buf[..n]),
                        }
                    }
                    if head.starts_with(b"POST /api/tokens") {
                        counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    let _ = released.wait_for(|released| *released).await;
                    let _ = conn
                        .write_all(
                            b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                        )
                        .await;
                });
            }
        });
        (url, tokens, release)
    }

    #[tokio::test]
    async fn identical_pushes_behind_a_blocked_apply_run_at_most_two_applies() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let (npm_url, tokens, release) = blocking_npm().await;
        let (npm, cf) = test_clients(&npm_url);
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
        let snap = m1_snapshot(vec![ContainerEntry {
            name: "app".into(),
            labels: npm_host_labels("a.example.com"),
        }]);

        let worker = ingest_at(&reconciler, 1, &snap).await.unwrap();
        let running = tokio::spawn(worker.run());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokens.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the first apply never reached NPM"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // The pushes wait for the persist lock the blocked apply holds, as
        // their handlers would.
        reconciler.set_now(2);
        let pushes: Vec<_> = (2..=10)
            .map(|_| {
                let reconciler = reconciler.clone();
                let snap = snap.clone();
                tokio::spawn(async move { reconciler.ingest_snapshot(&snap).await.unwrap() })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(200)).await;
        release.send(true).unwrap();
        for push in pushes {
            assert!(
                push.await.unwrap().is_none(),
                "a worker is already applying for m1"
            );
        }
        running.await.unwrap();

        assert_eq!(tokens.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_removal_only_a_replaced_snapshot_carried_lands_in_pending_removals() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = gatus_reconciler(pool.clone(), &dir.path().join("gatus"));
        push(&reconciler, 1, vec![gatus_entry("x")]).await;
        storage::set_npm_host(&pool, "m1/x", 0, 50).await.unwrap();
        storage::set_cf_dns_record(&pool, "m1/x", "rec-1")
            .await
            .unwrap();

        // x leaves in the first snapshot, which is replaced before it applies.
        let worker = ingest_at(&reconciler, 10, &m1_snapshot(vec![]))
            .await
            .unwrap();
        assert!(
            ingest_at(&reconciler, 20, &m1_snapshot(vec![gatus_entry("y")]))
                .await
                .is_none()
        );
        worker.run().await;

        assert_eq!(
            storage::list_pending_npm_removals(&pool).await.unwrap(),
            vec![("m1/x".to_string(), 0, 50, 1)]
        );
        assert_eq!(
            storage::list_pending_cf_removals(&pool).await.unwrap(),
            vec![("m1/x".to_string(), "rec-1".to_string(), 1)]
        );
    }

    #[tokio::test]
    async fn a_removal_only_a_replaced_snapshot_carried_is_deleted_past_grace() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut server = cf_server_with_zone().await;
        let _token = server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .create_async()
            .await;
        let _certs = server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .create_async()
            .await;
        let npm_delete = server
            .mock("DELETE", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body("true")
            .expect(1)
            .create_async()
            .await;
        let cf_delete = server
            .mock("DELETE", "/zones/z/dns_records/rec-1")
            .with_status(200)
            .with_body(cf_one(serde_json::json!({"id": "rec-1"})))
            .expect(1)
            .create_async()
            .await;
        let (npm, cf) = test_clients_with_cf(&server.url(), &server.url());
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
        push(&reconciler, 1, vec![gatus_entry("x")]).await;
        storage::set_npm_host(&pool, "m1/x", 0, 50).await.unwrap();
        storage::set_cf_dns_record(&pool, "m1/x", "rec-1")
            .await
            .unwrap();

        let past_grace = 1 + GATUS_REMOVAL_GRACE_SECS + 10;
        let worker = ingest_at(&reconciler, past_grace, &m1_snapshot(vec![]))
            .await
            .unwrap();
        assert!(
            ingest_at(
                &reconciler,
                past_grace + 5,
                &m1_snapshot(vec![gatus_entry("y")])
            )
            .await
            .is_none()
        );
        worker.run().await;

        npm_delete.assert_async().await;
        cf_delete.assert_async().await;
        assert!(
            storage::list_pending_npm_removals(&pool)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            storage::list_pending_cf_removals(&pool)
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// An NPM and Cloudflare stand-in for a service `m1/x` that owns NPM host
    /// 50 and Cloudflare record rec-1.  It answers the token, certificate and
    /// zone lookups; the delete mocks are returned for the caller to assert.
    async fn x_resources_server(deletes: usize) -> (mockito::ServerGuard, Vec<mockito::Mock>) {
        let mut server = cf_server_with_zone().await;
        server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .expect_at_least(0)
            .create_async()
            .await;
        server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .expect_at_least(0)
            .create_async()
            .await;
        server
            .mock("GET", "/zones/z/dns_records/rec-1")
            .with_status(200)
            .with_body(cf_one(serde_json::json!({
                "id": "rec-1", "name": "app.example.com", "content": "home.example.com",
                "proxied": false, "comment": "docktornet:m1/x"
            })))
            .expect_at_least(0)
            .create_async()
            .await;
        let npm_delete = server
            .mock("DELETE", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body("true")
            .expect(deletes)
            .create_async()
            .await;
        let cf_delete = server
            .mock("DELETE", "/zones/z/dns_records/rec-1")
            .with_status(200)
            .with_body(cf_one(serde_json::json!({"id": "rec-1"})))
            .expect(deletes)
            .create_async()
            .await;
        (server, vec![npm_delete, cf_delete])
    }

    fn reconciler_on(
        pool: Db,
        gatus_dir: &std::path::Path,
        server: &mockito::ServerGuard,
    ) -> Reconciler {
        let (npm, cf) = test_clients_with_cf(&server.url(), &server.url());
        Reconciler::with_clients(
            pool,
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: gatus_dir.to_path_buf(),
            },
            npm,
            cf,
        )
    }

    /// `x` with a Gatus monitor and a Cloudflare record.
    fn x_entry() -> ContainerEntry {
        let mut labels = gatus_labels();
        labels.extend(cf_dns_labels("app", "home.example.com"));
        ContainerEntry {
            name: "x".into(),
            labels,
        }
    }

    #[tokio::test]
    async fn a_service_back_in_the_newest_snapshot_keeps_its_resources() {
        // x leaves, past its grace period, in a snapshot that is replaced by
        // one holding x again before it applies.  The apply must restore x,
        // not delete what it owns.
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let (server, deletes) = x_resources_server(0).await;
        let reconciler = reconciler_on(pool.clone(), &gatus_dir, &server);
        storage::upsert_snapshot(&pool, &m1_snapshot(vec![x_entry()]), 1)
            .await
            .unwrap();
        storage::set_npm_host(&pool, "m1/x", 0, 50).await.unwrap();
        storage::set_cf_dns_record(&pool, "m1/x", "rec-1")
            .await
            .unwrap();

        let past_grace = 1 + GATUS_REMOVAL_GRACE_SECS + 10;
        let worker = ingest_at(&reconciler, past_grace, &m1_snapshot(vec![]))
            .await
            .unwrap();
        assert!(
            ingest_at(&reconciler, past_grace + 5, &m1_snapshot(vec![x_entry()]))
                .await
                .is_none()
        );
        worker.run().await;

        for mock in deletes {
            mock.assert_async().await;
        }
        assert!(gatus::service_file_path(&gatus_dir, "m1/x").exists());
        assert_eq!(
            storage::get_npm_host(&pool, "m1/x", 0).await.unwrap(),
            Some(50)
        );
        assert_eq!(
            storage::get_cf_dns_record(&pool, "m1/x").await.unwrap(),
            Some("rec-1".to_string())
        );
        assert!(
            storage::list_pending_npm_removals(&pool)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            storage::list_pending_cf_removals(&pool)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_service_that_returns_and_leaves_again_while_coalesced_keeps_its_grace() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = gatus_reconciler(pool.clone(), &dir.path().join("gatus"));
        push(&reconciler, 1, vec![gatus_entry("x")]).await;
        storage::set_npm_host(&pool, "m1/x", 0, 50).await.unwrap();
        storage::set_cf_dns_record(&pool, "m1/x", "rec-1")
            .await
            .unwrap();
        // x leaves: its removal waits out the grace period from t=1.
        ingest_at(&reconciler, 10, &m1_snapshot(vec![]))
            .await
            .unwrap()
            .run()
            .await;

        // x is back at t=20 and gone again at t=30, both behind one apply.
        let worker = ingest_at(&reconciler, 20, &m1_snapshot(vec![gatus_entry("x")]))
            .await
            .unwrap();
        assert!(
            ingest_at(&reconciler, 30, &m1_snapshot(vec![]))
                .await
                .is_none()
        );
        worker.run().await;

        // x was seen at t=20, so its grace period runs from there.
        assert_eq!(
            storage::list_pending_npm_removals(&pool).await.unwrap(),
            vec![("m1/x".to_string(), 0, 50, 20)]
        );
        assert_eq!(
            storage::list_pending_cf_removals(&pool).await.unwrap(),
            vec![("m1/x".to_string(), "rec-1".to_string(), 20)]
        );
    }

    #[tokio::test]
    async fn sweep_deletes_what_a_stored_service_without_labels_left_behind() {
        // x keeps running but its only docktornet label is malformed, so it is
        // stored without any intent.  Its removal must still go through.
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let (server, deletes) = x_resources_server(1).await;
        let reconciler = reconciler_on(pool.clone(), &gatus_dir, &server);
        storage::upsert_snapshot(&pool, &m1_snapshot(vec![x_entry()]), 1)
            .await
            .unwrap();
        storage::set_npm_host(&pool, "m1/x", 0, 50).await.unwrap();
        storage::set_cf_dns_record(&pool, "m1/x", "rec-1")
            .await
            .unwrap();
        let malformed = ContainerEntry {
            name: "x".into(),
            labels: [(
                "docktornet.npm.0.host".to_string(),
                "a.example.com".to_string(),
            )]
            .into(),
        };
        push(&reconciler, 2, vec![malformed]).await;

        reconciler.set_now(2 + GATUS_REMOVAL_GRACE_SECS + 60);
        reconciler.sweep_pending_removals().await.unwrap();

        for mock in deletes {
            mock.assert_async().await;
        }
    }

    #[tokio::test]
    async fn a_push_after_the_worker_finished_starts_a_new_worker() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = gatus_reconciler(pool, &dir.path().join("gatus"));
        let worker = ingest_at(&reconciler, 1, &m1_snapshot(vec![gatus_entry("x")]))
            .await
            .unwrap();
        worker.run().await;
        assert!(
            ingest_at(&reconciler, 2, &m1_snapshot(vec![gatus_entry("x")]))
                .await
                .is_some(),
            "the push would be stranded"
        );
    }

    #[tokio::test]
    async fn a_dropped_worker_lets_the_next_push_start_one() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = gatus_reconciler(pool, &gatus_dir);
        let worker = ingest_at(&reconciler, 1, &m1_snapshot(vec![gatus_entry("x")]))
            .await
            .unwrap();
        drop(worker);
        let worker = ingest_at(&reconciler, 2, &m1_snapshot(vec![gatus_entry("y")]))
            .await
            .expect("the dropped worker must hand m1 back");
        worker.run().await;
        assert!(gatus::service_file_path(&gatus_dir, "m1/y").exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn racing_pushes_end_with_the_newest_snapshot_applied() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = gatus_reconciler(pool, &gatus_dir);
        let entry = |i: u32| {
            let mut labels = gatus_labels();
            labels.insert("docktornet.gatus.0.url".into(), format!("https://v{i}"));
            ContainerEntry {
                name: "app".into(),
                labels,
            }
        };
        for i in 0..50 {
            if let Some(worker) = ingest_at(&reconciler, 1, &m1_snapshot(vec![entry(i)])).await {
                tokio::spawn(worker.run());
            }
            if i % 3 == 0 {
                tokio::task::yield_now().await;
            }
        }
        reconciler.wait_for_applies().await;
        let file = std::fs::read_to_string(gatus::service_file_path(&gatus_dir, "m1/app")).unwrap();
        assert!(file.contains("https://v49"), "{file}");
    }

    #[tokio::test]
    async fn sweep_keeps_a_pending_removal_of_a_service_that_is_back() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut server = mockito::Server::new_async().await;
        let mut untouched = Vec::new();
        for method in ["GET", "POST", "PUT", "DELETE"] {
            untouched.push(
                server
                    .mock(method, mockito::Matcher::Any)
                    .expect(0)
                    .create_async()
                    .await,
            );
        }
        let (npm, cf) = test_clients_with_cf(&server.url(), &server.url());
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
        // x is stored again, and its removal waits for the apply to restore it.
        push(&reconciler, 1, vec![gatus_entry("x")]).await;
        storage::add_pending_npm_removal(&pool, "m1/x", 0, 50, 1)
            .await
            .unwrap();
        storage::add_pending_cf_removal(&pool, "m1/x", "rec-1", 1)
            .await
            .unwrap();
        reconciler.set_now(1 + GATUS_REMOVAL_GRACE_SECS + 60);

        reconciler.sweep_pending_removals().await.unwrap();

        for mock in untouched {
            mock.assert_async().await;
        }
        assert_eq!(
            storage::list_pending_npm_removals(&pool)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            storage::list_pending_cf_removals(&pool)
                .await
                .unwrap()
                .len(),
            1
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
        let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
        assert!(service_file.exists());

        // Remove the service within the grace period so the file is kept.
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "127.0.0.1".into(),
                taken_at: 31,
                agent_version: None,
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
        let service_file = gatus::service_file_path(&gatus_dir, "m1/app");
        assert!(service_file.exists());

        // Remove the service within the grace period so apply_inner keeps the
        // file (its mtime stays recent, standing in for last-seen).
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "127.0.0.1".into(),
                taken_at: 31,
                agent_version: None,
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

    /// Push a snapshot of machine `m1` holding `containers`, with the server's
    /// clock and the agent's `taken_at` both at `at`.
    async fn push(reconciler: &Reconciler, at: u64, containers: Vec<ContainerEntry>) {
        reconciler.set_now(at as i64);
        push_stamped(reconciler, at, containers).await;
    }

    /// Push a snapshot of machine `m1` holding `containers`, stamped `taken_at`
    /// by the agent, leaving the server's clock alone.
    async fn push_stamped(reconciler: &Reconciler, taken_at: u64, containers: Vec<ContainerEntry>) {
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at,
                containers,
                agent_version: None,
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
            reconciler.clock.now() - GATUS_REMOVAL_GRACE_SECS - 60,
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

    #[tokio::test]
    async fn pending_removal_sweep_removes_old_temp_files_only() {
        let dir = tempdir().unwrap();
        let gatus_dir = dir.path().join("gatus");
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let reconciler = gatus_reconciler(pool, &gatus_dir);
        std::fs::create_dir_all(&gatus_dir).unwrap();
        let file = |name: &str, aged: bool| {
            let path = gatus_dir.join(name);
            std::fs::write(&path, "x").unwrap();
            if aged {
                set_mtime(&path, aged_past_grace());
            }
            path
        };
        let old_service_tmp = file("x--0123456789abcdef.yaml.tmp-1-2-3", true);
        let old_map_tmp = file("docktornet-classification.json.tmp-1-2-4", true);
        let young_tmp = file("y--0123456789abcdef.yaml.tmp-1-2-5", false);
        let not_ours = [
            file("notes.yaml.tmp", true),
            file("x.yaml.tmp-1-2", true),
            file("x.yaml.tmp-1-2-z", true),
            // An operator's own dated copy of their base config.
            file("00-base.yaml.tmp-2026-10-21", true),
        ];

        reconciler.sweep_pending_removals().await.unwrap();

        assert!(!old_service_tmp.exists());
        assert!(!old_map_tmp.exists());
        assert!(young_tmp.exists(), "a write may still be using it");
        for path in not_ours {
            assert!(path.exists(), "{} is not a temp file", path.display());
        }
    }

    #[tokio::test]
    async fn a_delayed_apply_decides_on_the_time_of_ingest() {
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

        // The service is gone 5 s later, but the apply only runs after the
        // grace period, held up behind a slow pass.
        reconciler.set_now(6);
        let gone = Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 6,
            agent_version: None,
            containers: vec![],
        };
        let worker = reconciler.ingest_snapshot(&gone).await.unwrap().unwrap();
        reconciler.set_now(6 + GATUS_REMOVAL_GRACE_SECS + 60);
        worker.run().await;

        assert!(
            gatus::service_file_path(&gatus_dir, "m1/app").exists(),
            "the service was gone 5 s when ingested, so its removal is deferred"
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
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 31,
            agent_version: None,
            containers: vec![],
        };
        let worker = reconciler.ingest_snapshot(&gone).await.unwrap().unwrap();
        reconciler.sweep_pending_removals().await.unwrap();
        worker.run().await;

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
                r#"{"id":50,"domain_names":["a.example.com"],"forward_scheme":"http","forward_host":"10.0.0.1","forward_port":3000,"certificate_id":0,"ssl_forced":false,"http2_support":false,"hsts_enabled":false,"block_exploits":true,"allow_websocket_upgrade":true,"enabled":true,"meta":{"docktornet":{"service_id":"m1/app","npm_index":0}}}"#,
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
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                agent_version: None,
                containers: vec![app()],
            },
            1,
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
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 32,
                agent_version: None,
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
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                agent_version: None,
                containers: vec![app()],
            },
            1,
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
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                agent_version: None,
                containers: vec![app()],
            },
            1,
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
    async fn pending_removal_sweep_clears_an_npm_host_already_gone_without_feeding_the_breaker() {
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
        // Someone already deleted the host in NPM.
        let delete = npm_server
            .mock("DELETE", "/api/nginx/proxy-hosts/50")
            .with_status(404)
            .with_body(r#"{"error":{"code":404,"message":"Not Found"}}"#)
            .expect(NPM_BREAKER_THRESHOLD as usize)
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
        reconciler.set_now(1 + GATUS_REMOVAL_GRACE_SECS + 60);

        // As many sweeps as it takes failures to open the breaker, each with a
        // removal past its grace period whose delete finds nothing.
        for sweep in 0..NPM_BREAKER_THRESHOLD {
            storage::add_pending_npm_removal(&pool, "m1/app", 0, 50, 1)
                .await
                .unwrap();

            let report = reconciler.sweep_pending_removals().await.unwrap();

            assert_eq!(report.failed, 0, "sweep {sweep}: {report:?}");
            assert!(
                storage::list_pending_npm_removals(&pool)
                    .await
                    .unwrap()
                    .is_empty(),
                "sweep {sweep}: a host already gone clears its pending row"
            );
            assert_eq!(
                reconciler.npm_breaker.consecutive_failures(),
                0,
                "sweep {sweep}"
            );
            assert_eq!(reconciler.npm_breaker_open_until(), None, "sweep {sweep}");
        }
        delete.assert_async().await;
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
        // No host is marked for the service, so marker adoption finds nothing.
        let _list = npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body("[]")
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
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                agent_version: None,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels: npm_labels("a.example.com"),
                }],
            })
            .await
            .unwrap();

        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m2").unwrap(),
                forward_host: "10.0.0.2".into(),
                taken_at: 1,
                agent_version: None,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_ingests_for_two_machines_both_persist() {
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
        let _list = npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body("[]")
            .create_async()
            .await;
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

        // Both ingests wait at the barrier and run at once; the applies start
        // only once both snapshots are queued, so neither push's pending apply
        // can be taken before the other machine's push lands.
        let start = Arc::new(tokio::sync::Barrier::new(2));
        let ingest = |machine: &str, forward_host: &str, host: &str| {
            let reconciler = reconciler.clone();
            let start = start.clone();
            let snap = Snapshot {
                machine_id: MachineId::new(machine).unwrap(),
                forward_host: forward_host.into(),
                taken_at: 1,
                agent_version: None,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels: npm_host_labels(host),
                }],
            };
            tokio::spawn(async move {
                start.wait().await;
                reconciler.ingest_snapshot(&snap).await.unwrap()
            })
        };
        let m1 = ingest("m1", "10.0.0.1", "a.example.com");
        let m2 = ingest("m2", "10.0.0.2", "b.example.com");
        let workers = [m1.await.unwrap(), m2.await.unwrap()];
        assert!(
            workers.iter().all(Option::is_some),
            "each machine's first push starts its own worker"
        );
        for worker in workers.into_iter().flatten() {
            tokio::spawn(worker.run());
        }
        reconciler.wait_for_applies().await;

        for (machine, npm_id) in [("m1", 1), ("m2", 2)] {
            let service_id = format!("{machine}/app");
            let services = storage::list_services_for_machine(&pool, machine)
                .await
                .unwrap();
            assert_eq!(
                services
                    .iter()
                    .map(|(id, _)| id.as_str())
                    .collect::<Vec<_>>(),
                vec![service_id.as_str()],
                "{machine}'s service persists"
            );
            assert_eq!(
                storage::get_npm_host(&pool, &service_id, 0).await.unwrap(),
                Some(npm_id),
                "{machine}'s NPM mapping persists"
            );
        }
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
                    "hsts_subdomains":0,"block_exploits":1,"allow_websocket_upgrade":1,
                    "meta":{"docktornet":{"service_id":"m1/app","npm_index":0}}}"#,
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
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels("a.example.com"),
            }],
        };
        // Seed the service row (npm_hosts has an FK to it) and the mapping so
        // reconcile takes the get/verify path rather than creating.
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
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
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels("a.example.com"),
            }],
        };
        // Seed the service row and a stale mapping so reconcile takes the
        // get/verify path against id 50.
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
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
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                agent_version: None,
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
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 100,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels("a.example.com"),
            }],
        };
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

        // Service gone only 30s later — within the grace period.
        reconciler.set_now(130);
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 130,
                agent_version: None,
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

    /// A reconciler with NPM and Cloudflare mocks that refuse any delete, for
    /// tests where a removal must stay deferred.  The NPM mock still serves the
    /// token and certificates an apply with NPM removals fetches.
    async fn reconciler_refusing_deletes(
        dir: &std::path::Path,
        pool: Db,
    ) -> (
        Reconciler,
        mockito::ServerGuard,
        mockito::ServerGuard,
        Vec<mockito::Mock>,
    ) {
        let mut npm_server = mockito::Server::new_async().await;
        let mut cf_server = mockito::Server::new_async().await;
        let mocks = vec![
            npm_server
                .mock("POST", "/api/tokens")
                .with_status(200)
                .with_body(r#"{"token":"t"}"#)
                .expect_at_least(1)
                .create_async()
                .await,
            npm_server
                .mock("GET", "/api/nginx/certificates")
                .with_status(200)
                .with_body("[]")
                .expect_at_least(1)
                .create_async()
                .await,
            npm_server
                .mock("DELETE", "/api/nginx/proxy-hosts/50")
                .with_status(200)
                .with_body("{}")
                .expect(0)
                .create_async()
                .await,
            cf_server
                .mock("DELETE", "/zones/z/dns_records/rec-1")
                .with_status(200)
                .with_body(r#"{"success":true}"#)
                .expect(0)
                .create_async()
                .await,
        ];
        let (npm, cf) = test_clients_with_cf(&npm_server.url(), &cf_server.url());
        let reconciler = Reconciler::with_clients(
            pool,
            ReconcileOptions {
                dry_run: false,
                adopt_existing: false,
                gatus_config_dir: dir.join("gatus"),
            },
            npm,
            cf,
        );
        (reconciler, npm_server, cf_server, mocks)
    }

    /// Push `m1/app` with `taken_at`, give it NPM host 50 and Cloudflare record
    /// `rec-1`, then push a snapshot without it at `left_at`.
    async fn publish_then_drop(reconciler: &Reconciler, taken_at: u64, left_at: u64) {
        push_stamped(
            reconciler,
            taken_at,
            vec![ContainerEntry {
                name: "app".into(),
                labels: gatus_labels(),
            }],
        )
        .await;
        storage::set_npm_host(reconciler.pool(), "m1/app", 0, 50)
            .await
            .unwrap();
        storage::set_cf_dns_record(reconciler.pool(), "m1/app", "rec-1")
            .await
            .unwrap();
        push_stamped(reconciler, left_at, vec![]).await;
    }

    /// No delete reached NPM or Cloudflare, and both removals still wait.
    async fn assert_removals_deferred(pool: &Db, mocks: &[mockito::Mock]) {
        for mock in mocks {
            mock.assert_async().await;
        }
        assert_eq!(
            storage::list_pending_npm_removals(pool)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            storage::list_pending_cf_removals(pool).await.unwrap().len(),
            1
        );
    }

    fn agent_now() -> u64 {
        unix_now() as u64
    }

    #[tokio::test]
    async fn agent_clock_behind_the_server_keeps_the_grace_period() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let (reconciler, _npm, _cf, mocks) =
            reconciler_refusing_deletes(dir.path(), pool.clone()).await;

        // The agent's clock runs an hour behind; by it, the service left 10 s
        // after it was last seen.
        let agent_time = agent_now() - 3600;
        publish_then_drop(&reconciler, agent_time, agent_time + 10).await;
        reconciler.sweep_pending_removals().await.unwrap();

        assert_removals_deferred(&pool, &mocks).await;
    }

    #[tokio::test]
    async fn snapshot_stamped_zero_keeps_the_grace_period() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let (reconciler, _npm, _cf, mocks) =
            reconciler_refusing_deletes(dir.path(), pool.clone()).await;

        // An agent that could not read its clock sends taken_at = 0.
        publish_then_drop(&reconciler, 0, agent_now()).await;
        reconciler.sweep_pending_removals().await.unwrap();

        assert_removals_deferred(&pool, &mocks).await;
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
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 200,
                agent_version: None,
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
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: cf_dns_labels("app", "home.example.com"),
            }],
        };
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
            .await
            .unwrap();

        // The container is decommissioned: an empty snapshot, well past grace.
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: (1 + GATUS_REMOVAL_GRACE_SECS + 1) as u64,
                agent_version: None,
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
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 100,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: cf_dns_labels("app", "home.example.com"),
            }],
        };
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
            .await
            .unwrap();

        // Gone only 30s later — within grace.
        reconciler.set_now(130);
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 130,
                agent_version: None,
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
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 200,
                agent_version: None,
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
                r#"{"success":true,"result":{"id":"rec-1","name":"app.example.com","content":"home.example.com","proxied":false,"comment":"docktornet:m1/app"}}"#,
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
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: cf_dns_labels("app", "home.example.com"),
            }],
        };
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
            .await
            .unwrap();

        reconciler.reconcile_snapshot(&snap).await.unwrap();

        get.assert_async().await;
        update.assert_async().await;
    }

    #[tokio::test]
    async fn cf_record_deleted_when_a_running_container_drops_its_cf_labels() {
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

        let reconciler = reconciler_on(pool.clone(), &dir.path().join("gatus"), &cf_server);

        // The service owns a Cloudflare record and a Gatus monitor.
        let mut labels = gatus_labels();
        labels.extend(cf_dns_labels("app", "home.example.com"));
        storage::upsert_snapshot(
            &pool,
            &m1_snapshot(vec![ContainerEntry {
                name: "app".into(),
                labels,
            }]),
            1,
        )
        .await
        .unwrap();
        storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
            .await
            .unwrap();

        // The container keeps running with only its Gatus labels.
        push(&reconciler, 31, vec![gatus_entry("app")]).await;

        delete.assert_async().await;
        assert_eq!(
            storage::get_cf_dns_record(&pool, "m1/app").await.unwrap(),
            None,
            "the cf record mapping goes with the record"
        );
        assert!(
            storage::list_pending_cf_removals(&pool)
                .await
                .unwrap()
                .is_empty(),
            "a service still running owes no pending removal"
        );
        assert_eq!(
            storage::list_services_for_machine(&pool, "m1")
                .await
                .unwrap()
                .len(),
            1,
            "the service itself stays"
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
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                agent_version: None,
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
        // No host is marked for the service, so marker adoption finds nothing.
        let list = npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
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
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                agent_version: None,
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
        // Both hosts are unmapped, but the host list is fetched once per pass.
        list.assert_async().await;
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
        // No host is marked for the service, so marker adoption finds nothing.
        let _list = npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body("[]")
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
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                agent_version: None,
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
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                agent_version: None,
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
        // No host is marked for the service, so marker adoption finds nothing.
        let _list = npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body("[]")
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
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels("a.example.com"),
            }],
            agent_version: None,
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
    async fn npm_adopts_marked_host_without_create_or_flag() {
        // The database is empty and DOCKTORNET_ADOPT_EXISTING is off, but the
        // host carries this service's marker, so it is adopted with no create.
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
        let create = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(400)
            .with_body(r#"{"error":{"message":"domain already in use"}}"#)
            .expect(0)
            .create_async()
            .await;
        let _list = npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(
                r#"[{"id":90,"domain_names":["a.example.com"],
                     "meta":{"docktornet":{"service_id":"m1/app","npm_index":0}}}]"#,
            )
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
                    "hsts_subdomains":0,"block_exploits":1,"allow_websocket_upgrade":1,
                    "meta":{"docktornet":{"service_id":"m1/app","npm_index":0}}}"#,
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
                adopt_existing: false,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );

        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                agent_version: None,
                containers: vec![ContainerEntry {
                    name: "app".into(),
                    labels: npm_host_labels("a.example.com"),
                }],
            })
            .await
            .unwrap();

        // No create and no PUT was issued, but the mapping was still adopted.
        create.assert_async().await;
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
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: 1,
                agent_version: None,
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
        // No host is marked for the service, so marker adoption finds nothing.
        let _list = npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body("[]")
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
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
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
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels: npm_host_labels("a.example.com"),
            }],
        };
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

        // Service leaves the snapshot, well past the grace period.
        reconciler
            .reconcile_snapshot(&Snapshot {
                machine_id: MachineId::new("m1").unwrap(),
                forward_host: "10.0.0.1".into(),
                taken_at: (1 + GATUS_REMOVAL_GRACE_SECS + 1) as u64,
                agent_version: None,
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

    /// An NPM mock answering the token and certificate calls every pass makes.
    async fn npm_server_with_auth() -> mockito::ServerGuard {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"t"}"#)
            .create_async()
            .await;
        server
            .mock("GET", "/api/nginx/certificates")
            .with_status(200)
            .with_body("[]")
            .create_async()
            .await;
        server
    }

    fn reconciler_for(
        pool: Db,
        dir: &std::path::Path,
        npm_url: &str,
        cf_url: &str,
        dry_run: bool,
        adopt_existing: bool,
    ) -> Reconciler {
        let (npm, cf) = test_clients_with_cf(npm_url, cf_url);
        Reconciler::with_clients(
            pool,
            ReconcileOptions {
                dry_run,
                adopt_existing,
                gatus_config_dir: dir.join("gatus"),
            },
            npm,
            cf,
        )
    }

    /// Mocks on `server` that fail the test if any request reaches it.
    async fn refuse_every_request(server: &mut mockito::ServerGuard) -> Vec<mockito::Mock> {
        let mut mocks = Vec::new();
        for method in ["GET", "POST", "PUT", "DELETE"] {
            mocks.push(
                server
                    .mock(method, mockito::Matcher::Any)
                    .with_status(200)
                    .with_body("{}")
                    .expect(0)
                    .create_async()
                    .await,
            );
        }
        mocks
    }

    fn snap_with(labels: BTreeMap<String, String>) -> Snapshot {
        Snapshot {
            machine_id: MachineId::new("m1").unwrap(),
            forward_host: "10.0.0.1".into(),
            taken_at: 1,
            agent_version: None,
            containers: vec![ContainerEntry {
                name: "app".into(),
                labels,
            }],
        }
    }

    #[tokio::test]
    async fn npm_create_writes_ownership_marker() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut npm_server = npm_server_with_auth().await;
        npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body("[]")
            .create_async()
            .await;
        let create = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "meta": {"docktornet": {"service_id": "m1/app", "npm_index": 0}}
            })))
            .with_status(200)
            .with_body(r#"{"id":5,"domain_names":["a.example.com"]}"#)
            .expect(1)
            .create_async()
            .await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &npm_server.url(),
            &npm_server.url(),
            false,
            false,
        );
        reconciler
            .reconcile_snapshot(&snap_with(npm_host_labels("a.example.com")))
            .await
            .unwrap();

        create.assert_async().await;
        assert_eq!(
            storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
            Some(5)
        );
    }

    /// NPM's stored state for host 50 serving `a.example.com` exactly as
    /// `npm_host_labels` asks, with `meta` as given. `certificate_id` 0 is how
    /// NPM stores a host with no certificate.
    fn stored_host_50(meta: serde_json::Value) -> String {
        serde_json::json!({
            "id": 50, "domain_names": ["a.example.com"], "forward_scheme": "http",
            "forward_host": "10.0.0.1", "forward_port": 3000, "certificate_id": 0,
            "ssl_forced": 0, "http2_support": 0, "hsts_enabled": 0,
            "hsts_subdomains": 0, "block_exploits": 1, "allow_websocket_upgrade": 1,
            "meta": meta
        })
        .to_string()
    }

    fn app_marker() -> serde_json::Value {
        serde_json::json!({"service_id": "m1/app", "npm_index": 0})
    }

    #[tokio::test]
    async fn npm_update_keeps_every_other_meta_key() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut npm_server = npm_server_with_auth().await;
        // The port drifted, so the host is updated; its meta holds NPM's own
        // keys and one set in the UI.
        let mut stored: serde_json::Value =
            serde_json::from_str(&stored_host_50(serde_json::json!({
                "nginx_online": true,
                "nginx_err": null,
                "dns_challenge": true,
                "custom": "kept",
                "docktornet": app_marker()
            })))
            .unwrap();
        stored["forward_port"] = 9999.into();
        npm_server
            .mock("GET", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(stored.to_string())
            .create_async()
            .await;
        let update = npm_server
            .mock("PUT", "/api/nginx/proxy-hosts/50")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "forward_port": 3000,
                "meta": {
                    "nginx_online": true,
                    "nginx_err": null,
                    "dns_challenge": true,
                    "custom": "kept",
                    "docktornet": app_marker()
                }
            })))
            .with_status(200)
            .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
            .expect(1)
            .create_async()
            .await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &npm_server.url(),
            &npm_server.url(),
            false,
            false,
        );
        let snap = snap_with(npm_host_labels("a.example.com"));
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

        reconciler.reconcile_snapshot(&snap).await.unwrap();

        update.assert_async().await;
    }

    #[tokio::test]
    async fn npm_nginx_status_keys_alone_are_not_drift() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut npm_server = npm_server_with_auth().await;
        npm_server
            .mock("GET", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(stored_host_50(serde_json::json!({
                "nginx_online": false,
                "nginx_err": "nginx: [emerg] something broke",
                "docktornet": app_marker()
            })))
            .create_async()
            .await;
        let update = npm_server
            .mock("PUT", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
            .expect(0)
            .create_async()
            .await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &npm_server.url(),
            &npm_server.url(),
            false,
            false,
        );
        let snap = snap_with(npm_host_labels("a.example.com"));
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

        reconciler.reconcile_snapshot(&snap).await.unwrap();
        reconciler.full_resync().await.unwrap();

        update.assert_async().await;
    }

    /// A reconciler over `m1/app` serving `host` as NPM host 50, with NPM
    /// answering `stored` for that host and expecting `puts` updates.
    async fn npm_resync_fixture(
        dir: &std::path::Path,
        host: &str,
        stored: String,
        puts: usize,
    ) -> (Reconciler, mockito::ServerGuard, mockito::Mock) {
        let pool = db::connect(&dir.join("t.db")).await.unwrap();
        let mut npm_server = npm_server_with_auth().await;
        npm_server
            .mock("GET", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(stored)
            .create_async()
            .await;
        let update = npm_server
            .mock("PUT", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
            .expect(puts)
            .create_async()
            .await;
        let reconciler = reconciler_for(
            pool.clone(),
            dir,
            &npm_server.url(),
            &npm_server.url(),
            false,
            false,
        );
        let snap = snap_with(npm_host_labels(host));
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();
        (reconciler, npm_server, update)
    }

    #[tokio::test]
    async fn full_resync_repairs_drifted_npm_forward_port() {
        let dir = tempdir().unwrap();
        let mut stored: serde_json::Value = serde_json::from_str(&stored_host_50(
            serde_json::json!({"docktornet": app_marker()}),
        ))
        .unwrap();
        stored["forward_port"] = 9999.into();
        let (reconciler, _npm_server, update) =
            npm_resync_fixture(dir.path(), "a.example.com", stored.to_string(), 1).await;

        let report = reconciler.full_resync().await.unwrap();

        update.assert_async().await;
        assert_eq!(
            report,
            ResyncReport {
                verified: 0,
                repaired: 1,
                failed: 0
            }
        );
    }

    #[tokio::test]
    async fn full_resync_leaves_npm_host_in_sync_alone() {
        let dir = tempdir().unwrap();
        let stored = stored_host_50(serde_json::json!({"docktornet": app_marker()}));
        let (reconciler, _npm_server, update) =
            npm_resync_fixture(dir.path(), "a.example.com", stored, 0).await;

        let report = reconciler.full_resync().await.unwrap();

        update.assert_async().await;
        assert_eq!(
            report,
            ResyncReport {
                verified: 1,
                repaired: 0,
                failed: 0
            }
        );
    }

    #[tokio::test]
    async fn full_resync_ignores_npm_sorting_the_domain_names() {
        let dir = tempdir().unwrap();
        // NPM sorts domain_names on every write, whatever order the labels use.
        let mut stored: serde_json::Value = serde_json::from_str(&stored_host_50(
            serde_json::json!({"docktornet": app_marker()}),
        ))
        .unwrap();
        stored["domain_names"] = serde_json::json!(["a.example.com", "b.example.com"]);
        let (reconciler, _npm_server, update) = npm_resync_fixture(
            dir.path(),
            "b.example.com,a.example.com",
            stored.to_string(),
            0,
        )
        .await;

        let report = reconciler.full_resync().await.unwrap();

        update.assert_async().await;
        assert_eq!(report.verified, 1);
    }

    #[tokio::test]
    async fn full_resync_writes_missing_npm_marker_once() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut npm_server = npm_server_with_auth().await;
        // Before the upgrade the host carries no marker...
        let unmarked = npm_server
            .mock("GET", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(stored_host_50(serde_json::json!({"nginx_online": true})))
            .expect(1)
            .create_async()
            .await;
        // ...so the first resync writes it, keeping NPM's own key.
        let update = npm_server
            .mock("PUT", "/api/nginx/proxy-hosts/50")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "meta": {"nginx_online": true, "docktornet": app_marker()}
            })))
            .with_status(200)
            .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
            .expect(1)
            .create_async()
            .await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &npm_server.url(),
            &npm_server.url(),
            false,
            false,
        );
        let snap = snap_with(npm_host_labels("a.example.com"));
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

        let first = reconciler.full_resync().await.unwrap();
        unmarked.assert_async().await;
        update.assert_async().await;
        assert_eq!(
            first,
            ResyncReport {
                verified: 0,
                repaired: 1,
                failed: 0
            }
        );

        // NPM now holds the marker, and the second resync leaves it alone.
        unmarked.remove_async().await;
        npm_server
            .mock("GET", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(stored_host_50(serde_json::json!({
                "nginx_online": true,
                "docktornet": app_marker()
            })))
            .create_async()
            .await;
        let second = reconciler.full_resync().await.unwrap();
        update.assert_async().await;
        assert_eq!(
            second,
            ResyncReport {
                verified: 1,
                repaired: 0,
                failed: 0
            }
        );
    }

    #[tokio::test]
    async fn full_resync_counts_a_recreated_npm_host_as_repaired() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut npm_server = npm_server_with_auth().await;
        npm_server
            .mock("GET", "/api/nginx/proxy-hosts/50")
            .with_status(404)
            .create_async()
            .await;
        let create = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(r#"{"id":51,"domain_names":["a.example.com"]}"#)
            .expect(1)
            .create_async()
            .await;
        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &npm_server.url(),
            &npm_server.url(),
            false,
            false,
        );
        let snap = snap_with(npm_host_labels("a.example.com"));
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

        let report = reconciler.full_resync().await.unwrap();

        create.assert_async().await;
        assert_eq!(
            report,
            ResyncReport {
                verified: 0,
                repaired: 1,
                failed: 0
            }
        );
        assert_eq!(
            storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
            Some(51)
        );
    }

    #[tokio::test]
    async fn npm_marker_adoption_drops_domains_the_labels_no_longer_list() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut npm_server = npm_server_with_auth().await;
        let extra = serde_json::json!({
            "id": 50, "domain_names": ["a.example.com", "old.example.com"],
            "forward_scheme": "http", "forward_host": "10.0.0.1", "forward_port": 3000,
            "certificate_id": 0, "meta": {"docktornet": app_marker()}
        });
        npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(format!("[{extra}]"))
            .create_async()
            .await;
        npm_server
            .mock("GET", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(extra.to_string())
            .create_async()
            .await;
        let create = npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(r#"{"id":51,"domain_names":["a.example.com"]}"#)
            .expect(0)
            .create_async()
            .await;
        let update = npm_server
            .mock("PUT", "/api/nginx/proxy-hosts/50")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "domain_names": ["a.example.com"]
            })))
            .with_status(200)
            .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
            .expect(1)
            .create_async()
            .await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &npm_server.url(),
            &npm_server.url(),
            false,
            false,
        );
        reconciler
            .reconcile_snapshot(&snap_with(npm_host_labels("a.example.com")))
            .await
            .unwrap();

        create.assert_async().await;
        update.assert_async().await;
        assert_eq!(
            storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
            Some(50)
        );
    }

    #[tokio::test]
    async fn npm_hostname_adoption_writes_the_marker() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut npm_server = npm_server_with_auth().await;
        npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(r#"[{"id":50,"domain_names":["a.example.com"],"meta":{}}]"#)
            .create_async()
            .await;
        npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(400)
            .with_body(r#"{"error":{"message":"domain already in use"}}"#)
            .create_async()
            .await;
        // In sync apart from the missing marker.
        npm_server
            .mock("GET", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(stored_host_50(serde_json::json!({})))
            .create_async()
            .await;
        let update = npm_server
            .mock("PUT", "/api/nginx/proxy-hosts/50")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "meta": {"docktornet": app_marker()}
            })))
            .with_status(200)
            .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
            .expect(1)
            .create_async()
            .await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &npm_server.url(),
            &npm_server.url(),
            false,
            true,
        );
        reconciler
            .reconcile_snapshot(&snap_with(npm_host_labels("a.example.com")))
            .await
            .unwrap();

        update.assert_async().await;
        assert_eq!(
            storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
            Some(50)
        );
    }

    #[tokio::test]
    async fn npm_hostname_adoption_refuses_a_host_marked_for_another_service() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut npm_server = npm_server_with_auth().await;
        npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(
                r#"[{"id":50,"domain_names":["a.example.com"],
                     "meta":{"docktornet":{"service_id":"m2/other","npm_index":0}}}]"#,
            )
            .create_async()
            .await;
        npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(400)
            .with_body(r#"{"error":{"message":"domain already in use"}}"#)
            .create_async()
            .await;
        let get = npm_server
            .mock("GET", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(stored_host_50(serde_json::json!({})))
            .expect(0)
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
                adopt_existing: true,
                gatus_config_dir: dir.path().join("gatus"),
            },
            npm,
            cf,
        );
        let snap = snap_with(npm_host_labels("a.example.com"));
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        let host = docktornet_core::labels::parse(&npm_host_labels("a.example.com"))
            .npm
            .remove(0);
        let (token, certs) = reconciler.fetch_npm_context().await.unwrap();

        let err = reconciler
            .reconcile_npm(
                "m1/app",
                &host,
                "10.0.0.1",
                &token,
                &certs,
                &NpmHostList::default(),
            )
            .await
            .unwrap_err();

        match err {
            ReconcileError::MarkedForOtherService {
                service_id, owner, ..
            } => {
                assert_eq!(service_id, "m1/app");
                assert_eq!(owner, "m2/other (index 0)");
            }
            other => panic!("expected MarkedForOtherService, got {other:?}"),
        }
        get.assert_async().await;
        update.assert_async().await;
        assert_eq!(
            storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn npm_host_adopted_earlier_in_the_pass_is_refused_to_the_next_service() {
        // Two containers label the same domain and one unmarked host serves
        // it. The first adopts it by hostname and marks it; the second must
        // see that marker, though the host list was fetched before it.
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut npm_server = npm_server_with_auth().await;
        let list = npm_server
            .mock("GET", "/api/nginx/proxy-hosts")
            .with_status(200)
            .with_body(r#"[{"id":50,"domain_names":["a.example.com"],"meta":{}}]"#)
            .expect(1)
            .create_async()
            .await;
        npm_server
            .mock("POST", "/api/nginx/proxy-hosts")
            .with_status(400)
            .with_body(r#"{"error":{"message":"domain already in use"}}"#)
            .create_async()
            .await;
        npm_server
            .mock("GET", "/api/nginx/proxy-hosts/50")
            .with_status(200)
            .with_body(stored_host_50(serde_json::json!({})))
            .create_async()
            .await;
        let update = npm_server
            .mock("PUT", "/api/nginx/proxy-hosts/50")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "meta": {"docktornet": app_marker()}
            })))
            .with_status(200)
            .with_body(r#"{"id":50,"domain_names":["a.example.com"]}"#)
            .expect(1)
            .create_async()
            .await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &npm_server.url(),
            &npm_server.url(),
            false,
            true,
        );
        let mut snap = snap_with(npm_host_labels("a.example.com"));
        snap.containers.push(ContainerEntry {
            name: "app2".into(),
            labels: npm_host_labels("a.example.com"),
        });

        let result = reconciler.reconcile_snapshot(&snap).await;

        assert!(result.is_err(), "the second service must be refused");
        list.assert_async().await;
        update.assert_async().await;
        assert_eq!(
            storage::get_npm_host(&pool, "m1/app", 0).await.unwrap(),
            Some(50)
        );
        assert_eq!(
            storage::get_npm_host(&pool, "m1/app2", 0).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn dry_run_writes_no_npm_marker() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut npm_server = mockito::Server::new_async().await;
        // Dry-run reaches NPM for nothing: no token, no read, no write.
        let refused = refuse_every_request(&mut npm_server).await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &npm_server.url(),
            &npm_server.url(),
            true,
            true,
        );
        let snap = snap_with(npm_host_labels("a.example.com"));
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        storage::set_npm_host(&pool, "m1/app", 0, 50).await.unwrap();

        reconciler.reconcile_snapshot(&snap).await.unwrap();
        assert_eq!(reconciler.full_resync().await.unwrap().failed, 0);

        for mock in refused {
            mock.assert_async().await;
        }
    }

    /// A Cloudflare mock answering the zone call every pass makes.
    async fn cf_server_with_zone() -> mockito::ServerGuard {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/zones/z")
            .with_status(200)
            .with_body(r#"{"success":true,"result":{"id":"z","name":"example.com"}}"#)
            .create_async()
            .await;
        server
    }

    fn cf_list(records: serde_json::Value) -> String {
        serde_json::json!({"success": true, "result": records}).to_string()
    }

    fn cf_record(id: &str, comment: Option<&str>) -> serde_json::Value {
        serde_json::json!({
            "id": id, "name": "app.example.com", "content": "home.example.com",
            "proxied": false, "comment": comment
        })
    }

    fn cf_one(record: serde_json::Value) -> String {
        serde_json::json!({"success": true, "result": record}).to_string()
    }

    #[tokio::test]
    async fn cf_create_writes_ownership_marker() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut cf_server = cf_server_with_zone().await;
        let lookup = cf_server
            .mock("GET", "/zones/z/dns_records")
            .match_query(mockito::Matcher::UrlEncoded(
                "comment.exact".into(),
                "docktornet:m1/app".into(),
            ))
            .with_status(200)
            .with_body(cf_list(serde_json::json!([])))
            .expect(1)
            .create_async()
            .await;
        let create = cf_server
            .mock("POST", "/zones/z/dns_records")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "name": "app.example.com",
                "comment": "docktornet:m1/app"
            })))
            .with_status(200)
            .with_body(cf_one(cf_record("rec-1", Some("docktornet:m1/app"))))
            .expect(1)
            .create_async()
            .await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &cf_server.url(),
            &cf_server.url(),
            false,
            false,
        );
        reconciler
            .reconcile_snapshot(&snap_with(cf_dns_labels("app", "home.example.com")))
            .await
            .unwrap();

        lookup.assert_async().await;
        create.assert_async().await;
        assert_eq!(
            storage::get_cf_dns_record(&pool, "m1/app").await.unwrap(),
            Some("rec-1".to_string())
        );
    }

    #[tokio::test]
    async fn full_resync_writes_missing_cf_marker_once() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut cf_server = cf_server_with_zone().await;
        let unmarked = cf_server
            .mock("GET", "/zones/z/dns_records/rec-1")
            .with_status(200)
            .with_body(cf_one(cf_record("rec-1", None)))
            .expect(1)
            .create_async()
            .await;
        let update = cf_server
            .mock("PUT", "/zones/z/dns_records/rec-1")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "comment": "docktornet:m1/app"
            })))
            .with_status(200)
            .with_body(cf_one(cf_record("rec-1", Some("docktornet:m1/app"))))
            .expect(1)
            .create_async()
            .await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &cf_server.url(),
            &cf_server.url(),
            false,
            false,
        );
        let snap = snap_with(cf_dns_labels("app", "home.example.com"));
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
            .await
            .unwrap();

        let first = reconciler.full_resync().await.unwrap();
        unmarked.assert_async().await;
        update.assert_async().await;
        assert_eq!(
            first,
            ResyncReport {
                verified: 0,
                repaired: 1,
                failed: 0
            }
        );

        unmarked.remove_async().await;
        cf_server
            .mock("GET", "/zones/z/dns_records/rec-1")
            .with_status(200)
            .with_body(cf_one(cf_record("rec-1", Some("docktornet:m1/app"))))
            .create_async()
            .await;
        let second = reconciler.full_resync().await.unwrap();
        update.assert_async().await;
        assert_eq!(
            second,
            ResyncReport {
                verified: 1,
                repaired: 0,
                failed: 0
            }
        );
    }

    #[tokio::test]
    async fn full_resync_counts_a_recreated_cf_record_as_repaired() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut cf_server = cf_server_with_zone().await;
        cf_server
            .mock("GET", "/zones/z/dns_records/rec-1")
            .with_status(404)
            .create_async()
            .await;
        let create = cf_server
            .mock("POST", "/zones/z/dns_records")
            .with_status(200)
            .with_body(cf_one(cf_record("rec-2", Some("docktornet:m1/app"))))
            .expect(1)
            .create_async()
            .await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &cf_server.url(),
            &cf_server.url(),
            false,
            false,
        );
        let snap = snap_with(cf_dns_labels("app", "home.example.com"));
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
            .await
            .unwrap();

        let report = reconciler.full_resync().await.unwrap();

        create.assert_async().await;
        assert_eq!(
            report,
            ResyncReport {
                verified: 0,
                repaired: 1,
                failed: 0
            }
        );
    }

    /// A snapshot of `m1/app` and `m1/web`, each asking for a Cloudflare record.
    fn two_cf_services_snap() -> Snapshot {
        let mut snap = snap_with(cf_dns_labels("app", "home.example.com"));
        snap.containers.push(ContainerEntry {
            name: "web".into(),
            labels: cf_dns_labels("web", "home.example.com"),
        });
        snap
    }

    /// The Cloudflare record `id` for service `m1/<name>`, as its labels want it.
    fn cf_service_record(id: &str, name: &str) -> String {
        cf_one(serde_json::json!({
            "id": id, "name": format!("{name}.example.com"), "content": "home.example.com",
            "proxied": false, "comment": format!("docktornet:m1/{name}")
        }))
    }

    /// A Cloudflare mock holding `rec-app` and `rec-web` as
    /// `two_cf_services_snap` wants them, and expecting one zone fetch.
    async fn cf_server_with_two_records() -> (mockito::ServerGuard, mockito::Mock) {
        let mut cf_server = mockito::Server::new_async().await;
        let zone = cf_server
            .mock("GET", "/zones/z")
            .with_status(200)
            .with_body(r#"{"success":true,"result":{"id":"z","name":"example.com"}}"#)
            .expect(1)
            .create_async()
            .await;
        for name in ["app", "web"] {
            cf_server
                .mock("GET", format!("/zones/z/dns_records/rec-{name}").as_str())
                .with_status(200)
                .with_body(cf_service_record(&format!("rec-{name}"), name))
                .create_async()
                .await;
        }
        (cf_server, zone)
    }

    #[tokio::test]
    async fn full_resync_fetches_the_cf_zone_once_for_two_services() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let (cf_server, zone) = cf_server_with_two_records().await;
        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &cf_server.url(),
            &cf_server.url(),
            false,
            false,
        );
        let snap = two_cf_services_snap();
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        for name in ["app", "web"] {
            storage::set_cf_dns_record(&pool, &format!("m1/{name}"), &format!("rec-{name}"))
                .await
                .unwrap();
        }

        let report = reconciler.full_resync().await.unwrap();

        zone.assert_async().await;
        assert_eq!(
            report,
            ResyncReport {
                verified: 2,
                repaired: 0,
                failed: 0
            }
        );
    }

    #[tokio::test]
    async fn cf_apply_then_resync_fetches_the_zone_once() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let (mut cf_server, zone) = cf_server_with_two_records().await;
        cf_server
            .mock("GET", "/zones/z/dns_records")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(cf_list(serde_json::json!([])))
            .create_async()
            .await;
        for name in ["app", "web"] {
            cf_server
                .mock("POST", "/zones/z/dns_records")
                .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                    "name": format!("{name}.example.com")
                })))
                .with_status(200)
                .with_body(cf_service_record(&format!("rec-{name}"), name))
                .expect(1)
                .create_async()
                .await;
        }
        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &cf_server.url(),
            &cf_server.url(),
            false,
            false,
        );

        reconciler
            .reconcile_snapshot(&two_cf_services_snap())
            .await
            .unwrap();
        let report = reconciler.full_resync().await.unwrap();

        zone.assert_async().await;
        assert_eq!(report.verified, 2);
        assert_eq!(report.failed, 0);
    }

    /// `m1/app` with NPM host 50, Cloudflare record `rec-1` and a Gatus monitor.
    async fn seed_published_app(pool: &Db) {
        let mut labels = npm_host_labels("a.example.com");
        labels.extend(cf_dns_labels("app", "home.example.com"));
        labels.extend(gatus_labels());
        storage::upsert_snapshot(pool, &snap_with(labels), 1)
            .await
            .unwrap();
        storage::set_npm_host(pool, "m1/app", 0, 50).await.unwrap();
        storage::set_cf_dns_record(pool, "m1/app", "rec-1")
            .await
            .unwrap();
    }

    /// A Cloudflare mock holding `rec-1` exactly as `seed_published_app` wants
    /// it, expecting `reads` reads of the record.
    async fn cf_server_with_app_record(reads: usize) -> (mockito::ServerGuard, mockito::Mock) {
        let mut cf_server = cf_server_with_zone().await;
        let get = cf_server
            .mock("GET", "/zones/z/dns_records/rec-1")
            .with_status(200)
            .with_body(cf_one(cf_record("rec-1", Some("docktornet:m1/app"))))
            .expect(reads)
            .create_async()
            .await;
        (cf_server, get)
    }

    #[tokio::test]
    async fn full_resync_npm_failures_open_the_breaker_and_then_skip_npm() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        seed_published_app(&pool).await;
        // A pending NPM removal past grace, so the sweep has NPM work too.
        storage::add_pending_npm_removal(&pool, "m1/old", 0, 60, 1)
            .await
            .unwrap();
        let mut npm_server = mockito::Server::new_async().await;
        let token = npm_server
            .mock("POST", "/api/tokens")
            .with_status(500)
            .expect(NPM_BREAKER_THRESHOLD as usize)
            .create_async()
            .await;
        let (cf_server, cf_get) =
            cf_server_with_app_record(NPM_BREAKER_THRESHOLD as usize + 1).await;
        let reconciler = reconciler_for(
            pool,
            dir.path(),
            &npm_server.url(),
            &cf_server.url(),
            false,
            false,
        );

        for _ in 0..NPM_BREAKER_THRESHOLD {
            reconciler.full_resync().await.unwrap();
        }
        // The breaker is open: this pass sends NPM nothing, while Gatus and
        // Cloudflare are still verified.
        let service_file = gatus::service_file_path(dir.path().join("gatus"), "m1/app");
        std::fs::remove_file(&service_file).unwrap();
        let report = reconciler.full_resync().await.unwrap();

        token.assert_async().await;
        cf_get.assert_async().await;
        assert!(service_file.exists(), "gatus resync must still run");
        assert_eq!(report.failed, 0, "skipped NPM hosts are not failures");
        assert_eq!(report.repaired, 1, "the rewritten gatus file");
        assert_eq!(
            report.verified, 1,
            "the cloudflare record, not the NPM host"
        );
    }

    #[tokio::test]
    async fn clean_full_resync_resets_the_npm_breaker() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        seed_published_app(&pool).await;
        let mut npm_server = mockito::Server::new_async().await;
        let (cf_server, _cf_get) = cf_server_with_app_record(5).await;
        let reconciler = reconciler_for(
            pool,
            dir.path(),
            &npm_server.url(),
            &cf_server.url(),
            false,
            false,
        );
        let failing_token = |server: &mut mockito::ServerGuard| {
            server
                .mock("POST", "/api/tokens")
                .with_status(500)
                .expect(NPM_BREAKER_THRESHOLD as usize - 1)
                .create()
        };

        // One failure short of the threshold.
        let failing = failing_token(&mut npm_server);
        for _ in 1..NPM_BREAKER_THRESHOLD {
            reconciler.full_resync().await.unwrap();
        }
        failing.assert_async().await;
        failing.remove_async().await;

        // A clean pass: the host is in sync.
        let healthy = vec![
            npm_server
                .mock("POST", "/api/tokens")
                .with_status(200)
                .with_body(r#"{"token":"t"}"#)
                .create_async()
                .await,
            npm_server
                .mock("GET", "/api/nginx/certificates")
                .with_status(200)
                .with_body("[]")
                .create_async()
                .await,
            npm_server
                .mock("GET", "/api/nginx/proxy-hosts/50")
                .with_status(200)
                .with_body(stored_host_50(
                    serde_json::json!({ "docktornet": app_marker() }),
                ))
                .create_async()
                .await,
        ];
        assert_eq!(reconciler.full_resync().await.unwrap().failed, 0);
        for mock in healthy {
            mock.assert_async().await;
            mock.remove_async().await;
        }

        // The success cleared the count, so the same run of failures again
        // still leaves the breaker closed: every pass reaches NPM.
        let failing = failing_token(&mut npm_server);
        for _ in 1..NPM_BREAKER_THRESHOLD {
            reconciler.full_resync().await.unwrap();
        }
        failing.assert_async().await;
        assert!(!reconciler.npm_breaker.is_open(std::time::Instant::now()));
    }

    #[tokio::test]
    async fn cf_adopts_marked_record_without_create_or_flag() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut cf_server = cf_server_with_zone().await;
        cf_server
            .mock("GET", "/zones/z/dns_records")
            .match_query(mockito::Matcher::UrlEncoded(
                "comment.exact".into(),
                "docktornet:m1/app".into(),
            ))
            .with_status(200)
            .with_body(cf_list(serde_json::json!([cf_record(
                "rec-9",
                Some("docktornet:m1/app")
            )])))
            .create_async()
            .await;
        let create = cf_server
            .mock("POST", "/zones/z/dns_records")
            .with_status(200)
            .with_body(cf_one(cf_record("rec-new", Some("docktornet:m1/app"))))
            .expect(0)
            .create_async()
            .await;
        let update = cf_server
            .mock("PUT", "/zones/z/dns_records/rec-9")
            .with_status(200)
            .with_body(cf_one(cf_record("rec-9", Some("docktornet:m1/app"))))
            .expect(0)
            .create_async()
            .await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &cf_server.url(),
            &cf_server.url(),
            false,
            false,
        );
        reconciler
            .reconcile_snapshot(&snap_with(cf_dns_labels("app", "home.example.com")))
            .await
            .unwrap();

        create.assert_async().await;
        update.assert_async().await;
        assert_eq!(
            storage::get_cf_dns_record(&pool, "m1/app").await.unwrap(),
            Some("rec-9".to_string())
        );
    }

    /// A Cloudflare mock where the marker lookup finds nothing, the create
    /// conflicts, and the name lookup finds `existing`.
    async fn cf_server_for_hostname_adoption(existing: serde_json::Value) -> mockito::ServerGuard {
        let mut cf_server = cf_server_with_zone().await;
        cf_server
            .mock("GET", "/zones/z/dns_records")
            .match_query(mockito::Matcher::UrlEncoded(
                "comment.exact".into(),
                "docktornet:m1/app".into(),
            ))
            .with_status(200)
            .with_body(cf_list(serde_json::json!([])))
            .create_async()
            .await;
        cf_server
            .mock("POST", "/zones/z/dns_records")
            .with_status(400)
            .with_body(
                r#"{"success":false,"errors":[{"code":81053,"message":"exists"}],"result":null}"#,
            )
            .create_async()
            .await;
        cf_server
            .mock("GET", "/zones/z/dns_records")
            .match_query(mockito::Matcher::UrlEncoded(
                "name".into(),
                "app.example.com".into(),
            ))
            .with_status(200)
            .with_body(cf_list(serde_json::json!([existing])))
            .create_async()
            .await;
        cf_server
    }

    #[tokio::test]
    async fn cf_hostname_adoption_writes_the_marker() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut cf_server =
            cf_server_for_hostname_adoption(cf_record("rec-5", Some("set by hand"))).await;
        let update = cf_server
            .mock("PUT", "/zones/z/dns_records/rec-5")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "comment": "docktornet:m1/app"
            })))
            .with_status(200)
            .with_body(cf_one(cf_record("rec-5", Some("docktornet:m1/app"))))
            .expect(1)
            .create_async()
            .await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &cf_server.url(),
            &cf_server.url(),
            false,
            true,
        );
        reconciler
            .reconcile_snapshot(&snap_with(cf_dns_labels("app", "home.example.com")))
            .await
            .unwrap();

        update.assert_async().await;
        assert_eq!(
            storage::get_cf_dns_record(&pool, "m1/app").await.unwrap(),
            Some("rec-5".to_string())
        );
    }

    #[tokio::test]
    async fn cf_hostname_adoption_refuses_a_record_marked_for_another_service() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut cf_server =
            cf_server_for_hostname_adoption(cf_record("rec-5", Some("docktornet:m2/other"))).await;
        let update = cf_server
            .mock("PUT", "/zones/z/dns_records/rec-5")
            .with_status(200)
            .with_body(cf_one(cf_record("rec-5", Some("docktornet:m1/app"))))
            .expect(0)
            .create_async()
            .await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &cf_server.url(),
            &cf_server.url(),
            false,
            true,
        );
        let dns = docktornet_core::labels::parse(&cf_dns_labels("app", "home.example.com"))
            .cf_dns
            .unwrap();
        let snap = snap_with(cf_dns_labels("app", "home.example.com"));
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();

        let err = reconciler
            .reconcile_cloudflare("m1/app", &dns)
            .await
            .unwrap_err();

        match err {
            ReconcileError::MarkedForOtherService {
                service_id, owner, ..
            } => {
                assert_eq!(service_id, "m1/app");
                assert_eq!(owner, "m2/other");
            }
            other => panic!("expected MarkedForOtherService, got {other:?}"),
        }
        update.assert_async().await;
        assert_eq!(
            storage::get_cf_dns_record(&pool, "m1/app").await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn cf_record_for_a_too_long_service_id_is_created_without_comment() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut cf_server = cf_server_with_zone().await;
        // "docktornet:" + "m1/" + 87 characters is 101 characters.
        let container = "c".repeat(87);
        let lookup = cf_server
            .mock("GET", "/zones/z/dns_records")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(cf_list(serde_json::json!([])))
            .expect(0)
            .create_async()
            .await;
        let create = cf_server
            .mock("POST", "/zones/z/dns_records")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "name": "app.example.com",
                "comment": ""
            })))
            .with_status(200)
            .with_body(cf_one(cf_record("rec-1", None)))
            .expect(1)
            .create_async()
            .await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &cf_server.url(),
            &cf_server.url(),
            false,
            false,
        );
        let mut snap = snap_with(cf_dns_labels("app", "home.example.com"));
        snap.containers[0].name = container.clone();
        reconciler.reconcile_snapshot(&snap).await.unwrap();

        lookup.assert_async().await;
        create.assert_async().await;
        assert_eq!(
            storage::get_cf_dns_record(&pool, &format!("m1/{container}"))
                .await
                .unwrap(),
            Some("rec-1".to_string())
        );
    }

    #[tokio::test]
    async fn dry_run_writes_no_cf_marker() {
        let dir = tempdir().unwrap();
        let pool = db::connect(&dir.path().join("t.db")).await.unwrap();
        let mut cf_server = mockito::Server::new_async().await;
        let refused = refuse_every_request(&mut cf_server).await;

        let reconciler = reconciler_for(
            pool.clone(),
            dir.path(),
            &cf_server.url(),
            &cf_server.url(),
            true,
            true,
        );
        let snap = snap_with(cf_dns_labels("app", "home.example.com"));
        storage::upsert_snapshot(&pool, &snap, snap.taken_at as i64)
            .await
            .unwrap();
        storage::set_cf_dns_record(&pool, "m1/app", "rec-1")
            .await
            .unwrap();

        reconciler.reconcile_snapshot(&snap).await.unwrap();
        assert_eq!(reconciler.full_resync().await.unwrap().failed, 0);

        for mock in refused {
            mock.assert_async().await;
        }
    }
}
