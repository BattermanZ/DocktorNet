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
/// The Caddy breaker trips and cools down like NPM's.
const CADDY_BREAKER_THRESHOLD: u32 = NPM_BREAKER_THRESHOLD;
const CADDY_BREAKER_COOLDOWN: Duration = NPM_BREAKER_COOLDOWN;

#[derive(Clone)]
pub struct Reconciler {
    db: Db,
    options: ReconcileOptions,
    npm: Option<crate::integrations::npm::NpmClient>,
    cloudflare: Option<crate::integrations::cloudflare::CloudflareClient>,
    /// `None` when Caddy isn't configured; set with [`Reconciler::with_caddy`].
    caddy: Option<crate::integrations::caddy::CaddyClient>,
    lock: Arc<Mutex<()>>,
    npm_breaker: Arc<CircuitBreaker>,
    caddy_breaker: Arc<CircuitBreaker>,
    /// What the last read of Caddy's anchor route found, for the health
    /// endpoint.
    caddy_anchor: Arc<std::sync::Mutex<CaddyAnchor>>,
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
    /// only copy of the NPM hosts, Cloudflare record and Caddy routes they
    /// owned.
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
                    for route in older.caddy_routes {
                        if !rs.caddy_routes.iter().any(|(index, _)| *index == route.0) {
                            rs.caddy_routes.push(route);
                        }
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
    /// `(caddy_index, hosts)` for every Caddy route the service owned,
    /// captured before the `caddy_routes` cascade wipes them.
    pub caddy_routes: Vec<(i64, Vec<String>)>,
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

/// What the last read of Caddy's anchor route found.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum CaddyAnchor {
    /// Not read yet, the read failed, or the Caddy breaker is open.
    #[default]
    Unknown,
    /// Caddy has the anchor route.
    Ok,
    /// Caddy answered that it has no route with the anchor's `@id`.
    Missing,
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

/// Whether `labels` hold a `docktornet.npm.<npm_index>` group.  A malformed
/// group counts while the labels still ask for a resource: it is still there,
/// so its host is kept.  Labels that ask for nothing are a service that left.
fn lists_npm_index(labels: &docktornet_core::labels::ParsedLabels, npm_index: i64) -> bool {
    let is_index = |index: u8| i64::from(index) == npm_index;
    labels.npm.iter().any(|host| is_index(host.index))
        || (crate::integrations::intended::has_intent(labels)
            && labels.npm_malformed.iter().any(|&index| is_index(index)))
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

    /// What the last read of Caddy's anchor route found, or `Unknown` while
    /// the Caddy breaker is open: no read happens then, and a failure since
    /// the last read may have opened it.
    pub fn caddy_anchor(&self) -> CaddyAnchor {
        if self.caddy_breaker.is_open(std::time::Instant::now()) {
            return CaddyAnchor::Unknown;
        }
        *self
            .caddy_anchor
            .lock()
            .expect("caddy anchor mutex poisoned")
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
            caddy: None,
            lock: Arc::new(Mutex::new(())),
            npm_breaker: Arc::new(CircuitBreaker::new(
                NPM_BREAKER_THRESHOLD,
                NPM_BREAKER_COOLDOWN,
            )),
            caddy_breaker: Arc::new(CircuitBreaker::new(
                CADDY_BREAKER_THRESHOLD,
                CADDY_BREAKER_COOLDOWN,
            )),
            caddy_anchor: Arc::default(),
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
            caddy: None,
            lock: Arc::new(Mutex::new(())),
            npm_breaker: Arc::new(CircuitBreaker::new(
                NPM_BREAKER_THRESHOLD,
                NPM_BREAKER_COOLDOWN,
            )),
            caddy_breaker: Arc::new(CircuitBreaker::new(
                CADDY_BREAKER_THRESHOLD,
                CADDY_BREAKER_COOLDOWN,
            )),
            caddy_anchor: Arc::default(),
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
    /// period is over: Gatus files kept through it, then pending NPM,
    /// Cloudflare and Caddy removals.  Then run the Caddy drift check.  The
    /// server runs this on a short timer so a removal lands soon after its
    /// grace period ends, rather than on the next `full_resync`.  Unlike
    /// `full_resync`, it re-verifies no live service's NPM host, DNS record
    /// or Gatus file.
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
            for (service_id, npm_index, _, last_seen) in
                crate::storage::list_pending_npm_removals(&self.db).await?
            {
                if past_grace(last_seen, now)
                    && !self
                        .stored_again(&service_id, |labels| lists_npm_index(labels, npm_index))
                        .await
                {
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

        // Routes the drift check found in sync count as verified, which
        // every tick with a Caddy route does, so they alone aren't logged.
        if report.repaired > 0 || report.failed > 0 {
            tracing::info!(
                repaired = report.repaired,
                failed = report.failed,
                "pending removal sweep complete"
            );
        }
        Ok(report)
    }

    /// The removal sweeps and Caddy drift check shared by `full_resync` and
    /// `sweep_pending_removals`.  Returns whether an NPM delete failed, for
    /// the caller's breaker.
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
        self.sweep_and_check_caddy(report).await;
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
    /// machine again, with labels that `wants` says still ask for the
    /// removal's resource.  It came back in a snapshot whose apply has not run
    /// yet, and that apply restores the resource, so no sweep may delete it.
    /// When the store cannot tell, the removal is kept for a later sweep.
    async fn stored_again(
        &self,
        service_id: &str,
        wants: impl Fn(&docktornet_core::labels::ParsedLabels) -> bool,
    ) -> bool {
        match crate::storage::get_service_labels(&self.db, service_id).await {
            Ok(labels) => {
                let back =
                    labels.is_some_and(|labels| wants(&docktornet_core::labels::parse(&labels)));
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
    /// past the grace period.  These are the removals `apply_inner` deferred:
    /// within grace, breaker open, or a failed delete.  A machine that stops
    /// reporting leaves no pending rows, so its hosts stay.
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
            if self
                .stored_again(&service_id, |labels| lists_npm_index(labels, npm_index))
                .await
            {
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
    /// are past the grace period.  These are the removals `apply_inner`
    /// deferred: within grace, no client, or a failed delete.  A machine that
    /// stops reporting leaves no pending rows, so its records stay.
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
            if self
                .stored_again(&service_id, crate::integrations::intended::has_intent)
                .await
            {
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
        // Capture NPM host, Cloudflare record and Caddy route mappings before
        // upsert_snapshot's cascade wipes them.
        let npm_hosts = crate::storage::get_npm_hosts_for_services(&self.db, &removed).await?;
        let cf_records = crate::storage::get_cf_records_for_services(&self.db, &removed).await?;
        let caddy_routes =
            crate::storage::get_caddy_routes_for_services(&self.db, &removed).await?;
        self.record_gatus_departures(now, &current_ids, &removed_last_seen)
            .await?;
        // A departed route keeps its hosts claimed from this moment, so no
        // apply that runs before this snapshot's can take them.
        let last_seen_of: HashMap<&str, i64> = removed_last_seen
            .iter()
            .map(|(service_id, last_seen)| (service_id.as_str(), *last_seen))
            .collect();
        for (service_id, caddy_index, hosts) in &caddy_routes {
            if let Some(&last_seen) = last_seen_of.get(service_id.as_str()) {
                crate::storage::add_pending_caddy_removal(
                    &self.db,
                    service_id,
                    *caddy_index,
                    hosts,
                    last_seen,
                )
                .await?;
            }
        }
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
        let mut caddy_by_service: HashMap<String, Vec<(i64, Vec<String>)>> = HashMap::new();
        for (service_id, caddy_index, hosts) in caddy_routes {
            caddy_by_service
                .entry(service_id)
                .or_default()
                .push((caddy_index, hosts));
        }
        Ok(removed_last_seen
            .into_iter()
            .map(|(service_id, last_seen)| RemovedService {
                npm_hosts: hosts_by_service.remove(&service_id).unwrap_or_default(),
                cf_record: cf_by_service.remove(&service_id),
                caddy_routes: caddy_by_service.remove(&service_id).unwrap_or_default(),
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

        // The mapped NPM hosts of live services whose index the labels no
        // longer list.  Pending rows restored below are all still labelled, so
        // they never add to this.
        let dropped_npm = self.dropped_npm_hosts(&intended).await?;

        // Fetch the NPM token + certificate list once for the whole snapshot,
        // shared across every host below.  If the circuit breaker is open
        // (NPM has been failing), skip NPM entirely this cycle so we stop
        // adding load to a struggling dependency.
        let has_npm_removals =
            removed.iter().any(|r| !r.npm_hosts.is_empty()) || !dropped_npm.is_empty();
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

        // Dropped indices next, before any host is created: a dropped group is
        // a label edit, so its host goes in this apply with no grace period,
        // and its domains are free in NPM before another group takes them.
        // A host that can't go now is parked already past grace, so the next
        // sweep retries the delete.
        for (service_id, npm_index, npm_id) in &dropped_npm {
            let deleted = match &npm_ctx {
                Some((token, _)) => {
                    let client = self.npm.as_ref().expect("npm_ctx implies npm client");
                    match client.delete_proxy_host(token, *npm_id).await {
                        Ok(()) => {
                            tracing::info!(
                                service_id,
                                npm_index,
                                npm_id,
                                "npm host deleted: its labels are gone"
                            );
                            true
                        }
                        Err(err) => {
                            tracing::warn!(service_id, npm_index, npm_id, error = %err, "npm host delete failed; deferring");
                            npm_failed = true;
                            failures += 1;
                            false
                        }
                    }
                }
                None => false,
            };
            if deleted {
                crate::storage::delete_npm_host(&self.db, service_id, *npm_index).await?;
            } else {
                crate::storage::park_npm_host(
                    &self.db,
                    service_id,
                    *npm_index,
                    *npm_id,
                    ingested_at.saturating_sub(GATUS_REMOVAL_GRACE_SECS),
                )
                .await?;
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
            // Only indices the labels still list come back; the rest stay
            // pending and the sweep deletes them past grace.
            for (npm_index, npm_id) in
                crate::storage::get_pending_npm_removals_for_service(&self.db, &service.service_id)
                    .await?
            {
                if !lists_npm_index(&service.labels, npm_index) {
                    continue;
                }
                crate::storage::set_npm_host(&self.db, &service.service_id, npm_index, npm_id)
                    .await?;
                crate::storage::delete_pending_npm_removal(
                    &self.db,
                    &service.service_id,
                    npm_index,
                )
                .await?;
            }
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

        // Caddy runs whatever NPM's outcome, so an NPM outage never stalls it.
        let caddy = self
            .apply_caddy(&intended, &removed, due, &snap.forward_host, ingested_at)
            .await?;
        failures += caddy.failures;

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
        // Record success only when NPM and Caddy actually ran (not when
        // skipped by a breaker), so a breaker-skipped cycle still retries once
        // it closes.
        if !skip_npm && !caddy.skipped {
            self.last_reconciled
                .lock()
                .await
                .insert(machine, intent_hash);
        }
        Ok(())
    }

    /// The `(service_id, npm_index, npm_id)` mappings of `intended` services
    /// whose index their labels no longer list.
    async fn dropped_npm_hosts(
        &self,
        intended: &[crate::integrations::intended::IntendedService],
    ) -> Result<Vec<(String, i64, i64)>, ReconcileError> {
        let ids: Vec<String> = intended.iter().map(|s| s.service_id.clone()).collect();
        let labels: HashMap<&str, &docktornet_core::labels::ParsedLabels> = intended
            .iter()
            .map(|s| (s.service_id.as_str(), &s.labels))
            .collect();
        Ok(crate::storage::get_npm_hosts_for_services(&self.db, &ids)
            .await?
            .into_iter()
            .filter(|(service_id, npm_index, _)| {
                labels
                    .get(service_id.as_str())
                    .is_some_and(|labels| !lists_npm_index(labels, *npm_index))
            })
            .collect())
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

mod caddy;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod caddy_tests;
