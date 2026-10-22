//! `docktornet-server audit`: compare NPM, Cloudflare and Caddy with
//! docktornet's ownership mappings and print the differences as one JSON
//! document.
//!
//! The audit only reads. It opens the database read-only and sends NPM,
//! Cloudflare and Caddy nothing but GET requests, apart from the NPM token
//! request, so it can run next to a live server.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use serde::Serialize;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::task::JoinSet;

use crate::config::ServerConfig;
use crate::db::{self, Db};
use crate::integrations::caddy::{CaddyClient, position_of};
use crate::integrations::cloudflare::{CloudflareClient, DnsRecord};
use crate::integrations::npm::{NpmClient, OwnershipMarker, ProxyHost};
use crate::storage;

/// Which providers the audit covers.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// NPM, Cloudflare, and Caddy when it is configured.
    #[default]
    All,
    /// Caddy alone, from `audit --only caddy`.
    Caddy,
}

/// Every difference the audit found. The report leaves out the section of a
/// provider the audit didn't cover. Within a section, and for `errors`, every
/// list is always serialized, empty when there is nothing to report.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct Report {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub npm: Option<NpmFindings>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloudflare: Option<CloudflareFindings>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caddy: Option<CaddyFindings>,
    /// Problems that belong to no one resource, such as a missing Caddy
    /// anchor route.
    pub errors: Vec<String>,
}

impl Report {
    pub fn is_clean(&self) -> bool {
        self.npm.as_ref().is_none_or(NpmFindings::is_empty)
            && self
                .cloudflare
                .as_ref()
                .is_none_or(CloudflareFindings::is_empty)
            && self.caddy.as_ref().is_none_or(CaddyFindings::is_empty)
            && self.errors.is_empty()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct NpmFindings {
    pub marked_unmapped: Vec<NpmMarkedUnmapped>,
    pub pending_removal: Vec<NpmPendingRemoval>,
    pub mapped_missing: Vec<NpmMappedMissing>,
    pub mapped_unmarked: Vec<NpmMappedUnmarked>,
    pub unmarked_unreachable: Vec<NpmUnmarkedUnreachable>,
}

impl NpmFindings {
    fn is_empty(&self) -> bool {
        self.marked_unmapped.is_empty()
            && self.pending_removal.is_empty()
            && self.mapped_missing.is_empty()
            && self.mapped_unmarked.is_empty()
            && self.unmarked_unreachable.is_empty()
    }
}

/// A proxy host whose marker names a service, but no mapping points to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NpmMarkedUnmapped {
    pub id: i64,
    pub domain_names: Vec<String>,
    pub service_id: String,
    pub npm_index: i64,
    /// When the marker's machine last pushed a snapshot, or `None` for a
    /// machine the database does not know.
    pub machine_last_seen: Option<i64>,
    pub machine_last_seen_secs_ago: Option<i64>,
}

/// A `pending_npm_removals` row. `exists` is false when NPM no longer has the
/// host, and `domain_names` is then empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NpmPendingRemoval {
    pub id: i64,
    pub domain_names: Vec<String>,
    pub exists: bool,
    pub service_id: String,
    pub npm_index: i64,
    pub last_seen: i64,
    pub last_seen_secs_ago: i64,
}

/// A mapping that points to a proxy host NPM no longer has.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NpmMappedMissing {
    pub id: i64,
    pub service_id: String,
    pub npm_index: i64,
}

/// A mapped proxy host with no marker, or a marker naming another service or
/// index. `marker` is what the host carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NpmMappedUnmarked {
    pub id: i64,
    pub domain_names: Vec<String>,
    pub service_id: String,
    pub npm_index: i64,
    pub marker: Option<OwnershipMarker>,
}

/// A proxy host with no marker and no mapping whose upstream refused or timed
/// out a TCP connect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NpmUnmarkedUnreachable {
    pub id: i64,
    pub domain_names: Vec<String>,
    pub forward_host: String,
    pub forward_port: u16,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct CloudflareFindings {
    pub marked_unmapped: Vec<CfMarkedUnmapped>,
    pub pending_removal: Vec<CfPendingRemoval>,
    pub mapped_missing: Vec<CfMappedMissing>,
    pub mapped_unmarked: Vec<CfMappedUnmarked>,
    pub unmarked_unmapped: Vec<CfUnmarkedUnmapped>,
}

impl CloudflareFindings {
    fn is_empty(&self) -> bool {
        self.marked_unmapped.is_empty()
            && self.pending_removal.is_empty()
            && self.mapped_missing.is_empty()
            && self.mapped_unmarked.is_empty()
            && self.unmarked_unmapped.is_empty()
    }
}

/// A DNS record whose marker names a service, but no mapping points to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CfMarkedUnmapped {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub record_type: String,
    pub service_id: String,
    pub machine_last_seen: Option<i64>,
    pub machine_last_seen_secs_ago: Option<i64>,
}

/// A `pending_cf_removals` row. `exists` is false when Cloudflare no longer
/// has the record, and `name` is then `None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CfPendingRemoval {
    pub id: String,
    pub name: Option<String>,
    pub exists: bool,
    pub service_id: String,
    pub last_seen: i64,
    pub last_seen_secs_ago: i64,
}

/// A mapping that points to a DNS record Cloudflare no longer has.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CfMappedMissing {
    pub id: String,
    pub service_id: String,
}

/// A mapped DNS record with no marker, or a marker naming another service.
/// `comment` is what the record carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CfMappedUnmarked {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub record_type: String,
    pub service_id: String,
    pub comment: Option<String>,
}

/// A DNS record with no marker and no mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CfUnmarkedUnmapped {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub record_type: String,
    pub content: String,
}

/// The Caddy routes that differ from the mappings. A route's `id` is its
/// `@id`, and its `hosts` are what its host matchers hold.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct CaddyFindings {
    pub marked_unmapped: Vec<CaddyMarkedUnmapped>,
    pub pending_removal: Vec<CaddyPendingRemoval>,
    pub mapped_missing: Vec<CaddyMappedMissing>,
    pub marked_outside_anchor: Vec<CaddyMarkedOutsideAnchor>,
}

impl CaddyFindings {
    fn is_empty(&self) -> bool {
        self.marked_unmapped.is_empty()
            && self.pending_removal.is_empty()
            && self.mapped_missing.is_empty()
            && self.marked_outside_anchor.is_empty()
    }
}

/// A route in the anchor whose `@id` is a docktornet marker that no mapping
/// or pending removal holds. A marker can't be read back into a service ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CaddyMarkedUnmapped {
    pub id: String,
    pub hosts: Vec<String>,
}

/// A `pending_caddy_removals` row, with the hosts it still claims. `exists`
/// is false when Caddy no longer has the route anywhere in its config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CaddyPendingRemoval {
    pub id: String,
    pub hosts: Vec<String>,
    pub exists: bool,
    pub service_id: String,
    pub caddy_index: i64,
    pub last_seen: i64,
    pub last_seen_secs_ago: i64,
}

/// A mapping whose route Caddy has nowhere in its config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CaddyMappedMissing {
    pub id: String,
    pub service_id: String,
    pub caddy_index: i64,
}

/// A route carrying a mapping's marker that sits outside the anchor. The
/// apply and the drift check won't add a duplicate to the anchor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CaddyMarkedOutsideAnchor {
    pub id: String,
    pub hosts: Vec<String>,
    pub service_id: String,
    pub caddy_index: i64,
}

/// How unmarked, unmapped proxy hosts' upstreams are probed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Probe {
    pub timeout: Duration,
    /// Most connects in flight at once.
    pub concurrency: usize,
}

impl Default for Probe {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(2),
            concurrency: 8,
        }
    }
}

impl Probe {
    /// Whether a TCP connect to `host:port` succeeds within the timeout.
    pub async fn reachable(&self, host: &str, port: u16) -> bool {
        matches!(
            tokio::time::timeout(self.timeout, TcpStream::connect((host, port))).await,
            Ok(Ok(_))
        )
    }

    /// For each target, in order, whether it is reachable.
    pub async fn reachable_all(&self, targets: Vec<(String, u16)>) -> anyhow::Result<Vec<bool>> {
        let mut reachable = vec![false; targets.len()];
        let mut running = JoinSet::new();
        for (position, (host, port)) in targets.into_iter().enumerate() {
            if running.len() >= self.concurrency.max(1)
                && let Some(done) = running.join_next().await
            {
                let (done_position, ok) = done.context("upstream probe failed")?;
                reachable[done_position] = ok;
            }
            let probe = *self;
            running.spawn(async move { (position, probe.reachable(&host, port).await) });
        }
        while let Some(done) = running.join_next().await {
            let (done_position, ok) = done.context("upstream probe failed")?;
            reachable[done_position] = ok;
        }
        Ok(reachable)
    }
}

/// Run the audit over `scope` with `cfg`'s database and providers.
pub async fn run(cfg: &ServerConfig, scope: Scope) -> anyhow::Result<Report> {
    if scope == Scope::Caddy && cfg.caddy.is_none() {
        anyhow::bail!("--only caddy needs Caddy configured: DOCKTORNET_CADDY_ADMIN_URL is unset");
    }
    let pool = db::connect_read_only(&cfg.db_path)
        .await
        .with_context(|| format!("open database {}", cfg.db_path.display()))?;
    let npm = NpmClient::new(cfg.npm.clone());
    let cloudflare =
        CloudflareClient::with_base_url(cfg.cloudflare.clone(), cfg.cloudflare_api_url.as_str());
    let caddy = cfg.caddy.clone().map(CaddyClient::new);
    let sources = Sources {
        npm_cloudflare: (scope == Scope::All).then_some((&npm, &cloudflare)),
        caddy: caddy.as_ref(),
    };
    let report = audit(&pool, &sources, Probe::default(), unix_now()).await;
    pool.close().await;
    report
}

/// Print `result` and return the exit code: 0 for a clean report, 1 for a
/// report with findings, 2 when the audit failed. A failure goes to `err` and
/// leaves `out` untouched.
pub fn report_outcome(
    result: anyhow::Result<Report>,
    out: &mut impl Write,
    err: &mut impl Write,
) -> i32 {
    let report = match result {
        Ok(report) => report,
        Err(e) => {
            let _ = writeln!(err, "docktornet-server audit: {}", describe(&e));
            return 2;
        }
    };
    let printed = serde_json::to_vec_pretty(&report)
        .map_err(anyhow::Error::from)
        .and_then(|mut json| {
            json.push(b'\n');
            out.write_all(&json)?;
            out.flush()?;
            Ok(())
        });
    match printed {
        Err(e) => {
            let _ = writeln!(
                err,
                "docktornet-server audit: write the report: {}",
                describe(&e)
            );
            2
        }
        Ok(()) if report.is_clean() => 0,
        Ok(()) => 1,
    }
}

/// `e` and its causes, joined by `: `. A cause is left out when the message
/// before it already holds its text, as the client errors' `http: {0}` does.
pub(crate) fn describe(e: &anyhow::Error) -> String {
    let mut parts: Vec<String> = Vec::new();
    for cause in e.chain() {
        let message = cause.to_string();
        if parts.last().is_none_or(|last| !last.contains(&message)) {
            parts.push(message);
        }
    }
    parts.join(": ")
}

/// The `audit` subcommand: read the config from the environment, audit
/// `scope`, print and return the exit code.
pub async fn cli(scope: Scope) -> i32 {
    let result = match ServerConfig::from_env() {
        Ok(cfg) => run(&cfg, scope).await,
        Err(e) => Err(anyhow::Error::from(e).context("read config")),
    };
    report_outcome(result, &mut std::io::stdout(), &mut std::io::stderr())
}

/// The providers an audit compares with the mappings.
pub struct Sources<'a> {
    /// NPM and Cloudflare, or `None` under `--only caddy`.
    pub npm_cloudflare: Option<(&'a NpmClient, &'a CloudflareClient)>,
    /// `None` when Caddy isn't configured.
    pub caddy: Option<&'a CaddyClient>,
}

/// Compare `sources` with the mappings in `pool`. `now` is the unix time the
/// `*_secs_ago` fields count back from.
pub async fn audit(
    pool: &Db,
    sources: &Sources<'_>,
    probe: Probe,
    now: i64,
) -> anyhow::Result<Report> {
    let listings = match sources.npm_cloudflare {
        Some((npm, cloudflare)) => {
            let token = npm.token().await.context("NPM login")?;
            let hosts = npm
                .list_proxy_host_details(&token)
                .await
                .context("list NPM proxy hosts")?;
            let records = cloudflare
                .list_records()
                .await
                .context("list Cloudflare DNS records")?;
            Some((hosts, records))
        }
        None => None,
    };
    let anchor = match sources.caddy {
        Some(caddy) => Some(
            caddy
                .anchor_routes()
                .await
                .context("read Caddy's anchor routes")?,
        ),
        None => None,
    };
    // After the listings, so a resource created meanwhile already has its
    // mapping.
    let stored = Stored::read(pool).await?;

    let mut report = Report::default();
    if let Some((hosts, records)) = listings {
        report.npm = Some(audit_npm(hosts, &stored, probe, now).await?);
        report.cloudflare = Some(audit_cloudflare(records, &stored, now));
    }
    if let (Some(caddy), Some(anchor)) = (sources.caddy, anchor) {
        let anchor = anchor.unwrap_or_else(|| {
            report.errors.push(format!(
                "Caddy anchor route \"{}\" not found",
                caddy.anchor_id()
            ));
            Vec::new()
        });
        report.caddy = Some(audit_caddy(caddy, &anchor, &stored, now).await?);
    }
    Ok(report)
}

/// What the database holds, read in one transaction so the reads agree with
/// each other. The server itself does not move a resource from its mapping to
/// a pending removal atomically, so the audit can still catch one in neither.
struct Stored {
    machine_last_seen: HashMap<String, i64>,
    npm_hosts: Vec<(String, i64, i64)>,
    pending_npm: Vec<(String, i64, i64, i64)>,
    cf_records: Vec<(String, String)>,
    pending_cf: Vec<(String, String, i64)>,
    caddy_routes: Vec<(String, storage::CaddyRouteRow)>,
    pending_caddy: Vec<storage::PendingCaddyRemoval>,
}

impl Stored {
    async fn read(pool: &Db) -> anyhow::Result<Self> {
        let mut tx = pool.begin().await.context("read the database")?;
        let machine_last_seen = storage::list_machines(&mut *tx)
            .await
            .context("read machines")?
            .into_iter()
            .map(|machine| (machine.machine_id, machine.last_seen))
            .collect();
        let npm_hosts = storage::list_npm_hosts(&mut *tx)
            .await
            .context("read NPM mappings")?;
        let pending_npm = storage::list_pending_npm_removals(&mut *tx)
            .await
            .context("read pending NPM removals")?;
        let cf_records = storage::list_cf_dns_records(&mut *tx)
            .await
            .context("read Cloudflare mappings")?;
        let pending_cf = storage::list_pending_cf_removals(&mut *tx)
            .await
            .context("read pending Cloudflare removals")?;
        let caddy_routes = storage::list_caddy_routes(&mut *tx)
            .await
            .context("read Caddy mappings")?;
        let pending_caddy = storage::list_pending_caddy_removals(&mut *tx)
            .await
            .context("read pending Caddy removals")?;
        tx.rollback().await.context("read the database")?;
        Ok(Self {
            machine_last_seen,
            npm_hosts,
            pending_npm,
            cf_records,
            pending_cf,
            caddy_routes,
            pending_caddy,
        })
    }

    /// When the machine of `service_id` last pushed, if the database knows it.
    fn machine_last_seen(&self, service_id: &str) -> Option<i64> {
        self.machine_last_seen.get(machine_of(service_id)).copied()
    }
}

async fn audit_npm(
    mut hosts: Vec<ProxyHost>,
    stored: &Stored,
    probe: Probe,
    now: i64,
) -> anyhow::Result<NpmFindings> {
    hosts.sort_by_key(|host| host.id);
    let by_id: HashMap<i64, &ProxyHost> = hosts.iter().map(|host| (host.id, host)).collect();
    let mut pointed_to = HashSet::new();
    let mut findings = NpmFindings::default();

    for (service_id, npm_index, npm_id) in &stored.npm_hosts {
        pointed_to.insert(*npm_id);
        let Some(host) = by_id.get(npm_id) else {
            findings.mapped_missing.push(NpmMappedMissing {
                id: *npm_id,
                service_id: service_id.clone(),
                npm_index: *npm_index,
            });
            continue;
        };
        let marker = host.marker();
        if marker.as_ref() != Some(&OwnershipMarker::new(service_id.as_str(), *npm_index)) {
            findings.mapped_unmarked.push(NpmMappedUnmarked {
                id: *npm_id,
                domain_names: host.domain_names.clone(),
                service_id: service_id.clone(),
                npm_index: *npm_index,
                marker,
            });
        }
    }

    for (service_id, npm_index, npm_id, last_seen) in &stored.pending_npm {
        pointed_to.insert(*npm_id);
        let host = by_id.get(npm_id);
        findings.pending_removal.push(NpmPendingRemoval {
            id: *npm_id,
            domain_names: host.map(|h| h.domain_names.clone()).unwrap_or_default(),
            exists: host.is_some(),
            service_id: service_id.clone(),
            npm_index: *npm_index,
            last_seen: *last_seen,
            last_seen_secs_ago: now - last_seen,
        });
    }

    let mut unmarked = Vec::new();
    for host in hosts.iter().filter(|host| !pointed_to.contains(&host.id)) {
        match host.marker() {
            Some(marker) => {
                let machine_last_seen = stored.machine_last_seen(&marker.service_id);
                findings.marked_unmapped.push(NpmMarkedUnmapped {
                    id: host.id,
                    domain_names: host.domain_names.clone(),
                    service_id: marker.service_id,
                    npm_index: marker.npm_index,
                    machine_last_seen,
                    machine_last_seen_secs_ago: machine_last_seen.map(|seen| now - seen),
                });
            }
            None => unmarked.push(host),
        }
    }

    let targets = unmarked
        .iter()
        .map(|host| (host.forward_host.clone(), host.forward_port))
        .collect();
    let reachable = probe.reachable_all(targets).await?;
    findings.unmarked_unreachable = unmarked
        .into_iter()
        .zip(reachable)
        .filter(|(_, reachable)| !reachable)
        .map(|(host, _)| NpmUnmarkedUnreachable {
            id: host.id,
            domain_names: host.domain_names.clone(),
            forward_host: host.forward_host.clone(),
            forward_port: host.forward_port,
        })
        .collect();

    Ok(findings)
}

fn audit_cloudflare(mut records: Vec<DnsRecord>, stored: &Stored, now: i64) -> CloudflareFindings {
    records.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
    let by_id: HashMap<&str, &DnsRecord> = records
        .iter()
        .map(|record| (record.id.as_str(), record))
        .collect();
    let mut pointed_to = HashSet::new();
    let mut findings = CloudflareFindings::default();

    for (service_id, record_id) in &stored.cf_records {
        pointed_to.insert(record_id.as_str());
        let Some(record) = by_id.get(record_id.as_str()) else {
            findings.mapped_missing.push(CfMappedMissing {
                id: record_id.clone(),
                service_id: service_id.clone(),
            });
            continue;
        };
        if record.marker_owner() != Some(service_id.as_str()) {
            findings.mapped_unmarked.push(CfMappedUnmarked {
                id: record_id.clone(),
                name: record.name.clone(),
                record_type: record.record_type.clone(),
                service_id: service_id.clone(),
                comment: record.comment().map(str::to_string),
            });
        }
    }

    for (service_id, record_id, last_seen) in &stored.pending_cf {
        pointed_to.insert(record_id.as_str());
        let record = by_id.get(record_id.as_str());
        findings.pending_removal.push(CfPendingRemoval {
            id: record_id.clone(),
            name: record.map(|r| r.name.clone()),
            exists: record.is_some(),
            service_id: service_id.clone(),
            last_seen: *last_seen,
            last_seen_secs_ago: now - last_seen,
        });
    }

    for record in records
        .iter()
        .filter(|r| !pointed_to.contains(r.id.as_str()))
    {
        match record.marker_owner() {
            Some(owner) => {
                let machine_last_seen = stored.machine_last_seen(owner);
                findings.marked_unmapped.push(CfMarkedUnmapped {
                    id: record.id.clone(),
                    name: record.name.clone(),
                    record_type: record.record_type.clone(),
                    service_id: owner.to_string(),
                    machine_last_seen,
                    machine_last_seen_secs_ago: machine_last_seen.map(|seen| now - seen),
                });
            }
            None => findings.unmarked_unmapped.push(CfUnmarkedUnmapped {
                id: record.id.clone(),
                name: record.name.clone(),
                record_type: record.record_type.clone(),
                content: record.content.clone(),
            }),
        }
    }

    findings
}

/// Compare `anchor`, Caddy's anchor routes, with the Caddy mappings. The
/// audit looks up a marker missing from the anchor in the rest of Caddy's
/// config, which tells a route that is gone from one outside the anchor.
async fn audit_caddy(
    caddy: &CaddyClient,
    anchor: &[Value],
    stored: &Stored,
    now: i64,
) -> anyhow::Result<CaddyFindings> {
    let mut pointed_to = HashSet::new();
    let mut findings = CaddyFindings::default();
    // The anchor read again, once, the first time a lookup finds a route.
    let mut reread: Option<Vec<Value>> = None;

    for (service_id, row) in &stored.caddy_routes {
        let marker = caddy_route_marker(service_id, row.caddy_index)?;
        if position_of(anchor, &marker).is_none() {
            let found = lookup(caddy, &marker).await?;
            if found.is_some() {
                // The anchor was read before the database, so a route the
                // server added since then is in the anchor now, not outside.
                if reread.is_none() {
                    reread = Some(
                        caddy
                            .anchor_routes()
                            .await
                            .context("read Caddy's anchor routes")?
                            .unwrap_or_default(),
                    );
                }
                if reread
                    .as_deref()
                    .is_some_and(|routes| position_of(routes, &marker).is_some())
                {
                    pointed_to.insert(marker);
                    continue;
                }
            }
            match found {
                Some(route) => findings
                    .marked_outside_anchor
                    .push(CaddyMarkedOutsideAnchor {
                        hosts: route_hosts(&route),
                        id: marker.clone(),
                        service_id: service_id.clone(),
                        caddy_index: row.caddy_index,
                    }),
                None => findings.mapped_missing.push(CaddyMappedMissing {
                    id: marker.clone(),
                    service_id: service_id.clone(),
                    caddy_index: row.caddy_index,
                }),
            }
        }
        pointed_to.insert(marker);
    }

    for row in &stored.pending_caddy {
        let marker = caddy_route_marker(&row.service_id, row.caddy_index)?;
        let exists =
            position_of(anchor, &marker).is_some() || lookup(caddy, &marker).await?.is_some();
        findings.pending_removal.push(CaddyPendingRemoval {
            id: marker.clone(),
            hosts: row.hosts.clone(),
            exists,
            service_id: row.service_id.clone(),
            caddy_index: row.caddy_index,
            last_seen: row.last_seen,
            last_seen_secs_ago: now - row.last_seen,
        });
        pointed_to.insert(marker);
    }

    for route in anchor {
        let Some(id) = route.get("@id").and_then(Value::as_str) else {
            continue;
        };
        if id.starts_with(docktornet_core::CADDY_ROUTE_MARKER_PREFIX) && !pointed_to.contains(id) {
            findings.marked_unmapped.push(CaddyMarkedUnmapped {
                id: id.to_string(),
                hosts: route_hosts(route),
            });
        }
    }

    Ok(findings)
}

/// The marker of a stored Caddy index.
fn caddy_route_marker(service_id: &str, caddy_index: i64) -> anyhow::Result<String> {
    let index = u8::try_from(caddy_index)
        .with_context(|| format!("stored caddy index {caddy_index} of {service_id}"))?;
    Ok(docktornet_core::caddy_route_marker(service_id, index))
}

/// The route with `marker` anywhere in Caddy's config.
async fn lookup(caddy: &CaddyClient, marker: &str) -> anyhow::Result<Option<Value>> {
    caddy
        .get_route(marker)
        .await
        .with_context(|| format!("look up Caddy route {marker}"))
}

/// The hosts in `route`'s host matchers.
fn route_hosts(route: &Value) -> Vec<String> {
    route
        .get("match")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|matcher| matcher.get("host").and_then(Value::as_array))
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

/// The machine half of a `machine/container` service ID.
fn machine_of(service_id: &str) -> &str {
    service_id
        .split_once('/')
        .map_or(service_id, |(machine, _)| machine)
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ServerConfig;
    use mockito::{Matcher, Mock, Server, ServerGuard};
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use std::path::Path;
    use tempfile::{TempDir, tempdir};

    const NOW: i64 = 10_000;

    /// A migrated database in a temp dir, seeded by `seed` and then closed.
    async fn seeded_db(seed: &[&str]) -> (TempDir, std::path::PathBuf) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("audit.db");
        let pool = db::connect(&path).await.unwrap();
        for statement in seed {
            sqlx::query(statement).execute(&pool).await.unwrap();
        }
        pool.close().await;
        (dir, path)
    }

    /// Mocks failing the test on any write: a POST to `post_path` (any POST
    /// when `None`), and every PUT, PATCH and DELETE. A request no mock
    /// matches gets a 501, which fails the audit.
    async fn forbid_writes(server: &mut ServerGuard, post_path: Option<&str>) -> Vec<Mock> {
        let post = post_path.map_or(Matcher::Any, |path| Matcher::Regex(path.to_string()));
        let mut guards = vec![server.mock("POST", post).expect(0).create_async().await];
        for method in ["PUT", "PATCH", "DELETE"] {
            guards.push(
                server
                    .mock(method, Matcher::Any)
                    .expect(0)
                    .create_async()
                    .await,
            );
        }
        guards
    }

    async fn npm_server(hosts: Value) -> (ServerGuard, Vec<Mock>) {
        let mut server = Server::new_async().await;
        let token = server
            .mock("POST", "/api/tokens")
            .with_status(200)
            .with_body(r#"{"token":"tok"}"#)
            .create_async()
            .await;
        let list = server
            .mock("GET", "/api/nginx/proxy-hosts")
            .match_header("authorization", "Bearer tok")
            .with_status(200)
            .with_body(hosts.to_string())
            .create_async()
            .await;
        let mut guards = forbid_writes(&mut server, Some("^/api/nginx/")).await;
        guards.push(token);
        guards.push(list);
        (server, guards)
    }

    async fn cf_server(records: Value) -> (ServerGuard, Vec<Mock>) {
        let mut server = Server::new_async().await;
        let list = server
            .mock("GET", "/client/v4/zones/zone-1/dns_records")
            .match_header("authorization", "Bearer cf-token")
            .match_query(Matcher::Any)
            .with_status(200)
            .with_body(json!({"success": true, "result": records}).to_string())
            .create_async()
            .await;
        let mut guards = forbid_writes(&mut server, None).await;
        guards.push(list);
        (server, guards)
    }

    fn config(db_path: &Path, npm: &ServerGuard, cf: &ServerGuard) -> ServerConfig {
        config_with_caddy(db_path, npm, cf, None)
    }

    /// The config, with Caddy's admin API at `caddy` when it is given.
    fn config_with_caddy(
        db_path: &Path,
        npm: &ServerGuard,
        cf: &ServerGuard,
        caddy: Option<&ServerGuard>,
    ) -> ServerConfig {
        let mut env: HashMap<String, String> = [
            ("DOCKTORNET_LISTEN", "127.0.0.1:0".to_string()),
            ("DOCKTORNET_SECRET", "0123456789abcdef".repeat(4)),
            ("DOCKTORNET_DB_PATH", db_path.display().to_string()),
            ("DOCKTORNET_GATUS_CONFIG_DIR", "/nonexistent".to_string()),
            ("DOCKTORNET_DRY_RUN", "false".to_string()),
            ("DOCKTORNET_NPM_URL", npm.url()),
            ("DOCKTORNET_NPM_USER", "u".to_string()),
            ("DOCKTORNET_NPM_PASSWORD", "p".to_string()),
            ("DOCKTORNET_CF_API_URL", format!("{}/client/v4", cf.url())),
            ("DOCKTORNET_CF_API_TOKEN", "cf-token".to_string()),
            ("DOCKTORNET_CF_ZONE_ID", "zone-1".to_string()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        if let Some(caddy) = caddy {
            env.insert("DOCKTORNET_CADDY_ADMIN_URL".into(), caddy.url());
        }
        ServerConfig::from_map(env).unwrap()
    }

    fn marker(service_id: &str, npm_index: i64) -> Value {
        json!({"docktornet": {"service_id": service_id, "npm_index": npm_index}})
    }

    fn host(id: i64, domain: &str, forward: (&str, u16), meta: Value) -> Value {
        json!({
            "id": id,
            "domain_names": [domain],
            "forward_host": forward.0,
            "forward_port": forward.1,
            "meta": meta,
        })
    }

    fn record(id: &str, name: &str, comment: Option<&str>) -> Value {
        json!({
            "id": id,
            "type": "CNAME",
            "name": name,
            "content": "home.example.com",
            "proxied": false,
            "comment": comment,
        })
    }

    fn closed_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// The mapped service rows and machine every fixture shares.
    const SEED: &[&str] = &[
        "INSERT INTO machines (machine_id, forward_host, last_seen) VALUES ('m1', '10.0.0.1', 9000)",
        "INSERT INTO services (id, machine_id, container, labels_json, last_seen) VALUES \
         ('m1/ok', 'm1', 'ok', '{}', 9000), \
         ('m1/lost', 'm1', 'lost', '{}', 9000), \
         ('m1/drift', 'm1', 'drift', '{}', 9000)",
        "INSERT INTO npm_hosts (service_id, npm_index, npm_id) VALUES \
         ('m1/ok', 0, 1), ('m1/lost', 0, 4), ('m1/drift', 0, 5)",
        "INSERT INTO pending_npm_removals (service_id, npm_index, npm_id, last_seen) VALUES \
         ('m1/old', 0, 3, 9900)",
        "INSERT INTO cf_dns_records (service_id, record_id) VALUES \
         ('m1/ok', 'rec-ok'), ('m1/lost', 'rec-lost'), ('m1/drift', 'rec-drift')",
        "INSERT INTO pending_cf_removals (service_id, record_id, last_seen) VALUES \
         ('m1/old', 'rec-old', 9900)",
    ];

    #[tokio::test]
    async fn each_category_is_filled_from_one_case_of_each() {
        let open = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let open_port = open.local_addr().unwrap().port();
        let dead_port = closed_port();
        let (_dir, path) = seeded_db(SEED).await;
        let (npm, npm_mocks) = npm_server(json!([
            host(1, "ok.example.com", ("10.0.0.1", 80), marker("m1/ok", 0)),
            host(
                2,
                "leftover.example.com",
                ("10.0.0.1", 80),
                marker("m1/gone", 1)
            ),
            host(3, "old.example.com", ("10.0.0.1", 80), marker("m1/old", 0)),
            host(
                5,
                "drift.example.com",
                ("10.0.0.1", 80),
                json!({"nginx_online": true})
            ),
            host(6, "dead.example.com", ("127.0.0.1", dead_port), Value::Null),
            host(
                7,
                "stray.example.com",
                ("10.0.0.1", 80),
                marker("m9/stray", 0)
            ),
            host(8, "alive.example.com", ("127.0.0.1", open_port), json!({})),
        ]))
        .await;
        let (cf, cf_mocks) = cf_server(json!([
            record("rec-ok", "ok.example.com", Some("docktornet:m1/ok")),
            record(
                "rec-left",
                "leftover.example.com",
                Some("docktornet:m1/gone")
            ),
            record("rec-old", "old.example.com", Some("docktornet:m1/old")),
            record(
                "rec-drift",
                "drift.example.com",
                Some("docktornet:m2/other")
            ),
            record("rec-hand", "hand.example.com", Some("made by hand")),
        ]))
        .await;
        let cfg = config(&path, &npm, &cf);
        let pool = db::connect_read_only(&path).await.unwrap();
        let npm_client = NpmClient::new(cfg.npm.clone());
        let cf_client = CloudflareClient::with_base_url(
            cfg.cloudflare.clone(),
            cfg.cloudflare_api_url.as_str(),
        );

        let sources = Sources {
            npm_cloudflare: Some((&npm_client, &cf_client)),
            caddy: None,
        };
        let report = audit(&pool, &sources, Probe::default(), NOW).await.unwrap();
        for mock in npm_mocks.iter().chain(&cf_mocks) {
            mock.assert_async().await;
        }

        let domains = |d: &str| vec![d.to_string()];
        assert_eq!(report.caddy, None);
        assert_eq!(
            report.npm.unwrap(),
            NpmFindings {
                marked_unmapped: vec![
                    NpmMarkedUnmapped {
                        id: 2,
                        domain_names: domains("leftover.example.com"),
                        service_id: "m1/gone".into(),
                        npm_index: 1,
                        machine_last_seen: Some(9000),
                        machine_last_seen_secs_ago: Some(1000),
                    },
                    NpmMarkedUnmapped {
                        id: 7,
                        domain_names: domains("stray.example.com"),
                        service_id: "m9/stray".into(),
                        npm_index: 0,
                        machine_last_seen: None,
                        machine_last_seen_secs_ago: None,
                    },
                ],
                pending_removal: vec![NpmPendingRemoval {
                    id: 3,
                    domain_names: domains("old.example.com"),
                    exists: true,
                    service_id: "m1/old".into(),
                    npm_index: 0,
                    last_seen: 9900,
                    last_seen_secs_ago: 100,
                }],
                mapped_missing: vec![NpmMappedMissing {
                    id: 4,
                    service_id: "m1/lost".into(),
                    npm_index: 0,
                }],
                mapped_unmarked: vec![NpmMappedUnmarked {
                    id: 5,
                    domain_names: domains("drift.example.com"),
                    service_id: "m1/drift".into(),
                    npm_index: 0,
                    marker: None,
                }],
                unmarked_unreachable: vec![NpmUnmarkedUnreachable {
                    id: 6,
                    domain_names: domains("dead.example.com"),
                    forward_host: "127.0.0.1".into(),
                    forward_port: dead_port,
                }],
            }
        );
        assert_eq!(
            report.cloudflare.unwrap(),
            CloudflareFindings {
                marked_unmapped: vec![CfMarkedUnmapped {
                    id: "rec-left".into(),
                    name: "leftover.example.com".into(),
                    record_type: "CNAME".into(),
                    service_id: "m1/gone".into(),
                    machine_last_seen: Some(9000),
                    machine_last_seen_secs_ago: Some(1000),
                }],
                pending_removal: vec![CfPendingRemoval {
                    id: "rec-old".into(),
                    name: Some("old.example.com".into()),
                    exists: true,
                    service_id: "m1/old".into(),
                    last_seen: 9900,
                    last_seen_secs_ago: 100,
                }],
                mapped_missing: vec![CfMappedMissing {
                    id: "rec-lost".into(),
                    service_id: "m1/lost".into(),
                }],
                mapped_unmarked: vec![CfMappedUnmarked {
                    id: "rec-drift".into(),
                    name: "drift.example.com".into(),
                    record_type: "CNAME".into(),
                    service_id: "m1/drift".into(),
                    comment: Some("docktornet:m2/other".into()),
                }],
                unmarked_unmapped: vec![CfUnmarkedUnmapped {
                    id: "rec-hand".into(),
                    name: "hand.example.com".into(),
                    record_type: "CNAME".into(),
                    content: "home.example.com".into(),
                }],
            }
        );
        drop(open);
    }

    #[tokio::test]
    async fn a_marker_naming_another_index_is_mapped_unmarked() {
        let (_dir, path) = seeded_db(&SEED[..3]).await;
        let (npm, _npm_mocks) = npm_server(json!([
            host(1, "ok.example.com", ("10.0.0.1", 80), marker("m1/ok", 1)),
            host(
                4,
                "lost.example.com",
                ("10.0.0.1", 80),
                marker("m1/lost", 0)
            ),
            host(
                5,
                "drift.example.com",
                ("10.0.0.1", 80),
                marker("m1/drift", 0)
            ),
        ]))
        .await;
        let (cf, _cf_mocks) = cf_server(json!([])).await;
        let report = run(&config(&path, &npm, &cf), Scope::All).await.unwrap();

        assert_eq!(report.npm.as_ref().unwrap().mapped_unmarked.len(), 1);
        assert_eq!(report.npm.as_ref().unwrap().mapped_unmarked[0].id, 1);
        assert_eq!(
            report.npm.as_ref().unwrap().mapped_unmarked[0].marker,
            Some(OwnershipMarker::new("m1/ok", 1u8))
        );
    }

    #[tokio::test]
    async fn a_clean_fixture_prints_every_key_empty_and_exits_0() {
        let (_dir, path) = seeded_db(&[]).await;
        let (npm, npm_mocks) = npm_server(json!([])).await;
        let (cf, cf_mocks) = cf_server(json!([])).await;
        let (mut out, mut err) = (Vec::new(), Vec::new());

        let code = report_outcome(
            run(&config(&path, &npm, &cf), Scope::All).await,
            &mut out,
            &mut err,
        );
        for mock in npm_mocks.iter().chain(&cf_mocks) {
            mock.assert_async().await;
        }

        assert_eq!(code, 0, "stderr: {}", String::from_utf8_lossy(&err));
        let printed: Value = serde_json::from_slice(&out).unwrap();
        let empty: Vec<Value> = Vec::new();
        assert_eq!(
            printed,
            json!({
                "npm": {
                    "marked_unmapped": empty,
                    "pending_removal": empty,
                    "mapped_missing": empty,
                    "mapped_unmarked": empty,
                    "unmarked_unreachable": empty,
                },
                "cloudflare": {
                    "marked_unmapped": empty,
                    "pending_removal": empty,
                    "mapped_missing": empty,
                    "mapped_unmarked": empty,
                    "unmarked_unmapped": empty,
                },
                "errors": empty,
            })
        );
    }

    #[tokio::test]
    async fn one_finding_exits_1() {
        let (_dir, path) = seeded_db(&[]).await;
        let (npm, _npm_mocks) = npm_server(json!([])).await;
        let (cf, _cf_mocks) =
            cf_server(json!([record("rec-hand", "hand.example.com", None)])).await;
        let (mut out, mut err) = (Vec::new(), Vec::new());

        let code = report_outcome(
            run(&config(&path, &npm, &cf), Scope::All).await,
            &mut out,
            &mut err,
        );

        assert_eq!(code, 1, "stderr: {}", String::from_utf8_lossy(&err));
        let printed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(
            printed["cloudflare"]["unmarked_unmapped"][0]["id"],
            "rec-hand"
        );
        assert_eq!(
            printed["cloudflare"]["unmarked_unmapped"][0]["type"],
            "CNAME"
        );
    }

    #[tokio::test]
    async fn an_npm_login_failure_exits_2_with_nothing_on_stdout() {
        let (_dir, path) = seeded_db(&[]).await;
        let mut npm = Server::new_async().await;
        let _token = npm
            .mock("POST", "/api/tokens")
            .with_status(401)
            .create_async()
            .await;
        let (cf, _cf_mocks) = cf_server(json!([])).await;
        let (mut out, mut err) = (Vec::new(), Vec::new());

        let code = report_outcome(
            run(&config(&path, &npm, &cf), Scope::All).await,
            &mut out,
            &mut err,
        );

        assert_eq!(code, 2);
        assert!(out.is_empty());
        let err = String::from_utf8_lossy(&err);
        assert!(err.contains("NPM login"), "{err}");
        assert_eq!(err.matches("401 Unauthorized").count(), 1, "{err}");
    }

    #[tokio::test]
    async fn a_missing_database_exits_2_and_is_not_created() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("missing.db");
        let (npm, _npm_mocks) = npm_server(json!([])).await;
        let (cf, _cf_mocks) = cf_server(json!([])).await;
        let (mut out, mut err) = (Vec::new(), Vec::new());

        let code = report_outcome(
            run(&config(&path, &npm, &cf), Scope::All).await,
            &mut out,
            &mut err,
        );

        assert_eq!(code, 2);
        assert!(out.is_empty());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn the_audit_reads_next_to_a_live_server_and_writes_nothing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("audit.db");
        // The server's own pool stays open, as it does in production.
        let server_pool = db::connect(&path).await.unwrap();
        for statement in SEED {
            sqlx::query(statement).execute(&server_pool).await.unwrap();
        }
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&server_pool)
            .await
            .unwrap();
        let wal = dir.path().join("audit.db-wal");
        let before = (std::fs::read(&path).unwrap(), std::fs::read(&wal).unwrap());
        let (npm, _npm_mocks) = npm_server(json!([])).await;
        let (cf, _cf_mocks) = cf_server(json!([])).await;

        let report = run(&config(&path, &npm, &cf), Scope::All).await.unwrap();

        assert_eq!(report.npm.as_ref().unwrap().mapped_missing.len(), 3);
        assert_eq!(report.cloudflare.as_ref().unwrap().pending_removal.len(), 1);
        let after = (std::fs::read(&path).unwrap(), std::fs::read(&wal).unwrap());
        assert!(after == before, "the audit changed the database or its WAL");
        server_pool.close().await;
    }

    /// A Caddy admin API whose anchor answers `anchor`, or 404 when it is
    /// `None`, to every read, and that fails the test on any write.
    async fn caddy_server(anchor: Option<Value>) -> (ServerGuard, Vec<Mock>) {
        let mut server = Server::new_async().await;
        let read = match anchor {
            Some(routes) => server
                .mock("GET", "/id/docktornet/handle/0/routes")
                .with_status(200)
                .with_body(routes.to_string()),
            None => server
                .mock("GET", "/id/docktornet/handle/0/routes")
                .with_status(404)
                .with_body(r#"{"error":"unknown object ID"}"#),
        }
        .expect_at_least(1)
        .create_async()
        .await;
        let mut guards = forbid_writes(&mut server, None).await;
        guards.push(read);
        (server, guards)
    }

    /// Caddy answers a lookup of `marker` with `route`, or 404 when it is
    /// `None`.
    async fn caddy_lookup(server: &mut ServerGuard, marker: &str, route: Option<Value>) -> Mock {
        let mock = server.mock("GET", format!("/id/{marker}").as_str());
        match route {
            Some(route) => mock.with_status(200).with_body(route.to_string()),
            None => mock
                .with_status(404)
                .with_body(r#"{"error":"unknown object ID"}"#),
        }
        .expect(1)
        .create_async()
        .await
    }

    fn caddy_marker(service_id: &str, index: u8) -> String {
        docktornet_core::caddy_route_marker(service_id, index)
    }

    fn caddy_route(id: &str, host: &str) -> Value {
        json!({
            "@id": id,
            "handle": [{"handler": "reverse_proxy", "upstreams": [{"dial": "10.0.0.1:80"}]}],
            "match": [{"host": [host]}],
            "terminal": true,
        })
    }

    const CADDY_SEED: &[&str] = &[
        "INSERT INTO machines (machine_id, forward_host, last_seen) VALUES ('m1', '10.0.0.1', 9000)",
        "INSERT INTO services (id, machine_id, container, labels_json, last_seen) VALUES \
         ('m1/ok', 'm1', 'ok', '{}', 9000), \
         ('m1/lost', 'm1', 'lost', '{}', 9000), \
         ('m1/moved', 'm1', 'moved', '{}', 9000)",
        "INSERT INTO caddy_routes (service_id, caddy_index, hosts_json, route_json) VALUES \
         ('m1/ok', 0, '[\"ok.example.com\"]', '{}'), \
         ('m1/lost', 0, '[\"lost.example.com\"]', '{}'), \
         ('m1/moved', 0, '[\"moved.example.com\"]', '{}')",
        "INSERT INTO pending_caddy_removals (service_id, caddy_index, hosts_json, last_seen) VALUES \
         ('m1/gone', 1, '[\"gone.example.com\"]', 9800), \
         ('m1/old', 0, '[\"old.example.com\"]', 9900)",
    ];

    #[tokio::test]
    async fn each_caddy_list_is_filled_from_one_case_of_each() {
        let (_dir, path) = seeded_db(CADDY_SEED).await;
        let (npm, _npm_mocks) = npm_server(json!([])).await;
        let (cf, _cf_mocks) = cf_server(json!([])).await;
        let mut unnamed = caddy_route("x", "plain.example.com");
        unnamed.as_object_mut().unwrap().remove("@id");
        let (mut caddy, caddy_mocks) = caddy_server(Some(json!([
            caddy_route(&caddy_marker("m1/ok", 0), "ok.example.com"),
            caddy_route(&caddy_marker("m1/old", 0), "old.example.com"),
            caddy_route(&caddy_marker("m1/stray", 2), "stray.example.com"),
            caddy_route("hand-written", "hand.example.com"),
            unnamed,
        ])))
        .await;
        let lookups = [
            caddy_lookup(&mut caddy, &caddy_marker("m1/lost", 0), None).await,
            caddy_lookup(
                &mut caddy,
                &caddy_marker("m1/moved", 0),
                Some(caddy_route(
                    &caddy_marker("m1/moved", 0),
                    "moved.example.com",
                )),
            )
            .await,
            caddy_lookup(&mut caddy, &caddy_marker("m1/gone", 1), None).await,
        ];
        let cfg = config_with_caddy(&path, &npm, &cf, Some(&caddy));
        let pool = db::connect_read_only(&path).await.unwrap();
        let npm_client = NpmClient::new(cfg.npm.clone());
        let cf_client = CloudflareClient::with_base_url(
            cfg.cloudflare.clone(),
            cfg.cloudflare_api_url.as_str(),
        );
        let caddy_client = CaddyClient::new(cfg.caddy.clone().unwrap());
        let sources = Sources {
            npm_cloudflare: Some((&npm_client, &cf_client)),
            caddy: Some(&caddy_client),
        };

        let report = audit(&pool, &sources, Probe::default(), NOW).await.unwrap();
        for mock in caddy_mocks.iter().chain(&lookups) {
            mock.assert_async().await;
        }

        let hosts = |h: &str| vec![h.to_string()];
        assert_eq!(
            report.caddy,
            Some(CaddyFindings {
                marked_unmapped: vec![CaddyMarkedUnmapped {
                    id: caddy_marker("m1/stray", 2),
                    hosts: hosts("stray.example.com"),
                }],
                pending_removal: vec![
                    CaddyPendingRemoval {
                        id: caddy_marker("m1/gone", 1),
                        hosts: hosts("gone.example.com"),
                        exists: false,
                        service_id: "m1/gone".into(),
                        caddy_index: 1,
                        last_seen: 9800,
                        last_seen_secs_ago: 200,
                    },
                    CaddyPendingRemoval {
                        id: caddy_marker("m1/old", 0),
                        hosts: hosts("old.example.com"),
                        exists: true,
                        service_id: "m1/old".into(),
                        caddy_index: 0,
                        last_seen: 9900,
                        last_seen_secs_ago: 100,
                    },
                ],
                mapped_missing: vec![CaddyMappedMissing {
                    id: caddy_marker("m1/lost", 0),
                    service_id: "m1/lost".into(),
                    caddy_index: 0,
                }],
                marked_outside_anchor: vec![CaddyMarkedOutsideAnchor {
                    id: caddy_marker("m1/moved", 0),
                    hosts: hosts("moved.example.com"),
                    service_id: "m1/moved".into(),
                    caddy_index: 0,
                }],
            })
        );
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(!report.is_clean());
    }

    #[tokio::test]
    async fn a_route_appended_to_the_anchor_during_the_audit_is_no_finding() {
        // The anchor read misses the route, the database read finds its
        // mapping, and the route lands in the anchor before the lookup.
        let (_dir, path) = seeded_db(&CADDY_SEED[..3]).await;
        let (npm, _npm_mocks) = npm_server(json!([])).await;
        let (cf, _cf_mocks) = cf_server(json!([])).await;
        let mut caddy = Server::new_async().await;
        let ok = caddy_route(&caddy_marker("m1/ok", 0), "ok.example.com");
        let lost = caddy_route(&caddy_marker("m1/lost", 0), "lost.example.com");
        let moved = caddy_route(&caddy_marker("m1/moved", 0), "moved.example.com");
        let before = caddy
            .mock("GET", "/id/docktornet/handle/0/routes")
            .with_status(200)
            .with_body(json!([ok]).to_string())
            .expect(1)
            .create_async()
            .await;
        let after = caddy
            .mock("GET", "/id/docktornet/handle/0/routes")
            .with_status(200)
            .with_body(json!([ok, lost]).to_string())
            .create_async()
            .await;
        let lookups = [
            caddy_lookup(&mut caddy, &caddy_marker("m1/lost", 0), Some(lost.clone())).await,
            caddy_lookup(&mut caddy, &caddy_marker("m1/moved", 0), Some(moved)).await,
        ];
        let _writes = forbid_writes(&mut caddy, None).await;
        let cfg = config_with_caddy(&path, &npm, &cf, Some(&caddy));

        let report = run(&cfg, Scope::All).await.unwrap();

        let caddy_findings = report.caddy.unwrap();
        assert_eq!(caddy_findings.mapped_missing, vec![]);
        assert_eq!(
            caddy_findings
                .marked_outside_anchor
                .iter()
                .map(|found| found.service_id.as_str())
                .collect::<Vec<_>>(),
            ["m1/moved"]
        );
        before.assert_async().await;
        after.assert_async().await;
        for mock in &lookups {
            mock.assert_async().await;
        }
    }

    #[tokio::test]
    async fn a_missing_caddy_anchor_is_an_error_and_exits_1() {
        let (_dir, path) = seeded_db(&[]).await;
        let (npm, _npm_mocks) = npm_server(json!([])).await;
        let (cf, _cf_mocks) = cf_server(json!([])).await;
        let (caddy, caddy_mocks) = caddy_server(None).await;
        let cfg = config_with_caddy(&path, &npm, &cf, Some(&caddy));
        let (mut out, mut err) = (Vec::new(), Vec::new());

        let code = report_outcome(run(&cfg, Scope::All).await, &mut out, &mut err);
        for mock in &caddy_mocks {
            mock.assert_async().await;
        }

        assert_eq!(code, 1, "stderr: {}", String::from_utf8_lossy(&err));
        let printed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(
            printed["errors"],
            json!(["Caddy anchor route \"docktornet\" not found"]),
            "{printed}"
        );
        assert_eq!(printed["caddy"]["mapped_missing"], json!([]), "{printed}");
    }

    #[tokio::test]
    async fn only_caddy_audits_caddy_alone() {
        let (_dir, path) = seeded_db(&[]).await;
        let mut npm = Server::new_async().await;
        let mut cf = Server::new_async().await;
        let mut untouched = Vec::new();
        for server in [&mut npm, &mut cf] {
            for method in ["GET", "POST", "PUT", "PATCH", "DELETE"] {
                untouched.push(
                    server
                        .mock(method, Matcher::Any)
                        .expect(0)
                        .create_async()
                        .await,
                );
            }
        }
        let (caddy, caddy_mocks) = caddy_server(Some(json!([]))).await;
        let cfg = config_with_caddy(&path, &npm, &cf, Some(&caddy));
        let (mut out, mut err) = (Vec::new(), Vec::new());

        let code = report_outcome(run(&cfg, Scope::Caddy).await, &mut out, &mut err);
        for mock in untouched.iter().chain(&caddy_mocks) {
            mock.assert_async().await;
        }

        assert_eq!(code, 0, "stderr: {}", String::from_utf8_lossy(&err));
        let printed: Value = serde_json::from_slice(&out).unwrap();
        let empty: Vec<Value> = Vec::new();
        assert_eq!(
            printed,
            json!({
                "caddy": {
                    "marked_unmapped": empty,
                    "pending_removal": empty,
                    "mapped_missing": empty,
                    "marked_outside_anchor": empty,
                },
                "errors": empty,
            })
        );
    }

    #[tokio::test]
    async fn only_caddy_without_caddy_configured_exits_2() {
        let (_dir, path) = seeded_db(&[]).await;
        let (npm, _npm_mocks) = npm_server(json!([])).await;
        let (cf, _cf_mocks) = cf_server(json!([])).await;
        let (mut out, mut err) = (Vec::new(), Vec::new());

        let code = report_outcome(
            run(&config(&path, &npm, &cf), Scope::Caddy).await,
            &mut out,
            &mut err,
        );

        assert_eq!(code, 2);
        assert!(out.is_empty());
        let err = String::from_utf8_lossy(&err);
        assert!(err.contains("DOCKTORNET_CADDY_ADMIN_URL"), "{err}");
    }

    #[tokio::test]
    async fn an_unreachable_caddy_exits_2() {
        let (_dir, path) = seeded_db(&[]).await;
        let (npm, _npm_mocks) = npm_server(json!([])).await;
        let (cf, _cf_mocks) = cf_server(json!([])).await;
        let mut caddy = Server::new_async().await;
        let _down = caddy
            .mock("GET", "/id/docktornet/handle/0/routes")
            .with_status(502)
            .create_async()
            .await;
        let cfg = config_with_caddy(&path, &npm, &cf, Some(&caddy));
        let (mut out, mut err) = (Vec::new(), Vec::new());

        let code = report_outcome(run(&cfg, Scope::All).await, &mut out, &mut err);

        assert_eq!(code, 2);
        assert!(out.is_empty());
        let err = String::from_utf8_lossy(&err);
        assert!(err.contains("Caddy"), "{err}");
    }

    #[tokio::test]
    async fn a_closed_port_is_unreachable_within_the_timeout() {
        let probe = Probe {
            timeout: Duration::from_millis(500),
            concurrency: 8,
        };
        let start = std::time::Instant::now();
        assert!(!probe.reachable("127.0.0.1", closed_port()).await);
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn an_unanswered_connect_is_unreachable_at_the_timeout() {
        // 192.0.2.0/24 is TEST-NET-1: nothing answers, so a connect hangs.
        // Where the network settles it either way first (no route, or a proxy
        // that accepts everything), the timeout cannot be tested.
        let target = ("192.0.2.1", 9);
        if let Ok(settled) =
            tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(target)).await
        {
            eprintln!("skipped: a connect to {target:?} settled at once: {settled:?}");
            return;
        }
        let probe = Probe {
            timeout: Duration::from_millis(300),
            concurrency: 8,
        };
        let start = std::time::Instant::now();
        assert!(!probe.reachable(target.0, target.1).await);
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(300),
            "gave up after {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "gave up after {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn an_open_listener_is_reachable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        assert!(Probe::default().reachable("127.0.0.1", port).await);
    }

    #[tokio::test]
    async fn reachable_all_keeps_target_order_past_the_concurrency_limit() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let open = listener.local_addr().unwrap().port();
        let closed = closed_port();
        let probe = Probe {
            timeout: Duration::from_millis(500),
            concurrency: 2,
        };
        let targets = [open, closed, closed, open, closed]
            .into_iter()
            .map(|port| ("127.0.0.1".to_string(), port))
            .collect();

        assert_eq!(
            probe.reachable_all(targets).await.unwrap(),
            [true, false, false, true, false]
        );
    }
}
