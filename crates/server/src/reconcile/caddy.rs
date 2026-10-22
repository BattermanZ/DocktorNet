//! The Caddy side of a machine's apply: departed services, dropped indices,
//! host claims and route writes inside the anchor route.

use std::collections::HashSet;

use docktornet_core::caddy_route_marker;
use docktornet_core::labels::ParsedLabels;
use serde_json::Value;

use super::{
    GATUS_REMOVAL_GRACE_SECS, ReconcileError, Reconciler, RemovedService, ResyncReport, past_grace,
};
use crate::integrations::caddy::{CaddyClient, CaddyError, position_of, render_route};
use crate::integrations::intended::IntendedService;
use crate::storage::{CaddyClaim, CaddyRouteRow};

/// How the Caddy part of an apply went, for the apply's failure count and
/// intent-hash caching.
#[derive(Debug, Default)]
pub(super) struct CaddyApply {
    pub failures: usize,
    /// The breaker was open, so the Caddy work waits for a later apply.
    pub skipped: bool,
}

/// Why one route couldn't be written.
enum WriteError {
    Caddy(CaddyError),
    /// A route carrying the marker sits elsewhere in Caddy's config.  Adding
    /// it to the anchor would be a duplicate `@id`, which Caddy rejects.
    OutsideAnchor,
}

impl Reconciler {
    /// Route this reconciler's `caddy.*` work through `caddy`.
    pub fn with_caddy(mut self, caddy: CaddyClient) -> Self {
        self.caddy = Some(caddy);
        self
    }

    /// Reconcile the Caddy routes of a machine's live services, forwarding to
    /// the machine's `forward_host`, and delete the routes of the services in
    /// `removed` that left it, once `due` says a removal is past its grace
    /// period with its service still gone.  `now` is when the snapshot was
    /// persisted.
    pub(super) async fn apply_caddy(
        &self,
        intended: &[IntendedService],
        removed: &[RemovedService],
        due: impl Fn(&RemovedService) -> bool,
        forward_host: &str,
        now: i64,
    ) -> Result<CaddyApply, ReconcileError> {
        let mut out = CaddyApply::default();
        let departed: Vec<&RemovedService> = removed
            .iter()
            .filter(|rs| !rs.caddy_routes.is_empty())
            .collect();
        let mut work: Vec<(&IntendedService, Vec<CaddyRouteRow>)> = Vec::new();
        for service in intended {
            let rows = crate::storage::list_caddy_routes_for_service(&self.db, &service.service_id)
                .await?;
            if !service.labels.caddy.is_empty() || !rows.is_empty() {
                work.push((service, rows));
            }
        }
        if work.is_empty() && departed.is_empty() {
            return Ok(out);
        }

        let Some(client) = &self.caddy else {
            for (service, rows) in &work {
                if !service.labels.caddy.is_empty() {
                    tracing::warn!(
                        service_id = %service.service_id,
                        "Caddy labels present but Caddy is not configured; skipping them"
                    );
                }
                self.park_dropped_caddy_routes(service, rows, now).await?;
            }
            return Ok(out);
        };
        if self.caddy_breaker.is_open(std::time::Instant::now()) {
            tracing::warn!("caddy circuit breaker open; skipping Caddy reconcile this cycle");
            for (service, rows) in &work {
                self.park_dropped_caddy_routes(service, rows, now).await?;
            }
            out.skipped = true;
            return Ok(out);
        }

        let mut breaker_failed = false;
        // Whether this apply sent Caddy a request, so the breaker hears only
        // about calls that were made.
        let mut contacted = false;

        // The routes of services that left are pending removals already,
        // parked when the snapshot was persisted.  Past the grace period they
        // are deleted here; otherwise the sweep deletes them once it ends.  A
        // pending route keeps serving and keeps its hosts.  A service back in
        // this snapshot gets its labelled routes back from the pending rows
        // when its groups claim them below.
        for rs in departed.iter().filter(|rs| due(rs)) {
            for (caddy_index, _) in &rs.caddy_routes {
                let marker = caddy_route_marker(&rs.service_id, index_u8(*caddy_index));
                contacted = true;
                match client.delete_route(&marker).await {
                    Ok(()) => {
                        tracing::info!(service_id = %rs.service_id, marker, "caddy route deleted: its service is gone");
                        crate::storage::delete_pending_caddy_removal(
                            &self.db,
                            &rs.service_id,
                            *caddy_index,
                        )
                        .await?;
                    }
                    Err(err) => {
                        tracing::warn!(service_id = %rs.service_id, marker, error = %err, "caddy route delete failed; deferring");
                        out.failures += 1;
                        breaker_failed |= err.feeds_breaker();
                    }
                }
            }
        }

        // Dropped indices next, for every service: a dropped group is a label
        // edit, so its route goes in this apply with no grace period, and its
        // hosts are free before any group claims them.
        for (service, rows) in &work {
            for row in dropped(service, rows) {
                let marker = caddy_route_marker(&service.service_id, index_u8(row.caddy_index));
                contacted = true;
                match client.delete_route(&marker).await {
                    Ok(()) => {
                        tracing::info!(service_id = %service.service_id, marker, "caddy route deleted: its labels are gone");
                        crate::storage::delete_caddy_route(
                            &self.db,
                            &service.service_id,
                            row.caddy_index,
                        )
                        .await?;
                    }
                    Err(err) => {
                        tracing::warn!(service_id = %service.service_id, marker, error = %err, "caddy route delete failed; deferring");
                        out.failures += 1;
                        breaker_failed |= err.feeds_breaker();
                        self.park_caddy_route(&service.service_id, row, now).await?;
                    }
                }
            }
        }

        if work
            .iter()
            .any(|(service, _)| !service.labels.caddy.is_empty())
        {
            let pending = crate::storage::list_pending_caddy_removals(&self.db).await?;
            contacted = true;
            match client.anchor_routes().await {
                Ok(Some(mut anchor)) => {
                    for (service, rows) in &work {
                        // A pending row the claim moves back is a restore, not
                        // an adoption, so it counts as mapped.
                        let mapped: HashSet<i64> = rows
                            .iter()
                            .map(|row| row.caddy_index)
                            .chain(
                                pending
                                    .iter()
                                    .filter(|row| row.service_id == service.service_id)
                                    .map(|row| row.caddy_index),
                            )
                            .collect();
                        out.failures += self
                            .claim_and_write(
                                client,
                                &mut anchor,
                                service,
                                &mapped,
                                forward_host,
                                &mut breaker_failed,
                            )
                            .await?;
                    }
                }
                Ok(None) => {
                    tracing::error!(
                        anchor = client.anchor_id(),
                        "caddy anchor route not found; add it to Caddy's config, docktornet never creates it"
                    );
                    out.failures += 1;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "caddy reconcile skipped: reading the anchor's routes failed");
                    out.failures += 1;
                    breaker_failed |= err.feeds_breaker();
                }
            }
        }

        if breaker_failed {
            self.caddy_breaker.record_failure(std::time::Instant::now());
        } else if contacted {
            self.caddy_breaker.record_success();
        }
        Ok(out)
    }

    /// Claim the hosts of each of `service`'s `caddy.N` groups and write the
    /// claimed routes into `anchor`, Caddy's anchor routes as last read.  A
    /// claim takes over the service's pending removal at its index, which
    /// restores the mapping of a service that came back.  `mapped` holds the
    /// indices the service has a mapping or pending removal at.  Returns how
    /// many groups failed.
    async fn claim_and_write(
        &self,
        client: &CaddyClient,
        anchor: &mut Vec<Value>,
        service: &IntendedService,
        mapped: &HashSet<i64>,
        forward_host: &str,
        breaker_failed: &mut bool,
    ) -> Result<usize, ReconcileError> {
        let service_id = service.service_id.as_str();
        let mut failures = 0;
        for route in &service.labels.caddy {
            let marker = caddy_route_marker(service_id, route.index);
            let rendered = render_route(&marker, route, forward_host);
            match crate::storage::claim_caddy_route(
                &self.db,
                service_id,
                route.index.into(),
                &route.hosts,
                &rendered,
            )
            .await?
            {
                CaddyClaim::Claimed => {}
                CaddyClaim::Clash {
                    host,
                    owner,
                    owner_index,
                } => {
                    tracing::error!(
                        service_id,
                        index = route.index,
                        host,
                        owner,
                        owner_index,
                        "refusing caddy route: another service's caddy route holds the host"
                    );
                    failures += 1;
                    continue;
                }
            }
            if !mapped.contains(&i64::from(route.index)) && position_of(anchor, &marker).is_some() {
                tracing::info!(service_id, marker, "caddy route adopted by marker");
            }
            match write_route(client, anchor, &marker, rendered).await {
                Ok(()) => {}
                Err(WriteError::Caddy(err)) => {
                    tracing::warn!(service_id, marker, error = %err, "caddy route write failed");
                    *breaker_failed |= err.feeds_breaker();
                    failures += 1;
                }
                Err(WriteError::OutsideAnchor) => {
                    tracing::error!(
                        service_id,
                        marker,
                        anchor = client.anchor_id(),
                        "a caddy route with this service's marker sits outside the anchor route; leaving it alone"
                    );
                    failures += 1;
                }
            }
        }
        Ok(failures)
    }

    /// Delete the Caddy routes recorded in `pending_caddy_removals` once they
    /// are past the grace period.  These are the removals the apply deferred:
    /// within grace, breaker open, Caddy unconfigured, or a failed delete.  A
    /// row whose service is stored again with its index labelled is left to
    /// that service's apply, and a row whose delete fails stays for the next
    /// sweep.  Caddy is reached only when a row is due and the breaker is
    /// closed.
    pub(super) async fn sweep_pending_caddy_removals(&self, report: &mut ResyncReport) {
        let pending = match crate::storage::list_pending_caddy_removals(&self.db).await {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(error = %err, "could not list pending caddy removals");
                return;
            }
        };
        let Some(client) = &self.caddy else {
            return;
        };
        let now = self.clock.now();
        let mut due = Vec::new();
        for row in pending {
            if past_grace(row.last_seen, now)
                && !self
                    .stored_again(&row.service_id, |labels| {
                        lists_caddy_index(labels, row.caddy_index)
                    })
                    .await
            {
                due.push(row);
            }
        }
        if due.is_empty() {
            return;
        }
        if self.caddy_breaker.is_open(std::time::Instant::now()) {
            tracing::warn!("caddy circuit breaker open; deferring pending caddy removals");
            return;
        }

        let mut breaker_failed = false;
        for row in due {
            let marker = caddy_route_marker(&row.service_id, index_u8(row.caddy_index));
            match client.delete_route(&marker).await {
                Ok(()) => {
                    tracing::info!(service_id = %row.service_id, marker, "pending caddy route deleted");
                    match crate::storage::delete_pending_caddy_removal(
                        &self.db,
                        &row.service_id,
                        row.caddy_index,
                    )
                    .await
                    {
                        Ok(()) => report.repaired += 1,
                        Err(err) => {
                            tracing::warn!(service_id = %row.service_id, error = %err, "failed to clear pending caddy removal");
                            report.failed += 1;
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(service_id = %row.service_id, marker, error = %err, "pending caddy route delete failed");
                    report.failed += 1;
                    breaker_failed |= err.feeds_breaker();
                }
            }
        }
        if breaker_failed {
            self.caddy_breaker.record_failure(std::time::Instant::now());
        } else {
            self.caddy_breaker.record_success();
        }
    }

    /// Park every mapping row of `service` whose index its labels dropped.
    async fn park_dropped_caddy_routes(
        &self,
        service: &IntendedService,
        rows: &[CaddyRouteRow],
        now: i64,
    ) -> Result<(), ReconcileError> {
        for row in dropped(service, rows) {
            self.park_caddy_route(&service.service_id, row, now).await?;
        }
        Ok(())
    }

    /// Move a dropped route's mapping to the pending removals, already past
    /// the grace period: a label edit gets none, so the next sweep retries the
    /// delete.
    async fn park_caddy_route(
        &self,
        service_id: &str,
        row: &CaddyRouteRow,
        now: i64,
    ) -> Result<(), ReconcileError> {
        crate::storage::park_caddy_route(
            &self.db,
            service_id,
            row.caddy_index,
            &row.hosts,
            now.saturating_sub(GATUS_REMOVAL_GRACE_SECS),
        )
        .await?;
        Ok(())
    }
}

/// Whether `labels` hold a `docktornet.caddy.<caddy_index>` group.
fn lists_caddy_index(labels: &ParsedLabels, caddy_index: i64) -> bool {
    labels
        .caddy
        .iter()
        .any(|route| i64::from(route.index) == caddy_index)
}

/// The mapping rows of `service` whose index is no longer in its labels.
fn dropped<'a>(
    service: &'a IntendedService,
    rows: &'a [CaddyRouteRow],
) -> impl Iterator<Item = &'a CaddyRouteRow> {
    rows.iter()
        .filter(|row| !lists_caddy_index(&service.labels, row.caddy_index))
}

fn index_u8(caddy_index: i64) -> u8 {
    u8::try_from(caddy_index).expect("caddy indices are stored from a u8")
}

/// Make the anchor hold `rendered` under `marker`.  A matching route is left
/// alone, a differing one is patched in place, and a missing one is appended
/// unless the marker sits elsewhere in Caddy's config.  Routes are compared
/// as parsed JSON, because Caddy stores them with sorted keys.
async fn write_route(
    client: &CaddyClient,
    anchor: &mut Vec<Value>,
    marker: &str,
    rendered: Value,
) -> Result<(), WriteError> {
    match position_of(anchor, marker) {
        Some(i) if anchor[i] == rendered => {
            tracing::debug!(marker, "caddy route already in sync");
        }
        Some(i) => {
            client
                .patch_route(marker, &rendered)
                .await
                .map_err(WriteError::Caddy)?;
            tracing::info!(marker, "caddy route updated");
            anchor[i] = rendered;
        }
        None => {
            if client
                .get_route(marker)
                .await
                .map_err(WriteError::Caddy)?
                .is_some()
            {
                return Err(WriteError::OutsideAnchor);
            }
            client
                .append_route(&rendered)
                .await
                .map_err(WriteError::Caddy)?;
            tracing::info!(marker, "caddy route created");
            anchor.push(rendered);
        }
    }
    Ok(())
}
