-- last_seen used to hold the agent's clock (the snapshot's taken_at); it now
-- holds the server's.  Restamp the values grace periods are judged by with the
-- upgrade time, so no agent-clock value is left to cut a grace period short.
-- A pending removal restarts its grace period, which only delays a deletion.
-- machines.last_seen decides no grace period; /healthz and the audit show it,
-- and it is overwritten at the next push, so a machine that is gone keeps its
-- last contact rather than looking freshly seen.
UPDATE services SET last_seen = CAST(strftime('%s', 'now') AS INTEGER);
UPDATE pending_npm_removals SET last_seen = CAST(strftime('%s', 'now') AS INTEGER);
UPDATE pending_cf_removals SET last_seen = CAST(strftime('%s', 'now') AS INTEGER);
