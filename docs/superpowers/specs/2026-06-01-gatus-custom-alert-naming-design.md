# Gatus custom alert provider + clean endpoint naming

**Date:** 2026-06-01
**Status:** Approved design, ready for implementation plan

## Problem

Docktornet generates per-service Gatus endpoint files. Today those files use the
built-in `telegram` alert provider, which hardcodes a verbose, non-customisable
message. The operator wants short Telegram alerts of the shape **state + stack
name + URL**:

```
🔴 DOWN — homepage
🔗 http://192.168.31.49:3000
```

The built-in `telegram` provider cannot be customised. Gatus's `custom` provider
posts a body the operator fully controls, but it performs only plain string
replacement on placeholders — no templating or trimming. So whatever
`[ENDPOINT_NAME]` docktornet emits is shown verbatim. Two current behaviours
block the clean message:

1. The endpoint `name` is `{service_id}/{index}` (e.g. `battercode/homepage/0`),
   not a clean stack name.
2. The alert `type` is `telegram`, not `custom`.

The cross-repo target shape is documented in batterlab
`docs/gatus-telegram-message-format.md`. This spec covers the **docktornet-side**
changes plus the host-side coordination required for a safe rollout.

## Goals

- Emit a clean, human-readable endpoint `name` (bare stack name by default).
- Never lose disambiguation when a single stack has multiple gatus monitors.
- Switch the emitted alert `type` from `telegram` to `custom`.
- Default the endpoint `group` to the host/machine for sensible dashboard
  grouping.
- Keep existing container labels working (no breaking label renames).

## Non-goals

- The host-side `alerting.custom` provider config, `placeholders`, and message
  `body` template. Those live in the operator's base Gatus config
  (`00-base.yaml`) in batterlab, not in docktornet. They are documented here only
  for rollout coordination.
- Renaming the `docktornet.gatus.N.telegram` toggle label.
- Preserving Gatus dashboard history across the cutover (see Migration).

## Design

### 1. New optional label: `docktornet.gatus.N.name`

Add a `name: Option<String>` field to `GatusMonitor`
(`crates/core/src/labels.rs`), parsed from `docktornet.gatus.N.name`. Add `name`
to the gatus field allowlist (`is_gatus_field`) and populate it in
`build_gatus`. The label is optional with no default. An empty string is treated
as unset (same handling as the existing `group` label).

Purpose: let the operator hand-write a display name per monitor when the
auto-derived name isn't desirable — e.g. a prettier label than the container
name, or meaningful labels for multi-monitor stacks (`Homepage (LAN)` /
`Homepage (public)`) instead of numeric suffixes.

### 2. Name resolution (two-pass, in `render_gatus_yaml`)

The collision check needs every monitor of the service at once, so resolution
happens in the renderer where all monitors are available.

**Pass 1 — resolve each monitor's display name:**

- If the `name` label is set → use it verbatim.
- Else → **bare stack name** = the container portion of `service_id`, i.e.
  everything after the first `/`. If `service_id` has no `/`, use it whole.
  Example: `battercode/homepage` → `homepage`.

**Pass 2 — collision tiebreak:**

- Group monitors by resolved name. For any name shared by 2+ monitors, append
  ` {index}` (a space followed by the raw monitor index) to **each** colliding
  member: `homepage` → `homepage 0`, `homepage 1`.
- Monitors with a unique resolved name are left bare.
- The collision pass operates on the final resolved name regardless of source, so
  two monitors that were given identical explicit `name` labels are still
  disambiguated. This keeps Gatus endpoint names unique within the file.
- The raw monitor index (not a re-sequenced counter) is used so the suffix is
  stable across restarts and label reordering.

### 3. Default group → host

In `render_gatus_yaml`, change the group default
(`gatus.rs`, currently `unwrap_or_else(|| service_id.to_string())`) from the full
`service_id` to the **machine/host portion** of `service_id` (the part before the
first `/`; the whole id if there is no `/`). Example: `battercode/homepage` →
`battercode`.

The `docktornet.gatus.N.group` label still overrides the default. `group` is
still always written to every endpoint (unchanged serialization behaviour).

### 4. Alert type → `custom`

In `render_gatus_yaml`, change the emitted `GatusAlert.kind` from
`"telegram"` to `"custom"`. The `send-on-resolved: true` field is unchanged.

The `telegram` bool on `GatusMonitor` and the `docktornet.gatus.N.telegram`
label name are **kept unchanged** — they remain the on/off toggle for whether an
alert block is emitted. Only the rendered `type` string changes. This avoids
breaking every existing container's labels.

### Resulting endpoint (default case)

```yaml
endpoints:
- name: homepage
  group: battercode
  url: http://192.168.31.49:3000
  interval: 30s
  conditions:
  - '[STATUS] == 200'
  alerts:
  - type: custom
    send-on-resolved: true
```

### Resulting Telegram alert

Produced host-side by the `body` template + `placeholders` override in
`00-base.yaml`. Target template (name + URL only; host is dashboard-only and
does not appear in the message):

```
🔴 DOWN — homepage          🟢 BACK UP — homepage
🔗 http://...               🔗 http://...
```

Multi-monitor stacks read as `homepage 0` / `homepage 1` (or the hand-written
`name` labels if set).

## Files touched (docktornet)

- `crates/core/src/labels.rs` — add `name` field to `GatusMonitor`, parse the
  `name` label, extend `is_gatus_field`, populate in `build_gatus`, add tests.
- `crates/server/src/integrations/gatus.rs` — name resolution (two-pass) +
  collision tiebreak, group default → host, alert `kind` → `custom`, update
  existing tests.
- `docs/deployment.md` — replace the `alerting.telegram` base-config example with
  `alerting.custom` (incl. `placeholders` + body template), document the new
  `docktornet.gatus.N.name` label, and update the "required" note.
- `smoke-data/gatus-config/smokebox__...yaml` — regenerate to the new shape (and
  any smoke assertions referencing the old name/group/type).

## Verified against Gatus documentation

- `type: custom` + `send-on-resolved: true` on a per-endpoint alert is valid
  (matches the official README example).
- `[ENDPOINT_NAME]`, `[ENDPOINT_GROUP]`, `[ENDPOINT_URL]`,
  `[ALERT_TRIGGERED_OR_RESOLVED]` are official placeholders; substitution is
  plain string replacement (no templating/trimming) — which is exactly why the
  clean `name` must come from docktornet.
- `alerting.custom.placeholders.ALERT_TRIGGERED_OR_RESOLVED` with
  `TRIGGERED:`/`RESOLVED:` keys is a real, supported override field.

Sources: Gatus README (`Configuring Custom alerts`),
`alerting/provider/custom/custom.go`, gatus.io alerting docs.

## Migration / rollout (host-side coordination)

Existing per-service files are keyed by `service_id`, which is unchanged, so the
**same files are rewritten in place** — no renames, no new files, no orphan
sweep. `resync_gatus` compares rendered content against the file on disk and
rewrites on drift, so every existing managed file is migrated automatically on
the next `full_resync` (runs on startup and periodically).

Consequences to be aware of:

- **Gatus dashboard history resets** for these endpoints. Gatus keys state and
  history by `group` + `name`, both of which change, so each endpoint is seen as
  new. No docktornet data is lost; only Gatus's dashboard graphs restart.
- **Cutover edge case:** a monitor that is DOWN at the moment Gatus reloads will
  not emit a "BACK UP / resolved" for its old identity — the old endpoint
  disappears and a fresh one starts clean.

### Required deploy ordering (hazard)

Gatus validates each endpoint's alert `type` against a configured provider at
config load. The moment docktornet rewrites a file to `type: custom`, the host
`00-base.yaml` **must already** define an `alerting.custom` block, or Gatus fails
to load its config. Because `full_resync` can fire on startup, this can happen
seconds after the new binary boots. Safe sequence:

1. Add the `alerting.custom` block (URL, body template, `placeholders`) to the
   host `00-base.yaml` — **first**. Env vars (`GATUS_TELEGRAM_TOKEN`,
   `GATUS_TELEGRAM_CHAT_ID`) already exist host-side.
2. Deploy the new docktornet binary.
3. Let `full_resync` rewrite the per-service files (or trigger it).
4. Once all files reference `custom`, optionally remove the now-unused
   `alerting.telegram` block.

## Testing

- `labels.rs`: `name` label parsed; empty `name` treated as unset; `name` absent
  → `None`.
- `gatus.rs` renderer:
  - default → bare stack name, group = host, `type: custom`.
  - `name` label set → used verbatim.
  - `group` label set → overrides host default.
  - two monitors, no `name` → ` 0` / ` 1` suffixes; single monitor → no suffix.
  - two monitors with identical explicit `name` → still suffixed.
  - `telegram: false` → no alerts block.
- Smoke fixture regenerated and smoke assertions updated.
