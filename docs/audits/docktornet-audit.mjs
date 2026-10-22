export const meta = {
  name: 'docktornet-audit',
  description: 'Resumable, adversarially-verified correctness + reliability + security audit of the Docktornet Rust workspace (core/agent/server label-driven reconciler), per subsystem, severity-gated and checkpointed',
  phases: [
    { title: 'Scan', detail: 'detect which categories already have a final report on disk' },
    { title: 'Find', detail: 'one Opus-medium finder per remaining category (checkpointed to state/)', model: 'opus' },
    { title: 'Verify', detail: 'diverse-lens panel (code-truth / real-trigger / already-mitigated) per category, severity-gated, Sonnet-medium', model: 'sonnet' },
    { title: 'Write', detail: 'Haiku scribe writes the final category report' },
    { title: 'Summarize', detail: 'regenerate SUMMARY.md operational-risk rollup' },
  ],
}

const ROOT = '/home/battermanz/coding/docktornet'
const DIR = `${ROOT}/docs/audits`
const STATE = `${DIR}/state`
const STACK =
  'Rust workspace (edition 2021), three crates: `core` (label parsing + shared snapshot types), `agent` (polls Docker/Podman sockets via bollard, extracts docktornet.* container labels, pushes snapshots), `server` (axum HTTP ingest + SQLite via sqlx + reconciler). The server persists desired state in SQLite and reconciles THREE external systems: Nginx Proxy Manager (REST), Cloudflare DNS (REST), and Gatus (per-service YAML files in a shared config dir). A single static bearer token authenticates agent->server. Deployment target is a single-operator, LAN-only homelab, BUT it writes to PUBLIC DNS (Cloudflare) and a public-facing reverse proxy (NPM), so a correctness bug can expose or orphan internet-facing resources. Reconciliation runs under a single global mutex, with a circuit breaker and flapping detection guarding the external integrations.'

const CATEGORIES = [
  {
    slug: '01-label-parsing',
    title: 'Container-label parsing into desired state',
    scope: 'correctness of turning raw docktornet.* container labels into typed desired state: index parsing for npm.<index>.*, comma-separated multi-domain host splitting, boolean coercion (true/1/yes vs false/0/no) and the conditional defaults (hsts/http2/etc. derived from ssl), required-vs-optional enforcement, malformed/absurd/adversarial label values, silent drops of a whole entry on one bad field, panics/unwrap on attacker-or-operator-controlled label strings, and ID/hash derivation collisions. A bug here silently mis-routes a public hostname or drops a service.',
    sources: 'crates/core/src/labels.rs, crates/core/src/ids.rs, crates/core/src/snapshot.rs, crates/core/src/lib.rs',
  },
  {
    slug: '02-agent-socket-snapshot',
    title: 'Agent Docker/Podman socket enumeration & snapshot build',
    scope: 'the agent-side snapshot pipeline: bollard container enumeration and error handling when the socket is missing/unreadable/slow, which container states are included vs skipped, label extraction fidelity, snapshot filtering of non-docktornet containers, and construction of the snapshot payload. Look for panics on malformed container inspect data, containers silently missing from the snapshot (=> server treats them as removed), and incorrect forward-address / machine-id derivation.',
    sources: 'crates/agent/src/sockets.rs, crates/agent/src/snapshot.rs, crates/agent/src/main.rs',
  },
  {
    slug: '03-agent-loop-push',
    title: 'Agent tick loop, signal handling, retry & push',
    scope: 'the agent run loop and delivery: SIGTERM/SIGINT handling during an in-flight push (signals recreated inside select! per iteration can be dropped => delayed shutdown), retry/backoff correctness, at-least-once vs duplicate snapshot delivery and whether a duplicate push causes redundant reconciliation, error classification (retryable vs fatal), tick-interval drift, and agent config loading/validation (bad env => crash vs silent misconfig).',
    sources: 'crates/agent/src/loop_.rs, crates/agent/src/push.rs, crates/agent/src/config.rs',
  },
  {
    slug: '04-ingest-auth-transport',
    title: 'Snapshot ingest, bearer-token auth & transport surface',
    scope: 'the server ingress: bearer-token comparison (constant-time vs early-exit timing side-channel), token handling in logs/errors, plaintext-HTTP transport posture and bind address, request body-size limits (unbounded snapshot => memory DoS), deserialization/validation of the ingested snapshot, and the /healthz endpoint (does it leak machine IDs, forward addresses, or internal topology to an unauthenticated caller?).',
    sources: 'crates/server/src/auth.rs, crates/server/src/handlers/snapshot.rs, crates/server/src/handlers/health.rs, crates/server/src/handlers/mod.rs, crates/server/src/routes.rs',
  },
  {
    slug: '05-storage-schema-lifecycle',
    title: 'SQLite persistence, schema & external-ID lifecycle',
    scope: 'the data layer: transaction boundaries and atomicity of multi-statement snapshot persistence, ON DELETE CASCADE behaviour and whether it wipes stored EXTERNAL resource IDs (npm host id / cf record id) at the moment they are still needed for cleanup, migration correctness and forward/backward compatibility (0001-0004), the pending_npm_removals table semantics, dynamically built vs parameterized SQL, concurrent access, and any place a partial write leaves desired-state and external-ID mappings inconsistent.',
    sources: 'crates/server/src/storage.rs, crates/server/src/db.rs, crates/server/migrations/0001_init.sql, crates/server/migrations/0002_external_resources.sql, crates/server/migrations/0003_machines.sql, crates/server/migrations/0004_pending_npm_removals.sql',
  },
  {
    slug: '06-reconcile-core',
    title: 'Reconcile apply path — mutex, isolation & diffing',
    scope: 'the core apply loop: correctness of the single global reconcile mutex (deadlock / long-held-lock stalling ALL machines if one external call hangs), per-machine and per-service failure isolation (one failing service must not block others), snapshot intent-hash diffing (can a real change be skipped as unchanged, or an unchanged snapshot trigger redundant external writes?), ordering of NPM/CF/Gatus steps, and the ingest -> spawn(apply_reconcile) handoff (lost reconciliations, overlapping runs, ordering under rapid pushes).',
    sources: 'crates/server/src/reconcile.rs, crates/server/src/handlers/snapshot.rs',
  },
  {
    slug: '07-reconcile-lifecycle',
    title: 'Reconcile deletion, grace periods & drift repair',
    scope: 'external-resource lifecycle on service removal and drift: grace-period handling before deleting an absent service, whether NPM proxy hosts and Cloudflare DNS records are actually deleted (or leaked forever) when a container disappears, pending-NPM-removal sweep correctness, orphan-Gatus-file sweeps, full_resync drift detection & repair (does dry-run truly not write? does resync converge? can it thrash?), and computation of intended state. This is the highest-risk area: it governs public DNS records and public proxy hosts.',
    sources: 'crates/server/src/reconcile.rs, crates/server/src/integrations/intended.rs',
  },
  {
    slug: '08-npm-integration',
    title: 'Nginx Proxy Manager REST integration',
    scope: 'the NPM client: request timeouts (hung API stalling the reconcile mutex), auth-token fetch & caching, proxy-host create/update/delete, certificate resolution across multi-domain hosts, adoption of a pre-existing host on create-conflict and correct persistence of the adopted id (even if a follow-up update fails), idempotency (skip update when already in sync), comma-domain SAN handling, and error handling / partial-update states that leave NPM diverged from desired state.',
    sources: 'crates/server/src/integrations/npm.rs',
  },
  {
    slug: '09-cloudflare-integration',
    title: 'Cloudflare DNS REST integration',
    scope: 'the Cloudflare client: presence of an HTTP request timeout (a hung CF connection blocks reconcile for every machine), DNS record create/update/delete, record matching / idempotency (proxied flag, type, name, content), zone/record-id handling, API error and rate-limit handling, and any path that leaves a stale or duplicate PUBLIC DNS record. Includes correctness of cf_record_matches.',
    sources: 'crates/server/src/integrations/cloudflare.rs',
  },
  {
    slug: '10-gatus-integration',
    title: 'Gatus per-service YAML file management',
    scope: 'the Gatus file writer: atomicity of writes (temp-file + rename vs partial file), PATH SAFETY when a service/host name is interpolated into a filename (traversal / absolute path / weird chars escaping the config dir), deletion of files for removed services, orphan-file detection, YAML injection or malformed-output from unusual label values, and correctness of what is written (endpoint URL, group, conditions).',
    sources: 'crates/server/src/integrations/gatus.rs, crates/server/src/integrations/mod.rs',
  },
  {
    slug: '11-resilience-breaker-flapping',
    title: 'Circuit breaker & flapping detection',
    scope: 'the resilience layer: circuit-breaker state-machine correctness (open/half-open/closed transitions, threshold & reset-timer math, does it ever get stuck open or fail to trip?), flapping-detection thresholds and window math, concurrency races on the shared breaker/flapping state under overlapping reconciliations, and whether a tripped breaker or flapping-suppressed service leaves external state diverged without recovery.',
    sources: 'crates/server/src/breaker.rs, crates/server/src/flapping.rs',
  },
  {
    slug: '12-server-config-secrets',
    title: 'Server configuration, secrets & startup',
    scope: 'server config loading and validation: required-vs-defaulted settings, fail-fast vs silent-misconfig on bad env, secret/token handling and any exposure in logs or error messages, .env / example-env leakage of real values, bind-address and port defaults, and startup ordering (migrations run before serving? pool configured with sane limits/timeouts?).',
    sources: 'crates/server/src/config.rs, crates/server/src/main.rs, crates/server/src/lib.rs',
  },
]

const FINDING_PROPS = {
  title: { type: 'string' },
  severity: { type: 'string', enum: ['critical', 'high', 'medium', 'low'] },
  category: { type: 'string', enum: ['security', 'correctness'] },
  file: { type: 'string' },
  line: { type: 'string' },
  description: { type: 'string' },
  attackOrTrigger: { type: 'string' },
  why: { type: 'string' },
  fixSketch: { type: 'string' },
}

const FINDINGS_SCHEMA = {
  type: 'object',
  required: ['findings'],
  properties: {
    findings: { type: 'array', items: { type: 'object', required: Object.keys(FINDING_PROPS), properties: FINDING_PROPS } },
  },
}

// A panelist reviews many findings at once and returns one verdict per finding.
const PANEL_SCHEMA = {
  type: 'object',
  required: ['verdicts'],
  properties: {
    verdicts: {
      type: 'array',
      items: {
        type: 'object',
        required: ['title', 'refuted', 'reason'],
        properties: { title: { type: 'string' }, refuted: { type: 'boolean' }, reason: { type: 'string' } },
      },
    },
  },
}

const STATE_SCHEMA = {
  type: 'object',
  required: ['findings', 'panel', 'verified'],
  properties: {
    findings: { type: 'array', items: { type: 'object' } },
    panel: { type: 'array', items: { type: 'object' } }, // [{idx, verdicts:[...]}] — per-panelist checkpoints
    verified: { type: 'array', items: { type: 'object' } },
  },
}

const OK_SCHEMA = { type: 'object', required: ['ok'], properties: { ok: { type: 'boolean' } } }

function finderPrompt(cat) {
  return `You are auditing the Docktornet codebase for CORRECTNESS, RELIABILITY, and SECURITY defects. Docktornet is a label-driven reconciler that writes to PUBLIC DNS (Cloudflare) and a PUBLIC-FACING reverse proxy (NPM), so correctness bugs have real internet-facing impact even though it runs LAN-only.

Stack: ${STACK}
Repo root: ${ROOT} (this repo is indexed by CodeGraph — use codegraph_explore to pull verbatim source + call paths across crate boundaries, then Read specific line ranges to confirm).

Category: ${cat.title}
Scope: ${cat.scope}
Primary sources to read: ${cat.sources}

Find REAL defects that exist in THIS code — not generic Rust lore. Every finding MUST cite an actual file and line you have read. For each, state whether it is a "security" or "correctness" issue (put reliability/race/resource-leak/data-loss bugs under "correctness"), and describe the concrete trigger path: what real sequence — a container label, a snapshot, a reconcile tick, an external-API response, a restart, a signal — makes it bite, and through which functions. Prefer fewer, concrete, code-grounded findings over a long speculative list. If a sub-area is genuinely solid, do not invent issues.

Severity reflects OPERATIONAL impact, not launch-security:
- critical = data loss, an orphaned or wrong PUBLIC DNS record / proxy host, a reconcile deadlock or long-held-mutex stalling ALL machines, or an auth bypass.
- high = per-service breakage, a leaked external resource, a race that causes persistent drift, or a DoS of the reconcile loop.
- medium = a correctness bug with real operator-visible impact, or a hardening gap on the public-writing paths.
- low = defense-in-depth / minor / cosmetic.

Reachability matters: a finding is only real if the code path is actually reached given the mutex, circuit breaker, flapping guard, grace-period logic, and intent-hash short-circuit around it. Check that the guarding logic doesn't already prevent it before claiming it.

CHECKPOINT: After analysis, use the Write tool to save the findings as {"findings": [...]} to ${STATE}/${cat.slug}.findings.json (the Write tool creates parent dirs). Then return the same object via the schema.`
}

// Each panelist reviews through ONE independent lens, so the three votes catch
// different failure modes instead of redundantly agreeing (diverse-lens panel).
const LENSES = {
  1: {
    name: 'code-truth',
    focus: `LENS — CODE TRUTH. Read the exact cited lines and decide whether the Rust, as written, actually does what the finding claims. Refute (refuted=true) if: the cited line/symbol is wrong or stale, the claim misreads the control flow / ownership / async / error-propagation behaviour, the mechanism simply isn't in the code, or an in-function guard/branch/? -operator/match arm already prevents it. You are judging "is the mechanism real in this source?" — nothing else.`,
  },
  2: {
    name: 'real-trigger',
    focus: `LENS — REAL-TRIGGER / REACHABILITY. Assume the code is as described; decide whether a real operational sequence can actually reach and trigger it. Refute (refuted=true) if: the reconcile mutex / circuit breaker / flapping guard / grace-period logic / intent-hash short-circuit prevents the path, the triggering snapshot or label combination can't actually be produced, the "controlled" input is server-controlled or validated upstream, or the ordering the finding assumes cannot occur under the single-mutex reconcile flow. You are judging "can this actually happen end-to-end in a running deployment?" — not whether the code is locally imperfect.`,
  },
  3: {
    name: 'already-mitigated',
    focus: `LENS — ALREADY MITIGATED / SEVERITY. Decide whether the case is already handled or overstated. Refute (refuted=true) if: a guard, a retry, an atomic temp-file+rename, a DB constraint (NOT NULL / FK / UNIQUE), a parameterized/checked query, an existing timeout, a grace period, an error mapper, or the upstream crate already covers it; another code path makes it unreachable; or the impact is trivial/self-healing on the next reconcile so the severity is wrong. You MAY glance at the cited files' imports, the migrations, and adjacent helpers to confirm an existing guard, but do NOT explore the wider repo.`,
  },
}

// Batched panelist: apply ONE lens to a list of findings in a single read pass.
function panelPrompt(cat, subset, idx) {
  const lens = LENSES[idx] || LENSES[1]
  const list = subset
    .map((f, n) => `${n + 1}. [${f.severity}/${f.category}] "${f.title}"\n   at ${f.file}:${f.line}\n   claim: ${f.description}\n   trigger: ${f.attackOrTrigger}\n   reason given: ${f.why}`)
    .join('\n\n')
  const cited = [...new Set(subset.map((f) => f.file))].join(', ')
  return `You are reviewer #${idx} (lens: ${lens.name}) on a panel verifying correctness/reliability/security findings for Docktornet (category: ${cat.title}). Repo root: ${ROOT}.

EFFICIENCY: read ONLY the specific cited files/line ranges below — do NOT explore the wider codebase. The cited files are: ${cited}. Read each one once; it backs several findings.

Stack reminder: ${STACK}

${lens.focus}

Judge each finding THROUGH YOUR LENS ONLY. Set refuted=true only when your lens exposes a genuine reason the finding is not a real, reachable defect. Set refuted=false if, from your lens, it holds up — or if your lens is simply not the right angle to judge it (another reviewer covers the other angles). Do not refute for reasons outside your lens.

Findings to review:
${list}

Return one verdict per finding (match by exact title): {title, refuted, reason}. State your lens's specific reason.`
}

phase('Scan')
const scan = await agent(
  `List the files in ${DIR} (use: ls -la ${DIR} 2>/dev/null). Return the set of category slugs whose "<slug>.md" FINAL report file already exists and is non-empty. Candidate slugs: ${CATEGORIES.map((c) => c.slug).join(', ')}. If the directory does not exist yet, return an empty done list.`,
  { label: 'scan-ledger', phase: 'Scan', model: 'haiku', effort: 'low', schema: { type: 'object', required: ['done'], properties: { done: { type: 'array', items: { type: 'string' } } } } },
)

const doneSet = new Set((scan && scan.done) || [])
const remaining = CATEGORIES.filter((c) => !doneSet.has(c.slug))
log(`${doneSet.size} categories complete; running ${remaining.length}: ${remaining.map((c) => c.slug).join(', ') || '(none)'}`)

for (const cat of remaining) {
  // --- Load checkpointed state (cheap Haiku reader) ---
  const st = await agent(
    `Read these three files if they exist (use: cat <file> 2>/dev/null) and parse each as JSON.
- ${STATE}/${cat.slug}.findings.json -> {"findings":[...]} (or a bare array)
- ${STATE}/${cat.slug}.panel.json -> an array of {"idx":N,"verdicts":[...]} per-panelist checkpoints
- ${STATE}/${cat.slug}.verdicts.json -> an array of already-verified finding objects
Return {"findings": <findings array or []>, "panel": <panel array or []>, "verified": <verified array or []>}. Missing/empty file -> []. Do not invent data.`,
    { label: `load:${cat.slug}`, phase: 'Find', model: 'haiku', effort: 'low', schema: STATE_SCHEMA },
  )

  let findings = (st && st.findings) || []
  let panel = (st && st.panel) || [] // [{idx, verdicts}]
  let verified = (st && st.verified) || []

  // --- Stage 1: find (Opus-medium), only if not checkpointed ---
  if (findings.length === 0) {
    log(`▶ ${cat.slug}: finding (Opus-medium)…`)
    const r = await agent(finderPrompt(cat), { label: `find:${cat.slug}`, phase: 'Find', model: 'opus', effort: 'medium', schema: FINDINGS_SCHEMA })
    findings = (r && r.findings) || []
  } else {
    log(`▶ ${cat.slug}: resumed — ${findings.length} findings, ${panel.length} panelist(s) done, verified=${verified.length}`)
  }

  // --- Stage 2: batched, severity-gated adversarial panel (only if not done) ---
  if (verified.length === 0 && findings.length > 0) {
    const sev = (f) => f.severity
    const highCrit = findings.filter((f) => sev(f) === 'critical' || sev(f) === 'high')
    const medium = findings.filter((f) => sev(f) === 'medium')

    // Panelist subsets: p1 (code-truth lens) covers high/crit + medium (medium gets that 1 lens);
    // p2 (real-trigger) and p3 (already-mitigated) cover high/crit only => 3 diverse lenses on high/crit.
    const panelists = []
    if (highCrit.length > 0) {
      panelists.push({ idx: 1, subset: [...highCrit, ...medium] })
      panelists.push({ idx: 2, subset: highCrit })
      panelists.push({ idx: 3, subset: highCrit })
    } else if (medium.length > 0) {
      panelists.push({ idx: 1, subset: medium })
    }
    log(`  ${cat.slug}: ${highCrit.length} high/crit (3 diverse lenses), ${medium.length} medium (code-truth lens), ${findings.length - highCrit.length - medium.length} low (unverified); ${panelists.length} panelist(s)`)

    // SEQUENTIAL: run each panelist one at a time, checkpoint panel.json after each,
    // so an interrupt loses at most a single panelist.
    const doneIdx = new Set(panel.map((p) => p.idx))
    for (const p of panelists) {
      if (doneIdx.has(p.idx)) continue
      const r = await agent(panelPrompt(cat, p.subset, p.idx), { label: `panel:${cat.slug}#${p.idx}`, phase: 'Verify', model: 'sonnet', effort: 'medium', schema: PANEL_SCHEMA })
      panel.push({ idx: p.idx, verdicts: (r && r.verdicts) || [] })

      const panelBlob = JSON.stringify(panel, null, 2)
      await agent(
        `Use the Write tool to write the following EXACT text to ${STATE}/${cat.slug}.panel.json. Copy byte-for-byte: do not change, summarize, reformat, or comment. Then return {"ok": true}.

<<<JSON
${panelBlob}
JSON`,
        { label: `ckpt-panel:${cat.slug}#${p.idx}`, phase: 'Verify', model: 'haiku', effort: 'low', schema: OK_SCHEMA },
      )
      log(`    ${cat.slug}: panelist ${p.idx}/${panelists.length} done & checkpointed`)
    }

    // Aggregate verdicts across panelists by finding title.
    const verdictsFor = (title) => panel.map((pr) => (pr.verdicts || []).find((v) => v.title === title)).filter(Boolean)
    verified = findings.map((f) => {
      const s = sev(f)
      if (s === 'low') return { ...f, survives: true, status: 'unverified', refutes: 0, votes: 0, verdicts: [] }
      const vs = verdictsFor(f.title)
      const refutes = vs.filter((v) => v.refuted).length
      const needed = s === 'medium' ? 1 : 2 // medium: 1 refute kills; high/crit: majority of 3
      return { ...f, survives: refutes < needed, status: 'verified', refutes, votes: vs.length, verdicts: vs }
    })

    // checkpoint final aggregated verdicts (verify-complete marker)
    const blob = JSON.stringify(verified, null, 2)
    await agent(
      `Use the Write tool to write the following EXACT text to ${STATE}/${cat.slug}.verdicts.json. Copy byte-for-byte: do not change, summarize, reformat, or comment. Then return {"ok": true}.

<<<JSON
${blob}
JSON`,
      { label: `ckpt:${cat.slug}`, phase: 'Verify', model: 'haiku', effort: 'low', schema: OK_SCHEMA },
    )
  }

  // --- Stage 3: final report (Haiku) ---
  const confirmed = verified.filter((v) => v.survives)
  const rejected = verified.filter((v) => !v.survives)
  const payload = JSON.stringify({ confirmed, rejected }, null, 2)
  await agent(
    `Write a markdown audit report to ${DIR}/${cat.slug}.md using the Write tool. Category title: "${cat.title}".

Structure:
- H1 with the category title.
- One-line summary: N confirmed (by severity), M refuted. Note that LOW findings are included as "unverified" (panel did not vote on them).
- H2 "Confirmed findings": for EACH confirmed finding an H3 "SEVERITY [security|correctness]: title" (append " (unverified)" if its status is unverified), then bullets: Trigger, Location (\`file:line\`), What happens, Why, Fix sketch. Order critical > high > medium > low.
- H2 "Refuted (not real / already handled)": each rejected finding title + one-line reason from its verdicts.

Data (JSON, each finding has severity/category/status/verdicts):
${payload}

After writing, return {"ok": true}.`,
    { label: `write:${cat.slug}`, phase: 'Write', model: 'haiku', effort: 'low', schema: OK_SCHEMA },
  )
  log(`✔ ${cat.slug}: banked (${confirmed.length} confirmed, ${rejected.length} refuted)`)
}

phase('Summarize')
const SUMMARY_SCHEMA = { type: 'object', required: ['markdown'], properties: { markdown: { type: 'string' } } }
const summaryGenPrompt = `Read every NN-*.md category report in ${DIR} (files named NN-*.md, NOT SUMMARY.md, NOT anything under state/). Produce an operational-risk rollup as MARKDOWN: a table of all confirmed findings across categories sorted by severity (columns: Severity, Type, Category, Title, Location), plus a "Top operational risks" section listing the critical/high items with one line each. Mark unverified (low) rows as such. Keep it concise. Do NOT write any file — RETURN the full markdown text as {"markdown": "..."}.`

// Split "read/think" from "write": the generator only RETURNS markdown (schema-forced),
// so it cannot silently no-op. Retry once on empty.
let summaryMd = ''
for (let attempt = 1; attempt <= 2 && !summaryMd.trim(); attempt++) {
  const s = await agent(summaryGenPrompt, { label: `summary-gen#${attempt}`, phase: 'Summarize', model: 'haiku', effort: 'low', schema: SUMMARY_SCHEMA })
  summaryMd = (s && s.markdown) || ''
  if (!summaryMd.trim()) log(`summary generator returned empty (attempt ${attempt})`)
}

// Write, then VERIFY on disk with an independent ground-truth check (wc -c) — a write agent
// can return {ok:true} without ever calling Write, so never trust its own report.
// Retry the write until the bytes are actually present.
const target = `${DIR}/SUMMARY.md`
let summaryWritten = false
if (summaryMd.trim()) {
  for (let attempt = 1; attempt <= 3 && !summaryWritten; attempt++) {
    await agent(
      `Use the Write tool to write the EXACT text between the markers to ${target} — byte-for-byte, no changes, no commentary, no code fence. Then Read ${target} back to confirm it is present. Return {"ok": true}.

<<<MARKDOWN
${summaryMd}
MARKDOWN`,
      { label: `summary-write#${attempt}`, phase: 'Summarize', model: 'haiku', effort: 'low', schema: OK_SCHEMA },
    )
    const chk = await agent(
      `Run exactly this shell command: wc -c < "${target}" 2>/dev/null || echo 0 . Return {"ok": true} if the printed byte count is greater than 50, otherwise {"ok": false}. Do not write or modify anything.`,
      { label: `summary-verify#${attempt}`, phase: 'Summarize', model: 'haiku', effort: 'low', schema: OK_SCHEMA },
    )
    summaryWritten = !!(chk && chk.ok)
    if (!summaryWritten) log(`SUMMARY.md not confirmed on disk (attempt ${attempt}) — retrying write`)
  }
  log(summaryWritten ? `SUMMARY.md written & verified (${summaryMd.length} chars)` : 'ERROR: SUMMARY.md could not be written after 3 attempts — regenerate manually from the NN-*.md reports')
} else {
  log('WARNING: summarizer returned empty markdown twice; SUMMARY.md NOT written — regenerate manually from the NN-*.md reports')
}

return { done: [...doneSet], ran: remaining.map((c) => c.slug), summaryWritten }
