export const meta = {
  name: 'docktornet-fix',
  description: 'Sequential, resumable TDD implementation of the confirmed docktornet-audit findings — one finding at a time (red→green→refactor→commit), checkpointed after each so an interrupt loses at most one fix',
  phases: [
    { title: 'Plan', detail: 'build (or reload) the ordered fix work-list from the audit verdicts, checkpointed to state/fixplan.json' },
    { title: 'Fix', detail: 'sequential per-finding TDD cycle on Opus; commit to development + checkpoint fixlog after each', model: 'opus' },
    { title: 'Summarize', detail: 'regenerate FIX-SUMMARY.md from the fix log' },
  ],
}

const ROOT = '/home/battermanz/coding/docktornet'
const DIR = `${ROOT}/docs/audits`
const STATE = `${DIR}/state`
const BRANCH = 'development'
const TEST_CMD = 'just test' // cargo test --workspace
const LINT_CMD = 'just lint' // cargo clippy --workspace --all-targets -- -D warnings
const FMT_CMD = 'just fmt'

// Which severities to implement. Low/unverified are INCLUDED this run per operator request.
const FIX_SEVERITIES = ['critical', 'high', 'medium', 'low']
const SEV_RANK = { critical: 0, high: 1, medium: 2, low: 3 }

const OK_SCHEMA = { type: 'object', required: ['ok'], properties: { ok: { type: 'boolean' } } }

// One planned fix item.
const PLAN_ITEM_PROPS = {
  id: { type: 'string' }, // stable: "<slug>::<severity>::<title>"
  slug: { type: 'string' },
  severity: { type: 'string' },
  category: { type: 'string' },
  title: { type: 'string' },
  file: { type: 'string' },
  line: { type: 'string' },
  description: { type: 'string' },
  attackOrTrigger: { type: 'string' },
  why: { type: 'string' },
  fixSketch: { type: 'string' },
}
const PLAN_SCHEMA = {
  type: 'object',
  required: ['items'],
  properties: { items: { type: 'array', items: { type: 'object', required: ['slug', 'severity', 'title'], properties: PLAN_ITEM_PROPS } } },
}

// A fixer's structured result for one finding.
const FIX_RESULT_SCHEMA = {
  type: 'object',
  required: ['status', 'commit', 'testsPassing', 'notes'],
  properties: {
    status: { type: 'string', enum: ['fixed', 'skipped', 'blocked'] },
    commit: { type: 'string' }, // commit hash, or '' if none
    filesTouched: { type: 'array', items: { type: 'string' } },
    testsPassing: { type: 'boolean' },
    notes: { type: 'string' },
  },
}

const FIXLOG_SCHEMA = {
  type: 'object',
  required: ['plan', 'log'],
  properties: {
    plan: { type: 'array', items: { type: 'object' } },
    log: { type: 'array', items: { type: 'object' } },
  },
}

const idOf = (slug, f) => `${slug}::${f.severity}::${f.title}`

function fixerPrompt(item) {
  return `You are implementing ONE confirmed defect from the Docktornet audit, using strict Test-Driven Development. Repo root: ${ROOT} (Rust workspace; indexed by CodeGraph — use codegraph_explore to pull verbatim source + call paths, then Read exact lines before editing). You are on git branch "${BRANCH}".

FINDING [${item.severity}/${item.category}] "${item.title}"
Location: ${item.file}:${item.line}
What happens: ${item.description}
Trigger: ${item.attackOrTrigger || '(see description)'}
Why it matters: ${item.why || ''}
Suggested fix direction: ${item.fixSketch || '(design the minimal correct fix yourself)'}

Follow this sequence EXACTLY:
1. RED — write a test that reproduces the defect and currently FAILS. Put it with the existing tests for the affected crate/module (match the surrounding test style). Run \`${TEST_CMD}\` (or a narrower \`cargo test -p <crate> <name>\`) and CONFIRM the new test fails for the stated reason. If you cannot write a failing test that captures the defect, do NOT guess — set status "blocked" with a note explaining why, make NO commit, and ensure the working tree is clean (revert any partial edits).
2. GREEN — make the minimal code change that fixes the defect and makes the test pass. Do not refactor unrelated code or add scope beyond this finding.
3. REFACTOR — tidy only what you just touched, keeping tests green.
4. VERIFY — run the FULL \`${TEST_CMD}\` and \`${LINT_CMD}\`, plus \`${FMT_CMD}\`. Everything must pass with no clippy warnings. If you cannot reach green + clean lint, revert ALL your changes (\`git checkout -- .\` / restore touched files) so the tree is clean for the next fix, and set status "blocked" with a note. NEVER commit a red or lint-failing tree.
5. COMMIT — stage and commit ONLY the files for this fix to branch ${BRANCH}, with a message like: "fix(<area>): <short desc> [audit ${item.severity}]" and a body referencing the finding title. Committing to ${BRANCH} is pre-approved for this run. Capture the commit hash (\`git rev-parse --short HEAD\`).

If on inspection the finding is already handled in the current code (a prior fix, or it was a false positive that slipped through), set status "skipped", make no commit, note why.

Leave the working tree CLEAN when you finish (everything either committed or reverted). Return {status, commit, filesTouched, testsPassing, notes}.`
}

// ---------------- Plan phase ----------------
phase('Plan')

// Reload an existing plan first (resumability), else build it from the audit verdicts.
const loaded = await agent(
  `Read these two files if they exist (use: cat <file> 2>/dev/null) and parse each as JSON.
- ${STATE}/fixplan.json -> {"items":[...]} (the ordered fix work-list) or a bare array
- ${STATE}/fixlog.json -> an array of per-finding result records {"id":...,"status":...}
Return {"plan": <items array or []>, "log": <log array or []>}. Missing/empty file -> []. Do not invent data.`,
  { label: 'load-fixstate', phase: 'Plan', model: 'haiku', effort: 'low', schema: FIXLOG_SCHEMA },
)

let plan = (loaded && loaded.plan) || []
let fixlog = (loaded && loaded.log) || []

if (plan.length === 0) {
  // Build the plan from the audit's structured verdicts (survives === true).
  const built = await agent(
    `Read EVERY file matching ${STATE}/*.verdicts.json (first: ls ${STATE}/*.verdicts.json 2>/dev/null, then cat each). Each file is a JSON array of finding objects; the filename stem before ".verdicts.json" is that finding's category slug (e.g. 06-reconcile-core). Each object has: title, severity, category, file, line, description, attackOrTrigger, why, fixSketch, and a boolean "survives".
Collect ONLY objects where survives === true AND severity is one of ${JSON.stringify(FIX_SEVERITIES)}. For each, emit an item: {slug:<category slug from filename>, severity, category, title, file, line, description, attackOrTrigger, why, fixSketch}. Do NOT invent, merge, or reword findings. Return {"items":[ ... ]}. If there are no verdicts files, return {"items":[]}.`,
    { label: 'build-plan', phase: 'Plan', model: 'sonnet', effort: 'low', schema: PLAN_SCHEMA },
  )
  const items = ((built && built.items) || []).map((f) => ({ ...f, id: idOf(f.slug, f) }))
  // Deterministic order: severity (crit→high→med), then slug, then title.
  items.sort((a, b) => (SEV_RANK[a.severity] - SEV_RANK[b.severity]) || a.slug.localeCompare(b.slug) || a.title.localeCompare(b.title))
  plan = items

  const blob = JSON.stringify({ items: plan }, null, 2)
  await agent(
    `Use the Write tool to write the following EXACT text to ${STATE}/fixplan.json. Copy byte-for-byte: no changes, no commentary. Then return {"ok": true}.

<<<JSON
${blob}
JSON`,
    { label: 'ckpt-fixplan', phase: 'Plan', model: 'haiku', effort: 'low', schema: OK_SCHEMA },
  )
}

if (plan.length === 0) {
  log('No confirmed critical/high/medium findings to fix (empty plan). Nothing to do.')
  return { planned: 0, fixed: 0, skipped: 0, blocked: 0 }
}

// Any id already recorded (fixed/skipped/blocked) is not re-attempted — same
// resumability contract as the audit: an interrupt loses at most one finding.
const doneIds = new Set(fixlog.map((e) => e.id))
const todo = plan.filter((it) => !doneIds.has(it.id))
log(`Fix plan: ${plan.length} finding(s) total; ${doneIds.size} already handled; ${todo.length} remaining → ${todo.map((t) => t.severity[0].toUpperCase() + ':' + t.slug).join(', ') || '(none)'}`)

// ---------------- Fix phase (STRICTLY SEQUENTIAL) ----------------
phase('Fix')
for (const item of todo) {
  log(`▶ fixing [${item.severity}] ${item.slug} — "${item.title}" (TDD, Opus)…`)
  const r = await agent(fixerPrompt(item), { label: `fix:${item.slug}:${item.severity}`, phase: 'Fix', model: 'opus', effort: 'medium', schema: FIX_RESULT_SCHEMA })
  const rec = {
    id: item.id,
    slug: item.slug,
    severity: item.severity,
    title: item.title,
    status: (r && r.status) || 'blocked',
    commit: (r && r.commit) || '',
    filesTouched: (r && r.filesTouched) || [],
    testsPassing: !!(r && r.testsPassing),
    notes: (r && r.notes) || '(no result returned)',
  }
  fixlog.push(rec)

  // Checkpoint the machine-readable log (source of truth for resume).
  const blob = JSON.stringify(fixlog, null, 2)
  await agent(
    `Use the Write tool to write the following EXACT text to ${STATE}/fixlog.json. Copy byte-for-byte: no changes, no commentary. Then return {"ok": true}.

<<<JSON
${blob}
JSON`,
    { label: `ckpt-fixlog:${item.slug}`, phase: 'Fix', model: 'haiku', effort: 'low', schema: OK_SCHEMA },
  )

  // Append a human-readable line to FIX-LOG.md (best-effort, idempotent enough).
  const mdLine = `- [${rec.status}] **${rec.severity}** \`${rec.slug}\` — ${rec.title}${rec.commit ? ` — commit \`${rec.commit}\`` : ''}${rec.testsPassing ? ' — tests green' : ''}${rec.notes ? ` — ${rec.notes.replace(/\n+/g, ' ')}` : ''}`
  await agent(
    `Append EXACTLY one line (create the file with an "# Docktornet fix log" H1 first if it does not exist) to ${DIR}/FIX-LOG.md. The line to append verbatim:

${mdLine}

Use Read to get current contents (if any), then Write the file back with your line added at the end. Return {"ok": true}.`,
    { label: `mdlog:${item.slug}`, phase: 'Fix', model: 'haiku', effort: 'low', schema: OK_SCHEMA },
  )
  log(`  ${rec.status} — ${item.slug} ${rec.commit ? `(${rec.commit})` : ''}`)
}

// ---------------- Summarize ----------------
phase('Summarize')
const counts = fixlog.reduce((a, e) => ((a[e.status] = (a[e.status] || 0) + 1), a), {})
const SUMMARY_SCHEMA = { type: 'object', required: ['markdown'], properties: { markdown: { type: 'string' } } }
const gen = await agent(
  `Produce a concise MARKDOWN fix-run summary. Data (JSON array of per-finding results):
${JSON.stringify(fixlog, null, 2)}

Structure: H1 "Docktornet fix run"; a one-line tally (fixed/skipped/blocked); a table (Status, Severity, Category slug, Title, Commit); then an H2 "Needs manual attention" listing every BLOCKED finding with its note. Do NOT write any file — RETURN the markdown as {"markdown":"..."}.`,
  { label: 'fixsummary-gen', phase: 'Summarize', model: 'haiku', effort: 'low', schema: SUMMARY_SCHEMA },
)
const summaryMd = (gen && gen.markdown) || ''
if (summaryMd.trim()) {
  const target = `${DIR}/FIX-SUMMARY.md`
  let written = false
  for (let attempt = 1; attempt <= 3 && !written; attempt++) {
    await agent(
      `Use the Write tool to write the EXACT text between the markers to ${target} — byte-for-byte, no changes, no code fence. Then Read it back to confirm. Return {"ok": true}.

<<<MARKDOWN
${summaryMd}
MARKDOWN`,
      { label: `fixsummary-write#${attempt}`, phase: 'Summarize', model: 'haiku', effort: 'low', schema: OK_SCHEMA },
    )
    const chk = await agent(
      `Run exactly: wc -c < "${target}" 2>/dev/null || echo 0 . Return {"ok": true} if the printed number is greater than 50, else {"ok": false}. Do not modify anything.`,
      { label: `fixsummary-verify#${attempt}`, phase: 'Summarize', model: 'haiku', effort: 'low', schema: OK_SCHEMA },
    )
    written = !!(chk && chk.ok)
  }
  log(written ? 'FIX-SUMMARY.md written & verified' : 'ERROR: FIX-SUMMARY.md not confirmed on disk')
}

log(`Fix run complete: ${counts.fixed || 0} fixed, ${counts.skipped || 0} skipped, ${counts.blocked || 0} blocked (of ${plan.length} planned).`)
return { planned: plan.length, fixed: counts.fixed || 0, skipped: counts.skipped || 0, blocked: counts.blocked || 0 }
