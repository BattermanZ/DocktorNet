# Scheduling the audit + fix workflows

How we set up (and what went wrong with) running the Docktornet audit-and-fix
workflows on a timer. Written 2026-07-17 after the first attempt.

## What we're scheduling

Two ultracode **Workflow** scripts (run via the Claude Code Workflow tool, NOT
`node`):

- `docs/audits/docktornet-audit.mjs` — 12-category, resumable, adversarially-verified
  audit. Checkpoints to `docs/audits/state/`; writes `NN-slug.md` reports + `SUMMARY.md`.
- `docs/audits/docktornet-fix.mjs` — sequential, resumable TDD implementation of the
  confirmed critical/high/medium findings. Checkpoints to `state/fixplan.json` +
  `state/fixlog.json`; commits each fix to `development`; writes `FIX-LOG.md` + `FIX-SUMMARY.md`.

Both **resume from on-disk checkpoints** — re-running skips already-completed work.

## The plan we tried (two-phase, 5 hours apart)

- **Phase 1 @ 23:51** — run the audit; if it finished, run the fix workflow.
- **Phase 2 @ 04:51** (5 h later) — resume the audit if unfinished, then run the fix
  workflow (skipping any finding Phase 1 already did).

The 5-hour gap was chosen to line up with the session usage-limit reset window (see below).

## How it was set up — the in-session `CronCreate` scheduler

We used Claude Code's **in-session** scheduler (the `CronCreate` tool), NOT an OS cron.
Key properties of `CronCreate`:

- 5-field cron in **local time** (Europe/Amsterdam). `51 23 16 7 *` = 23:51 on Jul 16.
- `recurring: false` = one-shot (fires once, then auto-deletes). Recurring jobs auto-expire
  after 7 days.
- **Session-only / in-memory.** The `durable` flag is a documented no-op. Jobs live only
  in the running Claude session and are **gone when Claude exits**.
- Jobs **only fire while the REPL is idle** (not mid-query).

The two jobs created (one-shot, `recurring: false`):

| Phase | cron expr      | Local time     | Prompt (summary)                                             |
|-------|----------------|----------------|-------------------------------------------------------------|
| 1     | `51 23 16 7 *` | 23:51 Jul 16   | `ultracode` → run audit workflow; if complete, run fix workflow |
| 2     | `51 4 17 7 *`  | 04:51 Jul 17   | `ultracode` → resume audit if needed, then run fix workflow  |

The word **`ultracode`** must be in the fired prompt — that's what authorizes the
Workflow tool to run. The prompts pointed at absolute local paths
(`/home/battermanz/coding/docktornet/docs/audits/*.mjs`), which is correct for an
in-session run (the workflow executes on the local machine with the real working tree).

Manage jobs with: `CronList` (list), `CronDelete <id>` (cancel). There is no edit —
delete + recreate to change one.

## What actually happened (the failure) — READ THIS

The overnight run **did not work**. Only 4 of 12 audit categories completed (from a
manual kickoff at ~23:06), and neither scheduled job produced anything. Two root causes,
compounding:

1. **Session usage limit.** The real bottleneck is NOT a per-run time cap — it's the
   rolling **session usage limit** (~1M subagent tokens ≈ ~4 audit categories ≈ ~27 min,
   resets roughly every ~5 h). A full 12-category audit spans 2–3 usage windows; audit+fix
   more. When the limit is hit, every agent in the workflow fails immediately.

2. **In-session crons need the session to be genuinely "fireable" at the exact minute.**
   The jobs did NOT fire at 23:51 / 04:51 — they queued and only flushed at **07:14**, the
   moment the session next became active. For an in-session job to fire it needs ALL of:
   - the Claude Code **process alive** and actively running its event loop,
   - **usage budget** available at that minute,
   - the REPL in the plain **idle** state — **an open `/workflows` live viewer (or any
     interactive overlay) keeps the REPL non-idle, so jobs defer instead of firing.**

   Overnight none of these reliably held, so nothing fired on time.

### Lessons

- **`CronCreate` is the wrong tool for unattended overnight work.** It's fine only if you
  keep the session open, the machine awake, budget available, and **no `/workflows` window
  or other overlay open** at the fire time. Too many fragile preconditions.
- The **usage limit paces everything** regardless of scheduler. Nothing finishes a big run
  in one window.
- The workflows themselves worked perfectly and are fully resumable — that part is solid.

## Recommended approaches going forward

1. **Foreground, on demand (most reliable).** When you're around with fresh budget, just
   ask to resume: invoke the Workflow tool on `docktornet-audit.mjs` (skips done categories),
   then `docktornet-fix.mjs`. Repeat each usage window until done. No timer to misfire.

2. **OS `crontab` / systemd timer (durable, unattended).** The only thing that fires
   regardless of session state. Caveat: each run is a **cold headless `claude -p` session**,
   and whether the ultracode Workflow tool runs in headless `-p` mode is **unverified** —
   needs testing before relying on it. Sketch:

   ```cron
   # NOT yet verified to work with the Workflow tool — test first.
   51 23 * * * cd /home/battermanz/coding/docktornet && claude -p "ultracode — invoke the Workflow tool with scriptPath docs/audits/docktornet-audit.mjs, then docs/audits/docktornet-fix.mjs" >> /home/battermanz/coding/docktornet/docs/audits/cron.log 2>&1
   ```

   Even here the usage limit still paces it — expect it to bank partial progress and
   continue next run.

## Reproducing the in-session setup (for reference)

Ask Claude (with the session kept open) to create the jobs, e.g.:

> "Create two one-shot in-session crons: one at 23:51 tonight that runs the docktornet-audit
> workflow then the fix workflow, and one at 04:51 tomorrow that resumes/continues. Use the
> word ultracode in each prompt. Commit fixes to development."

Then **keep the terminal open, the machine awake, and do not leave the `/workflows` viewer
open** across the fire times. Verify with `CronList`.
