# AGENTS.md

Project conventions for Docktornet. Host-specific conventions, including where
the issue tracker lives, are in `AGENTS.local.md`; vocabulary is in
`GLOSSARY.md`; decisions are in `docs/adr/`.

## Local-only instructions

If `AGENTS.local.md` exists at the repository root, read it before touching an
issue tracker, a remote, a deployment or a credential. It holds machine-local
operator context. It stays untracked: never stage or commit it, and never copy
what it says into the repository, an issue, a commit message or a release note.

## CodeGraph

When a `.codegraph/` directory exists at the repository root, locate and read
code with the `codegraph_explore` MCP tool, or `codegraph explore "<symbols or
question>"` when the tool is unavailable, before grep, find or reading source
files. Creating an index is the maintainer's decision.

## Building and testing

Everything runs through `just` at the repository root (`just --list`). Before a
change is handed off or committed, `just fmt`, `just lint` and `just test` all
pass. `just lint` is clippy with `-D warnings`, so a warning fails it.

A bug fix starts with a regression test that goes red on the bug. Use that
failure to confirm the cause before changing implementation code, then make it
pass.

Add a crate with `cargo add`, so the registry writes its current version. The
compiler is pinned in `rust-toolchain.toml`, and the cargo-chef base image in
both Dockerfiles carries the same Rust version: bump all three together.

## Live test environment

`deploy/dev.compose.yaml` is the live test environment: a throwaway NPM, a
Cloudflare API stub, a real Caddy, Gatus, and a server and agent built from the
working tree.
Production NPM and Cloudflare are never a test environment.

Run labelled test containers inside the stack's `dind` daemon only. A
production agent on the host reads `docktornet.*` labels from both the host's
Docker and Podman sockets and reconciles them into production NPM and
Cloudflare.

1) Start (rebuilds both images from the working tree; the server runs as your uid so `dev-data/` stays yours):
   `DEV_UID=$(id -u) DEV_GID=$(id -g) docker compose -f deploy/dev.compose.yaml up -d --build --wait`
2) Run a labelled container inside `dind`:
   ```bash
   docker compose -f deploy/dev.compose.yaml exec dind docker run -d --name whoami -p 18080:80 \
     -l docktornet.npm.0.host=whoami.dev.test -l docktornet.npm.0.port=18080 \
     -l docktornet.npm.0.scheme=http -l docktornet.npm.0.ssl=false \
     -l docktornet.caddy.0.host=whoami.dev.test -l docktornet.caddy.0.port=18080 \
     -l docktornet.cf.dns.name=whoami -l docktornet.cf.dns.target=dev.test -l docktornet.cf.dns.proxied=false \
     -l docktornet.gatus.0.url=http://dind:18080 -l docktornet.gatus.0.interval=10s \
     traefik/whoami:v1.10.3
   ```
3) Read the result. The agent polls every 5s.
   - NPM: `T=$(curl -s -X POST 127.0.0.1:18081/api/tokens -H 'Content-Type: application/json' -d '{"identity":"dev@example.com","secret":"dev-npm-password"}' | jq -r .token); curl -s 127.0.0.1:18081/api/nginx/proxy-hosts -H "Authorization: Bearer $T"`, and the proxy itself: `curl -H 'Host: whoami.dev.test' 127.0.0.1:18080`
   - Caddy: `docker compose -f deploy/dev.compose.yaml exec caddy curl -s localhost:2019/id/docktornet/handle/0/routes` lists the anchor's routes (the admin port is not published), and the proxy itself: `curl -H 'Host: whoami.dev.test' 127.0.0.1:18087`
   - Cloudflare stub: `curl -s 127.0.0.1:18086/_stub/records`
   - Gatus: `ls dev-data/gatus-config` and `curl -s 127.0.0.1:18085/api/v1/endpoints/statuses`
   - Server log: `docker compose -f deploy/dev.compose.yaml logs docktornet-server`
4) Remove: `docker compose -f deploy/dev.compose.yaml exec dind docker rm -f whoami`. Deletions wait out the 300s grace period. Pending removals then drain within a minute: the pending-removal sweep runs every 60s, and this stack's full resync, every 30s, sweeps too. Gatus drops the monitor at its next 30s config check. Rerun the step 3 reads: the proxy host, the anchor's Caddy route, the stub record, the `devbox__whoami--*.yaml` file and the Gatus endpoint are gone.
5) Tear down: `docker compose -f deploy/dev.compose.yaml down -v && rm -rf dev-data`

## Branching

`development` is the integration branch and `main` is the release branch. Work
is committed to `development` and pushed; `main` receives only merges from
`development`.

Every merge is a merge commit, including `development` into `main`. Squash and
rebase merges are not used, and `development` and `main` are never
force-pushed.

## Commits

The subject is `type(scope): concise summary`, for example
`test(client): close remaining frontend coverage gaps`. The body is required
and is a list of `- ` bullets, each saying what changed and why. A commit that
finishes an issue ends its bullets with `- Closes #N.`

## Changelog

`CHANGELOG.md` opens with `## Unreleased`, and a release is cut from it.

A commit that changes what an operator sees or does adds its line under
`## Unreleased` in the same commit: a label, a setting, a subcommand, a
`/healthz` field, what Docktornet writes to NPM, Caddy, Cloudflare or Gatus, or
a step an upgrade needs. A refactor, a test or a change to these instructions
adds none.

- A line goes under `### After upgrading`, `### Added`, `### Changed` or
  `### Fixed`. A step the operator must take sits under `### After upgrading`.
- End the line with its issue as `[#N]`, and add `[#N]: <url>` to the
  definitions at the foot of the section, in number order.
- A fix to something still under `## Unreleased` corrects the line already
  there, and a change reverted before a release takes its line out.

## Releasing

A release is cut by hand, in this order. The version is bare, as in `1.2.0`;
the tag and the GitHub Release add the `v`.

1. On `development`, set the version in every `crates/*/Cargo.toml` and let
   `cargo check` carry it into `Cargo.lock`. Rename `## Unreleased` to
   `## v<version> - <today>` and put a new, empty `## Unreleased` above it.
2. Make each check, and fix what it finds on `development` before going on:
   - The dated section lists everything in
     `git log --no-merges v<previous>..development` that an operator would
     notice, and nothing reverted.
   - `README.md` and `docs/deployment.md` describe what shipped, and the README
     names this version as the current release.
   - No ADR is contradicted by what shipped, and `GLOSSARY.md` holds every term
     the release added.
   - `cargo audit` is clean, or each advisory it reports is ignored in
     `.cargo/audit.toml` with its reasoning.
   - `just fmt`, `just lint` and `just test` pass.
3. Commit and push `development`.
4. Draft the GitHub Release with
   `gh release create v<version> --draft --target main --title 'v<version> - <name>' --notes-file <file>`.
   The notes follow v1.2.0's shape: an opening on what the release does for an
   operator, how to build the images from the tag, an `[!IMPORTANT]` callout
   when the database migrates or a setting is refused, `## What's new` grouped
   by theme, `## Upgrade`, and a link to the changelog.
5. Show the maintainer the title and notes as they stand on the draft, and
   stop. Go on only after the reply "approved" in this session. Ask again when
   the text changed or the session is new.
6. Merge `development` into `main`, create the annotated tag `v<version>` on
   that merge, push both with `git push origin main v<version>`, and publish
   the draft with `gh release edit v<version> --draft=false --latest`.
7. Build both images from a clean checkout of the tag and push them to the
   private registry, as `AGENTS.local.md` says. No public registry gets them.

A tag is never moved and a published version is never replaced. A release found
broken is fixed by the next patch version.
