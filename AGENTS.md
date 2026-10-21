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
Cloudflare API stub, Gatus, and a server and agent built from the working tree.
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
     -l docktornet.cf.dns.name=whoami -l docktornet.cf.dns.target=dev.test -l docktornet.cf.dns.proxied=false \
     -l docktornet.gatus.0.url=http://dind:18080 -l docktornet.gatus.0.interval=10s \
     traefik/whoami:v1.10.3
   ```
3) Read the result. The agent polls every 5s.
   - NPM: `T=$(curl -s -X POST 127.0.0.1:18081/api/tokens -H 'Content-Type: application/json' -d '{"identity":"dev@example.com","secret":"dev-npm-password"}' | jq -r .token); curl -s 127.0.0.1:18081/api/nginx/proxy-hosts -H "Authorization: Bearer $T"`, and the proxy itself: `curl -H 'Host: whoami.dev.test' 127.0.0.1:18080`
   - Cloudflare stub: `curl -s 127.0.0.1:18086/_stub/records`
   - Gatus: `ls dev-data/gatus-config` and `curl -s 127.0.0.1:18085/api/v1/endpoints/statuses`
   - Server log: `docker compose -f deploy/dev.compose.yaml logs docktornet-server`
4) Remove: `docker compose -f deploy/dev.compose.yaml exec dind docker rm -f whoami`. Deletions wait out the 300s grace period. Pending removals then drain within a minute: the pending-removal sweep runs every 60s, and this stack's full resync, every 30s, sweeps too. Gatus drops the monitor at its next 30s config check. Rerun the step 3 reads: the proxy host, the stub record, the `devbox__whoami--*.yaml` file and the Gatus endpoint are gone.
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
