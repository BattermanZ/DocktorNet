# Docktornet v1 Cutover Runbook

Use this runbook to safely flip docktornet from dry-run to live writes against NPM, Cloudflare DNS, and Gatus. Reusable for re-cutovers; not a one-off checklist.

## 1. Pre-flight

- [ ] `git status` is clean on `development`.
- [ ] `just test` passes.
- [ ] `just lint` passes.
- [ ] `docker build -f crates/server/Dockerfile -t docktornet-server:dev .` succeeds.
- [ ] `docker build -f crates/agent/Dockerfile -t docktornet-agent:dev .` succeeds.
- [ ] At least one labelled container exists on a host you can monitor closely (a "canary" service).

## 2. Backup

```bash
./scripts/backup_external_state.sh
ls docktornet-backups/$(date +%F)/
git tag pre-cutover-$(date +%F)
```

Verify the backup directory contains:

- `npm-proxy-hosts.json`
- `cloudflare-dns.json`
- `gatus-config.tar.gz`

Do **not** proceed if any file is missing.

## 3. Shadow phase (dry-run)

- [ ] In `server.env`: set `DOCKTORNET_DRY_RUN=true`.
- [ ] Deploy `deploy/main-host.compose.yaml`.
- [ ] Tail logs for at least one `DOCKTORNET_FULL_RESYNC_INTERVAL` (default 1h):

```bash
docker compose -f deploy/main-host.compose.yaml logs -f docktornet-server
```

Expected:
- `server ready` and periodic `snapshot received` lines.
- `full resync complete` exactly once per interval.
- `dry-run: would ...` lines describing the actions that *would* run against NPM/CF/Gatus.

Bail out if anything in the dry-run plan looks wrong (unexpected creates, deletes, or updates).

## 4. Single-service cutover

Pick **one** low-stakes service to flip first. The simplest way is:

1. Stop the server only (keep agents running):

   ```bash
   docker compose -f deploy/main-host.compose.yaml stop docktornet-server
   ```

2. In `server.env`: `DOCKTORNET_DRY_RUN=false`.
3. Restart:

   ```bash
   docker compose -f deploy/main-host.compose.yaml up -d docktornet-server
   ```

4. Verify the canary service in:
   - NPM admin UI — the proxy host exists and points where intended.
   - Cloudflare dashboard — the CNAME exists with the right target / proxy state.
   - Gatus dashboard — the endpoint appears, the telegram alert (if enabled) fires on a forced failure.

If anything is wrong: set `DRY_RUN=true`, restart, then jump to §7 Rollback.

## 5. Full cutover

After the canary is healthy:

- Leave `DRY_RUN=false`.
- The next diff cycle (≤ 30 s) reconciles every other tracked service.
- Watch logs for ~10 minutes; spot-check 2–3 services across NPM/CF/Gatus.

## 6. Post-cutover

- [ ] Push:
  ```bash
  git push origin development
  git push batterforge development
  ```
- [ ] After 24 hours of clean operation, tag `v1.0.0`:
  ```bash
  git tag v1.0.0
  git push origin v1.0.0
  git push batterforge v1.0.0
  ```

## 7. Rollback

Use the most recent `docktornet-backups/<date>/` directory.

1. Stop the server:
   ```bash
   docker compose -f deploy/main-host.compose.yaml stop docktornet-server
   ```

2. Restore NPM hosts. Snippet (adjust to backup JSON shape):
   ```bash
   jq -c '.[]' docktornet-backups/<date>/npm-proxy-hosts.json | \
   while read -r host; do
     curl -fsS -X POST "$DOCKTORNET_NPM_URL/api/nginx/proxy-hosts" \
       -H "Authorization: Bearer $NPM_TOKEN" \
       -H "Content-Type: application/json" \
       -d "$host"
   done
   ```

3. Restore Cloudflare records:
   ```bash
   jq -c '.[]' docktornet-backups/<date>/cloudflare-dns.json | \
   while read -r rec; do
     curl -fsS -X POST \
       "https://api.cloudflare.com/client/v4/zones/$DOCKTORNET_CF_ZONE_ID/dns_records" \
       -H "Authorization: Bearer $DOCKTORNET_CF_API_TOKEN" \
       -H "Content-Type: application/json" \
       -d "$rec"
   done
   ```

4. Restore Gatus config:
   ```bash
   sudo tar -xzf docktornet-backups/<date>/gatus-config.tar.gz -C /etc/gatus/config.d
   ```

5. Wipe docktornet tracked state so it cannot try to manage resources it no longer "owns":
   ```bash
   rm ./data/docktornet.db
   ```

6. Bring docktornet back up in dry-run for a second look before reattempting:
   ```bash
   # server.env: DOCKTORNET_DRY_RUN=true
   docker compose -f deploy/main-host.compose.yaml up -d
   ```
