#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${1:-$ROOT/.env.local}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "missing env file: $ENV_FILE" >&2
  exit 2
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

need() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "missing $name in $ENV_FILE" >&2
    exit 2
  fi
}

for name in \
  DOCKTORNET_NPM_URL \
  DOCKTORNET_NPM_USER \
  DOCKTORNET_NPM_PASSWORD \
  DOCKTORNET_CF_API_TOKEN \
  DOCKTORNET_CF_ZONE_ID
do
  need "$name"
done

# Optional settings, from the env file or the environment.
backup_root="${DOCKTORNET_BACKUP_DIR:-$ROOT/docktornet-backups}"
server_container="${DOCKTORNET_BACKUP_SERVER_CONTAINER:-docktornet-server}"
gatus_dir="${DOCKTORNET_BACKUP_GATUS_DIR:-/etc/gatus/config.d}"
keep="${DOCKTORNET_BACKUP_KEEP:-14}"
cf_api="${DOCKTORNET_CF_API_URL:-https://api.cloudflare.com/client/v4}"
cf_api="${cf_api%/}"

if [[ ! "$keep" =~ ^[1-9][0-9]*$ ]]; then
  echo "DOCKTORNET_BACKUP_KEEP must be a whole number of at least 1, got '$keep'" >&2
  exit 2
fi
if [[ ! -d "$gatus_dir" ]]; then
  echo "Gatus config dir not found: $gatus_dir (set DOCKTORNET_BACKUP_GATUS_DIR)" >&2
  exit 2
fi

for tool in curl jq docker tar; do
  command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 2; }
done

# The backup is written to <stamp>.partial and renamed to <stamp> only once
# every part has succeeded, so retention never counts a failed run.
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
final="$backup_root/$stamp"
out="$final.partial"
mkdir -p "$backup_root"
if [[ -e "$final" ]]; then
  echo "backup already exists: $final" >&2
  exit 1
fi
mkdir "$out"
mkdir "$out/npm" "$out/cloudflare"

complete=false
on_exit() {
  if [[ "$complete" != true ]]; then
    echo "backup FAILED; incomplete files left in $out" >&2
  fi
}
trap on_exit EXIT

echo "writing backup to $out"

npm_token="$(
  jq -n \
    --arg identity "$DOCKTORNET_NPM_USER" \
    --arg secret "$DOCKTORNET_NPM_PASSWORD" \
    '{identity:$identity, secret:$secret}' |
  curl -fsS "$DOCKTORNET_NPM_URL/api/tokens" \
    -H "Content-Type: application/json" \
    --data-binary @- |
  jq -r ".token"
)"

if [[ -z "$npm_token" || "$npm_token" == "null" ]]; then
  echo "NPM token request did not return a token" >&2
  exit 1
fi

npm_get() {
  local path="$1"
  local file="$2"
  curl -fsS "$DOCKTORNET_NPM_URL$path" \
    -H "Authorization: Bearer $npm_token" |
    jq "." > "$out/npm/$file"
}

npm_get "/api/nginx/proxy-hosts" "proxy-hosts.json"
npm_get "/api/nginx/certificates" "certificates.json"
npm_get "/api/nginx/redirection-hosts" "redirection-hosts.json"
npm_get "/api/nginx/dead-hosts" "dead-hosts.json"
npm_get "/api/nginx/streams" "streams.json"
npm_get "/api/nginx/access-lists" "access-lists.json"
npm_get "/api/settings" "settings.json"

cf_get() {
  local path="$1"
  local file="$2"
  curl -fsS "$cf_api$path" \
    -H "Authorization: Bearer $DOCKTORNET_CF_API_TOKEN" |
    jq "." > "$out/cloudflare/$file"
}

cf_get "/user/tokens/verify" "token-verify.json"
cf_get "/zones/$DOCKTORNET_CF_ZONE_ID" "zone.json"

zone_settings_tmp="$out/cloudflare/zone-settings.json.tmp"
zone_settings_code="$(
  curl -sS -w '%{http_code}' \
    -o "$zone_settings_tmp" \
    "$cf_api/zones/$DOCKTORNET_CF_ZONE_ID/settings" \
    -H "Authorization: Bearer $DOCKTORNET_CF_API_TOKEN"
)"
zone_settings_status=$?
if [[ "$zone_settings_status" -ne 0 ]]; then
  rm -f "$zone_settings_tmp"
  exit "$zone_settings_status"
fi

if [[ "$zone_settings_code" == 2* ]]; then
  jq "." "$zone_settings_tmp" > "$out/cloudflare/zone-settings.json"
  rm -f "$zone_settings_tmp"
elif [[ "$zone_settings_code" == "403" ]]; then
  rm -f "$zone_settings_tmp"
  echo "warning: Cloudflare zone settings backup skipped because the endpoint returned 403; continuing" >&2
else
  rm -f "$zone_settings_tmp"
  echo "Cloudflare zone settings backup failed with HTTP $zone_settings_code" >&2
  exit 1
fi

page=1
all="$out/cloudflare/dns-records.jsonl"
: > "$all"
while true; do
  body="$(
    curl -fsS "$cf_api/zones/$DOCKTORNET_CF_ZONE_ID/dns_records?per_page=100&page=$page" \
      -H "Authorization: Bearer $DOCKTORNET_CF_API_TOKEN"
  )"
  jq -c ".result[]" <<<"$body" >> "$all"
  total_pages="$(jq -r ".result_info.total_pages" <<<"$body")"
  if [[ "$page" -ge "$total_pages" ]]; then
    break
  fi
  page=$((page + 1))
done
jq -s "." "$all" > "$out/cloudflare/dns-records.json"
rm "$all"

# The server image has no shell or sqlite3, so the server binary takes the
# copy itself and writes it to stdout.
docker exec -i "$server_container" docktornet-server backup > "$out/docktornet.db"
if [[ "$(head -c 15 "$out/docktornet.db")" != "SQLite format 3" ]]; then
  echo "the database backup from $server_container is not a SQLite database" >&2
  exit 1
fi

# The server may replace a file while tar reads the directory. GNU tar then
# exits 1, and the archive still holds each file whole, old or new, because
# the server writes every file to a temporary name and renames it into place.
tar_status=0
tar -czf "$out/gatus-config.tar.gz" -C "$gatus_dir" . || tar_status=$?
if (( tar_status > 1 )); then
  echo "archiving $gatus_dir failed with tar exit $tar_status" >&2
  exit 1
fi

mv "$out" "$final"
complete=true
find "$final" -maxdepth 3 -type f -print | sort
echo "backup complete: $final"

# Retention: keep the newest $keep stamped directories. Only names in the
# stamp format are counted or deleted; the stamp sorts by time.
stamps=()
shopt -s nullglob
for dir in "$backup_root"/*/; do
  dir="${dir%/}"
  name="${dir##*/}"
  if [[ "$name" =~ ^[0-9]{8}T[0-9]{6}Z$ && ! -L "$dir" ]]; then
    stamps+=("$name")
  fi
done
shopt -u nullglob
if (( ${#stamps[@]} > 0 )); then
  mapfile -t stamps < <(printf '%s\n' "${stamps[@]}" | LC_ALL=C sort)
fi
excess=$(( ${#stamps[@]} - keep ))
for (( i = 0; i < excess; i++ )); do
  rm -rf -- "${backup_root:?}/${stamps[i]}"
  echo "pruned $backup_root/${stamps[i]}"
done
