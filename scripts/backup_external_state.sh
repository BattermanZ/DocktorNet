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

command -v curl >/dev/null || { echo "curl is required" >&2; exit 2; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
out="$ROOT/docktornet-backups/$stamp"
mkdir -p "$out/npm" "$out/cloudflare"

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

cf_api="https://api.cloudflare.com/client/v4"
cf_get() {
  local path="$1"
  local file="$2"
  curl -fsS "$cf_api$path" \
    -H "Authorization: Bearer $DOCKTORNET_CF_API_TOKEN" |
    jq "." > "$out/cloudflare/$file"
}

cf_get "/user/tokens/verify" "token-verify.json"
cf_get "/zones/$DOCKTORNET_CF_ZONE_ID" "zone.json"
cf_get "/zones/$DOCKTORNET_CF_ZONE_ID/settings" "zone-settings.json"

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

find "$out" -type f -maxdepth 3 -print | sort
echo "backup complete: $out"
