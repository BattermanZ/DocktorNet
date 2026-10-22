"""Stateful stand-in for the Cloudflare DNS API, for the dev stack only.

Serves the calls CloudflareClient makes (zone, list the whole zone or by
type, name or exact comment, create, update, get, delete) with Cloudflare's
response envelope, keeps records in memory, and logs every write. A list is
always one page. It also answers the token check and the zone settings read
that scripts/backup_external_state.sh makes.
GET /_stub/records lists what it holds.

Standard library only, so it runs on a stock python image.
"""

import json
import os
import re
import sys
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock
from urllib.parse import parse_qs, urlparse

TOKEN = os.environ.get("CF_STUB_TOKEN", "dev-cf-token")
ZONE_ID = os.environ.get("CF_STUB_ZONE_ID", "dev-zone")
ZONE_NAME = os.environ.get("CF_STUB_ZONE_NAME", "dev.test")
PORT = int(os.environ.get("CF_STUB_PORT", "8080"))

TOKEN_VERIFY_PATH = "/client/v4/user/tokens/verify"
ZONE_SETTINGS_PATH = re.compile(rf"^/client/v4/zones/{re.escape(ZONE_ID)}/settings$")
RECORD_LIST_PATH = re.compile(rf"^/client/v4/zones/{re.escape(ZONE_ID)}/dns_records/?$")
RECORD_ITEM_PATH = re.compile(rf"^/client/v4/zones/{re.escape(ZONE_ID)}/dns_records/([^/]+)$")
ZONE_PATH = re.compile(rf"^/client/v4/zones/{re.escape(ZONE_ID)}$")

records = {}
lock = Lock()


def log(message):
    print(f"cf-stub: {message}", file=sys.stderr, flush=True)


def ok(result):
    return 200, {"success": True, "errors": [], "messages": [], "result": result}


def error(status, code, message):
    return status, {
        "success": False,
        "errors": [{"code": code, "message": message}],
        "messages": [],
        "result": None,
    }


def not_found():
    return error(404, 81044, "Record does not exist.")


def name_conflict():
    return error(400, 81053, "An A, AAAA, or CNAME record with that host already exists.")


def normalise(name):
    return name.lower().rstrip(".")


def record_from(body, record_id):
    return {
        "id": record_id,
        "type": body["type"],
        "name": normalise(body["name"]),
        "content": body["content"],
        "proxied": bool(body.get("proxied", False)),
        "ttl": int(body.get("ttl", 1)),
        # Like Cloudflare, a write without a comment leaves none.
        "comment": body.get("comment"),
    }


def name_taken(name, except_id=None):
    return any(
        r["name"] == name and r["id"] != except_id for r in records.values()
    )


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def send(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def body(self):
        length = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(length) or b"{}")

    def authorized(self):
        return self.headers.get("Authorization") == f"Bearer {TOKEN}"

    def dispatch(self, method):
        url = urlparse(self.path)
        if method == "GET" and url.path == "/_stub/records":
            with lock:
                return self.send(200, sorted(records.values(), key=lambda r: r["name"]))
        if not self.authorized():
            return self.send(*error(403, 10000, "Authentication error"))
        with lock:
            self.send(*self.route(method, url))

    def route(self, method, url):
        if url.path == TOKEN_VERIFY_PATH and method == "GET":
            return ok({"id": "dev-token", "status": "active"})

        if ZONE_PATH.match(url.path) and method == "GET":
            return ok({"id": ZONE_ID, "name": ZONE_NAME})

        if ZONE_SETTINGS_PATH.match(url.path) and method == "GET":
            return ok([])

        if RECORD_LIST_PATH.match(url.path):
            if method == "GET":
                query = parse_qs(url.query)
                wanted_type = query.get("type", [None])[0]
                wanted_name = query.get("name", [None])[0]
                wanted_comment = query.get("comment.exact", [None])[0]
                found = [
                    r
                    for r in records.values()
                    if (wanted_type is None or r["type"] == wanted_type)
                    and (wanted_name is None or r["name"] == normalise(wanted_name))
                    # Cloudflare's comment filters ignore case.
                    and (
                        wanted_comment is None
                        or (r["comment"] or "").lower() == wanted_comment.lower()
                    )
                ]
                status, payload = ok(found)
                payload["result_info"] = {
                    "page": 1,
                    "per_page": len(found),
                    "count": len(found),
                    "total_count": len(found),
                    "total_pages": 1,
                }
                return status, payload
            if method == "POST":
                record = record_from(self.body(), uuid.uuid4().hex)
                if name_taken(record["name"]):
                    log(f"create rejected, {record['name']} already exists")
                    return name_conflict()
                records[record["id"]] = record
                log(f"created {record['id']} {record['name']} -> {record['content']} comment={record['comment']!r}")
                return ok(record)

        match = RECORD_ITEM_PATH.match(url.path)
        if match:
            record_id = match.group(1)
            if record_id not in records:
                return not_found()
            if method == "GET":
                return ok(records[record_id])
            if method == "PUT":
                record = record_from(self.body(), record_id)
                if name_taken(record["name"], except_id=record_id):
                    return name_conflict()
                records[record_id] = record
                log(f"updated {record_id} {record['name']} -> {record['content']} comment={record['comment']!r}")
                return ok(record)
            if method == "DELETE":
                removed = records.pop(record_id)
                log(f"deleted {record_id} {removed['name']}")
                return ok({"id": record_id})

        return error(404, 7003, "Could not route to the requested path")

    def do_GET(self):
        self.dispatch("GET")

    def do_POST(self):
        self.dispatch("POST")

    def do_PUT(self):
        self.dispatch("PUT")

    def do_DELETE(self):
        self.dispatch("DELETE")


if __name__ == "__main__":
    log(f"serving zone {ZONE_ID} ({ZONE_NAME}) on :{PORT}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
