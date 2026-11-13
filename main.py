#!/usr/bin/env python3
"""Continuously import FitGirl releases into Repack X via the admin API.

Each cycle:
  1. Fetch all existing game slugs from the site (to detect every missed game).
  2. Read .fg_import_state.json for the last scanned page and API lastPage.
  3. Scan FitGirl pages (page 1 for new releases + backfill pages for missed games).
  4. POST each missing game to the Repack X admin API.
  5. Advance scan state and persist .fg_import_state.json.

Health web server (started in every mode):
  GET /            HTML status page (auto-refreshes)
  GET /api/health  {"working": true}
  GET /api/status  detailed JSON status

Environment variables:
  REPACK_X_ADMIN_SECRET     (required) Admin secret for POST /api/games/admin
  REPACK_X_SITE_URL         default https://repackx.vercel.app
  FITGIRL_API_BASE_URL      default https://fitgirl-api-5rbz.onrender.com
  FITGIRL_IMPORT_LIMIT      default 5      (max games imported per cycle)
  FITGIRL_PAGES_PER_CYCLE   default 5      (backfill pages scanned per cycle)
  FITGIRL_IMPORT_DELAY      default 0.25   (seconds between detail fetches)
  FITGIRL_IMPORT_INTERVAL   default 600    (seconds between cycles)
  FITGIRL_IMPORT_ONCE       default false
  FITGIRL_IMPORT_DRY_RUN    default false
  FITGIRL_STATE_PATH        default .fg_import_state.json (local file)
  HEALTH_PORT               default 8080
  HEALTH_HOST               default 0.0.0.0

Quick catch-up (import many games fast):
  FITGIRL_IMPORT_LIMIT=50 FITGIRL_PAGES_PER_CYCLE=20 FITGIRL_IMPORT_INTERVAL=120
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Iterable

DEFAULT_FITGIRL_API = "https://fitgirl-api-5rbz.onrender.com"
DEFAULT_SITE_URL = "https://repackx.vercel.app"
IMAGE_URL_RE = re.compile(r"\.(?:jpe?g|png|webp|gif|avif)(?:[?#].*)?$", re.IGNORECASE)
SKIP_RE = re.compile(r"(?:upcoming[- ]repacks?|updates?[- ]digest|digest[- ]updates?)", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Shared status (exposed by the health web server)
# ---------------------------------------------------------------------------

class ServiceStatus:
    def __init__(self):
        self._lock = threading.Lock()
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.cycles = 0
        self.last_cycle_at = None
        self.last_cycle_result = "never run"
        self.total_imported = 0
        self.last_error = None
        self.total_existing = 0
        self.last_scanned_page = 0
        self.api_last_page = 0
        self.backfill_complete = False

    def update(self, **kwargs):
        with self._lock:
            for key, value in kwargs.items():
                setattr(self, key, value)

    def snapshot(self):
        with self._lock:
            return {
                "working": True,
                "started_at": self.started_at,
                "cycles": self.cycles,
                "last_cycle_at": self.last_cycle_at,
                "last_cycle_result": self.last_cycle_result,
                "total_imported": self.total_imported,
                "last_error": self.last_error,
                "total_existing": self.total_existing,
                "last_scanned_page": self.last_scanned_page,
                "api_last_page": self.api_last_page,
                "backfill_complete": self.backfill_complete,
            }


STATUS = ServiceStatus()


# ---------------------------------------------------------------------------
# Health web server (stdlib only)
# ---------------------------------------------------------------------------

_HTML_HEAD = """<!doctype html>
<html><head><meta charset="utf-8"><title>Repack X Importer</title>
<meta http-equiv="refresh" content="30">
<style>
body{background:#0d1117;color:#c9d1d9;font-family:system-ui,sans-serif;margin:2rem}
h1{color:#58a6ff} .ok{color:#3fb950}
table{border-collapse:collapse} td,th{border:1px solid #30363d;padding:.4rem .8rem;text-align:left}
code{color:#79c0ff}
</style></head><body>
"""


class HealthHandler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: str, ctype: str = "application/json") -> None:
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        snap = STATUS.snapshot()
        if path == "/api/health":
            self._send(200, json.dumps({"working": True}))
        elif path == "/api/status":
            self._send(200, json.dumps(snap, indent=2))
        elif path in ("/", "/index.html"):
            rows = "".join(
                f"<tr><th>{k}</th><td><code>{v}</code></td></tr>"
                for k, v in snap.items()
            )
            html = (
                _HTML_HEAD
                + "<h1>Repack X — FitGirl importer</h1>"
                + '<p class="ok">&#9679; working: true</p>'
                + f"<table>{rows}</table>"
                + "<p>JSON: <a href='/api/health'>/api/health</a> · "
                + "<a href='/api/status'>/api/status</a> (auto-refresh 30s)</p>"
                + "</body></html>"
            )
            self._send(200, html, "text/html; charset=utf-8")
        else:
            self._send(404, json.dumps({"error": "not found"}))

    def log_message(self, *args: Any) -> None:  # silence request logs
        pass


def start_health_server(host: str, port: int) -> None:
    server = HTTPServer((host, port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"Health web server on http://{host}:{port}  (/api/health -> {{\"working\": true}})")


# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

def env(name: str, default: str) -> str:
    return os.environ.get(name, default).strip()


def env_int(name: str, default: int) -> int:
    value = env(name, str(default))
    try:
        return int(value)
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer, received: {value!r}") from error


def env_float(name: str, default: float) -> float:
    value = env(name, str(default))
    try:
        return float(value)
    except ValueError as error:
        raise RuntimeError(f"{name} must be a number, received: {value!r}") from error


def env_bool(name: str, default: bool = False) -> bool:
    value = env(name, "true" if default else "false").lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{name} must be true or false, received: {value!r}")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def api_json(
    url: str,
    *,
    token: str | None = None,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
) -> tuple[Any, dict[str, str]]:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers: dict[str, str] = {
        "Accept": "application/vnd.github+json" if "api.github.com" in url else "application/json",
        "User-Agent": "Repack-X-FitGirl-Importer/3.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
        headers["X-GitHub-Api-Version"] = "2022-11-28"
    if payload is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            return (
                json.loads(response.read().decode("utf-8")),
                {k.lower(): v for k, v in response.headers.items()},
            )
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")[:1000]
        raise RuntimeError(f"HTTP {error.code} from {url}: {detail}") from error


def get_with_retries(url: str, retries: int = 4) -> Any:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            return api_json(url)[0]
        except Exception as error:
            last_error = error
            if attempt + 1 < retries:
                time.sleep(min(30, 2 ** attempt))
    raise RuntimeError(f"Could not fetch {url}: {last_error}")


def post_json(url: str, payload: dict[str, Any], extra_headers: dict[str, str]) -> Any:
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Repack-X-FitGirl-Importer/3.0",
        **extra_headers,
    }
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")[:1000]
        raise RuntimeError(f"HTTP {error.code} from {url}: {detail}") from error


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def text(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("name", "title", "text", "url", "slug"):
            if value.get(key):
                return str(value[key]).strip()
        return ""
    return str(value).strip() if value is not None else ""


def as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def unique_strings(values: Iterable[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = text(value)
        if item and item.casefold() not in seen:
            seen.add(item.casefold())
            result.append(item)
    return result


def nested_urls(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value.strip()
    elif isinstance(value, list):
        for item in value:
            yield from nested_urls(item)
    elif isinstance(value, dict):
        for key in ("url", "image", "src", "thumbnail"):
            if value.get(key):
                yield from nested_urls(value[key])


def image_urls(value: Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for url in nested_urls(value):
        if re.match(r"^https?://", url, re.IGNORECASE) and IMAGE_URL_RE.search(url) and url not in seen:
            seen.add(url)
            result.append(url)
    return result


def should_skip(item: dict[str, Any]) -> bool:
    title = text(item.get("title"))
    slug = text(item.get("slug"))
    return not slug or bool(SKIP_RE.search(title) or SKIP_RE.search(slug))


def has_download_links(detail: dict[str, Any]) -> bool:
    def walk(value: Any) -> Iterable[str]:
        if isinstance(value, str):
            if value.strip():
                yield value.strip()
        elif isinstance(value, list):
            for item in value:
                yield from walk(item)
        elif isinstance(value, dict):
            for item in value.values():
                yield from walk(item)

    fields = (
        detail.get("downloadMirrors"),
        detail.get("torrents"),
        detail.get("magnets"),
        detail.get("mirrors"),
        detail.get("downloads"),
        detail.get("downloadLinks"),
        detail.get("download_links"),
    )
    return any(
        re.match(r"^(?:https?://|magnet:\?)", value, re.IGNORECASE)
        for field in fields
        for value in walk(field)
    )


def description(detail: dict[str, Any]) -> str:
    paragraphs = unique_strings(as_list(detail.get("gameDescription")))
    raw_features = detail.get("repackFeatures")
    if isinstance(raw_features, str):
        features = unique_strings(
            line.strip() for line in raw_features.splitlines() if line.strip()
        )
    else:
        features = unique_strings(as_list(raw_features))
    sections: list[str] = []
    if paragraphs:
        sections.append("\n\n".join(paragraphs))
    if features:
        sections.append("**Repack Features**\n\n" + "\n".join(f"- {item}" for item in features))
    return "\n\n".join(sections) or "Not Found"


# ---------------------------------------------------------------------------
# FitGirl API helpers
# ---------------------------------------------------------------------------

def api_last_page(payload: Any) -> int | None:
    if not isinstance(payload, dict):
        return None
    for key in ("lastPage", "last_page", "totalPages", "total_pages", "pages"):
        value = payload.get(key)
        if isinstance(value, int) and value > 0:
            return value
    meta = payload.get("pagination") or payload.get("meta")
    if isinstance(meta, dict):
        for key in ("lastPage", "last_page", "totalPages", "total_pages", "pages"):
            value = meta.get(key)
            if isinstance(value, int) and value > 0:
                return value
    return None


def result_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("results", "repacks", "data", "items"):
            if isinstance(payload.get(key), list):
                return [item for item in payload[key] if isinstance(item, dict)]
    return []


# ---------------------------------------------------------------------------
# Repack X API
# ---------------------------------------------------------------------------

def fetch_existing_slugs(site_url: str) -> set[str]:
    """Fetch every game slug currently on the site."""
    url = f"{site_url}/api/games/all-slugs"
    payload = get_with_retries(url)
    slugs: set[str] = set()
    if isinstance(payload, list):
        for item in payload:
            slug = text(item.get("slug") if isinstance(item, dict) else item) if isinstance(item, (dict, str)) else ""
            if slug:
                slugs.add(slug)
    elif isinstance(payload, dict):
        for key in ("slugs", "data", "items", "games", "results"):
            if isinstance(payload.get(key), list):
                for item in payload[key]:
                    if isinstance(item, dict):
                        slug = text(item.get("slug"))
                    elif isinstance(item, str):
                        slug = item.strip()
                    else:
                        slug = ""
                    if slug:
                        slugs.add(slug)
                break
    return slugs


def create_game(site_url: str, admin_secret: str, payload: dict[str, Any]) -> Any:
    """POST a new game to the Repack X admin API."""
    url = f"{site_url}/api/games/admin"
    headers = {"x-admin-secret": admin_secret}
    return post_json(url, payload, headers)


# ---------------------------------------------------------------------------
# Game mapping -> admin API payload
# ---------------------------------------------------------------------------

def map_to_api_payload(summary: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    slug = text(detail.get("slug") or summary.get("slug"))
    title = text(detail.get("title") or summary.get("title"))

    categories = [
        c
        for c in unique_strings(
            [
                *as_list(detail.get("genres")),
                *as_list(detail.get("tags")),
                detail.get("category"),
            ]
        )
        if c.casefold() != "lossless repack"
    ]

    desc = description(detail)

    size_original = text(detail.get("originalSize")) or "Not Found"
    size_repack = text(detail.get("repackSize")) or "Not Found"
    size_repack = re.sub(
        r"\s*\[selective download\]\s*$", "", size_repack, flags=re.IGNORECASE
    ).strip() or "Not Found"

    cover = text(detail.get("cover"))
    screenshots = image_urls(detail.get("screenshots"))

    return {
        "title": title,
        "slug": slug,
        "download_url": f"https://fitgirl-repacks.site/{slug}/",
        "repacker": "Fitgirl",
        "size_original": size_original,
        "size_repack": size_repack,
        "categories": categories,
        "description": desc if desc != "Not Found" else "",
        "cover_url": cover or "",
        "screenshot_urls": screenshots,
        "system_requirements": "Not Found",
        "is_published": True,
        "featured": False,
        "trending": False,
    }


# ---------------------------------------------------------------------------
# Local state management
# ---------------------------------------------------------------------------

def local_state_path() -> Path:
    configured = os.environ.get("FITGIRL_STATE_PATH", "").strip()
    if configured:
        path = Path(configured).expanduser()
        return path if path.is_absolute() else Path(__file__).resolve().parent / path
    return Path(__file__).resolve().parent / ".fg_import_state.json"


def read_state(file_path: Path) -> dict[str, Any]:
    if not file_path.exists() or not file_path.read_bytes().strip():
        return {"lastScannedPage": 0, "lastPage": 0}
    try:
        value = json.loads(file_path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {"lastScannedPage": 0, "lastPage": 0}
    except json.JSONDecodeError:
        print(f"Warning: corrupt {file_path}; starting fresh", file=sys.stderr)
        return {"lastScannedPage": 0, "lastPage": 0}


def write_state(file_path: Path, state: dict[str, Any]) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = file_path.with_suffix(file_path.suffix + ".tmp")
    temporary.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    temporary.replace(file_path)


# ---------------------------------------------------------------------------
# Import cycle
# ---------------------------------------------------------------------------

def run_cycle(args: argparse.Namespace) -> int:
    fitgirl_base = env("FITGIRL_API_BASE_URL", DEFAULT_FITGIRL_API).rstrip("/")
    site_url = env("REPACK_X_SITE_URL", DEFAULT_SITE_URL).rstrip("/")
    admin_secret = env("REPACK_X_ADMIN_SECRET", "")

    if not admin_secret and not args.dry_run:
        raise RuntimeError("REPACK_X_ADMIN_SECRET is required (unless --dry-run).")

    # 1) Fetch existing slugs from the site -------------------------------
    existing_slugs = fetch_existing_slugs(site_url)
    print(f"[slugs] {len(existing_slugs)} existing game slug(s) on site")

    # 2) Load local state --------------------------------------------------
    state_file = local_state_path()
    state = read_state(state_file)
    last_scanned = int(state.get("lastScannedPage", 0))
    saved_last_page = int(state.get("lastPage", 0))

    # 3) Fetch page 1 (new releases + detect lastPage) ---------------------
    print("[fitgirl] fetching page 1 …")
    page1_payload = get_with_retries(f"{fitgirl_base}/latest?page=1")
    api_last = api_last_page(page1_payload) or saved_last_page or 1
    print(f"[fitgirl] page 1: {len(result_items(page1_payload))} result(s) | API lastPage = {api_last}")

    # 4) Determine backfill status and pages to scan -----------------------
    backfill_complete = last_scanned >= api_last > 0

    if backfill_complete:
        pages_to_scan = [1]
        print("[backfill] complete — only scanning page 1 for new releases")
    else:
        start = max(1, last_scanned + 1)
        end = min(start + args.pages_per_cycle - 1, api_last)
        backfill_pages = list(range(start, end + 1))
        pages_to_scan = [1] + [p for p in backfill_pages if p != 1]
        print(f"[backfill] scanning pages {backfill_pages} + page 1 (last scanned: {last_scanned}, API last: {api_last})")

    # 5) Fetch remaining pages ---------------------------------------------
    page_payloads: dict[int, Any] = {1: page1_payload}
    for page in pages_to_scan:
        if page not in page_payloads:
            try:
                page_payloads[page] = get_with_retries(f"{fitgirl_base}/latest?page={page}")
                print(f"[fitgirl] page {page}: {len(result_items(page_payloads[page]))} result(s)")
            except Exception as exc:
                print(f"[warn] page {page} fetch failed: {exc}")
                page_payloads[page] = {}
            time.sleep(max(0.0, args.delay))

    # 6) Find and import missing games -------------------------------------
    imported: list[dict[str, Any]] = []
    seen: set[str] = set()
    new_last_scanned = last_scanned

    # Page 1 first (new releases), then backfill pages in order
    backfill_in_order = sorted(p for p in pages_to_scan if p > 1)
    process_order = [1] + backfill_in_order

    limit_hit = False
    for page in process_order:
        page_fully_processed = True
        for summary in result_items(page_payloads.get(page)):
            if should_skip(summary):
                continue
            slug = text(summary.get("slug"))
            if not slug or slug in seen:
                continue
            seen.add(slug)
            if slug in existing_slugs:
                continue

            # This game is missing from the site — fetch its detail
            print(f"  ⬇  [page {page}] {slug} — MISSING, fetching detail …")
            try:
                detail = get_with_retries(f"{fitgirl_base}/repack/{urllib.parse.quote(slug, safe='')}")
            except Exception as exc:
                print(f"  ⚠  [page {page}] {slug} — detail fetch failed: {exc}")
                continue

            if not isinstance(detail, dict) or should_skip(detail):
                continue
            exact_slug = text(detail.get("slug") or slug)
            if not exact_slug or exact_slug in existing_slugs:
                continue
            if not has_download_links(detail):
                print(f"  ⏭  [page {page}] {exact_slug} — no download links, skipped")
                continue

            payload = map_to_api_payload(summary, detail)

            if args.dry_run:
                print(f"  🔍 [dry-run] would import: {payload['title']} ({exact_slug})")
                imported.append(payload)
            else:
                try:
                    result = create_game(site_url, admin_secret, payload)
                    game_data = result.get("game", payload) if isinstance(result, dict) else payload
                    imported.append(game_data)
                    existing_slugs.add(exact_slug)
                    print(f"  ✅ [page {page}] imported: {payload['title']} ({exact_slug})")
                except RuntimeError as exc:
                    err = str(exc)
                    if "409" in err or "already exists" in err.lower() or "duplicate" in err.lower():
                        existing_slugs.add(exact_slug)
                        print(f"  ⏭  [page {page}] {exact_slug} — already exists on site")
                    else:
                        print(f"  ⚠  [page {page}] {payload['title']} — create failed: {exc}")
                    continue

            if len(imported) >= args.limit:
                page_fully_processed = False
                limit_hit = True
                break
            time.sleep(max(0.0, args.delay))

        # Advance backfill state only for fully-processed backfill pages
        if page_fully_processed and page > 1 and not backfill_complete:
            new_last_scanned = max(new_last_scanned, page)

        if limit_hit:
            break

    # 7) Update state -------------------------------------------------------
    final_last_scanned = api_last if backfill_complete else new_last_scanned

    new_state = {
        "lastScannedPage": final_last_scanned,
        "lastPage": api_last,
        "lastCycleAt": datetime.now(timezone.utc).isoformat(),
        "lastImportedSlugs": [text(g.get("slug")) for g in imported],
    }
    if not args.dry_run:
        write_state(state_file, new_state)
        bc = "complete" if final_last_scanned >= api_last else f"{final_last_scanned}/{api_last}"
        print(f"[state] saved: lastScannedPage={final_last_scanned} lastPage={api_last} backfill={bc}")
    else:
        print(f"[dry-run] state: {json.dumps(new_state, indent=2)}")

    # 8) Update STATUS -------------------------------------------------------
    STATUS.update(
        cycles=STATUS.cycles + 1,
        last_cycle_at=datetime.now(timezone.utc).isoformat(),
        last_cycle_result=f"imported {len(imported)} new game(s)",
        total_imported=STATUS.total_imported + len(imported),
        last_error=None,
        total_existing=len(existing_slugs),
        last_scanned_page=final_last_scanned,
        api_last_page=api_last,
        backfill_complete=final_last_scanned >= api_last,
    )

    print(f"CYCLE END — imported {len(imported)} game(s) | scanned up to page {final_last_scanned}/{api_last}")
    return len(imported)


# ---------------------------------------------------------------------------
# CLI / main loop
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", type=float, default=env_float("FITGIRL_IMPORT_INTERVAL", 600))
    parser.add_argument("--delay", type=float, default=env_float("FITGIRL_IMPORT_DELAY", 0.25))
    parser.add_argument("--limit", type=int, default=env_int("FITGIRL_IMPORT_LIMIT", 5))
    parser.add_argument("--pages-per-cycle", type=int, default=env_int("FITGIRL_PAGES_PER_CYCLE", 5))
    parser.add_argument("--once", action="store_true", default=env_bool("FITGIRL_IMPORT_ONCE"))
    parser.add_argument("--dry-run", action="store_true", default=env_bool("FITGIRL_IMPORT_DRY_RUN"))
    parser.add_argument("--health-port", type=int, default=env_int("HEALTH_PORT", 8080))
    parser.add_argument("--health-host", default=env("HEALTH_HOST", "0.0.0.0"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.interval < 1 or args.delay < 0 or args.limit < 1:
        raise RuntimeError("interval and limit must be positive; delay cannot be negative")

    start_health_server(args.health_host, args.health_port)

    if args.once or args.dry_run:
        return run_cycle(args)

    print("Continuous FitGirl import mode started.")
    while True:
        started = time.monotonic()
        try:
            run_cycle(args)
        except Exception as error:
            print(f"Cycle failed: {error}", file=sys.stderr)
            STATUS.update(
                last_error=f"{type(error).__name__}: {error}",
                last_cycle_result="error",
            )
        wait_seconds = max(1, args.interval - (time.monotonic() - started))
        print(f"Next cycle in {wait_seconds:.0f} seconds.", flush=True)
        time.sleep(wait_seconds)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
