#!/usr/bin/env python3
"""Import FitGirl latest releases into the Repack X GitHub JSON catalogue.

The script fetches /latest?page=N, skips non-game digest/upcoming entries, fetches
full details for each release, and upserts records in data/games.json through the
GitHub Contents API.

Rate-limit optimization: after each interval the script always checks page 1 for
new arrivals first, then continues from the last scanned page stored in a local
state file (skipping already-processed pages in between).

The real total page count is discovered dynamically from the `lastPage` value
returned by the FitGirl API on the first request.  FITGIRL_IMPORT_PAGES=0 (the
default) uses that value; any positive number acts as an optional override.

A lightweight health-check web server runs in a background thread so you can
verify the service is alive.

Required environment variables for a real write:
  GITHUB_CONTENTS_TOKEN   Fine-grained token with Contents: Read and write

Optional environment variables:
  GITHUB_REPO_OWNER       Default: LittleHacker999
  GITHUB_REPO_NAME        Default: musical-guacamole
  GITHUB_REPO_BRANCH      Default: main
  GITHUB_DATA_PATH        Default: data/games.json
  FITGIRL_API_BASE_URL    Default: https://fitgirl-api-5rbz.onrender.com
  REPACK_X_DOWNLOAD_BASE  Default: https://repack-x-database.vercel.app
  FITGIRL_IMPORT_PAGES    0 = use API lastPage (default); >0 = manual override cap
  FITGIRL_IMPORT_LIMIT    Default: 1
  FITGIRL_IMPORT_DELAY     Default: 0.25 seconds
  FITGIRL_IMPORT_INTERVAL  Default: 3600 seconds (1 hour)
  FITGIRL_IMPORT_ONCE      Default: false
  FITGIRL_IMPORT_DRY_RUN   Default: false
  FITGIRL_IMPORT_FORCE     Default: false
  FITGIRL_FETCH_GOG        Default: true; enrich cover, genres, screenshots and system requirements
  GOG_IMPORT_DELAY         Default: 0.25 seconds between GOG requests
  FITGIRL_STATE_PATH       Default: .fitgirl_import_state.json
  HEALTH_PORT              Default: 8080 (health-check web server)
  HEALTH_HOST              Default: 0.0.0.0 (health-check bind address)

Continuous mode imports at most one new release, then waits one hour before
checking again. It runs until interrupted with Ctrl+C.

Page-scan optimization:
  Cycle N   → scan page 1 (new arrivals), then continue from stored page
  Stored page advances as pages are scanned. When all pages are exhausted
  the stored page resets to 1 and a full scan begins on the next cycle.

Health-check endpoints:
  GET /             → simple HTML status page
  GET /api/health   → {"working": true}
  GET /api/status   → detailed JSON status

Examples:
  python scripts/import_fitgirl_latest.py --dry-run
  python scripts/import_fitgirl_latest.py --once
  python scripts/import_fitgirl_latest.py
  python scripts/import_fitgirl_latest.py --reset-state
"""

from __future__ import annotations

import argparse
import base64
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
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Iterable

DEFAULT_FITGIRL_API = "https://fitgirl-api-5rbz.onrender.com"
DEFAULT_DOWNLOAD_BASE = "https://repack-x-database.vercel.app"
DEFAULT_GOG_DELAY = 0.25
DEFAULT_STATE_PATH = ".fitgirl_import_state.json"
DEFAULT_HEALTH_PORT = 8080
DEFAULT_HEALTH_HOST = "0.0.0.0"
IMAGE_URL_RE = re.compile(
    r"\.(?:jpe?g|png|webp|gif|avif)(?:[?#].*)?$", re.IGNORECASE
)
SKIP_RE = re.compile(
    r"(?:upcoming[- ]repacks?|updates?[- ]digest|digest[- ]updates?)",
    re.IGNORECASE,
)


# ── Shared runtime status (updated by import thread, read by web server) ────


class ServiceStatus:
    """Thread-safe store for the current service state."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.started_at: str = datetime.now(timezone.utc).isoformat()
        self.last_cycle_at: str = ""
        self.last_cycle_result: str = ""
        self.total_imported: int = 0
        self.last_error: str = ""
        self.page_state: dict[str, Any] = {}
        self.api_last_page: int = 0

    def update_cycle(self, result: str, error: str = "") -> None:
        with self._lock:
            self.last_cycle_at = datetime.now(timezone.utc).isoformat()
            self.last_cycle_result = result
            self.last_error = error

    def increment_imported(self, count: int = 1) -> None:
        with self._lock:
            self.total_imported += count

    def update_page_state(self, state: dict[str, Any]) -> None:
        with self._lock:
            self.page_state = dict(state)

    def update_api_last_page(self, page: int) -> None:
        with self._lock:
            self.api_last_page = page

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "working": True,
                "started_at": self.started_at,
                "last_cycle_at": self.last_cycle_at,
                "last_cycle_result": self.last_cycle_result,
                "total_imported": self.total_imported,
                "last_error": self.last_error,
                "page_state": dict(self.page_state),
                "api_last_page": self.api_last_page,
            }


STATUS = ServiceStatus()


# ── Health-check web server ─────────────────────────────────────────────────


class HealthHandler(BaseHTTPRequestHandler):
    """Serve a simple health page and JSON endpoints."""

    def do_GET(self) -> None:
        path = self.path.split("?")[0].rstrip("/") or "/"

        if path == "/":
            self._send_html()
        elif path == "/api/health":
            self._send_json({"working": True})
        elif path == "/api/status":
            self._send_json(STATUS.snapshot())
        else:
            self._send_json(
                {
                    "error": "Not found",
                    "endpoints": ["/", "/api/health", "/api/status"],
                },
                404,
            )

    def _send_json(self, data: dict[str, Any], code: int = 200) -> None:
        body = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self) -> None:
        snap = STATUS.snapshot()
        error_badge = (
            f'<span class="badge error">{snap["last_error"]}</span>'
            if snap["last_error"]
            else '<span class="badge ok">No errors</span>'
        )
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>FitGirl Importer – Status</title>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
      font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
      background: #0d1117; color: #c9d1d9;
      display: flex; justify-content: center; align-items: center;
      min-height: 100vh;
    }}
    .card {{
      background: #161b22; border: 1px solid #30363d; border-radius: 12px;
      padding: 40px 48px; max-width: 560px; width: 90%;
    }}
    h1 {{ color: #58a6ff; font-size: 1.6rem; margin-bottom: 6px; }}
    .subtitle {{ color: #8b949e; margin-bottom: 28px; font-size: 0.95rem; }}
    .row {{ display: flex; justify-content: space-between; padding: 10px 0;
            border-bottom: 1px solid #21262d; }}
    .row:last-child {{ border-bottom: none; }}
    .label {{ color: #8b949e; font-size: 0.88rem; }}
    .value {{ color: #c9d1d9; font-size: 0.88rem; font-weight: 600;
              text-align: right; max-width: 300px; word-break: break-word; }}
    .badge {{ padding: 3px 10px; border-radius: 20px; font-size: 0.82rem;
              font-weight: 600; }}
    .badge.ok {{ background: #1a3c2a; color: #3fb950; }}
    .badge.error {{ background: #3c1a1a; color: #f85149; }}
    .pulse {{ display: inline-block; width: 10px; height: 10px;
              background: #3fb950; border-radius: 50%; margin-right: 6px;
              animation: pulse 1.5s ease-in-out infinite; }}
    @keyframes pulse {{ 0%,100% {{ opacity: 1; }} 50% {{ opacity: 0.4; }} }}
    .endpoints {{ margin-top: 24px; padding-top: 16px;
                  border-top: 1px solid #21262d; }}
    .endpoints a {{ color: #58a6ff; text-decoration: none; display: block;
                    padding: 4px 0; font-size: 0.88rem; }}
    .endpoints a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <div class="card">
    <h1><span class="pulse"></span>FitGirl Importer</h1>
    <p class="subtitle">Health-check service for the Repack X catalogue</p>

    <div class="row"><span class="label">Status</span>
      <span class="value"><span class="badge ok">Working</span></span></div>
    <div class="row"><span class="label">Started</span>
      <span class="value">{snap["started_at"]}</span></div>
    <div class="row"><span class="label">Last cycle</span>
      <span class="value">{snap["last_cycle_at"] or "Not yet"}</span></div>
    <div class="row"><span class="label">Last result</span>
      <span class="value">{snap["last_cycle_result"] or "\u2014"}</span></div>
    <div class="row"><span class="label">Total imported</span>
      <span class="value">{snap["total_imported"]}</span></div>
    <div class="row"><span class="label">API last page</span>
      <span class="value">{snap["api_last_page"] or "Unknown"}</span></div>
    <div class="row"><span class="label">Errors</span>
      <span class="value">{error_badge}</span></div>
    <div class="row"><span class="label">Page state</span>
      <span class="value">{json.dumps(snap["page_state"])}</span></div>

    <div class="endpoints">
      <a href="/api/health">/api/health</a>
      <a href="/api/status">/api/status</a>
    </div>
  </div>
</body>
</html>"""
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        pass  # Silence default request logging


def start_health_server(host: str, port: int) -> None:
    """Start the health-check web server in a daemon thread."""
    server = HTTPServer((host, port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"[health] web server running at http://{host}:{port}")
    print(f"[health]   GET /             → status page")
    print(f"[health]   GET /api/health   → {{\"working\": true}}")
    print(f"[health]   GET /api/status   → detailed status")


# ── Environment helpers ──────────────────────────────────────────────────────


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


# ── State persistence (rate-limit optimization) ──────────────────────────────


def load_state(path: str) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def save_state(path: str, state: dict[str, Any]) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def get_last_scanned_page(state: dict[str, Any]) -> int:
    try:
        return max(1, int(state.get("last_scanned_page", 1)))
    except (ValueError, TypeError):
        return 1


# ── HTTP layer ───────────────────────────────────────────────────────────────


def api_json(
    url: str,
    *,
    token: str | None = None,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
) -> tuple[Any, dict[str, str]]:
    body = None
    headers = {
        "Accept": "application/vnd.github+json"
        if "api.github.com" in url
        else "application/json",
        "User-Agent": "Repack-X-FitGirl-Importer/1.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
        headers["X-GitHub-Api-Version"] = "2022-11-28"
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            raw = response.read()
            return json.loads(raw.decode("utf-8")), {
                k.lower(): v for k, v in response.headers.items()
            }
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")[:1000]
        raise RuntimeError(f"HTTP {error.code} from {url}: {detail}") from error


def get_with_retries(url: str, retries: int = 3) -> Any:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            return api_json(url)[0]
        except Exception as error:
            last_error = error
            if attempt + 1 < retries:
                time.sleep(2**attempt)
    raise RuntimeError(f"Could not fetch {url}: {last_error}")


# ── lastPage extraction from FitGirl API ─────────────────────────────────────


def extract_last_page(response: Any) -> int:
    """
    Read the total page count from the FitGirl /latest response.

    Tries common field names and nested locations:
      { "lastPage": 150 }
      { "last_page": 150 }
      { "totalPages": 150 }
      { "pagination": { "lastPage": 150 } }
      { "meta": { "lastPage": 150 } }
      { "pagination": { "totalPages": 150 } }
    """
    if not isinstance(response, dict):
        return 0

    # Top-level keys
    for key in ("lastPage", "last_page", "totalPages", "total_pages", "pages"):
        value = response.get(key)
        if value is not None:
            try:
                return max(1, int(value))
            except (ValueError, TypeError):
                continue

    # Nested: pagination / meta / pageInfo
    for container_key in ("pagination", "meta", "pageInfo", "page_info"):
        container = response.get(container_key)
        if isinstance(container, dict):
            for key in ("lastPage", "last_page", "totalPages", "total_pages", "pages"):
                value = container.get(key)
                if value is not None:
                    try:
                        return max(1, int(value))
                    except (ValueError, TypeError):
                        continue

    return 0  # Not found


# ── Data-cleaning helpers ────────────────────────────────────────────────────


def as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def text(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("name", "title", "text", "url"):
            if value.get(key):
                return str(value[key]).strip()
        return ""
    return str(value).strip() if value is not None else ""


def unique_strings(values: Iterable[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = text(value)
        if not item:
            continue
        key = item.casefold()
        if key not in seen:
            seen.add(key)
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
        if not url or not re.match(r"^https?://", url, re.IGNORECASE):
            continue
        if not IMAGE_URL_RE.search(url):
            continue
        if url not in seen:
            seen.add(url)
            result.append(url)
    return result


def clean_slug(value: Any) -> str:
    return re.sub(r"[^a-z0-9-]", "", text(value).lower())


# ── GOG enrichment ───────────────────────────────────────────────────────────


def clean_title_for_gog(title: str) -> str:
    """Remove FitGirl/build/DLC noise so GOG search receives the game title."""
    value = re.sub(r"\[.*?\]", " ", title or "")
    value = re.sub(
        r"\((?:[^)]*(?:repack|build|dlc|patch|update|v\s*\d|fitgirl|dodi)[^)]*)\)",
        " ",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(
        r"\b(?:fitgirl|dodi|elamigos|mr\s*dj|xatab|codex|skidrow|reloaded|plaza|empress)\b.*$",
        " ",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(r"\+\s*(?:\d+|all)?\s*dlcs?\b.*$", " ", value, flags=re.IGNORECASE)
    value = re.sub(
        r"\b(?:v|ver\.?|version|build|patch)\s*[\d._-]+[a-z]?\b.*$",
        " ",
        value,
        flags=re.IGNORECASE,
    )
    return re.sub(r"\s{2,}", " ", re.sub(r"[:\-–—/]", " ", value)).strip()


def title_relevance_score(search_title: str, product_title: str) -> float:
    stop_words = {"the", "and", "for", "with", "all", "edition", "bundle"}
    search_words = [re.sub(r"[^a-z0-9]", "", w) for w in search_title.lower().split()]
    product_words = [re.sub(r"[^a-z0-9]", "", w) for w in product_title.lower().split()]
    search_words = [w for w in search_words if len(w) > 2 and w not in stop_words]
    product_words = [w for w in product_words if len(w) > 2 and w not in stop_words]
    if not search_words:
        return 1.0
    return sum(
        any(pw in sw or sw in pw for pw in product_words) for sw in search_words
    ) / len(search_words)


def gog_system_requirements(os_list: Any) -> str:
    if not isinstance(os_list, list) or not os_list:
        return ""
    windows = next(
        (
            item
            for item in os_list
            if str(item.get("operatingSystem", {}).get("name", "")).lower() == "windows"
        ),
        os_list[0],
    )
    groups = windows.get("systemRequirements") if isinstance(windows, dict) else None
    if not isinstance(groups, list):
        return ""
    sections = []
    for group in groups:
        if not isinstance(group, dict):
            continue
        lines = []
        for requirement in group.get("requirements", []):
            if not isinstance(requirement, dict):
                continue
            name = str(requirement.get("name") or requirement.get("id") or "").rstrip(":").strip()
            desc = str(requirement.get("description") or "").strip()
            if name and desc:
                lines.append(f"{name}: {desc}")
            elif desc:
                lines.append(desc)
        if group.get("description"):
            lines.append(str(group["description"]).strip())
        if lines:
            sections.append(
                f"{str(group.get('type') or 'Requirement').upper()}:\n" + "\n".join(lines)
            )
    return "\n\n".join(sections).strip()


def clean_gog_url(value: Any) -> str:
    url = text(value)
    url = re.sub(r"_{formatter}", "", url)
    return "https://" + url[7:] if url.startswith("http://") else url


def fetch_gog_metadata(title: str, delay: float = DEFAULT_GOG_DELAY) -> dict[str, Any]:
    """Fetch optional GOG metadata, returning an empty result on a safe miss."""
    query = clean_title_for_gog(title)
    if not query:
        return {}
    try:
        catalog = get_with_retries(
            f"https://catalog.gog.com/v1/catalog?query={urllib.parse.quote(query)}&limit=10"
        )
        products = catalog.get("products", []) if isinstance(catalog, dict) else []
        products = [
            p for p in products
            if isinstance(p, dict)
            and title_relevance_score(query, text(p.get("title"))) >= 0.25
        ]
        if not products:
            return {}
        product = next(
            (item for item in products if text(item.get("title")).casefold() == query.casefold()),
            products[0],
        )
        product_id = product.get("id")
        if not product_id:
            return {}
        time.sleep(max(0, delay))
        detail = get_with_retries(
            f"https://api.gog.com/v2/games/{urllib.parse.quote(str(product_id), safe='')}"
        )
        screenshots = image_urls(
            [clean_gog_url(url) for url in as_list(product.get("screenshots"))]
        )
        genres = unique_strings(
            [
                *(
                    item.get("name") if isinstance(item, dict) else item
                    for item in as_list(product.get("genres"))
                ),
                *(
                    item.get("name") if isinstance(item, dict) else item
                    for item in as_list(product.get("tags"))
                ),
            ]
        )
        return {
            "cover_url": clean_gog_url(product.get("coverVertical") or product.get("image")),
            "screenshots": screenshots,
            "genres": [g for g in genres if g.casefold() != "lossless repack"],
            "system_requirements": gog_system_requirements(
                detail.get("_embedded", {}).get("supportedOperatingSystems")
            ),
        }
    except Exception as error:
        print(f"skip GOG enrichment for {title}: {error}")
        return {}


# ── Record helpers ───────────────────────────────────────────────────────────


def should_skip(item: dict[str, Any]) -> bool:
    title = text(item.get("title"))
    slug = text(item.get("slug"))
    return not slug or bool(SKIP_RE.search(title) or SKIP_RE.search(slug))


def has_download_links(detail: dict[str, Any]) -> bool:
    link_fields = (detail.get("downloadMirrors"), detail.get("torrents"))

    def walk(value: Any) -> Iterable[str]:
        if isinstance(value, str):
            candidate = value.strip()
            if candidate:
                yield candidate
        elif isinstance(value, list):
            for item in value:
                yield from walk(item)
        elif isinstance(value, dict):
            for item in value.values():
                yield from walk(item)

    return any(
        re.match(r"^(?:https?://|magnet:\?)", value, re.IGNORECASE)
        for field in link_fields
        for value in walk(field)
    )


def description(detail: dict[str, Any]) -> str:
    paragraphs = unique_strings(as_list(detail.get("gameDescription")))
    features = unique_strings(as_list(detail.get("repackFeatures")))
    sections: list[str] = []
    if paragraphs:
        sections.append("\n\n".join(paragraphs))
    if features:
        sections.append("**Repack Features**\n\n" + "\n".join(f"- {item}" for item in features))
    return "\n\n".join(sections) or "Not Found"


def deterministic_id(slug: str) -> str:
    digest = hashlib.sha256(f"fitgirl:{slug}".encode("utf-8")).hexdigest()
    return (
        f"{digest[:8]}-{digest[8:12]}-5{digest[13:16]}-"
        f"{digest[16:20]}-{digest[20:32]}"
    )


def map_game(
    summary: dict[str, Any],
    detail: dict[str, Any],
    existing: dict[str, Any] | None,
    download_base: str,
    gog: dict[str, Any] | None = None,
) -> dict[str, Any]:
    slug = clean_slug(detail.get("slug") or summary.get("slug"))
    categories = unique_strings(
        [*as_list(detail.get("genres")), *as_list(detail.get("tags")), detail.get("category")]
    )
    categories = [c for c in categories if c.casefold() != "lossless repack"]
    categories = unique_strings([*categories, *((gog or {}).get("genres") or [])])
    repack_size = text(detail.get("repackSize")) or "Not Found"
    repack_size = (
        re.sub(r"\s*\[selective download\]\s*$", "", repack_size, flags=re.IGNORECASE).strip()
        or "Not Found"
    )
    return {
        "id": (existing or {}).get("id") or deterministic_id(slug),
        "title": text(detail.get("title") or summary.get("title")),
        "slug": slug,
        "categories": categories,
        "size_original": text(detail.get("originalSize")) or "Not Found",
        "size_repack": repack_size,
        "description": description(detail),
        "cover_url": text(detail.get("cover")) or (gog or {}).get("cover_url") or None,
        "screenshots": unique_strings(
            [*image_urls(detail.get("screenshots")), *((gog or {}).get("screenshots") or [])]
        ),
        "system_requirements": (gog or {}).get("system_requirements") or "Not Found",
        "repacker": "Fitgirl",
        "download_url": f"{download_base.rstrip('/')}/fg-{urllib.parse.quote(slug, safe='-')}",
        "is_published": True,
        "featured": bool((existing or {}).get("featured", False)),
        "downloads_count": int((existing or {}).get("downloads_count", 0) or 0),
        "created_at": (existing or {}).get("created_at") or datetime.now(timezone.utc).isoformat(),
    }


# ── GitHub file helpers ──────────────────────────────────────────────────────


def github_file_url(owner: str, repo: str, path: str) -> str:
    encoded = "/".join(urllib.parse.quote(part, safe="") for part in path.split("/"))
    return (
        f"https://api.github.com/repos/{urllib.parse.quote(owner)}"
        f"/{urllib.parse.quote(repo)}/contents/{encoded}"
    )


def read_catalogue(
    owner: str, repo: str, branch: str, path: str, token: str
) -> tuple[list[dict[str, Any]], str | None]:
    url = github_file_url(owner, repo, path) + "?ref=" + urllib.parse.quote(branch)
    payload, _ = api_json(url, token=token)
    content = base64.b64decode(str(payload.get("content", "")).replace("\n", "")).decode("utf-8")
    parsed = json.loads(content)
    games = parsed if isinstance(parsed, list) else parsed.get("games", [])
    if not isinstance(games, list):
        raise RuntimeError("GitHub catalogue JSON must be an array or an object with a games array.")
    return games, payload.get("sha")


def write_catalogue(
    owner: str, repo: str, branch: str, path: str,
    token: str, games: list[dict[str, Any]], sha: str, message: str,
) -> None:
    raw = (json.dumps(games, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    if len(raw) > 900_000:
        raise RuntimeError(
            f"Catalogue is {len(raw) / 1024:.0f} KB; stop before the GitHub Contents API 1 MB range."
        )
    payload = {
        "message": message,
        "content": base64.b64encode(raw).decode("ascii"),
        "branch": branch,
        "sha": sha,
    }
    api_json(github_file_url(owner, repo, path), token=token, method="PUT", payload=payload)


# ── CLI ──────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--pages", type=int,
        default=env_int("FITGIRL_IMPORT_PAGES", 0),
        help=(
            "0 = discover total pages from the API lastPage field (default). "
            "Any positive value overrides and caps the scan."
        ),
    )
    parser.add_argument(
        "--limit", type=int,
        default=env_int("FITGIRL_IMPORT_LIMIT", 1),
        help="Override FITGIRL_IMPORT_LIMIT. Continuous mode always imports one.",
    )
    parser.add_argument(
        "--delay", type=float,
        default=env_float("FITGIRL_IMPORT_DELAY", 0.25),
        help="Override FITGIRL_IMPORT_DELAY.",
    )
    parser.add_argument(
        "--interval", type=float,
        default=env_float("FITGIRL_IMPORT_INTERVAL", 3600),
        help="Override FITGIRL_IMPORT_INTERVAL in seconds.",
    )
    parser.add_argument(
        "--once", action="store_true",
        default=env_bool("FITGIRL_IMPORT_ONCE"),
        help="Run one cycle and exit.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        default=env_bool("FITGIRL_IMPORT_DRY_RUN"),
        help="Fetch and print without writing GitHub.",
    )
    parser.add_argument(
        "--force", action="store_true",
        default=env_bool("FITGIRL_IMPORT_FORCE"),
        help="Update existing records.",
    )
    parser.add_argument(
        "--reset-state", action="store_true", default=False,
        help="Delete the page-tracking state file and start scanning from page 1.",
    )
    parser.add_argument(
        "--health-port", type=int,
        default=env_int("HEALTH_PORT", DEFAULT_HEALTH_PORT),
        help="Port for the health-check web server.",
    )
    parser.add_argument(
        "--health-host", type=str,
        default=env("HEALTH_HOST", DEFAULT_HEALTH_HOST),
        help="Bind address for the health-check web server.",
    )
    return parser.parse_args()


# ── Core import cycle ────────────────────────────────────────────────────────


def run_cycle(args: argparse.Namespace) -> int:
    fitgirl_base = env("FITGIRL_API_BASE_URL", DEFAULT_FITGIRL_API).rstrip("/")
    download_base = env("REPACK_X_DOWNLOAD_BASE", DEFAULT_DOWNLOAD_BASE)
    owner = env("GITHUB_REPO_OWNER", "LittleHacker999")
    repo = env("GITHUB_REPO_NAME", "musical-guacamole")
    branch = env("GITHUB_REPO_BRANCH", "main")
    path = env("GITHUB_DATA_PATH", "data/games.json")
    token = os.environ.get("GITHUB_CONTENTS_TOKEN", "").strip()
    fetch_gog = env_bool("FITGIRL_FETCH_GOG", True)
    gog_delay = env_float("GOG_IMPORT_DELAY", DEFAULT_GOG_DELAY)

    # ── State: remember which page to resume from ────────────────────────
    state_path = env("FITGIRL_STATE_PATH", DEFAULT_STATE_PATH)
    state = load_state(state_path)
    last_page = get_last_scanned_page(state)
    STATUS.update_page_state(state)

    # ── Read existing catalogue ──────────────────────────────────────────
    existing: list[dict[str, Any]] = []
    sha: str | None = None
    if not args.dry_run:
        if not token:
            raise RuntimeError("GITHUB_CONTENTS_TOKEN is required unless --dry-run is used.")
        existing, sha = read_catalogue(owner, repo, branch, path, token)
        if not sha:
            raise RuntimeError("GitHub catalogue did not return a file SHA.")

    by_slug = {text(game.get("slug")): game for game in existing if text(game.get("slug"))}
    imported: list[dict[str, Any]] = []
    seen: set[str] = set()

    cycle_limit = args.limit if (args.dry_run or args.once) else 1

    # ── Fetch page 1 first to discover the real lastPage ─────────────────
    page_one = get_with_retries(f"{fitgirl_base}/latest?page=1")
    api_last_page = extract_last_page(page_one)
    results_one = page_one.get("results", []) if isinstance(page_one, dict) else page_one

    if not isinstance(results_one, list):
        raise RuntimeError("Unexpected /latest response on page 1: expected results array.")

    # ── Determine the effective page limit ───────────────────────────────
    if args.pages > 0:
        # Manual override: user set a specific cap
        page_limit = max(args.pages, 2)
        print(f"[pages] using manual override: {page_limit} page(s) (FITGIRL_IMPORT_PAGES={args.pages})")
    elif api_last_page > 0:
        # Dynamic: use the API's lastPage
        page_limit = api_last_page
        print(f"[pages] discovered lastPage from API: {api_last_page}")
    else:
        # Fallback: API didn't return lastPage
        page_limit = max(args.pages, 2)
        print(f"[pages] API did not return lastPage; falling back to {page_limit}")

    STATUS.update_api_last_page(api_last_page)

    # Clamp stored last_page to valid range.
    if last_page > page_limit:
        last_page = 1

    # ── Build optimised page list ────────────────────────────────────────
    #   Always check page 1 first for new arrivals.
    #   Then continue from the stored page, skipping pages 2 … last_page-1.
    pages_to_check: list[int] = [1]
    resume_from = max(2, last_page)
    pages_to_check.extend(range(resume_from, page_limit + 1))

    skipped_pages = list(range(2, resume_from))
    if skipped_pages:
        print(
            f"[page-optimise] checking {pages_to_check} "
            f"(skipping {skipped_pages[0]}-{skipped_pages[-1]})"
        )
    else:
        print(f"[page-optimise] scanning all pages 1-{page_limit}")

    # ── Scan pages ───────────────────────────────────────────────────────
    scanned_any = False
    for page_index, page in enumerate(pages_to_check):
        scanned_any = True

        # Page 1 was already fetched above; reuse it.
        if page == 1:
            latest = page_one
            results = results_one
        else:
            latest = get_with_retries(f"{fitgirl_base}/latest?page={page}")
            results = latest.get("results", []) if isinstance(latest, dict) else latest

            if not isinstance(results, list):
                raise RuntimeError(
                    f"Unexpected /latest response on page {page}: expected results array."
                )

        if not results:
            print(f"[page-optimise] page {page} is empty → resetting state to page 1.")
            state["last_scanned_page"] = 1
            save_state(state_path, state)
            STATUS.update_page_state(state)
            break

        for summary in results:
            if not isinstance(summary, dict) or should_skip(summary):
                continue
            slug = clean_slug(summary.get("slug"))
            if not slug or slug in seen:
                continue
            if not args.force and slug in by_slug:
                print(f"skip existing: {slug}")
                seen.add(slug)
                continue

            detail = get_with_retries(
                f"{fitgirl_base}/repack/{urllib.parse.quote(slug, safe='-')}"
            )
            if not isinstance(detail, dict) or should_skip(detail):
                continue
            if not has_download_links(detail):
                print(f"skip without download links: {slug}")
                seen.add(slug)
                continue

            gog = (
                fetch_gog_metadata(text(detail.get("title") or summary.get("title")), gog_delay)
                if fetch_gog
                else {}
            )
            game = map_game(summary, detail, by_slug.get(slug), download_base, gog)
            imported.append(game)
            by_slug[slug] = game
            seen.add(slug)
            print(f"import: {game['title']} -> {game['download_url']}")

            if len(imported) >= cycle_limit:
                break
            time.sleep(max(0, args.delay))

        # Record how far we got so the next cycle can skip ahead.
        state["last_scanned_page"] = max(get_last_scanned_page(state), page)
        save_state(state_path, state)
        STATUS.update_page_state(state)

        if len(imported) >= cycle_limit:
            break

    # If we scanned every page and found nothing new, reset so the next
    # cycle performs a full scan again (new arrivals may shift pagination).
    if not imported:
        print("No new FitGirl releases found.")
        if scanned_any and pages_to_check and pages_to_check[-1] >= page_limit:
            state["last_scanned_page"] = 1
            save_state(state_path, state)
            STATUS.update_page_state(state)
            print("[page-optimise] all pages exhausted → next cycle starts from page 1.")
        STATUS.update_cycle("no new releases")
        return 0

    if args.dry_run:
        print(json.dumps(imported, ensure_ascii=False, indent=2))
        print(f"Dry run complete: {len(imported)} release(s) mapped; GitHub was not changed.")
        STATUS.update_cycle(f"dry run: {len(imported)} mapped")
        return 0

    merged = [
        game
        for game in existing
        if text(game.get("slug")) not in {item["slug"] for item in imported}
    ]
    merged.extend(imported)
    merged.sort(key=lambda game: game.get("created_at", ""), reverse=True)
    write_catalogue(
        owner, repo, branch, path, token, merged, sha or "",
        f"Import {len(imported)} FitGirl release(s)",
    )
    print(f"GitHub catalogue updated: {len(imported)} release(s), {len(merged)} total game(s).")
    STATUS.increment_imported(len(imported))
    STATUS.update_cycle(f"imported {len(imported)} release(s)")
    return 0


# ── Entry point ──────────────────────────────────────────────────────────────


def main() -> int:
    args = parse_args()

    # --reset-state: delete the tracking file before anything else.
    if args.reset_state:
        state_path = env("FITGIRL_STATE_PATH", DEFAULT_STATE_PATH)
        if os.path.exists(state_path):
            os.remove(state_path)
            print(f"State file {state_path} deleted; page tracking reset to 1.")
        else:
            print(f"No state file at {state_path}; nothing to reset.")

    if args.limit < 1 or args.interval < 1:
        raise RuntimeError("--limit and --interval must be positive.")
    if args.pages < 0:
        raise RuntimeError("--pages must be 0 (auto) or a positive integer.")

    # ── Start health-check web server (always) ───────────────────────────
    try:
        start_health_server(args.health_host, args.health_port)
    except OSError as error:
        print(f"[health] could not start on port {args.health_port}: {error}", file=sys.stderr)

    if args.dry_run or args.once:
        return run_cycle(args)

    print("Continuous mode started: maximum one FitGirl release per hour.")
    while True:
        started = time.monotonic()
        try:
            run_cycle(args)
        except Exception as error:
            # A temporary API cold start, GitHub timeout, or 409 conflict
            # should not permanently stop an unattended importer.
            print(f"Cycle failed: {error}", file=sys.stderr)
            STATUS.update_cycle("failed", error=str(error))
        elapsed = time.monotonic() - started
        wait_seconds = max(1, args.interval - elapsed)
        print(f"Next FitGirl check in {wait_seconds:.0f} seconds.", flush=True)
        time.sleep(wait_seconds)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
