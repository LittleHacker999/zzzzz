#!/usr/bin/env python3
"""Import FitGirl and GOG releases into Repack X via the admin API.

Every 1 hour:
  1. FitGirl: Check /latest?page=1 → add new releases (FitGirl data only, NO RAWG/GOG).
     Title: "[Game Name] (Fitgirl Repack)"  Slug: "fg-{slug}"
  2. GOG: Pick games from GOG scrape /api/all-games → enrich with RAWG
     (description/tags/screenshots) + GOG official API (system requirements) → add.
     Title: "[Game Name] (GOG Repack)"  Slug: "gog-{slug}"
     Cover: coverVertical from GOG Catalog API
  3. If GOG game already on site as FitGirl → rename FitGirl title to
     "[Game Name] (Fitgirl Repack)" and add GOG entry anyway.
  4. When GOG backlog is complete, only new GOG games are checked (same as FitGirl).

Download URLs:
  FitGirl: https://repack-x-database.vercel.app/fg-{slug}
  GOG:     https://repack-x-database.vercel.app/gog-{gog_id}

Health web server:
  GET /            HTML status page (auto-refreshes)
  GET /api/health  {"working": true}
  GET /api/status  detailed JSON

Env vars:
  REPACK_X_ADMIN_SECRET     (required) admin secret
  REPACK_X_SITE_URL         default https://repackx.apps.mistral.run
  REPACK_X_DOWNLOAD_BASE    default https://repack-x-database.vercel.app
  FITGIRL_API_BASE_URL      default https://fitgirl-api-5rbz.onrender.com
  RAWG_API_BASE_URL         default https://api.rawg.io
  RAWG_API_KEY              optional (recommended)
  GOG_SCRAPE_API_URL        default https://repackx-gscrape.onrender.com
  GOG_SCRAPE_API_KEY        default @Jithula2009
  FITGIRL_IMPORT_LIMIT      default 5   (max FitGirl games per cycle)
  GOG_IMPORT_LIMIT          default 1   (max GOG games per cycle)
  FITGIRL_IMPORT_DELAY      default 1.0 (seconds between enrichment calls)
  FITGIRL_IMPORT_INTERVAL   default 3600
  FITGIRL_IMPORT_ONCE       default false
  FITGIRL_IMPORT_DRY_RUN    default false
  HEALTH_PORT               default 8080
  HEALTH_HOST               default 0.0.0.0
"""

from __future__ import annotations

import argparse
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
from typing import Any

# ── Constants ─────────────────────────────────────────────────────────────

DEFAULT_FITGIRL_API = "https://fitgirl-api-5rbz.onrender.com"
DEFAULT_SITE_URL = "https://repackx.apps.mistral.run"
DEFAULT_DOWNLOAD_BASE = "https://repack-x-database.vercel.app"
DEFAULT_RAWG_API = "https://api.rawg.io"
DEFAULT_GOG_SCRAPE_API = "https://repackx-gscrape.onrender.com"
DEFAULT_GOG_SCRAPE_KEY = "@Jithula2009"
GOG_V2_API = "https://api.gog.com/v2/games"

IMAGE_URL_RE = re.compile(
    r"\.(?:jpe?g|png|webp|gif|avif)(?:[?#].*)?$", re.I
)
SKIP_RE = re.compile(
    r"(?:upcoming[- ]repacks?|updates?[- ]digest|digest[- ]updates?)", re.I
)

_UA = "Repack-X-Importer/5.0"


# ── Env helpers ───────────────────────────────────────────────────────────

def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def env_int(name: str, default: int) -> int:
    v = env(name, str(default))
    try:
        return int(v)
    except ValueError as e:
        raise RuntimeError(f"{name} must be integer, got {v!r}") from e


def env_float(name: str, default: float) -> float:
    v = env(name, str(default))
    try:
        return float(v)
    except ValueError as e:
        raise RuntimeError(f"{name} must be number, got {v!r}") from e


def env_bool(name: str, default: bool = False) -> bool:
    v = env(name, "true" if default else "false").lower()
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{name} must be true/false, got {v!r}")


# ── Shared status ─────────────────────────────────────────────────────────

class ServiceStatus:
    def __init__(self):
        self._lock = threading.Lock()
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.cycles = 0
        self.last_cycle_at: str | None = None
        self.last_cycle_result = "never run"
        self.total_fitgirl = 0
        self.total_gog = 0
        self.total_existing = 0
        self.rawg_calls = 0
        self.gog_api_calls = 0
        self.last_error: str | None = None

    def update(self, **kw):
        with self._lock:
            for k, v in kw.items():
                setattr(self, k, v)

    def bump(self, field: str, n: int = 1):
        with self._lock:
            setattr(self, field, getattr(self, field) + n)

    def snap(self):
        with self._lock:
            return {
                "working": True,
                "started_at": self.started_at,
                "cycles": self.cycles,
                "last_cycle_at": self.last_cycle_at,
                "last_cycle_result": self.last_cycle_result,
                "total_fitgirl_imported": self.total_fitgirl,
                "total_gog_imported": self.total_gog,
                "total_existing": self.total_existing,
                "rawg_calls": self.rawg_calls,
                "gog_api_calls": self.gog_api_calls,
                "last_error": self.last_error,
            }


STATUS = ServiceStatus()


# ── Health web server ─────────────────────────────────────────────────────

_HTML = """<!doctype html>
<html><head><meta charset="utf-8"><title>Repack X Importer</title>
<meta http-equiv="refresh" content="30">
<style>body{background:#0d1117;color:#c9d1d9;font-family:system-ui,sans-serif;margin:2rem}
h1{color:#58a6ff}.ok{color:#3fb950}
table{border-collapse:collapse}td,th{border:1px solid #30363d;padding:.4rem .8rem;text-align:left}
code{color:#79c0ff}</style></head><body>"""


class HealthHandler(BaseHTTPRequestHandler):
    def _send(self, code, body, ct="application/json"):
        d = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(d)))
        self.end_headers()
        self.wfile.write(d)

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        s = STATUS.snap()
        if path == "/api/health":
            self._send(200, json.dumps({"working": True}))
        elif path == "/api/status":
            self._send(200, json.dumps(s, indent=2))
        elif path in ("/", "/index.html"):
            rows = "".join(
                f"<tr><th>{k}</th><td><code>{v}</code></td></tr>"
                for k, v in s.items()
            )
            self._send(
                200,
                _HTML
                + f"<h1>Repack X Importer</h1>"
                + "<p class='ok'>&#9679; working: true</p>"
                + f"<table>{rows}</table>"
                + "<p><a href='/api/health'>/api/health</a> &middot; "
                + "<a href='/api/status'>/api/status</a></p></body></html>",
                "text/html; charset=utf-8",
            )
        else:
            self._send(404, '{"error":"not found"}')

    def log_message(self, *a):
        pass


def start_health_server(host, port):
    srv = HTTPServer((host, port), HealthHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f"[health] http://{host}:{port}")


# ── HTTP helpers ──────────────────────────────────────────────────────────

def _request_json(
    url,
    *,
    method="GET",
    payload=None,
    headers=None,
    timeout=60,
    retries=3,
):
    last = None
    for attempt in range(retries):
        body = json.dumps(payload).encode() if payload is not None else None
        h = {"Accept": "application/json", "User-Agent": _UA}
        if headers:
            h.update(headers)
        if payload is not None:
            h["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=body, headers=h, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read().decode("utf-8", "replace")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")[:800]
            if e.code in (400, 401, 403, 404, 409):
                raise RuntimeError(f"HTTP {e.code}: {detail}") from e
            if e.code in (429, 500, 502, 503, 504) and attempt + 1 < retries:
                last = RuntimeError(f"HTTP {e.code}: {detail}")
                time.sleep(min(30, 2 ** (attempt + 1)))
                continue
            raise RuntimeError(f"HTTP {e.code} from {url}: {detail}") from e
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            last = e
            if attempt + 1 < retries:
                time.sleep(min(30, 2 ** (attempt + 1)))
                continue
            raise RuntimeError(f"fetch failed {url}: {e}") from e
    raise RuntimeError(f"fetch failed {url}: {last}")


def get_json(url, **kw):
    return _request_json(url, **kw)


def post_json(url, payload, headers=None, **kw):
    return _request_json(
        url, method="POST", payload=payload, headers=headers, **kw
    )


def patch_json(url, payload, headers=None, **kw):
    return _request_json(
        url, method="PATCH", payload=payload, headers=headers, **kw
    )


# ── Data helpers ──────────────────────────────────────────────────────────

def text(v) -> str:
    if isinstance(v, dict):
        for k in ("name", "title", "text", "url", "slug"):
            if v.get(k):
                return str(v[k]).strip()
        return ""
    return str(v).strip() if v is not None else ""


def as_list(v) -> list:
    return [] if v is None else (v if isinstance(v, list) else [v])


def unique_strings(vals) -> list[str]:
    out, seen = [], set()
    for v in vals:
        s = text(v)
        if s and s.casefold() not in seen:
            seen.add(s.casefold())
            out.append(s)
    return out


def nested_urls(v):
    if isinstance(v, str):
        yield v.strip()
    elif isinstance(v, list):
        for i in v:
            yield from nested_urls(i)
    elif isinstance(v, dict):
        for k in ("url", "image", "src", "thumbnail"):
            if v.get(k):
                yield from nested_urls(v[k])


def image_urls(v) -> list[str]:
    out, seen = [], set()
    for u in nested_urls(v):
        if (
            re.match(r"^https?://", u, re.I)
            and IMAGE_URL_RE.search(u)
            and u not in seen
        ):
            seen.add(u)
            out.append(u)
    return out


def clean_html(raw: str) -> str:
    t = re.sub(r"<[^>]+>", "", raw)
    for a, b in [
        ("&#39;", "'"),
        ("&nbsp;", " "),
        ("&amp;", "&"),
        ("&lt;", "<"),
        ("&gt;", ">"),
        ("&quot;", '"'),
        ("&rsquo;", "\u2019"),
        ("&lsquo;", "\u2018"),
        ("&rdquo;", "\u201d"),
        ("&ldquo;", "\u201c"),
        ("&mdash;", "\u2014"),
        ("&ndash;", "\u2013"),
    ]:
        t = t.replace(a, b)
    return re.sub(r"\n{3,}", "\n\n", t).strip()


# ── Slug matching ─────────────────────────────────────────────────────────

def normalize_slug_for_match(slug: str) -> str:
    """Normalize GOG slug (underscores) to hyphen format for matching."""
    return slug.replace("_", "-").lower().strip("-")


# ── FitGirl helpers ───────────────────────────────────────────────────────

def fg_should_skip(item: dict) -> bool:
    title = text(item.get("title"))
    slug = text(item.get("slug"))
    return not slug or bool(SKIP_RE.search(title) or SKIP_RE.search(slug))


def fg_has_downloads(detail: dict) -> bool:
    def walk(v):
        if isinstance(v, str):
            if v.strip():
                yield v.strip()
        elif isinstance(v, list):
            for i in v:
                yield from walk(i)
        elif isinstance(v, dict):
            for i in v.values():
                yield from walk(i)

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
        re.match(r"^(?:https?://|magnet:\?)", v, re.I)
        for f in fields
        for v in walk(f)
    )


def fg_result_items(payload) -> list[dict]:
    if isinstance(payload, list):
        return [i for i in payload if isinstance(i, dict)]
    if isinstance(payload, dict):
        for k in ("results", "repacks", "data", "items"):
            if isinstance(payload.get(k), list):
                return [i for i in payload[k] if isinstance(i, dict)]
    return []


def fg_description(detail: dict) -> str:
    paras = unique_strings(as_list(detail.get("gameDescription")))
    raw_feat = detail.get("repackFeatures")
    if isinstance(raw_feat, str) and raw_feat.strip():
        feats = [l.strip() for l in raw_feat.splitlines() if l.strip()]
    else:
        feats = unique_strings(as_list(raw_feat))
    parts = []
    if paras:
        parts.append("\n\n".join(paras))
    if feats:
        parts.append("**Repack Features**\n\n" + "\n".join(f"- {f}" for f in feats))
    return "\n\n".join(parts)


# ── RAWG enrichment (for GOG games ONLY) ─────────────────────────────────

def rawg_enrich(title: str, rawg_base: str, rawg_key: str) -> dict:
    """Search RAWG by title → description, tags, screenshots, cover.
    Uses 2 requests: search + detail. Minimal to respect rate limits."""
    result = {"description": "", "tags": [], "screenshots": [], "cover": ""}

    q = urllib.parse.urlencode(
        {"search": title, **({"key": rawg_key} if rawg_key else {})}
    )
    try:
        data = get_json(f"{rawg_base}/api/games?{q}", retries=2)
        STATUS.bump("rawg_calls")
    except RuntimeError as e:
        print(f"  ⚠  RAWG search failed: {e}")
        return result

    games = data.get("results", []) if isinstance(data, dict) else []
    if not games:
        return result

    g = games[0]
    result["cover"] = g.get("background_image", "")
    result["tags"] = [
        t.get("name", "")
        for t in g.get("tags", [])
        if isinstance(t, dict) and t.get("name")
    ][:12]
    result["screenshots"] = [
        s.get("image", "")
        for s in g.get("short_screenshots", [])
        if isinstance(s, dict) and s.get("image")
    ]

    rawg_id = g.get("id")
    if rawg_id:
        dp = urllib.parse.urlencode({"key": rawg_key} if rawg_key else {})
        try:
            detail = get_json(
                f"{rawg_base}/api/games/{rawg_id}" + (f"?{dp}" if dp else ""),
                retries=2,
            )
            STATUS.bump("rawg_calls")
            result["description"] = clean_html(detail.get("description_raw", ""))
            for t in detail.get("tags", []):
                n = t.get("name", "") if isinstance(t, dict) else ""
                if n and n not in result["tags"]:
                    result["tags"].append(n)
        except RuntimeError:
            pass

    return result


# ── GOG official API (for GOG games ONLY) ────────────────────────────────

def gog_sysreqs(gog_id) -> str:
    """Fetch system requirements from api.gog.com/v2/games/{id}."""
    try:
        data = get_json(f"{GOG_V2_API}/{gog_id}", retries=2)
        STATUS.bump("gog_api_calls")
    except RuntimeError as e:
        print(f"  ⚠  GOG v2 sysreqs failed: {e}")
        return ""
    return _extract_sysreqs(data)


def _extract_sysreqs(data) -> str:
    if not isinstance(data, dict):
        return ""
    os_list = (
        data.get("_embedded", {}).get("supportedOperatingSystems", [])
        or data.get("supportedOperatingSystems", [])
    )
    parts = []
    for os_e in os_list:
        if not isinstance(os_e, dict):
            continue
        os_title = os_e.get("title", "Requirements")
        reqs = (
            os_e.get("_embedded", {}).get("requirements", [])
            or os_e.get("requirements", [])
        )
        for req in reqs:
            if not isinstance(req, dict):
                continue
            rtype = req.get("type", "minimum").capitalize()
            sysreqs = (
                req.get("_embedded", {}).get("systemRequirements", [])
                or req.get("systemRequirements", [])
            )
            lines = []
            for sr in sysreqs:
                if isinstance(sr, dict) and sr.get("name") and sr.get("value"):
                    lines.append(f"{sr['name']}: {sr['value']}")
            if lines:
                parts.append(f"**{os_title} ({rtype})**\n" + "\n".join(lines))
    return "\n\n".join(parts)


# ── GOG scrape API ───────────────────────────────────────────────────────

def gog_all_games(base: str, key: str) -> list[dict]:
    """GET /api/all-games from the GOG scrape API."""
    headers = {"X-API-Key": key} if key else {}
    STATUS.bump("gog_api_calls")
    data = get_json(f"{base}/api/all-games", headers=headers, retries=3)
    return data.get("games", []) if isinstance(data, dict) else []


def get_gog_cover_vertical(
    title: str, gog_scrape_base: str, gog_scrape_key: str
) -> str:
    """Get coverVertical image from GOG Catalog API."""
    # Try GOG scrape API catalog first
    try:
        q = urllib.parse.urlencode({"q": title, "limit": 1})
        headers = {"X-API-Key": gog_scrape_key} if gog_scrape_key else {}
        data = get_json(
            f"{gog_scrape_base}/api/catalog?{q}", headers=headers, retries=2
        )
        STATUS.bump("gog_api_calls")
        results = data.get("results", []) if isinstance(data, dict) else []
        if results:
            r = results[0]
            return r.get("coverVertical", "") or r.get("cover", "")
    except RuntimeError as e:
        print(f"  ⚠  GOG scrape catalog cover failed: {e}")

    # Fallback: direct GOG Catalog API
    try:
        q = urllib.parse.urlencode({"query": title, "limit": 1})
        data = get_json(
            f"https://catalog.gog.com/v1/catalog?{q}", retries=2
        )
        STATUS.bump("gog_api_calls")
        products = data.get("products", []) if isinstance(data, dict) else []
        if products:
            p = products[0]
            return p.get("coverVertical", "") or p.get("cover", "")
    except RuntimeError:
        pass
    return ""


# ── Repack X API ─────────────────────────────────────────────────────────

def fetch_existing_slugs(site_url: str) -> set[str]:
    data = get_json(f"{site_url}/api/games/all-slugs")
    slugs = set()
    items = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        for k in ("slugs", "data", "items", "games", "results"):
            if isinstance(data.get(k), list):
                items = data[k]
                break
    for item in items:
        s = (
            item.strip()
            if isinstance(item, str)
            else text(item.get("slug"))
            if isinstance(item, dict)
            else ""
        )
        if s:
            slugs.add(s)
    return slugs


def create_game(site_url: str, secret: str, payload: dict) -> Any:
    """Send secret via header, body, AND query param — max compatibility."""
    full_payload = {**payload, "secret": secret}
    headers = {
        "x-admin-secret": secret,
        "Authorization": f"Bearer {secret}",
    }
    url = f"{site_url}/api/games/admin?secret={urllib.parse.quote(secret)}"
    return post_json(url, full_payload, headers, timeout=60, retries=1)


def get_site_game_by_slug(site_url: str, slug: str) -> dict | None:
    """Get game details from the site by slug."""
    try:
        data = get_json(
            f"{site_url}/api/games/{urllib.parse.quote(slug, safe='')}",
            retries=1,
        )
        if isinstance(data, dict):
            return data.get("game", data)
    except RuntimeError:
        pass
    return None


def rename_game_title(
    site_url: str, secret: str, game_id: str, new_title: str
) -> None:
    """Rename an existing game's title via PATCH."""
    url = f"{site_url}/api/games/admin/{game_id}"
    headers = {"x-admin-secret": secret, "Authorization": f"Bearer {secret}"}
    patch_json(
        url,
        {"title": new_title},
        headers=headers,
        timeout=30,
        retries=1,
    )


# ── Payload builders ─────────────────────────────────────────────────────

def build_fitgirl_payload(
    summary: dict, detail: dict, download_base: str
) -> dict:
    """FitGirl game — FitGirl data ONLY. No RAWG, no GOG."""
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
    ][:15]

    desc = fg_description(detail) or "Not Found"
    cover = text(detail.get("cover"))
    shots = image_urls(detail.get("screenshots"))

    size_orig = text(detail.get("originalSize")) or "Not Found"
    size_repack = text(detail.get("repackSize")) or "Not Found"
    size_repack = (
        re.sub(
            r"\s*\[Selective Download\]\s*$", "", size_repack, flags=re.I
        ).strip()
        or "Not Found"
    )

    return {
        "title": f"{title} (Fitgirl Repack)",
        "slug": f"fg-{slug}",
        "download_url": f"{download_base}/fg-{slug}",
        "repacker": "Fitgirl",
        "size_original": size_orig,
        "size_repack": size_repack,
        "categories": categories,
        "description": desc,
        "cover_url": cover,
        "screenshot_urls": shots,
        "system_requirements": "Not Found",
        "is_published": True,
        "featured": False,
        "trending": False,
    }


def build_gog_payload(
    gog_game: dict,
    rawg: dict,
    sysreq: str,
    download_base: str,
    cover_vertical: str,
) -> dict:
    """GOG game — RAWG desc/tags/shots + GOG v2 sysreqs + coverVertical cover."""
    gog_id = gog_game.get("id", 0)
    title = text(gog_game.get("title"))
    slug = text(gog_game.get("slug"))

    categories = unique_strings(
        [
            gog_game.get("category"),
            *rawg.get("tags", []),
        ]
    )[:15]

    return {
        "title": f"{title} (GOG Repack)",
        "slug": f"gog-{slug}",
        "download_url": f"{download_base}/gog-{gog_id}",
        "repacker": "GOG",
        "size_original": "Not Found",
        "size_repack": "Not Found",
        "categories": categories,
        "description": rawg.get("description", "") or "Not Found",
        "cover_url": cover_vertical or rawg.get("cover", ""),
        "screenshot_urls": rawg.get("screenshots", []),
        "system_requirements": sysreq or "Not Found",
        "is_published": True,
        "featured": False,
        "trending": False,
    }


# ── Import cycle ─────────────────────────────────────────────────────────

def run_cycle(args) -> int:
    fitgirl_base = env("FITGIRL_API_BASE_URL", DEFAULT_FITGIRL_API).rstrip("/")
    site_url = env("REPACK_X_SITE_URL", DEFAULT_SITE_URL).rstrip("/")
    download_base = env("REPACK_X_DOWNLOAD_BASE", DEFAULT_DOWNLOAD_BASE).rstrip(
        "/"
    )
    admin_secret = env("REPACK_X_ADMIN_SECRET", "") or env("ADMIN_SECRET", "")
    rawg_base = env("RAWG_API_BASE_URL", DEFAULT_RAWG_API).rstrip("/")
    rawg_key = env("RAWG_API_KEY", "")
    gog_base = env("GOG_SCRAPE_API_URL", DEFAULT_GOG_SCRAPE_API).rstrip("/")
    gog_key = env("GOG_SCRAPE_API_KEY", DEFAULT_GOG_SCRAPE_KEY)

    if not admin_secret and not args.dry_run:
        raise RuntimeError("REPACK_X_ADMIN_SECRET is required (or --dry-run).")

    if admin_secret:
        print(
            f"[auth] admin secret loaded: {admin_secret[:8]}…{admin_secret[-4:]} "
            f"(len={len(admin_secret)})"
        )
    else:
        print("[auth] ⚠  NO admin secret set!")

    print("=" * 60)
    print(f"CYCLE START  {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    # 1) Existing slugs ---------------------------------------------------
    existing = fetch_existing_slugs(site_url)
    print(f"[site]  {len(existing)} existing slug(s)")

    fitgirl_new = 0
    gog_new = 0
    delay = max(0.0, args.delay)

    # ═══════════════════════════════════════════════════════════════════
    # 2) FITGIRL — page 1 only, add new releases (NO RAWG / NO GOG)
    #    Title: "[Game Name] (Fitgirl Repack)"  Slug: "fg-{slug}"
    # ═══════════════════════════════════════════════════════════════════
    print("\n── FITGIRL: checking page 1 for new releases ──")
    try:
        page1 = get_json(f"{fitgirl_base}/latest?page=1")
        fg_results = fg_result_items(page1)
        print(f"[fitgirl] page 1: {len(fg_results)} result(s)")
    except RuntimeError as e:
        print(f"[fitgirl] page 1 fetch failed: {e}")
        fg_results = []

    for summary in fg_results:
        if fitgirl_new >= args.fitgirl_limit:
            print(f"[fitgirl] limit ({args.fitgirl_limit}) reached")
            break
        if fg_should_skip(summary):
            continue
        slug = text(summary.get("slug"))
        if not slug:
            continue
        fg_slug = f"fg-{slug}"
        if fg_slug in existing:
            continue

        title = text(summary.get("title")) or slug
        print(
            f"\n  ⬇  [FITGIRL] {title} ({fg_slug}) — new, fetching detail…"
        )

        try:
            detail = get_json(
                f"{fitgirl_base}/repack/{urllib.parse.quote(slug, safe='')}"
            )
        except RuntimeError as e:
            print(f"  ⚠  detail fetch failed: {e}")
            continue

        if not isinstance(detail, dict) or fg_should_skip(detail):
            continue
        exact_slug = text(detail.get("slug") or slug)
        fg_exact = f"fg-{exact_slug}"
        if fg_exact in existing:
            continue
        if not fg_has_downloads(detail):
            print(f"  ⏭  {fg_exact} — no download links")
            continue

        payload = build_fitgirl_payload(summary, detail, download_base)

        if args.dry_run:
            print(
                f"  🔍 [dry-run] {payload['title']} | slug={payload['slug']} "
                f"| dl={payload['download_url']}"
            )
            fitgirl_new += 1
            existing.add(fg_exact)
            continue

        try:
            create_game(site_url, admin_secret, payload)
            existing.add(fg_exact)
            fitgirl_new += 1
            print(
                f"  ✅ [FITGIRL] {payload['title']} → {payload['download_url']}"
            )
        except RuntimeError as e:
            err = str(e)
            if (
                "409" in err
                or "exists" in err.lower()
                or "duplicate" in err.lower()
            ):
                existing.add(fg_exact)
                print(f"  ⏭  {fg_exact} — already on site")
            else:
                print(f"  ⚠  create failed: {e}")
        time.sleep(delay)

    # ═══════════════════════════════════════════════════════════════════
    # 3) GOG — pick games from all-games
    #    Title: "[Game Name] (GOG Repack)"  Slug: "gog-{slug}"
    #    Cover: coverVertical from GOG Catalog API
    #    If already on site as FitGirl → rename FitGirl + add GOG
    # ═══════════════════════════════════════════════════════════════════
    print(f"\n── GOG: importing from all-games (limit: {args.gog_limit}) ──")
    try:
        gog_list = gog_all_games(gog_base, gog_key)
        print(f"[gog] {len(gog_list)} game(s) in all-games list")
    except RuntimeError as e:
        print(f"[gog] all-games fetch failed: {e}")
        gog_list = []

    for gg in gog_list:
        if gog_new >= args.gog_limit:
            break

        gog_slug = text(gg.get("slug"))
        gog_id = gg.get("id")
        gog_title = text(gg.get("title"))

        if not gog_slug or not gog_id:
            continue

        # Check if already added as GOG
        gog_site_slug = f"gog-{gog_slug}"
        if gog_site_slug in existing:
            continue

        # Check if already on site as FitGirl (by normalized slug match)
        normalized = normalize_slug_for_match(gog_slug)
        fg_site_slug = f"fg-{normalized}"
        needs_rename = False
        fg_game_id = None

        if fg_site_slug in existing:
            needs_rename = True
            fg_game = get_site_game_by_slug(site_url, fg_site_slug)
            if fg_game:
                fg_game_id = fg_game.get("id")
            print(
                f"\n  🔄 [GOG] {gog_title} — already on site as "
                f"FitGirl ({fg_site_slug})"
            )

        print(
            f"\n  ⬇  [GOG] {gog_title} (id={gog_id}, slug={gog_site_slug})"
        )

        # Get coverVertical from GOG Catalog API
        cover_vert = get_gog_cover_vertical(
            gog_title, gog_base, gog_key
        )
        time.sleep(delay)

        # RAWG enrichment (2 requests: search + detail)
        rawg = rawg_enrich(gog_title, rawg_base, rawg_key)
        time.sleep(delay)

        # GOG official API — system requirements (1 request)
        sysreq = gog_sysreqs(gog_id)
        time.sleep(delay)

        # Build payload
        payload = build_gog_payload(
            gg, rawg, sysreq, download_base, cover_vert
        )

        if args.dry_run:
            print(
                f"  🔍 [dry-run] {payload['title']} "
                f"| slug={payload['slug']} "
                f"| dl={payload['download_url']}"
            )
            print(
                f"     cover={payload['cover_url'][:70]}  "
                f"shots={len(payload['screenshot_urls'])}"
            )
            if needs_rename:
                print(
                    f"  🔍 [dry-run] would rename: {fg_site_slug} → "
                    f"'{gog_title} (Fitgirl Repack)'"
                )
            gog_new += 1
            existing.add(gog_site_slug)
            continue

        # Rename existing FitGirl entry
        if needs_rename and fg_game_id:
            try:
                rename_game_title(
                    site_url,
                    admin_secret,
                    fg_game_id,
                    f"{gog_title} (Fitgirl Repack)",
                )
                print(
                    f"  🔄 renamed: {fg_site_slug} → "
                    f"'{gog_title} (Fitgirl Repack)'"
                )
            except RuntimeError as e:
                print(
                    f"  ⚠  rename failed (continuing with GOG add): {e}"
                )

        # Create GOG game
        try:
            create_game(site_url, admin_secret, payload)
            existing.add(gog_site_slug)
            gog_new += 1
            print(
                f"  ✅ [GOG] {payload['title']} → {payload['download_url']}"
            )
        except RuntimeError as e:
            err = str(e)
            if (
                "409" in err
                or "exists" in err.lower()
                or "duplicate" in err.lower()
            ):
                existing.add(gog_site_slug)
                print(f"  ⏭  {gog_site_slug} — already on site")
            else:
                print(f"  ⚠  create failed: {e}")
        time.sleep(delay)

    if gog_new == 0 and gog_list:
        missing = sum(
            1
            for gg in gog_list
            if text(gg.get("slug"))
            and f"gog-{text(gg.get('slug'))}" not in existing
        )
        if missing == 0:
            print(
                "[gog] all games imported — waiting for new GOG releases"
            )

    # 4) Status -----------------------------------------------------------
    STATUS.update(
        cycles=STATUS.cycles + 1,
        last_cycle_at=datetime.now(timezone.utc).isoformat(),
        last_cycle_result=f"FitGirl: {fitgirl_new} | GOG: {gog_new}",
        total_fitgirl=STATUS.total_fitgirl + fitgirl_new,
        total_gog=STATUS.total_gog + gog_new,
        total_existing=len(existing),
        last_error=None,
    )
    print(
        f"\nCYCLE END — FitGirl: {fitgirl_new} new | GOG: {gog_new} new | "
        f"total: {len(existing)}"
    )
    return fitgirl_new + gog_new


# ── CLI / main ───────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--interval",
        type=float,
        default=env_float("FITGIRL_IMPORT_INTERVAL", 3600),
    )
    p.add_argument(
        "--delay",
        type=float,
        default=env_float("FITGIRL_IMPORT_DELAY", 1.0),
    )
    p.add_argument(
        "--fitgirl-limit",
        type=int,
        default=env_int("FITGIRL_IMPORT_LIMIT", 5),
    )
    p.add_argument(
        "--gog-limit",
        type=int,
        default=env_int("GOG_IMPORT_LIMIT", 1),
    )
    p.add_argument(
        "--once",
        action="store_true",
        default=env_bool("FITGIRL_IMPORT_ONCE"),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=env_bool("FITGIRL_IMPORT_DRY_RUN"),
    )
    p.add_argument(
        "--health-port", type=int, default=env_int("HEALTH_PORT", 8080)
    )
    p.add_argument(
        "--health-host", default=env("HEALTH_HOST", "0.0.0.0")
    )
    return p.parse_args()


def main():
    args = parse_args()
    if (
        args.interval < 1
        or args.delay < 0
        or args.fitgirl_limit < 1
        or args.gog_limit < 1
    ):
        raise RuntimeError(
            "interval & limits must be positive; delay cannot be negative"
        )

    start_health_server(args.health_host, args.health_port)

    if args.once or args.dry_run:
        return run_cycle(args)

    print("Continuous importer started (FitGirl page 1 + GOG all-games).")
    while True:
        started = time.monotonic()
        try:
            run_cycle(args)
        except Exception as e:
            print(f"Cycle failed: {e}", file=sys.stderr)
            STATUS.update(
                last_error=f"{type(e).__name__}: {e}",
                last_cycle_result="error",
            )
        wait = max(1, args.interval - (time.monotonic() - started))
        print(f"\nNext cycle in {wait:.0f}s.\n", flush=True)
        time.sleep(wait)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1)
