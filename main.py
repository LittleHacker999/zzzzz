#!/usr/bin/env python3
"""Import FitGirl latest releases into the Repack X GitHub JSON catalogue.

At startup AND every cycle, the code:
  1. Asks GitHub "how many gamesN.json files exist in data/?" (auto-discovery)
  2. Reads ALL discovered files and merges all games
  3. Checks FitGirl /latest?page=1 for new games
  4. Writes games1.json, games2.json, ... back (auto-split at 850 KB)

Health-check endpoints:
  GET /             → HTML status page
  GET /api/health   → {"working": true}
  GET /api/status   → detailed JSON

Required:
  GITHUB_CONTENTS_TOKEN   Fine-grained token with Contents: Read and write

Optional:
  GITHUB_REPO_OWNER       Default: LittleHacker999
  GITHUB_REPO_NAME        Default: musical-guacamole
  GITHUB_REPO_BRANCH      Default: main
  GITHUB_DATA_DIR         Default: data
  FITGIRL_API_BASE_URL    Default: https://fitgirl-api-5rbz.onrender.com
  REPACK_X_DOWNLOAD_BASE  Default: https://repack-x-database.vercel.app
  FITGIRL_IMPORT_LIMIT    Default: 1
  FITGIRL_IMPORT_DELAY     Default: 0.25
  FITGIRL_IMPORT_INTERVAL  Default: 3600
  FITGIRL_IMPORT_ONCE      Default: false
  FITGIRL_IMPORT_DRY_RUN   Default: false
  FITGIRL_IMPORT_FORCE     Default: false
  FITGIRL_FETCH_GOG        Default: true
  GOG_IMPORT_DELAY         Default: 0.25
  HEALTH_PORT              Default: 8080
  HEALTH_HOST              Default: 0.0.0.0

Examples:
  python scripts/import_fitgirl_latest.py --dry-run
  python scripts/import_fitgirl_latest.py --once
  python scripts/import_fitgirl_latest.py
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
DEFAULT_HEALTH_PORT = 8080
DEFAULT_HEALTH_HOST = "0.0.0.0"
DEFAULT_DATA_DIR = "data"
CHUNK_MAX_BYTES = 850_000

IMAGE_URL_RE = re.compile(r"\.(?:jpe?g|png|webp|gif|avif)(?:[?#].*)?$", re.IGNORECASE)
SKIP_RE = re.compile(r"(?:upcoming[- ]repacks?|updates?[- ]digest|digest[- ]updates?)", re.IGNORECASE)
GAMES_FILE_RE = re.compile(r"^games(\d*)\.json$", re.IGNORECASE)


# ═══════════════════════════════════════════════════════════════════════════
#  SERVICE STATUS
# ═══════════════════════════════════════════════════════════════════════════


class ServiceStatus:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.last_cycle_at = ""
        self.last_cycle_result = ""
        self.total_imported = 0
        self.last_error = ""
        self.games_files: list[str] = []
        self.total_games = 0

    def update_cycle(self, result: str, error: str = "") -> None:
        with self._lock:
            self.last_cycle_at = datetime.now(timezone.utc).isoformat()
            self.last_cycle_result = result
            self.last_error = error

    def increment_imported(self, count: int = 1) -> None:
        with self._lock:
            self.total_imported += count

    def update_files(self, files: list[str], total: int) -> None:
        with self._lock:
            self.games_files = list(files)
            self.total_games = total

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "working": True,
                "started_at": self.started_at,
                "last_cycle_at": self.last_cycle_at,
                "last_cycle_result": self.last_cycle_result,
                "total_imported": self.total_imported,
                "last_error": self.last_error,
                "games_files": list(self.games_files),
                "total_games": self.total_games,
            }


STATUS = ServiceStatus()


# ═══════════════════════════════════════════════════════════════════════════
#  HEALTH-CHECK WEB SERVER
# ═══════════════════════════════════════════════════════════════════════════


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        path = self.path.split("?")[0].rstrip("/") or "/"
        if path == "/":
            self._send_html()
        elif path == "/api/health":
            self._send_json({"working": True})
        elif path == "/api/status":
            self._send_json(STATUS.snapshot())
        else:
            self._send_json({"error": "Not found"}, 404)

    def _send_json(self, data: dict[str, Any], code: int = 200) -> None:
        body = json.dumps(data, indent=2).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self) -> None:
        s = STATUS.snapshot()
        err = (
            f'<span class="badge error">{s["last_error"]}</span>'
            if s["last_error"]
            else '<span class="badge ok">No errors</span>'
        )
        files = ", ".join(s["games_files"]) or "None yet"
        html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>FitGirl Importer</title><style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Segoe UI',sans-serif;background:#0d1117;color:#c9d1d9;
display:flex;justify-content:center;align-items:center;min-height:100vh}}
.card{{background:#161b22;border:1px solid #30363d;border-radius:12px;
padding:40px 48px;max-width:580px;width:90%}}
h1{{color:#58a6ff;font-size:1.6rem;margin-bottom:6px}}
.sub{{color:#8b949e;margin-bottom:28px;font-size:.95rem}}
.row{{display:flex;justify-content:space-between;padding:10px 0;border-bottom:1px solid #21262d}}
.row:last-child{{border-bottom:none}}
.lab{{color:#8b949e;font-size:.88rem}}
.val{{color:#c9d1d9;font-size:.88rem;font-weight:600;text-align:right;max-width:300px;word-break:break-word}}
.badge{{padding:3px 10px;border-radius:20px;font-size:.82rem;font-weight:600}}
.badge.ok{{background:#1a3c2a;color:#3fb950}}
.badge.error{{background:#3c1a1a;color:#f85149}}
.pulse{{display:inline-block;width:10px;height:10px;background:#3fb950;
border-radius:50%;margin-right:6px;animation:p 1.5s ease-in-out infinite}}
@keyframes p{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}
.links{{margin-top:24px;padding-top:16px;border-top:1px solid #21262d}}
.links a{{color:#58a6ff;text-decoration:none;display:block;padding:4px 0}}
</style></head><body><div class="card">
<h1><span class="pulse"></span>FitGirl Importer</h1>
<p class="sub">Health-check service</p>
<div class="row"><span class="lab">Status</span><span class="val"><span class="badge ok">Working</span></span></div>
<div class="row"><span class="lab">Started</span><span class="val">{s["started_at"]}</span></div>
<div class="row"><span class="lab">Last cycle</span><span class="val">{s["last_cycle_at"] or "Not yet"}</span></div>
<div class="row"><span class="lab">Last result</span><span class="val">{s["last_cycle_result"] or "\u2014"}</span></div>
<div class="row"><span class="lab">Total imported</span><span class="val">{s["total_imported"]}</span></div>
<div class="row"><span class="lab">Total games</span><span class="val">{s["total_games"]}</span></div>
<div class="row"><span class="lab">Game files</span><span class="val">{files}</span></div>
<div class="row"><span class="lab">Errors</span><span class="val">{err}</span></div>
<div class="links"><a href="/api/health">/api/health</a><a href="/api/status">/api/status</a></div>
</div></body></html>"""
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a: Any) -> None:
        pass


def start_health_server(host: str, port: int) -> None:
    server = HTTPServer((host, port), HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    print(f"[health] http://{host}:{port}")


# ═══════════════════════════════════════════════════════════════════════════
#  ENV HELPERS
# ═══════════════════════════════════════════════════════════════════════════


def env(name: str, default: str) -> str:
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
    raise RuntimeError(f"{name} must be bool, got {v!r}")


# ═══════════════════════════════════════════════════════════════════════════
#  HTTP LAYER
# ═══════════════════════════════════════════════════════════════════════════


def fetch_json(url: str, headers: dict[str, str] | None = None, timeout: int = 90) -> Any:
    hdr = {"Accept": "application/json", "User-Agent": "Repack-X-Importer/1.0"}
    if headers:
        hdr.update(headers)
    req = urllib.request.Request(url, headers=hdr)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        d = e.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"HTTP {e.code} from {url}: {d}") from e


def get_with_retries(url: str, retries: int = 3) -> Any:
    last: Exception | None = None
    for i in range(retries):
        try:
            return fetch_json(url)
        except Exception as e:
            last = e
            if i + 1 < retries:
                time.sleep(2**i)
    raise RuntimeError(f"Could not fetch {url}: {last}")


# ═══════════════════════════════════════════════════════════════════════════
#  GITHUB API HELPERS
# ═══════════════════════════════════════════════════════════════════════════


def gh_headers() -> dict[str, str]:
    token = os.environ.get("GITHUB_CONTENTS_TOKEN", "").strip()
    h = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "Repack-X-Importer/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def gh_url(owner: str, repo: str, path: str) -> str:
    parts = "/".join(urllib.parse.quote(p, safe="") for p in path.strip("/").split("/"))
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{parts}"


def gh_request(url: str, method: str = "GET", payload: dict[str, Any] | None = None) -> Any:
    body = json.dumps(payload).encode() if payload else None
    hdr = gh_headers()
    if body:
        hdr["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=hdr, method=method)
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        d = e.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"HTTP {e.code} from {url}: {d}") from e


# ═══════════════════════════════════════════════════════════════════════════
#  ★ FILE DISCOVERY: Find all gamesN.json in data/ folder ★
# ═══════════════════════════════════════════════════════════════════════════


def discover_games_files(
    owner: str, repo: str, branch: str, data_dir: str
) -> list[dict[str, Any]]:
    """
    ★ Asks GitHub: "How many gamesN.json files exist in {data_dir}/?" ★

    Single API call:
      GET /repos/{owner}/{repo}/contents/{data_dir}?ref={branch}

    Scans every item, matches games*.json pattern, returns sorted by number.

    Returns:
      [
        {"name": "games1.json", "path": "data/games1.json", "sha": "abc", "number": 1},
        {"name": "games2.json", "path": "data/games2.json", "sha": "def", "number": 2},
        {"name": "games3.json", "path": "data/games3.json", "sha": "ghi", "number": 3},
      ]
    """
    url = gh_url(owner, repo, data_dir) + "?ref=" + urllib.parse.quote(branch)

    print(f"[discovery] Asking GitHub: what files exist in '{data_dir}/'?")
    print(f"[discovery] GET {url}")

    payload = gh_request(url)

    if not isinstance(payload, list):
        print(f"[discovery] WARNING: expected a list, got {type(payload).__name__}")
        return []

    print(f"[discovery] GitHub returned {len(payload)} item(s) in '{data_dir}/':")
    for item in payload:
        item_type = item.get("type", "?")
        item_name = item.get("name", "?")
        marker = "📄" if item_type == "file" else "📁"
        print(f"[discovery]   {marker} {item_name} ({item_type})")

    # Now filter: only games*.json files
    found: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict) or item.get("type") != "file":
            continue
        name = item.get("name", "")
        match = GAMES_FILE_RE.match(name)
        if match:
            num = int(match.group(1)) if match.group(1) else 0
            found.append({
                "name": name,
                "path": item.get("path", f"{data_dir}/{name}"),
                "sha": item.get("sha", ""),
                "number": num,
            })
            print(f"[discovery]   ✅ MATCH: {name} (number={num})")
        else:
            print(f"[discovery]   ❌ skip: {name} (not games*.json)")

    found.sort(key=lambda x: x["number"])

    print(f"[discovery] ═══════════════════════════════════════")
    print(f"[discovery] FOUND {len(found)} games file(s):")
    for f in found:
        print(f"[discovery]   {f['number']}. {f['name']}  sha={f['sha'][:8]}...")
    print(f"[discovery] ═══════════════════════════════════════")

    return found


# ═══════════════════════════════════════════════════════════════════════════
#  READ ALL gamesN.json
# ═══════════════════════════════════════════════════════════════════════════


def read_catalogue(
    owner: str, repo: str, branch: str, data_dir: str
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    files = discover_games_files(owner, repo, branch, data_dir)

    games: list[dict[str, Any]] = []
    file_info: dict[str, dict[str, Any]] = {}

    for fi in files:
        url = gh_url(owner, repo, fi["path"]) + "?ref=" + urllib.parse.quote(branch)
        try:
            payload = gh_request(url)
        except RuntimeError as e:
            if "404" in str(e):
                print(f"[read]   {fi['name']}: 404, skipping")
                continue
            raise

        file_info[fi["path"]] = {"sha": payload.get("sha", fi.get("sha", "")), "name": fi["name"]}

        try:
            content = base64.b64decode(
                str(payload.get("content", "")).replace("\n", "")
            ).decode("utf-8")
            parsed = json.loads(content)
        except Exception:
            print(f"[read]   {fi['name']}: invalid JSON, skipping")
            continue

        if isinstance(parsed, list):
            games.extend(parsed)
            print(f"[read]   {fi['name']}: loaded {len(parsed)} game(s)")
        elif isinstance(parsed, dict) and isinstance(parsed.get("games"), list):
            games.extend(parsed["games"])
            print(f"[read]   {fi['name']}: loaded {len(parsed['games'])} game(s)")
        else:
            print(f"[read]   {fi['name']}: unknown format, skipping")

    print(f"[read] TOTAL: {len(games)} game(s) from {len(file_info)} file(s)")
    return games, file_info


# ═══════════════════════════════════════════════════════════════════════════
#  WRITE CATALOGUE
# ═══════════════════════════════════════════════════════════════════════════


def _split_chunks(games: list[dict[str, Any]], max_bytes: int) -> list[list[dict[str, Any]]]:
    chunks: list[list[dict[str, Any]]] = []
    cur: list[dict[str, Any]] = []
    size = 4
    for g in games:
        raw = json.dumps(g, ensure_ascii=False, indent=2)
        lines = raw.count("\n") + 1
        item = len(raw.encode("utf-8")) + lines * 2 + 2
        if cur and size + item > max_bytes:
            chunks.append(cur)
            cur = [g]
            size = 4 + item
        else:
            cur.append(g)
            size += item
    if cur:
        chunks.append(cur)
    return chunks or [[]]


def write_catalogue(
    owner: str, repo: str, branch: str, data_dir: str,
    games: list[dict[str, Any]], file_info: dict[str, dict[str, Any]], message: str,
) -> None:
    chunks = _split_chunks(games, CHUNK_MAX_BYTES)
    total_kb = len(json.dumps(games, ensure_ascii=False, indent=2).encode()) / 1024
    print(f"[write] {len(games)} game(s) -> {len(chunks)} file(s), total {total_kb:.0f} KB")

    new_paths: set[str] = set()
    for i, chunk in enumerate(chunks):
        name = f"games{i + 1}.json"
        path = f"{data_dir}/{name}"
        raw = json.dumps(chunk, ensure_ascii=False, indent=2) + "\n"
        sz = len(raw.encode("utf-8"))
        if sz > 1_000_000:
            raise RuntimeError(f"{name} is {sz / 1024:.0f} KB; exceeds 1 MB")

        sha = file_info.get(path, {}).get("sha", "")
        data = {"message": message, "content": base64.b64encode(raw.encode()).decode("ascii"), "branch": branch}
        if sha:
            data["sha"] = sha
        gh_request(gh_url(owner, repo, path), method="PUT", payload=data)
        new_paths.add(path)
        print(f"[write]   {name}: {len(chunk)} game(s), {sz / 1024:.0f} KB")

    for path, info in file_info.items():
        if path not in new_paths:
            sha = info.get("sha")
            if sha:
                try:
                    gh_request(gh_url(owner, repo, path), method="DELETE", payload={
                        "message": f"Remove orphaned {info.get('name', path)}",
                        "sha": sha, "branch": branch,
                    })
                    print(f"[write]   removed {info.get('name', path)}")
                except Exception:
                    pass

    print(f"[write] done: {len(new_paths)} file(s)")


# ═══════════════════════════════════════════════════════════════════════════
#  DATA-CLEANING HELPERS
# ═══════════════════════════════════════════════════════════════════════════


def as_list(v: Any) -> list[Any]:
    if v is None:
        return []
    return v if isinstance(v, list) else [v]


def text(v: Any) -> str:
    if isinstance(v, dict):
        for k in ("name", "title", "text", "url"):
            if v.get(k):
                return str(v[k]).strip()
        return ""
    return str(v).strip() if v is not None else ""


def unique_strings(vals: Iterable[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for v in vals:
        s = text(v)
        if s and s.casefold() not in seen:
            seen.add(s.casefold())
            out.append(s)
    return out


def nested_urls(v: Any) -> Iterable[str]:
    if isinstance(v, str):
        yield v.strip()
    elif isinstance(v, list):
        for i in v:
            yield from nested_urls(i)
    elif isinstance(v, dict):
        for k in ("url", "image", "src", "thumbnail"):
            if v.get(k):
                yield from nested_urls(v[k])


def image_urls(v: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for u in nested_urls(v):
        if u and re.match(r"^https?://", u, re.IGNORECASE) and IMAGE_URL_RE.search(u) and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def clean_slug(v: Any) -> str:
    return re.sub(r"[^a-z0-9-]", "", text(v).lower())


# ═══════════════════════════════════════════════════════════════════════════
#  GOG ENRICHMENT
# ═══════════════════════════════════════════════════════════════════════════


def clean_title_for_gog(title: str) -> str:
    v = re.sub(r"\[.*?\]", " ", title or "")
    v = re.sub(r"\((?:[^)]*(?:repack|build|dlc|patch|update|v\s*\d|fitgirl|dodi)[^)]*)\)", " ", v, flags=re.IGNORECASE)
    v = re.sub(r"\b(?:fitgirl|dodi|elamigos|mr\s*dj|xatab|codex|skidrow|reloaded|plaza|empress)\b.*$", " ", v, flags=re.IGNORECASE)
    v = re.sub(r"\+\s*(?:\d+|all)?\s*dlcs?\b.*$", " ", v, flags=re.IGNORECASE)
    v = re.sub(r"\b(?:v|ver\.?|version|build|patch)\s*[\d._-]+[a-z]?\b.*$", " ", v, flags=re.IGNORECASE)
    return re.sub(r"\s{2,}", " ", re.sub(r"[:\-–—/]", " ", v)).strip()


def _title_score(s: str, p: str) -> float:
    sw = {"the", "and", "for", "with", "all", "edition", "bundle"}
    a = [re.sub(r"[^a-z0-9]", "", w) for w in s.lower().split()]
    b = [re.sub(r"[^a-z0-9]", "", w) for w in p.lower().split()]
    a = [w for w in a if len(w) > 2 and w not in sw]
    b = [w for w in b if len(w) > 2 and w not in sw]
    if not a:
        return 1.0
    return sum(any(x in y or y in x for x in b) for y in a) / len(a)


def _gog_sysreq(os_list: Any) -> str:
    if not isinstance(os_list, list) or not os_list:
        return ""
    win = next(
        (i for i in os_list if str(i.get("operatingSystem", {}).get("name", "")).lower() == "windows"),
        os_list[0],
    )
    groups = win.get("systemRequirements") if isinstance(win, dict) else None
    if not isinstance(groups, list):
        return ""
    sections = []
    for g in groups:
        if not isinstance(g, dict):
            continue
        lines = []
        for r in g.get("requirements", []):
            if not isinstance(r, dict):
                continue
            n = str(r.get("name") or r.get("id") or "").rstrip(":").strip()
            d = str(r.get("description") or "").strip()
            if n and d:
                lines.append(f"{n}: {d}")
            elif d:
                lines.append(d)
        if g.get("description"):
            lines.append(str(g["description"]).strip())
        if lines:
            sections.append(f"{str(g.get('type') or 'Req').upper()}:\n" + "\n".join(lines))
    return "\n\n".join(sections).strip()


def _clean_gog_url(v: Any) -> str:
    u = text(v)
    u = re.sub(r"\{formatter\}", "", u)
    return "https://" + u[7:] if u.startswith("http://") else u


def fetch_gog_metadata(title: str, delay: float = DEFAULT_GOG_DELAY) -> dict[str, Any]:
    q = clean_title_for_gog(title)
    if not q:
        return {}
    try:
        cat = get_with_retries(f"https://catalog.gog.com/v1/catalog?query={urllib.parse.quote(q)}&limit=10")
        products = [p for p in (cat.get("products", []) if isinstance(cat, dict) else []) if isinstance(p, dict) and _title_score(q, text(p.get("title"))) >= 0.25]
        if not products:
            return {}
        prod = next((i for i in products if text(i.get("title")).casefold() == q.casefold()), products[0])
        pid = prod.get("id")
        if not pid:
            return {}
        time.sleep(max(0, delay))
        det = get_with_retries(f"https://api.gog.com/v2/games/{urllib.parse.quote(str(pid), safe='')}")
        return {
            "cover_url": _clean_gog_url(prod.get("coverVertical") or prod.get("image")),
            "screenshots": image_urls([_clean_gog_url(u) for u in as_list(prod.get("screenshots"))]),
            "genres": [g for g in unique_strings([
                *(i.get("name") if isinstance(i, dict) else i for i in as_list(prod.get("genres"))),
                *(i.get("name") if isinstance(i, dict) else i for i in as_list(prod.get("tags"))),
            ]) if g.casefold() != "lossless repack"],
            "system_requirements": _gog_sysreq(det.get("_embedded", {}).get("supportedOperatingSystems")),
        }
    except Exception as e:
        print(f"[gog] skip for {title}: {e}")
        return {}


# ═══════════════════════════════════════════════════════════════════════════
#  RECORD HELPERS
# ═══════════════════════════════════════════════════════════════════════════


def should_skip(item: dict[str, Any]) -> bool:
    t = text(item.get("title"))
    s = text(item.get("slug"))
    return not s or bool(SKIP_RE.search(t) or SKIP_RE.search(s))


def has_download_links(detail: dict[str, Any]) -> bool:
    def walk(v: Any) -> Iterable[str]:
        if isinstance(v, str):
            if v.strip():
                yield v.strip()
        elif isinstance(v, list):
            for i in v:
                yield from walk(i)
        elif isinstance(v, dict):
            for i in v.values():
                yield from walk(i)
    return any(
        re.match(r"^(?:https?://|magnet:\?)", v, re.IGNORECASE)
        for f in (detail.get("downloadMirrors"), detail.get("torrents"))
        for v in walk(f)
    )


def description(detail: dict[str, Any]) -> str:
    p = unique_strings(as_list(detail.get("gameDescription")))
    f = unique_strings(as_list(detail.get("repackFeatures")))
    sec: list[str] = []
    if p:
        sec.append("\n\n".join(p))
    if f:
        sec.append("**Repack Features**\n\n" + "\n".join(f"- {i}" for i in f))
    return "\n\n".join(sec) or "Not Found"


def deterministic_id(slug: str) -> str:
    d = hashlib.sha256(f"fitgirl:{slug}".encode()).hexdigest()
    return f"{d[:8]}-{d[8:12]}-5{d[13:16]}-{d[16:20]}-{d[20:32]}"


def map_game(
    summary: dict[str, Any], detail: dict[str, Any],
    existing: dict[str, Any] | None, dl_base: str,
    gog: dict[str, Any] | None = None,
) -> dict[str, Any]:
    slug = clean_slug(detail.get("slug") or summary.get("slug"))
    cats = unique_strings([*as_list(detail.get("genres")), *as_list(detail.get("tags")), detail.get("category")])
    cats = [c for c in cats if c.casefold() != "lossless repack"]
    cats = unique_strings([*cats, *((gog or {}).get("genres") or [])])
    rs = text(detail.get("repackSize")) or "Not Found"
    rs = re.sub(r"\s*\[selective download\]\s*$", "", rs, flags=re.IGNORECASE).strip() or "Not Found"
    return {
        "id": (existing or {}).get("id") or deterministic_id(slug),
        "title": text(detail.get("title") or summary.get("title")),
        "slug": slug,
        "categories": cats,
        "size_original": text(detail.get("originalSize")) or "Not Found",
        "size_repack": rs,
        "description": description(detail),
        "cover_url": text(detail.get("cover")) or (gog or {}).get("cover_url") or None,
        "screenshots": unique_strings([*image_urls(detail.get("screenshots")), *((gog or {}).get("screenshots") or [])]),
        "system_requirements": (gog or {}).get("system_requirements") or "Not Found",
        "repacker": "Fitgirl",
        "download_url": f"{dl_base.rstrip('/')}/fg-{urllib.parse.quote(slug, safe='-')}",
        "is_published": True,
        "featured": bool((existing or {}).get("featured", False)),
        "downloads_count": int((existing or {}).get("downloads_count", 0) or 0),
        "created_at": (existing or {}).get("created_at") or datetime.now(timezone.utc).isoformat(),
    }


# ═══════════════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════════════


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--limit", type=int, default=env_int("FITGIRL_IMPORT_LIMIT", 1))
    p.add_argument("--delay", type=float, default=env_float("FITGIRL_IMPORT_DELAY", 0.25))
    p.add_argument("--interval", type=float, default=env_float("FITGIRL_IMPORT_INTERVAL", 3600))
    p.add_argument("--once", action="store_true", default=env_bool("FITGIRL_IMPORT_ONCE"))
    p.add_argument("--dry-run", action="store_true", default=env_bool("FITGIRL_IMPORT_DRY_RUN"))
    p.add_argument("--force", action="store_true", default=env_bool("FITGIRL_IMPORT_FORCE"))
    p.add_argument("--health-port", type=int, default=env_int("HEALTH_PORT", DEFAULT_HEALTH_PORT))
    p.add_argument("--health-host", type=str, default=env("HEALTH_HOST", DEFAULT_HEALTH_HOST))
    return p.parse_args()


# ═══════════════════════════════════════════════════════════════════════════
#  CORE IMPORT CYCLE
# ═══════════════════════════════════════════════════════════════════════════


def run_cycle(args: argparse.Namespace) -> int:
    fg_base = env("FITGIRL_API_BASE_URL", DEFAULT_FITGIRL_API).rstrip("/")
    dl_base = env("REPACK_X_DOWNLOAD_BASE", DEFAULT_DOWNLOAD_BASE)
    owner = env("GITHUB_REPO_OWNER", "LittleHacker999")
    repo = env("GITHUB_REPO_NAME", "musical-guacamole")
    branch = env("GITHUB_REPO_BRANCH", "main")
    data_dir = env("GITHUB_DATA_DIR", DEFAULT_DATA_DIR)
    token = os.environ.get("GITHUB_CONTENTS_TOKEN", "").strip()
    fetch_gog = env_bool("FITGIRL_FETCH_GOG", True)
    gog_delay = env_float("GOG_IMPORT_DELAY", DEFAULT_GOG_DELAY)

    if not token:
        raise RuntimeError("GITHUB_CONTENTS_TOKEN is required.")

    print(f"\n{'=' * 60}")
    print(f"CYCLE START at {datetime.now(timezone.utc).isoformat()}")
    print(f"{'=' * 60}")

    # ── 1. DISCOVER files: How many gamesN.json exist in data/? ────────
    existing, file_info = read_catalogue(owner, repo, branch, data_dir)
    file_names = [fi["name"] for fi in file_info.values()]
    STATUS.update_files(file_names, len(existing))

    by_slug = {text(g.get("slug")): g for g in existing if text(g.get("slug"))}
    imported: list[dict[str, Any]] = []
    cycle_limit = args.limit if (args.dry_run or args.once) else 1

    # ── 2. Fetch ONLY page 1 from FitGirl ─────────────────────────────
    page = get_with_retries(f"{fg_base}/latest?page=1")
    results = page.get("results", []) if isinstance(page, dict) else page
    if not isinstance(results, list):
        raise RuntimeError("Unexpected /latest response.")

    print(f"[scan] FitGirl page 1: {len(results)} result(s), checking for new...")

    # ── 3. Check each game ─────────────────────────────────────────────
    for summary in results:
        if not isinstance(summary, dict) or should_skip(summary):
            continue
        slug = clean_slug(summary.get("slug"))
        if not slug:
            continue
        if not args.force and slug in by_slug:
            print(f"  skip existing: {slug}")
            continue

        detail = get_with_retries(f"{fg_base}/repack/{urllib.parse.quote(slug, safe='-')}")
        if not isinstance(detail, dict) or should_skip(detail):
            continue
        if not has_download_links(detail):
            print(f"  skip no links: {slug}")
            continue

        gog = fetch_gog_metadata(text(detail.get("title") or summary.get("title")), gog_delay) if fetch_gog else {}
        game = map_game(summary, detail, by_slug.get(slug), dl_base, gog)
        imported.append(game)
        by_slug[slug] = game
        print(f"  ★ NEW: {game['title']} -> {game['download_url']}")

        if len(imported) >= cycle_limit:
            break
        time.sleep(max(0, args.delay))

    # ── 4. No new ──────────────────────────────────────────────────────
    if not imported:
        print("No new games on page 1.")
        STATUS.update_cycle("no new releases")
        return 0

    # ── 5. Dry run ─────────────────────────────────────────────────────
    if args.dry_run:
        print(json.dumps(imported, ensure_ascii=False, indent=2))
        print(f"Dry run: {len(imported)} mapped. GitHub unchanged.")
        STATUS.update_cycle(f"dry run: {len(imported)}")
        return 0

    # ── 6. Merge and write ─────────────────────────────────────────────
    merged = [
        g for g in existing
        if text(g.get("slug")) not in {item["slug"] for item in imported}
    ]
    merged.extend(imported)
    merged.sort(key=lambda g: g.get("created_at", ""), reverse=True)

    write_catalogue(owner, repo, branch, data_dir, merged, file_info,
                    f"Import {len(imported)} FitGirl release(s)")

    print(f"Updated: {len(imported)} new, {len(merged)} total.")
    STATUS.increment_imported(len(imported))
    STATUS.update_cycle(f"imported {len(imported)} release(s)")
    return 0


# ═══════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════


def main() -> int:
    args = parse_args()
    if args.limit < 1 or args.interval < 1:
        raise RuntimeError("--limit and --interval must be positive.")

    try:
        start_health_server(args.health_host, args.health_port)
    except OSError as e:
        print(f"[health] error: {e}", file=sys.stderr)

    if args.dry_run or args.once:
        return run_cycle(args)

    print("Continuous mode: 1 new game per hour from page 1.")
    while True:
        t0 = time.monotonic()
        try:
            run_cycle(args)
        except Exception as e:
            print(f"Cycle failed: {e}", file=sys.stderr)
            STATUS.update_cycle("failed", error=str(e))
        wait = max(1, args.interval - (time.monotonic() - t0))
        print(f"Next check in {wait:.0f}s.", flush=True)
        time.sleep(wait)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1)
