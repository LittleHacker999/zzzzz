#!/usr/bin/env python3
"""Import FitGirl latest releases into the Repack X GitHub JSON catalogue.

The script fetches /latest?page=N, skips non-game digest/upcoming entries, fetches
full details for each release, and upserts records in data/games.json through the
GitHub Contents API.

Required environment variables for a real write:
  GITHUB_CONTENTS_TOKEN   Fine-grained token with Contents: Read and write

Optional environment variables:
  GITHUB_REPO_OWNER       Default: LittleHacker999
  GITHUB_REPO_NAME        Default: musical-guacamole
  GITHUB_REPO_BRANCH      Default: main
  GITHUB_DATA_PATH        Default: data/games.json
  FITGIRL_API_BASE_URL    Default: https://fitgirl-api-5rbz.onrender.com
  REPACK_X_DOWNLOAD_BASE  Default: https://repack-x-database.vercel.app
  FITGIRL_IMPORT_PAGES    Maximum latest pages to scan per cycle. Default: 10
  FITGIRL_IMPORT_LIMIT    Default: 1
  FITGIRL_IMPORT_DELAY     Default: 0.25 seconds
  FITGIRL_IMPORT_INTERVAL  Default: 3600 seconds (1 hour)
  FITGIRL_IMPORT_ONCE      Default: false
  FITGIRL_IMPORT_DRY_RUN   Default: false
  FITGIRL_IMPORT_FORCE     Default: false
  FITGIRL_FETCH_GOG        Default: true; enrich cover, genres, screenshots and system requirements
  GOG_IMPORT_DELAY         Default: 0.25 seconds between GOG requests

Continuous mode imports at most one new release, then waits one hour before
checking again. It runs until interrupted with Ctrl+C.

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
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterable

DEFAULT_FITGIRL_API = "https://fitgirl-api-5rbz.onrender.com"
DEFAULT_DOWNLOAD_BASE = "https://repack-x-database.vercel.app"
DEFAULT_GOG_DELAY = 0.25
IMAGE_URL_RE = re.compile(r"\.(?:jpe?g|png|webp|gif|avif)(?:[?#].*)?$", re.IGNORECASE)
SKIP_RE = re.compile(r"(?:upcoming[- ]repacks?|updates?[- ]digest|digest[- ]updates?)", re.IGNORECASE)


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


def api_json(url: str, *, token: str | None = None, method: str = "GET", payload: dict[str, Any] | None = None) -> tuple[Any, dict[str, str]]:
    body = None
    headers = {
        "Accept": "application/vnd.github+json" if "api.github.com" in url else "application/json",
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
            return json.loads(raw.decode("utf-8")), {k.lower(): v for k, v in response.headers.items()}
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")[:1000]
        raise RuntimeError(f"HTTP {error.code} from {url}: {detail}") from error


def get_with_retries(url: str, retries: int = 3) -> Any:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            return api_json(url)[0]
        except Exception as error:  # network/cold-start errors are retriable
            last_error = error
            if attempt + 1 < retries:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"Could not fetch {url}: {last_error}")


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


def clean_title_for_gog(title: str) -> str:
    """Remove FitGirl/build/DLC noise so GOG search receives the game title."""
    value = re.sub(r"\[.*?\]", " ", title or "")
    value = re.sub(r"\((?:[^)]*(?:repack|build|dlc|patch|update|v\s*\d|fitgirl|dodi)[^)]*)\)", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\b(?:fitgirl|dodi|elamigos|mr\s*dj|xatab|codex|skidrow|reloaded|plaza|empress)\b.*$", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\+\s*(?:\d+|all)?\s*dlcs?\b.*$", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\b(?:v|ver\.?|version|build|patch)\s*[\d._-]+[a-z]?\b.*$", " ", value, flags=re.IGNORECASE)
    return re.sub(r"\s{2,}", " ", re.sub(r"[:\-–—/]", " ", value)).strip()


def title_relevance_score(search_title: str, product_title: str) -> float:
    stop_words = {"the", "and", "for", "with", "all", "edition", "bundle"}
    search_words = [re.sub(r"[^a-z0-9]", "", word) for word in search_title.lower().split()]
    product_words = [re.sub(r"[^a-z0-9]", "", word) for word in product_title.lower().split()]
    search_words = [word for word in search_words if len(word) > 2 and word not in stop_words]
    product_words = [word for word in product_words if len(word) > 2 and word not in stop_words]
    if not search_words:
        return 1.0
    return sum(any(product_word in search_word or search_word in product_word for product_word in product_words) for search_word in search_words) / len(search_words)


def gog_system_requirements(os_list: Any) -> str:
    if not isinstance(os_list, list) or not os_list:
        return ""
    windows = next((item for item in os_list if str(item.get("operatingSystem", {}).get("name", "")).lower() == "windows"), os_list[0])
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
            sections.append(f"{str(group.get('type') or 'Requirement').upper()}:\n" + "\n".join(lines))
    return "\n\n".join(sections).strip()


def fetch_gog_metadata(title: str, delay: float = DEFAULT_GOG_DELAY) -> dict[str, Any]:
    """Fetch optional GOG metadata, returning an empty result on a safe miss."""
    query = clean_title_for_gog(title)
    if not query:
        return {}
    try:
        catalog = get_with_retries(f"https://catalog.gog.com/v1/catalog?query={urllib.parse.quote(query)}&limit=10")
        products = catalog.get("products", []) if isinstance(catalog, dict) else []
        products = [product for product in products if isinstance(product, dict) and title_relevance_score(query, text(product.get("title"))) >= 0.25]
        if not products:
            return {}
        product = next((item for item in products if text(item.get("title")).casefold() == query.casefold()), products[0])
        product_id = product.get("id")
        if not product_id:
            return {}
        time.sleep(max(0, delay))
        detail = get_with_retries(f"https://api.gog.com/v2/games/{urllib.parse.quote(str(product_id), safe='')}")
        screenshots = image_urls([clean_gog_url(url) for url in as_list(product.get("screenshots"))])
        genres = unique_strings([
            *(item.get("name") if isinstance(item, dict) else item for item in as_list(product.get("genres"))),
            *(item.get("name") if isinstance(item, dict) else item for item in as_list(product.get("tags"))),
        ])
        return {
            "cover_url": clean_gog_url(product.get("coverVertical") or product.get("image")),
            "screenshots": screenshots,
            "genres": [genre for genre in genres if genre.casefold() != "lossless repack"],
            "system_requirements": gog_system_requirements(detail.get("_embedded", {}).get("supportedOperatingSystems")),
        }
    except Exception as error:
        print(f"skip GOG enrichment for {title}: {error}")
        return {}


def clean_gog_url(value: Any) -> str:
    url = text(value)
    url = re.sub(r"_{formatter}", "", url)
    return "https://" + url[7:] if url.startswith("http://") else url


def should_skip(item: dict[str, Any]) -> bool:
    title = text(item.get("title"))
    slug = text(item.get("slug"))
    return not slug or bool(SKIP_RE.search(title) or SKIP_RE.search(slug))


def has_download_links(detail: dict[str, Any]) -> bool:
    """Return True when the detail record exposes at least one usable link."""
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

    return any(re.match(r"^(?:https?://|magnet:\?)", value, re.IGNORECASE) for field in link_fields for value in walk(field))


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
    # Stable UUID-shaped identifier without requiring third-party packages.
    digest = hashlib.sha256(f"fitgirl:{slug}".encode("utf-8")).hexdigest()
    return f"{digest[:8]}-{digest[8:12]}-5{digest[13:16]}-{digest[16:20]}-{digest[20:32]}"


def map_game(summary: dict[str, Any], detail: dict[str, Any], existing: dict[str, Any] | None, download_base: str, gog: dict[str, Any] | None = None) -> dict[str, Any]:
    slug = clean_slug(detail.get("slug") or summary.get("slug"))
    categories = unique_strings([
        *as_list(detail.get("genres")),
        *as_list(detail.get("tags")),
        detail.get("category"),
    ])
    categories = [category for category in categories if category.casefold() != "lossless repack"]
    categories = unique_strings([*categories, *((gog or {}).get("genres") or [])])
    repack_size = text(detail.get("repackSize")) or "Not Found"
    repack_size = re.sub(r"\s*\[selective download\]\s*$", "", repack_size, flags=re.IGNORECASE).strip() or "Not Found"
    return {
        "id": (existing or {}).get("id") or deterministic_id(slug),
        "title": text(detail.get("title") or summary.get("title")),
        "slug": slug,
        "categories": categories,
        "size_original": text(detail.get("originalSize")) or "Not Found",
        "size_repack": repack_size,
        "description": description(detail),
        "cover_url": text(detail.get("cover")) or (gog or {}).get("cover_url") or None,
        "screenshots": unique_strings([*image_urls(detail.get("screenshots")), *((gog or {}).get("screenshots") or [])]),
        "system_requirements": (gog or {}).get("system_requirements") or "Not Found",
        "repacker": "Fitgirl",
        "download_url": f"{download_base.rstrip('/')}/fg-{urllib.parse.quote(slug, safe='-')}",
        "is_published": True,
        "featured": bool((existing or {}).get("featured", False)),
        "downloads_count": int((existing or {}).get("downloads_count", 0) or 0),
        "created_at": (existing or {}).get("created_at") or datetime.now(timezone.utc).isoformat(),
    }


def github_file_url(owner: str, repo: str, path: str) -> str:
    encoded = "/".join(urllib.parse.quote(part, safe="") for part in path.split("/"))
    return f"https://api.github.com/repos/{urllib.parse.quote(owner)}/{urllib.parse.quote(repo)}/contents/{encoded}"


def read_catalogue(owner: str, repo: str, branch: str, path: str, token: str) -> tuple[list[dict[str, Any]], str | None]:
    url = github_file_url(owner, repo, path) + "?ref=" + urllib.parse.quote(branch)
    payload, _ = api_json(url, token=token)
    content = base64.b64decode(str(payload.get("content", "")).replace("\n", "")).decode("utf-8")
    parsed = json.loads(content)
    games = parsed if isinstance(parsed, list) else parsed.get("games", [])
    if not isinstance(games, list):
        raise RuntimeError("GitHub catalogue JSON must be an array or an object with a games array.")
    return games, payload.get("sha")


def write_catalogue(owner: str, repo: str, branch: str, path: str, token: str, games: list[dict[str, Any]], sha: str, message: str) -> None:
    raw = (json.dumps(games, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    if len(raw) > 900_000:
        raise RuntimeError(f"Catalogue is {len(raw) / 1024:.0f} KB; stop before the GitHub Contents API 1 MB range.")
    payload = {
        "message": message,
        "content": base64.b64encode(raw).decode("ascii"),
        "branch": branch,
        "sha": sha,
    }
    api_json(github_file_url(owner, repo, path), token=token, method="PUT", payload=payload)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pages", type=int, default=env_int("FITGIRL_IMPORT_PAGES", 10), help="Override FITGIRL_IMPORT_PAGES.")
    parser.add_argument("--limit", type=int, default=env_int("FITGIRL_IMPORT_LIMIT", 1), help="Override FITGIRL_IMPORT_LIMIT. Continuous mode always imports one.")
    parser.add_argument("--delay", type=float, default=env_float("FITGIRL_IMPORT_DELAY", 0.25), help="Override FITGIRL_IMPORT_DELAY.")
    parser.add_argument("--interval", type=float, default=env_float("FITGIRL_IMPORT_INTERVAL", 3600), help="Override FITGIRL_IMPORT_INTERVAL in seconds.")
    parser.add_argument("--once", action="store_true", default=env_bool("FITGIRL_IMPORT_ONCE"), help="Run one cycle and exit.")
    parser.add_argument("--dry-run", action="store_true", default=env_bool("FITGIRL_IMPORT_DRY_RUN"), help="Fetch and print without writing GitHub.")
    parser.add_argument("--force", action="store_true", default=env_bool("FITGIRL_IMPORT_FORCE"), help="Update existing records.")
    return parser.parse_args()


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

    # Production/continuous mode is deliberately capped at one new release
    # per cycle. --limit remains useful for a dry-run or an explicit --once.
    cycle_limit = args.limit if (args.dry_run or args.once) else 1

    # Always allow a page-two fallback. This protects deployments that still
    # have FITGIRL_IMPORT_PAGES=1 from getting stuck on a completed page one.
    page_limit = max(args.pages, 2)
    for page in range(1, page_limit + 1):
        latest = get_with_retries(f"{fitgirl_base}/latest?page={page}")
        results = latest.get("results", []) if isinstance(latest, dict) else latest
        if not isinstance(results, list):
            raise RuntimeError(f"Unexpected /latest response on page {page}: expected results array.")

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
            detail = get_with_retries(f"{fitgirl_base}/repack/{urllib.parse.quote(slug, safe='-')}")
            if not isinstance(detail, dict) or should_skip(detail):
                continue
            if not has_download_links(detail):
                print(f"skip without download links: {slug}")
                seen.add(slug)
                continue
            gog = fetch_gog_metadata(text(detail.get("title") or summary.get("title")), gog_delay) if fetch_gog else {}
            game = map_game(summary, detail, by_slug.get(slug), download_base, gog)
            imported.append(game)
            by_slug[slug] = game
            seen.add(slug)
            print(f"import: {game['title']} -> {game['download_url']}")
            if len(imported) >= cycle_limit:
                break
            time.sleep(max(0, args.delay))
        if len(imported) >= cycle_limit:
            break

    if not imported:
        print("No new FitGirl releases found.")
        return 0

    if args.dry_run:
        print(json.dumps(imported, ensure_ascii=False, indent=2))
        print(f"Dry run complete: {len(imported)} release(s) mapped; GitHub was not changed.")
        return 0

    merged = [game for game in existing if text(game.get("slug")) not in {item["slug"] for item in imported}]
    merged.extend(imported)
    merged.sort(key=lambda game: game.get("created_at", ""), reverse=True)
    write_catalogue(owner, repo, branch, path, token, merged, sha or "", f"Import {len(imported)} FitGirl release(s)")
    print(f"GitHub catalogue updated: {len(imported)} release(s), {len(merged)} total game(s).")
    return 0


def main() -> int:
    args = parse_args()
    if args.pages < 1 or args.limit < 1 or args.interval < 1:
        raise RuntimeError("--pages, --limit, and --interval must be positive.")

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
