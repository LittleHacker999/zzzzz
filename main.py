#!/usr/bin/env python3
"""Continuously import FitGirl releases into a GitHub gamesN.json catalogue.

Each cycle:
  1. Clones the catalogue repository into a temporary directory.
  2. Finds every data/gamesN.json file, including intentionally empty files.
  3. Reads .fg_import_state.json for the last scanned page and API lastPage.
  4. Fetches the current page and page one to catch both scheduled and new releases.
  5. Imports eligible releases into the first gamesN.json file with capacity.
  6. Advances the scan state and writes .fg_import_state.json.

"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

DEFAULT_FITGIRL_API = "https://fitgirl-api-5rbz.onrender.com"
DEFAULT_DOWNLOAD_BASE = "https://repack-x-database.vercel.app"
DEFAULT_MAX_BYTES = 900_000
IMAGE_URL_RE = re.compile(r"\.(?:jpe?g|png|webp|gif|avif)(?:[?#].*)?$", re.IGNORECASE)
SKIP_RE = re.compile(r"(?:upcoming[- ]repacks?|updates?[- ]digest|digest[- ]updates?)", re.IGNORECASE)
GAMES_RE = re.compile(r"^games(?P<index>\d*)\.json$", re.IGNORECASE)


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
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {
        "Accept": "application/vnd.github+json" if "api.github.com" in url else "application/json",
        "User-Agent": "Repack-X-FitGirl-Importer/2.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
        headers["X-GitHub-Api-Version"] = "2022-11-28"
    if payload is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            return json.loads(response.read().decode("utf-8")), {k.lower(): v for k, v in response.headers.items()}
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


def text(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("name", "title", "text", "url"):
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


def clean_slug(value: Any) -> str:
    # Kept only for legacy callers. Imported records preserve the API slug exactly.
    return re.sub(r"[^a-z0-9-]", "", text(value).lower())


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

    fields = (detail.get("downloadMirrors"), detail.get("torrents"), detail.get("magnets"), detail.get("mirrors"))
    return any(re.match(r"^(?:https?://|magnet:\?)", value, re.IGNORECASE) for field in fields for value in walk(field))


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
    return f"{digest[:8]}-{digest[8:12]}-5{digest[13:16]}-{digest[16:20]}-{digest[20:32]}"


def map_game(summary: dict[str, Any], detail: dict[str, Any], existing: dict[str, Any] | None, download_base: str) -> dict[str, Any]:
    # The API slug is canonical and must not be normalized or changed.
    slug = text(detail.get("slug") or summary.get("slug"))
    categories = [category for category in unique_strings([
        *as_list(detail.get("genres")),
        *as_list(detail.get("tags")),
        detail.get("category"),
    ]) if category.casefold() != "lossless repack"]
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
        "cover_url": text(detail.get("cover")) or None,
        "screenshots": image_urls(detail.get("screenshots")),
        "system_requirements": "Not Found",
        "repacker": "Fitgirl",
        "download_url": f"{download_base.rstrip('/')}/fg-{urllib.parse.quote(slug, safe='')}",
        "is_published": True,
        "featured": bool((existing or {}).get("featured", False)),
        "downloads_count": int((existing or {}).get("downloads_count", 0) or 0),
        "created_at": (existing or {}).get("created_at") or datetime.now(timezone.utc).isoformat(),
    }


def github_file_url(owner: str, repo: str, path: str) -> str:
    encoded = "/".join(urllib.parse.quote(part, safe="") for part in path.split("/"))
    return f"https://api.github.com/repos/{urllib.parse.quote(owner)}/{urllib.parse.quote(repo)}/contents/{encoded}"


def parse_json_bytes(raw: bytes, path: str) -> list[dict[str, Any]]:
    if not raw.strip():
        return []
    try:
        data = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as error:
        raise RuntimeError(f"Invalid JSON in {path}: {error}") from error
    games = data if isinstance(data, list) else data.get("games", []) if isinstance(data, dict) else None
    if not isinstance(games, list):
        raise RuntimeError(f"{path} must contain a JSON array or an object with a games array.")
    return [game for game in games if isinstance(game, dict)]


def clone_repository(owner: str, repo: str, branch: str) -> Path:
    root = Path(tempfile.mkdtemp(prefix="repackx-fitgirl-"))
    destination = root / "repo"
    url = f"https://github.com/{urllib.parse.quote(owner)}/{urllib.parse.quote(repo)}.git"
    command = ["git", "clone", "--depth", "1", "--branch", branch, url, str(destination)]
    try:
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, timeout=180)
    except Exception:
        shutil.rmtree(root, ignore_errors=True)
        raise
    return destination


def discover_game_files(repo_root: Path, data_path: str) -> list[tuple[int, str, Path]]:
    data_dir = repo_root / Path(data_path).parent
    if not data_dir.exists():
        raise RuntimeError(f"Catalogue directory does not exist in cloned repository: {data_dir}")
    files: list[tuple[int, str, Path]] = []
    for path in data_dir.iterdir():
        match = GAMES_RE.fullmatch(path.name)
        if match and path.is_file():
            index = int(match.group("index") or 0)
            files.append((index, path.name, path))
    if not files:
        raise RuntimeError(f"No gamesN.json files found in {data_dir}")
    return sorted(files, key=lambda item: item[0])


def local_state_path() -> Path:
    configured = os.environ.get("FITGIRL_STATE_PATH", "").strip()
    if configured:
        path = Path(configured).expanduser()
        return path if path.is_absolute() else Path(__file__).resolve().parent / path
    return Path(__file__).resolve().parent / ".fg_import_state.json"


def read_state(file_path: Path) -> dict[str, Any]:
    if not file_path.exists() or not file_path.read_bytes().strip():
        return {"lastScannedPage": 1, "lastPage": 1}
    try:
        value = json.loads(file_path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {"lastScannedPage": 1, "lastPage": 1}
    except json.JSONDecodeError:
        print(f"Warning: invalid {file_path}; starting from page 1", file=sys.stderr)
        return {"lastScannedPage": 1, "lastPage": 1}


def write_state(file_path: Path, state: dict[str, Any]) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = file_path.with_suffix(file_path.suffix + ".tmp")
    temporary.write_bytes(json_bytes(state))
    temporary.replace(file_path)


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
    raise RuntimeError("Unexpected FitGirl /latest response: no results array found.")


def read_github_file(owner: str, repo: str, branch: str, path: str, token: str) -> tuple[bytes, str | None]:
    try:
        payload, _ = api_json(github_file_url(owner, repo, path) + "?ref=" + urllib.parse.quote(branch), token=token)
    except RuntimeError as error:
        if str(error).startswith("HTTP 404"):
            return b"", None
        raise
    content = base64.b64decode(str(payload.get("content", "")).replace("\n", ""))
    return content, payload.get("sha")


def write_github_file(owner: str, repo: str, branch: str, path: str, token: str, raw: bytes, sha: str | None, message: str) -> None:
    if len(raw) > DEFAULT_MAX_BYTES:
        raise RuntimeError(f"{path} is {len(raw) / 1024:.0f} KB; it exceeds the safe 900 KB file limit.")
    payload: dict[str, Any] = {
        "message": message,
        "content": base64.b64encode(raw).decode("ascii"),
        "branch": branch,
    }
    if sha:
        payload["sha"] = sha
    api_json(github_file_url(owner, repo, path), token=token, method="PUT", payload=payload)


def json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def choose_target(files: list[tuple[int, str, Path]], records_by_file: dict[str, list[dict[str, Any]]], new_game: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    for _, name, path in files:
        records = records_by_file[name]
        candidate = [*records, new_game]
        if len(json_bytes(candidate)) <= DEFAULT_MAX_BYTES:
            return name, candidate
    next_index = max(index for index, _, _ in files) + 1
    name = "games.json" if next_index == 0 else f"games{next_index}.json"
    return name, [new_game]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", type=float, default=env_float("FITGIRL_IMPORT_INTERVAL", 3600))
    parser.add_argument("--delay", type=float, default=env_float("FITGIRL_IMPORT_DELAY", 0.25))
    parser.add_argument("--limit", type=int, default=env_int("FITGIRL_IMPORT_LIMIT", 1))
    parser.add_argument("--once", action="store_true", default=env_bool("FITGIRL_IMPORT_ONCE"))
    parser.add_argument("--dry-run", action="store_true", default=env_bool("FITGIRL_IMPORT_DRY_RUN"))
    return parser.parse_args()


def run_cycle(args: argparse.Namespace) -> int:
    owner = env("GITHUB_REPO_OWNER", "LittleHacker999")
    repo = env("GITHUB_REPO_NAME", "musical-guacamole")
    branch = env("GITHUB_REPO_BRANCH", "main")
    data_path = env("GITHUB_DATA_PATH", "data/games.json")
    state_file = local_state_path()
    fitgirl_base = env("FITGIRL_API_BASE_URL", DEFAULT_FITGIRL_API).rstrip("/")
    download_base = env("REPACK_X_DOWNLOAD_BASE", DEFAULT_DOWNLOAD_BASE)
    token = os.environ.get("GITHUB_CONTENTS_TOKEN", "").strip()
    if not args.dry_run and not token:
        raise RuntimeError("GITHUB_CONTENTS_TOKEN is required unless --dry-run is used.")

    clone_root = clone_repository(owner, repo, branch)
    try:
        files = discover_game_files(clone_root, data_path)
        state = read_state(state_file)
        last_scanned = max(1, int(state.get("lastScannedPage", state.get("last_scanned_page", 1)) or 1))
        saved_last_page = max(1, int(state.get("lastPage", state.get("last_page", 1)) or 1))
        current_page = last_scanned + 1 if last_scanned < saved_last_page else 1

        records_by_file: dict[str, list[dict[str, Any]]] = {}
        by_slug: dict[str, dict[str, Any]] = {}
        for _, name, path in files:
            records = parse_json_bytes(path.read_bytes(), str(path))
            records_by_file[name] = records
            for game in records:
                slug = text(game.get("slug"))
                if slug:
                    by_slug[slug] = game

        page_payloads: dict[int, Any] = {}
        page_order = [current_page] if current_page == 1 else [current_page, 1]
        for page in page_order:
            if page in page_payloads:
                continue
            payload = get_with_retries(f"{fitgirl_base}/latest?page={page}")
            page_payloads[page] = payload

        latest_payload = page_payloads.get(current_page) or page_payloads[1]
        last_page = api_last_page(latest_payload) or saved_last_page
        imported: list[dict[str, Any]] = []
        seen: set[str] = set()
        for page in page_order:
            for summary in result_items(page_payloads[page]):
                if should_skip(summary):
                    continue
                slug = text(summary.get("slug"))
                if not slug or slug in seen:
                    continue
                seen.add(slug)
                if slug in by_slug:
                    print(f"skip existing: {slug}")
                    continue
                detail = get_with_retries(f"{fitgirl_base}/repack/{urllib.parse.quote(slug, safe='')}")
                if not isinstance(detail, dict) or should_skip(detail):
                    continue
                exact_slug = text(detail.get("slug") or slug)
                if not exact_slug or exact_slug in by_slug:
                    print(f"skip existing exact slug: {exact_slug or slug}")
                    continue
                if not has_download_links(detail):
                    print(f"skip without download links: {exact_slug}")
                    continue
                game = map_game(summary, detail, None, download_base)
                imported.append(game)
                by_slug[exact_slug] = game
                print(f"import: {game['title']} -> {game['download_url']}")
                if len(imported) >= args.limit:
                    break
                time.sleep(max(0, args.delay))
            if len(imported) >= args.limit:
                break

        if not imported:
            print(f"No new FitGirl releases found. scanned_page={current_page} last_page={last_page}")
        elif args.dry_run:
            print(json.dumps(imported, ensure_ascii=False, indent=2))
        else:
            changed_files: dict[str, tuple[bytes, str | None]] = {}
            for game in imported:
                target, candidate = choose_target(files, records_by_file, game)
                records_by_file[target] = candidate
                files = [(index, name, path) for index, name, path in files if name != target] + [
                    (int(GAMES_RE.fullmatch(target).group("index") or 0), target, clone_root / Path(data_path).parent / target)
                ]
                files.sort(key=lambda item: item[0])
                changed_files[target] = (json_bytes(candidate), None)
                print(f"write: {target} ({len(candidate)} games)")

            for name, (raw, _) in changed_files.items():
                target_path = str(Path(data_path).parent / name)
                _, sha = read_github_file(owner, repo, branch, target_path, token)
                write_github_file(owner, repo, branch, target_path, token, raw, sha, f"Import {len(imported)} FitGirl release(s) into {name}")

        next_page = current_page if current_page >= last_page else current_page
        new_state = {
            "lastScannedPage": next_page,
            "lastPage": last_page,
            "lastCycleAt": datetime.now(timezone.utc).isoformat(),
            "lastImportedSlugs": [game["slug"] for game in imported],
        }
        if not args.dry_run:
            write_state(state_file, new_state)
            print(f"Local import state updated: {state_file}")
        else:
            print(json.dumps(new_state, indent=2))
        return len(imported)
    finally:
        shutil.rmtree(clone_root.parent, ignore_errors=True)


def main() -> int:
    args = parse_args()
    if args.interval < 1 or args.delay < 0 or args.limit < 1:
        raise RuntimeError("interval and limit must be positive; delay cannot be negative")
    if args.once or args.dry_run:
        return run_cycle(args)
    print("Continuous FitGirl import mode started.")
    while True:
        started = time.monotonic()
        try:
            run_cycle(args)
        except Exception as error:
            print(f"Cycle failed: {error}", file=sys.stderr)
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
