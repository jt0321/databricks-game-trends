"""Local CLI extractor for Steam Store + SteamSpy.

Writes one JSON object per line (JSONL) under ``data/raw/`` so the output is
trivially ingestible by the Bronze notebook. Designed to be safe and friendly:

* honours a configurable User-Agent
* sleeps between requests (1.5 s Steam, 1.2 s SteamSpy)
* caps the sample size — 25 titles by default, never more than 100
* skips the network call entirely if a recent file already exists

Run it locally with ``uv run game-trends-extract`` or:

    uv run python -m game_trends.extract_steam --limit 10
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import requests  # type: ignore
except ImportError:  # pragma: no cover
    requests = None  # the CLI requires it; tests don't


STEAM_APPDETAILS = "https://store.steampowered.com/api/appdetails"
STEAMSPY_API = "https://steamspy.com/api.php"

DEFAULT_UA = os.environ.get("GAME_TRENDS_UA", "game-trend-lakehouse/0.1")
DEFAULT_OUT_DIR = Path(os.environ.get("GAME_TRENDS_OUT_DIR", "data/raw"))
DEFAULT_SAMPLE_N = int(os.environ.get("GAME_TRENDS_SAMPLE_N", "25"))
# Hard cap to keep this a focused extractor rather than a full Steam mirror;
# raise it deliberately if you have a real reason to pull more.
MAX_SAMPLE_N = 100

FRESH_HOURS = 24


def _is_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    age = time.time() - path.stat().st_mtime
    return age < FRESH_HOURS * 3600


def _session() -> "requests.Session":
    if requests is None:  # pragma: no cover
        raise RuntimeError("The 'requests' package is required for extract_steam")
    s = requests.Session()
    s.headers.update({"User-Agent": DEFAULT_UA, "Accept": "application/json"})
    return s


def fetch_steamspy_top(session: "requests.Session") -> dict[str, Any]:
    """Fetch the SteamSpy `top100in2weeks` list (one request, no per-app calls)."""
    resp = session.get(STEAMSPY_API, params={"request": "top100in2weeks"}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_steam_appdetails(session: "requests.Session", appid: int) -> dict[str, Any]:
    """Fetch full Steam Store metadata for a single appid."""
    resp = session.get(
        STEAM_APPDETAILS,
        params={"appids": appid, "cc": "us", "l": "english"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json() or {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_jsonl(records: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def extract(limit: int, out_dir: Path) -> tuple[Path, Path]:
    """Run the extraction; return (steamspy_path, steam_path)."""
    session = _session()
    steamspy_path = out_dir / "steamspy_top.jsonl"
    steam_path = out_dir / "steam_apps.jsonl"

    if _is_fresh(steamspy_path) and _is_fresh(steam_path):
        print(
            f"[extract_steam] Skipping — fresh files already in {out_dir}/ "
            f"(< {FRESH_HOURS}h old). Delete them to force a refresh.",
            file=sys.stderr,
        )
        return steamspy_path, steam_path

    print(f"[extract_steam] Fetching SteamSpy top list ...", file=sys.stderr)
    top = fetch_steamspy_top(session)
    appids = list(top.keys())[:limit]
    spy_rows = [
        {
            "source": "steamspy",
            "ingested_at": _now_iso(),
            "payload": json.dumps({k: top[k]}, ensure_ascii=False),
        }
        for k in appids
    ]
    write_jsonl(spy_rows, steamspy_path)
    print(f"[extract_steam] Wrote {len(spy_rows)} rows to {steamspy_path}", file=sys.stderr)

    print(
        f"[extract_steam] Fetching Steam Store details for {len(appids)} titles ...",
        file=sys.stderr,
    )
    steam_rows: list[dict[str, Any]] = []
    for i, appid_str in enumerate(appids, start=1):
        try:
            appid = int(appid_str)
        except ValueError:
            continue
        try:
            payload = fetch_steam_appdetails(session, appid)
        except Exception as exc:  # network blips shouldn't abort the whole run
            print(
                f"[extract_steam]   ! appid={appid} failed: {exc}",
                file=sys.stderr,
            )
            time.sleep(1.5)
            continue
        steam_rows.append(
            {
                "source": "steam_store",
                "ingested_at": _now_iso(),
                "payload": json.dumps(payload, ensure_ascii=False),
            }
        )
        print(f"[extract_steam]   [{i}/{len(appids)}] appid={appid}", file=sys.stderr)
        time.sleep(1.5)

    write_jsonl(steam_rows, steam_path)
    print(f"[extract_steam] Wrote {len(steam_rows)} rows to {steam_path}", file=sys.stderr)

    return steamspy_path, steam_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Extract a small Steam/SteamSpy sample.")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_SAMPLE_N,
        help=f"How many titles to fetch (default {DEFAULT_SAMPLE_N}, max {MAX_SAMPLE_N}).",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help=f"Output directory (default {DEFAULT_OUT_DIR}).",
    )
    args = parser.parse_args(argv)

    limit = max(1, min(args.limit, MAX_SAMPLE_N))
    extract(limit=limit, out_dir=args.out_dir)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
