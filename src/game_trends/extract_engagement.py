"""Local CLI extractor for engagement snapshots: Twitch Helix + Steam CCU.

Polls the curated seed universe (``data/seed/seed_titles.csv``) and writes
bronze-envelope JSONL under ``data/raw/``:

* ``steam_ccu.jsonl`` — official concurrent player count per seeded Steam
  title (no credentials needed).
* ``twitch_streams.jsonl`` — one snapshot per seeded Twitch category:
  viewers / channel count summarized from a capped page-walk of
  ``/helix/streams``. Requires ``TWITCH_CLIENT_ID`` + ``TWITCH_CLIENT_SECRET``
  (free app registration); skipped with a notice when unset.

Each JSONL row is ``{source, entity_key, extract_date, ingested_at, payload}``
so the Bronze notebook can append it directly. Snapshots are point-in-time —
unlike the Steam metadata extractor there is no freshness skip; every run
appends a new observation, and the Silver rollup averages them per day.

Run with ``uv run game-trends-extract-engagement`` or:

    uv run python -m game_trends.extract_engagement --skip-twitch
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

from .extract_steam import write_jsonl
from .transforms import load_seed_titles, summarize_twitch_streams

STEAM_CCU_API = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
TWITCH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
TWITCH_HELIX = "https://api.twitch.tv/helix"

DEFAULT_UA = os.environ.get("GAME_TRENDS_UA", "game-trend-lakehouse/0.1")
DEFAULT_OUT_DIR = Path(os.environ.get("GAME_TRENDS_OUT_DIR", "data/raw"))
DEFAULT_SEED = Path("data/seed/seed_titles.csv")

# Streams are returned 100/page ordered by viewers descending; 5 pages covers
# the overwhelming share of viewer mass even for the biggest categories.
MAX_STREAM_PAGES = 5


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _envelope(source: str, entity_key: str, payload: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    return {
        "source": source,
        "entity_key": entity_key,
        "extract_date": now.date().isoformat(),
        "ingested_at": now.isoformat(),
        "payload": json.dumps(payload, ensure_ascii=False),
    }


def _session() -> "requests.Session":
    if requests is None:  # pragma: no cover
        raise RuntimeError("The 'requests' package is required for extract_engagement")
    s = requests.Session()
    s.headers.update({"User-Agent": DEFAULT_UA, "Accept": "application/json"})
    return s


# ---------------------------------------------------------------------------
# Steam CCU
# ---------------------------------------------------------------------------


def extract_steam_ccu(
    session: "requests.Session", seed_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    steam_titles = [s for s in seed_rows if s["steam_appid"] is not None]
    print(f"[engagement] Steam CCU for {len(steam_titles)} titles ...", file=sys.stderr)
    for seed in steam_titles:
        appid = seed["steam_appid"]
        try:
            resp = session.get(STEAM_CCU_API, params={"appid": appid}, timeout=30)
            resp.raise_for_status()
            payload = resp.json() or {}
        except Exception as exc:
            print(f"[engagement]   ! appid={appid} failed: {exc}", file=sys.stderr)
            time.sleep(1.0)
            continue
        rows.append(_envelope("steam_ccu", str(appid), payload))
        time.sleep(1.0)
    return rows


# ---------------------------------------------------------------------------
# Twitch Helix
# ---------------------------------------------------------------------------


def twitch_app_token(session: "requests.Session", client_id: str, secret: str) -> str:
    resp = session.post(
        TWITCH_TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": secret,
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def resolve_twitch_categories(
    session: "requests.Session", names: list[str]
) -> dict[str, str]:
    """Resolve exact category names to Twitch game IDs (up to 100 per call)."""
    out: dict[str, str] = {}
    for i in range(0, len(names), 100):
        chunk = names[i : i + 100]
        resp = session.get(
            f"{TWITCH_HELIX}/games", params=[("name", n) for n in chunk], timeout=30
        )
        resp.raise_for_status()
        for g in resp.json().get("data", []):
            out[g["name"]] = g["id"]
    return out


def fetch_category_streams(
    session: "requests.Session", game_id: str, max_pages: int = MAX_STREAM_PAGES
) -> tuple[list[dict[str, Any]], bool]:
    """Page through /streams for one category; returns (streams, truncated)."""
    streams: list[dict[str, Any]] = []
    cursor: str | None = None
    for _ in range(max_pages):
        params: list[tuple[str, str]] = [("game_id", game_id), ("first", "100")]
        if cursor:
            params.append(("after", cursor))
        resp = session.get(f"{TWITCH_HELIX}/streams", params=params, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        streams.extend(body.get("data", []))
        cursor = (body.get("pagination") or {}).get("cursor")
        if not cursor:
            return streams, False
    return streams, True


def extract_twitch(
    session: "requests.Session", seed_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    client_id = os.environ.get("TWITCH_CLIENT_ID")
    secret = os.environ.get("TWITCH_CLIENT_SECRET")
    if not client_id or not secret:
        print(
            "[engagement] TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET unset — "
            "skipping Twitch (register a free app at https://dev.twitch.tv/console/apps).",
            file=sys.stderr,
        )
        return []

    token = twitch_app_token(session, client_id, secret)
    session.headers.update({"Client-Id": client_id, "Authorization": f"Bearer {token}"})

    categories = sorted({s["twitch_category"] for s in seed_rows if s["twitch_category"]})
    ids = resolve_twitch_categories(session, categories)
    missing = [c for c in categories if c not in ids]
    if missing:
        print(
            f"[engagement]   ! categories not found on Twitch (check seed CSV): {missing}",
            file=sys.stderr,
        )

    rows: list[dict[str, Any]] = []
    print(f"[engagement] Twitch snapshots for {len(ids)} categories ...", file=sys.stderr)
    for name, game_id in ids.items():
        try:
            streams, truncated = fetch_category_streams(session, game_id)
        except Exception as exc:
            print(f"[engagement]   ! category={name!r} failed: {exc}", file=sys.stderr)
            time.sleep(0.5)
            continue
        summary = summarize_twitch_streams(streams)
        summary["truncated"] = truncated
        summary["twitch_game_id"] = game_id
        rows.append(_envelope("twitch_helix", name, summary))
        time.sleep(0.5)
    return rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def extract(seed_path: Path, out_dir: Path, skip_twitch: bool = False) -> list[Path]:
    seed_rows = load_seed_titles(seed_path)
    session = _session()
    written: list[Path] = []

    ccu_rows = extract_steam_ccu(session, seed_rows)
    if ccu_rows:
        path = out_dir / "steam_ccu.jsonl"
        write_jsonl(ccu_rows, path)
        print(f"[engagement] Wrote {len(ccu_rows)} rows to {path}", file=sys.stderr)
        written.append(path)

    if not skip_twitch:
        twitch_rows = extract_twitch(session, seed_rows)
        if twitch_rows:
            path = out_dir / "twitch_streams.jsonl"
            write_jsonl(twitch_rows, path)
            print(f"[engagement] Wrote {len(twitch_rows)} rows to {path}", file=sys.stderr)
            written.append(path)

    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Snapshot Twitch viewership and Steam CCU for the seed universe."
    )
    parser.add_argument("--seed", type=Path, default=DEFAULT_SEED, help="Seed crosswalk CSV.")
    parser.add_argument(
        "--out-dir", type=Path, default=DEFAULT_OUT_DIR, help=f"Output dir (default {DEFAULT_OUT_DIR})."
    )
    parser.add_argument(
        "--skip-twitch", action="store_true", help="Only poll Steam CCU (no credentials needed)."
    )
    args = parser.parse_args(argv)
    extract(seed_path=args.seed, out_dir=args.out_dir, skip_twitch=args.skip_twitch)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
