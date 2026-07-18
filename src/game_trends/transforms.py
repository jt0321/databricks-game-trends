"""Pure-Python transforms used by both the Databricks notebooks and the tests.

Keeping the business logic dependency-light (no PySpark, no pandas) means:

* tests run in under a second on any machine with Python 3.10+;
* the Databricks notebooks can `import` these helpers directly via Repos;
* the same logic is exercised in CI and in the lakehouse, so they cannot drift.
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


# ---------------------------------------------------------------------------
# Seed crosswalk
# ---------------------------------------------------------------------------


def slugify(name: str) -> str:
    """Derive a stable `game_id` slug from a title name.

    Used only for Steam titles that are not in the seed CSV — seeded titles
    always keep their curated `game_id`.
    """
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return slug or "unknown"


def load_seed_titles(path: str | Path) -> list[dict[str, Any]]:
    """Load the curated crosswalk CSV (see `schema.SEED_COLUMNS`).

    Normalizes: `steam_appid` to int or None, `storefronts` to a list (the CSV
    uses `|` as the in-cell separator), blank crosswalk cells to None. Rows
    missing `game_id` or `name` are skipped.
    """
    rows: list[dict[str, Any]] = []
    with Path(path).open(newline="", encoding="utf-8") as f:
        for raw in csv.DictReader(f):
            game_id = (raw.get("game_id") or "").strip()
            name = (raw.get("name") or "").strip()
            if not game_id or not name:
                continue
            appid_text = (raw.get("steam_appid") or "").strip()
            storefronts = [
                s.strip()
                for s in (raw.get("storefronts") or "").split("|")
                if s.strip()
            ]
            rows.append(
                {
                    "game_id": game_id,
                    "name": name,
                    "steam_appid": int(appid_text) if appid_text else None,
                    "twitch_category": (raw.get("twitch_category") or "").strip() or None,
                    "wikipedia_article": (raw.get("wikipedia_article") or "").strip() or None,
                    "subreddit": (raw.get("subreddit") or "").strip() or None,
                    "trends_term": (raw.get("trends_term") or "").strip() or None,
                    "storefronts": storefronts,
                }
            )
    return rows


# ---------------------------------------------------------------------------
# Bronze → Silver
# ---------------------------------------------------------------------------

_RELEASE_DATE_FORMATS = (
    "%d %b, %Y",   # Steam: "10 Mar, 2017"
    "%b %d, %Y",   # Steam: "Mar 10, 2017"
    "%Y-%m-%d",
    "%Y",
)


def parse_release_year(raw: Any) -> int | None:
    """Extract a release year from Steam's free-form `release_date` payload."""
    if raw is None:
        return None
    if isinstance(raw, int) and 1970 <= raw <= 2100:
        return raw
    if isinstance(raw, dict):
        raw = raw.get("date")
    if not isinstance(raw, str) or not raw.strip():
        return None
    text = raw.strip()
    for fmt in _RELEASE_DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).year
        except ValueError:
            continue
    m = re.search(r"(19|20)\d{2}", text)
    return int(m.group(0)) if m else None


def _coerce_price_cents(data: dict[str, Any]) -> int:
    """Pull a normalized integer-cent price out of a Steam `appdetails` payload."""
    if data.get("is_free"):
        return 0
    overview = data.get("price_overview") or {}
    final = overview.get("final")
    if isinstance(final, (int, float)):
        return int(final)
    return 0


def _genres(data: dict[str, Any]) -> list[str]:
    raw = data.get("genres") or []
    out: list[str] = []
    for g in raw:
        if isinstance(g, dict):
            desc = g.get("description")
            if isinstance(desc, str) and desc.strip():
                out.append(desc.strip())
        elif isinstance(g, str) and g.strip():
            out.append(g.strip())
    return out or ["Unknown"]


def steam_payload_to_silver(payload: str | dict[str, Any]) -> dict[str, Any] | None:
    """Convert one bronze row (a raw Steam appdetails envelope) into a silver row.

    Returns ``None`` for unusable payloads — caller is expected to filter these
    out before writing to the Silver table.
    """
    if isinstance(payload, str):
        try:
            envelope = json.loads(payload)
        except json.JSONDecodeError:
            return None
    else:
        envelope = payload

    if not isinstance(envelope, dict) or not envelope:
        return None

    # Steam wraps the body as { "<appid>": { "success": bool, "data": {...} } }.
    # Accept either the wrapped or already-unwrapped shape.
    if "success" in envelope and "data" in envelope:
        wrapper = envelope
    else:
        # take the first key — there's always exactly one
        first_key = next(iter(envelope))
        wrapper = envelope[first_key]
        if not isinstance(wrapper, dict):
            return None

    if not wrapper.get("success"):
        return None
    data = wrapper.get("data") or {}
    if not isinstance(data, dict):
        return None

    appid = data.get("steam_appid")
    name = data.get("name")
    if not isinstance(appid, int) or not isinstance(name, str) or not name.strip():
        return None

    platforms = data.get("platforms") or {}
    genres = _genres(data)

    return {
        "appid": appid,
        "name": name.strip(),
        "release_year": parse_release_year(data.get("release_date")),
        "is_free": bool(data.get("is_free", False)),
        "price_cents": _coerce_price_cents(data),
        "supports_windows": bool(platforms.get("windows", False)),
        "supports_mac": bool(platforms.get("mac", False)),
        "supports_linux": bool(platforms.get("linux", False)),
        "primary_genre": genres[0],
        "genre_list": genres,
    }


def dedupe_silver(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the last seen row per ``appid`` — assumes input is in ingest order."""
    by_id: dict[int, dict[str, Any]] = {}
    for row in rows:
        appid = row.get("appid")
        if isinstance(appid, int):
            by_id[appid] = row
    return list(by_id.values())


def build_silver_titles(
    steam_rows: Iterable[dict[str, Any]],
    seed_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Assemble the canonical `silver_titles` rows (one per `game_id`).

    Inputs:
      * ``steam_rows`` — deduped output of :func:`steam_payload_to_silver`.
      * ``seed_rows`` — output of :func:`load_seed_titles`.

    Every seed title gets a row even with no Steam payload this batch (an
    off-Steam title, or a Steam title not yet ingested) so the dimension is
    always complete; Steam metadata columns stay None until a payload arrives.
    Steam titles absent from the seed still flow through with a slugified
    `game_id`, so the pipeline works on arbitrary samples — but only seeded
    titles carry the wikipedia/subreddit crosswalk needed by other sources.
    """
    seed_by_appid: dict[int, dict[str, Any]] = {}
    out: dict[str, dict[str, Any]] = {}

    for seed in seed_rows:
        if seed["steam_appid"] is not None:
            seed_by_appid[seed["steam_appid"]] = seed
        on_steam = seed["steam_appid"] is not None
        out[seed["game_id"]] = {
            "game_id": seed["game_id"],
            "name": seed["name"],
            "steam_appid": seed["steam_appid"],
            "on_steam": on_steam,
            "storefronts": list(seed["storefronts"]),
            "release_year": None,
            "is_free": None,
            "price_cents": None,
            # PC-only universe: every tracked title runs on Windows.
            "supports_windows": True,
            "supports_mac": None,
            "supports_linux": None,
            "primary_genre": None,
            "genre_list": [],
            "twitch_category": seed["twitch_category"],
            "wikipedia_article": seed["wikipedia_article"],
            "subreddit": seed["subreddit"],
            "trends_term": seed["trends_term"],
        }

    for row in steam_rows:
        appid = row.get("appid")
        if not isinstance(appid, int):
            continue
        seed = seed_by_appid.get(appid)
        game_id = seed["game_id"] if seed else slugify(row["name"])
        base = out.get(game_id) or {
            "game_id": game_id,
            "twitch_category": row["name"],
            "wikipedia_article": None,
            "subreddit": None,
            "trends_term": row["name"],
        }
        base.update(
            {
                "name": seed["name"] if seed else row["name"],
                "steam_appid": appid,
                "on_steam": True,
                "storefronts": list(seed["storefronts"]) if seed and seed["storefronts"] else ["steam"],
                "release_year": row["release_year"],
                "is_free": row["is_free"],
                "price_cents": row["price_cents"],
                "supports_windows": row["supports_windows"],
                "supports_mac": row["supports_mac"],
                "supports_linux": row["supports_linux"],
                "primary_genre": row["primary_genre"],
                "genre_list": list(row["genre_list"]),
            }
        )
        out[game_id] = base

    return sorted(out.values(), key=lambda r: r["game_id"])


# ---------------------------------------------------------------------------
# Silver → Gold
# ---------------------------------------------------------------------------

_PLATFORM_FIELDS = (
    ("windows", "supports_windows"),
    ("mac", "supports_mac"),
    ("linux", "supports_linux"),
)


def gold_platform_trends(silver: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate Silver into (release_year, platform) rows.

    A title contributes to a platform bucket whenever it supports that platform,
    so the same title can appear in up to three buckets — which matches how
    Steam itself reports cross-platform support.
    """
    buckets: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in silver:
        year = row.get("release_year")
        if not isinstance(year, int):
            continue
        for platform_name, field in _PLATFORM_FIELDS:
            if row.get(field):
                buckets[(year, platform_name)].append(row)

    out: list[dict[str, Any]] = []
    for (year, platform), rows in sorted(buckets.items()):
        n = len(rows)
        free = sum(1 for r in rows if r.get("is_free"))
        avg_price = sum(r.get("price_cents", 0) for r in rows) / n if n else 0.0
        out.append(
            {
                "release_year": year,
                "platform": platform,
                "title_count": n,
                "free_pct": round(free / n, 4) if n else 0.0,
                "avg_price_cents": round(avg_price, 2),
            }
        )
    return out


def gold_genre_trends(silver: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate Silver into (release_year, primary_genre) rows with share %."""
    materialized = [r for r in silver if isinstance(r.get("release_year"), int)]
    totals_per_year: Counter[int] = Counter(r["release_year"] for r in materialized)

    buckets: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in materialized:
        key = (row["release_year"], row.get("primary_genre") or "Unknown")
        buckets[key].append(row)

    out: list[dict[str, Any]] = []
    for (year, genre), rows in sorted(buckets.items()):
        n = len(rows)
        avg_price = sum(r.get("price_cents", 0) for r in rows) / n if n else 0.0
        share = n / totals_per_year[year] if totals_per_year[year] else 0.0
        out.append(
            {
                "release_year": year,
                "primary_genre": genre,
                "title_count": n,
                "release_share": round(share, 4),
                "avg_price_cents": round(avg_price, 2),
            }
        )
    return out
