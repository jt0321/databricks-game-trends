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
from collections import Counter, deque, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from .schema import HISTORY_START


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
# Engagement snapshots → silver_engagement_daily
# ---------------------------------------------------------------------------


def summarize_twitch_streams(streams: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Reduce one page-walk of Helix ``/streams`` for a category to a snapshot.

    Viewer mass on Twitch concentrates in the top streams, so a capped page
    walk (the extractor fetches up to ~500 streams) captures nearly all
    viewers even for huge categories; ``channels`` is a floor, not a total.
    """
    viewers = 0
    channels = 0
    peak = 0
    for s in streams:
        v = s.get("viewer_count")
        if not isinstance(v, int) or v < 0:
            continue
        viewers += v
        channels += 1
        peak = max(peak, v)
    return {"viewers": viewers, "channels": channels, "peak_stream_viewers": peak}


def _parse_payload(payload: str | dict[str, Any]) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        return payload
    try:
        parsed = json.loads(payload)
    except (TypeError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def build_engagement_daily(
    bronze_rows: Iterable[dict[str, Any]],
    seed_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Roll bronze snapshot sources up to game_id x date.

    Covers every snapshot-shaped source in `silver_engagement_daily`:
    `twitch_helix`, `steam_ccu`, `wikipedia_pageviews`, `google_trends`,
    `reddit`. (`steam_reviews` is handled separately by
    :func:`reviews_daily_from_silver`, since review-level dedup has to happen
    before daily aggregation.)

    Input rows follow the bronze envelope: ``source``, ``entity_key``,
    ``extract_date`` (ISO date string or date), ``payload``:

      * ``twitch_helix`` — per-category snapshot summary
        (``{"viewers", "channels", ...}``), entity_key = category name.
      * ``steam_ccu`` — raw ``GetNumberOfCurrentPlayers`` response,
        entity_key = appid.
      * ``wikipedia_pageviews`` — ``{"views": int}`` for that single day,
        entity_key = wikipedia article title. The extractor unpacks a
        multi-day API response into one bronze row per day, so no
        within-transform date-range handling is needed here.
      * ``google_trends`` — ``{"trends_index": float}``, already anchor-
        rescaled and expanded from a weekly value to one row per day by the
        extractor, entity_key = trends term.
      * ``reddit`` — ``{"subscribers": int, "posts_today": int}``,
        entity_key = subreddit name.

    Multiple snapshots per day aggregate per-signal (avg for rate-like
    values, max/sum for cumulative ones). Snapshots whose entity_key is not
    in the seed crosswalk are skipped — engagement is only tracked for the
    curated universe. Each source updates only its own output columns; the
    MERGE in the notebook layer writes only what this builder produces, so
    sources can run in any order without clobbering each other.
    """
    by_category: dict[str, str] = {}
    by_appid: dict[int, str] = {}
    by_article: dict[str, str] = {}
    by_term: dict[str, str] = {}
    by_subreddit: dict[str, str] = {}
    for seed in seed_rows:
        if seed.get("twitch_category"):
            by_category[seed["twitch_category"]] = seed["game_id"]
        if seed.get("steam_appid") is not None:
            by_appid[seed["steam_appid"]] = seed["game_id"]
        if seed.get("wikipedia_article"):
            by_article[seed["wikipedia_article"]] = seed["game_id"]
        if seed.get("trends_term"):
            by_term[seed["trends_term"]] = seed["game_id"]
        if seed.get("subreddit"):
            by_subreddit[seed["subreddit"]] = seed["game_id"]

    twitch: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    ccu: dict[tuple[str, str], list[int]] = defaultdict(list)
    wiki: dict[tuple[str, str], list[int]] = defaultdict(list)
    trends: dict[tuple[str, str], list[float]] = defaultdict(list)
    reddit_subs: dict[tuple[str, str], list[int]] = defaultdict(list)
    reddit_posts: dict[tuple[str, str], list[int]] = defaultdict(list)

    for row in bronze_rows:
        payload = _parse_payload(row.get("payload"))
        date = row.get("extract_date")
        entity_key = row.get("entity_key")
        if payload is None or not date or not entity_key:
            continue
        date = str(date)
        entity_key = str(entity_key)
        source = row.get("source")

        if source == "twitch_helix":
            game_id = by_category.get(entity_key)
            if game_id and isinstance(payload.get("viewers"), int):
                twitch[(game_id, date)].append(payload)
        elif source == "steam_ccu":
            try:
                game_id = by_appid.get(int(entity_key))
            except (TypeError, ValueError):
                continue
            count = (payload.get("response") or {}).get("player_count")
            if game_id and isinstance(count, int) and count >= 0:
                ccu[(game_id, date)].append(count)
        elif source == "wikipedia_pageviews":
            game_id = by_article.get(entity_key)
            views = payload.get("views")
            if game_id and isinstance(views, int) and views >= 0:
                wiki[(game_id, date)].append(views)
        elif source == "google_trends":
            game_id = by_term.get(entity_key)
            idx = payload.get("trends_index")
            if game_id and isinstance(idx, (int, float)):
                trends[(game_id, date)].append(float(idx))
        elif source == "reddit":
            game_id = by_subreddit.get(entity_key)
            if not game_id:
                continue
            subs = payload.get("subscribers")
            posts = payload.get("posts_today")
            if isinstance(subs, int) and subs >= 0:
                reddit_subs[(game_id, date)].append(subs)
            if isinstance(posts, int) and posts >= 0:
                reddit_posts[(game_id, date)].append(posts)

    out: dict[tuple[str, str], dict[str, Any]] = {}

    def _base(key: tuple[str, str]) -> dict[str, Any]:
        return out.setdefault(key, {"game_id": key[0], "date": key[1]})

    for key, snaps in twitch.items():
        row = _base(key)
        viewers = [s["viewers"] for s in snaps]
        channels = [s.get("channels", 0) for s in snaps]
        row["twitch_avg_viewers"] = round(sum(viewers) / len(viewers), 2)
        row["twitch_peak_viewers"] = max(viewers)
        row["twitch_avg_channels"] = round(sum(channels) / len(channels), 2)

    for key, counts in ccu.items():
        _base(key)["steam_ccu_peak"] = max(counts)

    for key, views in wiki.items():
        # One row/day is the norm; max guards against duplicate re-extracts.
        _base(key)["wiki_pageviews"] = max(views)

    for key, idxs in trends.items():
        _base(key)["trends_index"] = round(sum(idxs) / len(idxs), 2)

    for key, subs in reddit_subs.items():
        _base(key)["reddit_subscribers"] = round(sum(subs) / len(subs))

    for key, posts in reddit_posts.items():
        # Each poll recomputes the day's post count from a fresh page walk;
        # max avoids undercounting from an earlier, staler poll.
        _base(key)["reddit_posts"] = max(posts)

    return sorted(out.values(), key=lambda r: (r["game_id"], r["date"]))


# ---------------------------------------------------------------------------
# Steam reviews → silver_reviews
# ---------------------------------------------------------------------------

_HISTORY_START_EPOCH = int(
    datetime.fromisoformat(HISTORY_START).replace(tzinfo=timezone.utc).timestamp()
)


def steam_review_payload_to_rows(payload: str | dict[str, Any]) -> list[dict[str, Any]]:
    """Parse one bronze `steam_reviews` page (a raw `appreviews` response).

    Returns `[]` for an unsuccessful or malformed page rather than raising —
    backfill pagination hits plenty of edge-of-data pages.
    """
    parsed = _parse_payload(payload)
    if parsed is None or not parsed.get("success"):
        return []
    reviews = parsed.get("reviews")
    if not isinstance(reviews, list):
        return []

    out: list[dict[str, Any]] = []
    for r in reviews:
        if not isinstance(r, dict):
            continue
        review_id = r.get("recommendationid")
        ts = r.get("timestamp_created")
        if not review_id or not isinstance(ts, int):
            continue
        author = r.get("author") or {}
        out.append(
            {
                "review_id": str(review_id),
                "review_ts": ts,
                "voted_up": bool(r.get("voted_up", False)),
                "playtime_at_review_min": author.get("playtime_at_review"),
                "language": r.get("language"),
                "received_free": bool(r.get("received_for_free", False)),
            }
        )
    return out


def build_reviews_silver(
    bronze_rows: Iterable[dict[str, Any]],
    seed_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Assemble `silver_reviews` from bronze `steam_reviews` pages.

    Dedupes by `review_id` (backfill pagination can overlap across re-runs)
    and drops reviews before `HISTORY_START`.
    """
    by_appid = {
        seed["steam_appid"]: seed["game_id"]
        for seed in seed_rows
        if seed.get("steam_appid") is not None
    }

    by_id: dict[str, dict[str, Any]] = {}
    for row in bronze_rows:
        if row.get("source") != "steam_reviews":
            continue
        try:
            appid = int(row.get("entity_key"))
        except (TypeError, ValueError):
            continue
        game_id = by_appid.get(appid)
        if not game_id:
            continue
        for r in steam_review_payload_to_rows(row.get("payload")):
            if r["review_ts"] < _HISTORY_START_EPOCH:
                continue
            r["game_id"] = game_id
            by_id[r["review_id"]] = r

    return sorted(by_id.values(), key=lambda r: (r["game_id"], r["review_ts"]))


def reviews_daily_from_silver(
    review_rows: Iterable[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Aggregate `silver_reviews` into (game_id, date) rollups.

    Feeds the `reviews_posted` / `reviews_positive_share` columns of
    `silver_engagement_daily`. Run on the already-deduped Silver table (not
    raw Bronze pages) so overlapping pagination never double-counts.
    """
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in review_rows:
        game_id = r.get("game_id")
        ts = r.get("review_ts")
        if not game_id or not isinstance(ts, int):
            continue
        date = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
        buckets[(game_id, date)].append(r)

    out: list[dict[str, Any]] = []
    for (game_id, date), rows in sorted(buckets.items()):
        n = len(rows)
        positive = sum(1 for r in rows if r.get("voted_up"))
        out.append(
            {
                "game_id": game_id,
                "date": date,
                "reviews_posted": n,
                "reviews_positive_share": round(positive / n, 4) if n else 0.0,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Sales estimates → silver_sales_estimates
# ---------------------------------------------------------------------------


def _parse_owners_range(text: Any) -> tuple[int | None, int | None]:
    """Parse SteamSpy's `"1,000,000 .. 2,000,000"` owner-tier string."""
    if not isinstance(text, str) or ".." not in text:
        return None, None
    low_text, _, high_text = text.partition("..")
    try:
        return int(low_text.replace(",", "").strip()), int(high_text.replace(",", "").strip())
    except ValueError:
        return None, None


def steamspy_snapshot_to_estimate(
    payload: str | dict[str, Any], game_id: str, snapshot_date: str
) -> dict[str, Any] | None:
    """Parse one SteamSpy `appdetails` payload into an owner-tier estimate row."""
    parsed = _parse_payload(payload)
    if parsed is None:
        return None
    low, high = _parse_owners_range(parsed.get("owners"))
    if low is None:
        return None
    return {
        "game_id": game_id,
        "snapshot_date": snapshot_date,
        "est_owners_low": low,
        "est_owners_high": high,
        "est_units_lifetime": None,
        "est_revenue_lifetime_usd": None,
        "est_source": "steamspy",
    }


def gamalytic_payload_to_estimate(
    payload: str | dict[str, Any], game_id: str, snapshot_date: str
) -> dict[str, Any] | None:
    """Parse one Gamalytic lifetime-estimate payload into an estimate row."""
    parsed = _parse_payload(payload)
    if parsed is None:
        return None
    revenue = parsed.get("revenue")
    units = parsed.get("copiesSold")
    if revenue is None and units is None:
        return None
    return {
        "game_id": game_id,
        "snapshot_date": snapshot_date,
        "est_owners_low": None,
        "est_owners_high": None,
        "est_units_lifetime": int(units) if isinstance(units, (int, float)) else None,
        "est_revenue_lifetime_usd": float(revenue) if isinstance(revenue, (int, float)) else None,
        "est_source": "gamalytic",
    }


def build_sales_estimates(
    bronze_rows: Iterable[dict[str, Any]],
    seed_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Assemble `silver_sales_estimates` from bronze `steamspy_snapshot` /
    `gamalytic` rows. Append-only grain (game_id, snapshot_date, est_source) —
    every snapshot is kept, since the history of estimates is itself useful.
    """
    by_appid = {
        seed["steam_appid"]: seed["game_id"]
        for seed in seed_rows
        if seed.get("steam_appid") is not None
    }

    out: list[dict[str, Any]] = []
    for row in bronze_rows:
        source = row.get("source")
        if source not in ("steamspy_snapshot", "gamalytic"):
            continue
        try:
            appid = int(row.get("entity_key"))
        except (TypeError, ValueError):
            continue
        game_id = by_appid.get(appid)
        date = row.get("extract_date")
        if not game_id or not date:
            continue
        date = str(date)
        est = (
            steamspy_snapshot_to_estimate(row.get("payload"), game_id, date)
            if source == "steamspy_snapshot"
            else gamalytic_payload_to_estimate(row.get("payload"), game_id, date)
        )
        if est:
            out.append(est)

    return sorted(out, key=lambda r: (r["game_id"], r["snapshot_date"], r["est_source"]))


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


# ---------------------------------------------------------------------------
# Silver → Gold — cross-platform attention & revenue
# ---------------------------------------------------------------------------

_ATTENTION_SIGNALS = ("twitch_avg_viewers", "wiki_pageviews", "trends_index", "reddit_posts")
_ATTENTION_IDX_KEYS = {
    "twitch_avg_viewers": "twitch_idx",
    "wiki_pageviews": "wiki_idx",
    "trends_index": "trends_idx",
    "reddit_posts": "reddit_idx",
}
# Signals that accumulate over the week (sum) vs. rate-like signals (average).
_ATTENTION_SUM_SIGNALS = {"wiki_pageviews", "reddit_posts"}


def _week_start(date_str: str) -> str:
    """Monday of the ISO week containing `date_str` (YYYY-MM-DD)."""
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    return (d - timedelta(days=d.weekday())).isoformat()


def _month_start(date_str: str) -> str:
    return f"{date_str[:7]}-01"


def _weekly_engagement(
    engagement_daily: Iterable[dict[str, Any]]
) -> dict[tuple[str, str], dict[str, float]]:
    """Roll daily engagement rows up to (game_id, week) per attention signal."""
    buckets: dict[tuple[str, str], dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in engagement_daily:
        game_id = row.get("game_id")
        date = row.get("date")
        if not game_id or not date:
            continue
        week = _week_start(str(date))
        for sig in _ATTENTION_SIGNALS:
            v = row.get(sig)
            if isinstance(v, (int, float)):
                buckets[(game_id, week)][sig].append(v)

    weekly: dict[tuple[str, str], dict[str, float]] = {}
    for key, sig_map in buckets.items():
        weekly[key] = {
            sig: (sum(vals) if sig in _ATTENTION_SUM_SIGNALS else sum(vals) / len(vals))
            for sig, vals in sig_map.items()
        }
    return weekly


def gold_attention_trends(
    engagement_daily: Iterable[dict[str, Any]], baseline_weeks: int = 13
) -> list[dict[str, Any]]:
    """Weekly attention per title, each signal indexed to its own trailing
    `baseline_weeks` average (100 = that title's own recent normal).

    Indexing to each title's own history — rather than a global scale — is
    what makes a niche indie title and Fortnite comparable: a 200 means
    "double this title's usual attention" for either one. The baseline is
    strictly prior weeks (never includes the current week), so a title's
    first appearance always starts with null indices and fills in as history
    accumulates.
    """
    weekly = _weekly_engagement(engagement_daily)

    by_game: dict[str, list[str]] = defaultdict(list)
    for game_id, week in weekly:
        by_game[game_id].append(week)

    out: list[dict[str, Any]] = []
    for game_id, weeks in by_game.items():
        history: dict[str, deque] = {
            sig: deque(maxlen=baseline_weeks) for sig in _ATTENTION_SIGNALS
        }
        prev_attention_index: float | None = None
        for week in sorted(weeks):
            row = weekly[(game_id, week)]
            out_row: dict[str, Any] = {"game_id": game_id, "week": week}
            idx_values: list[float] = []

            for sig in _ATTENTION_SIGNALS:
                idx_key = _ATTENTION_IDX_KEYS[sig]
                current = row.get(sig)
                baseline_vals = history[sig]
                idx = None
                if current is not None and baseline_vals:
                    baseline = sum(baseline_vals) / len(baseline_vals)
                    idx = round(current / baseline * 100, 2) if baseline > 0 else None
                out_row[idx_key] = idx
                if idx is not None:
                    idx_values.append(idx)
                if current is not None:
                    baseline_vals.append(current)

            attention_index = round(sum(idx_values) / len(idx_values), 2) if idx_values else None
            out_row["attention_index"] = attention_index
            out_row["attention_wow_delta"] = (
                round(attention_index - prev_attention_index, 2)
                if attention_index is not None and prev_attention_index is not None
                else None
            )
            out_row["twitch_avg_viewers"] = row.get("twitch_avg_viewers")
            out_row["wiki_pageviews"] = row.get("wiki_pageviews")
            out.append(out_row)

            if attention_index is not None:
                prev_attention_index = attention_index

    return sorted(out, key=lambda r: (r["game_id"], r["week"]))


def gold_steam_vs_offsteam(
    silver_titles: Iterable[dict[str, Any]],
    engagement_daily: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Weekly attention split between the Steam catalog and everything else —
    the direct answer to "how much PC gaming attention would a Steam-only
    catalog miss."
    """
    cohort_by_game = {
        t["game_id"]: ("on_steam" if t.get("on_steam") else "off_steam") for t in silver_titles
    }
    weekly = _weekly_engagement(engagement_daily)
    attention_by_week: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in gold_attention_trends(engagement_daily):
        attention_by_week[row["week"]].append(row)

    weeks = sorted({week for _, week in weekly})
    out: list[dict[str, Any]] = []
    for week in weeks:
        stats = {
            "on_steam": {"titles": set(), "twitch": 0.0, "wiki": 0.0},
            "off_steam": {"titles": set(), "twitch": 0.0, "wiki": 0.0},
        }
        for (game_id, w), row in weekly.items():
            if w != week:
                continue
            cohort = cohort_by_game.get(game_id)
            if cohort is None:
                continue
            stats[cohort]["titles"].add(game_id)
            stats[cohort]["twitch"] += row.get("twitch_avg_viewers") or 0.0
            stats[cohort]["wiki"] += row.get("wiki_pageviews") or 0.0

        total_twitch = stats["on_steam"]["twitch"] + stats["off_steam"]["twitch"]
        total_wiki = stats["on_steam"]["wiki"] + stats["off_steam"]["wiki"]

        top20 = sorted(
            (r for r in attention_by_week.get(week, []) if r.get("attention_index") is not None),
            key=lambda r: r["attention_index"],
            reverse=True,
        )[:20]
        top20_cohorts = Counter(cohort_by_game.get(r["game_id"]) for r in top20)

        for cohort in ("on_steam", "off_steam"):
            s = stats[cohort]
            out.append(
                {
                    "week": week,
                    "cohort": cohort,
                    "title_count": len(s["titles"]),
                    "twitch_viewer_share": round(s["twitch"] / total_twitch, 4)
                    if total_twitch
                    else None,
                    "wiki_pageview_share": round(s["wiki"] / total_wiki, 4)
                    if total_wiki
                    else None,
                    "top20_attention_titles": top20_cohorts.get(cohort, 0),
                }
            )
    return out


def gold_title_monthly(
    engagement_daily: Iterable[dict[str, Any]],
    sales_estimates: Iterable[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Monthly leaderboard: average Twitch viewers, rank, and rank delta vs
    the prior month, plus review velocity and the latest revenue estimate.
    """
    monthly_twitch: dict[tuple[str, str], list[float]] = defaultdict(list)
    monthly_reviews: dict[tuple[str, str], int] = defaultdict(int)
    for row in engagement_daily:
        game_id = row.get("game_id")
        date = row.get("date")
        if not game_id or not date:
            continue
        month = _month_start(str(date))
        v = row.get("twitch_avg_viewers")
        if isinstance(v, (int, float)):
            monthly_twitch[(game_id, month)].append(v)
        rp = row.get("reviews_posted")
        if isinstance(rp, int):
            monthly_reviews[(game_id, month)] += rp

    latest_estimate: dict[str, dict[str, Any]] = {}
    for est in sorted(sales_estimates or [], key=lambda r: r["snapshot_date"]):
        latest_estimate[est["game_id"]] = est  # later snapshot_date wins

    avg_twitch = {key: sum(v) / len(v) for key, v in monthly_twitch.items()}

    rank_by_month: dict[str, dict[str, int]] = {}
    for month in {m for _, m in monthly_twitch}:
        ranked = sorted(
            ((gid, avg_twitch[(gid, m)]) for (gid, m) in avg_twitch if m == month),
            key=lambda kv: kv[1],
            reverse=True,
        )
        rank_by_month[month] = {gid: i + 1 for i, (gid, _) in enumerate(ranked)}

    game_months: dict[str, list[str]] = defaultdict(list)
    for game_id, month in monthly_twitch:
        game_months[game_id].append(month)

    out: list[dict[str, Any]] = []
    for game_id, months in game_months.items():
        prev_rank: int | None = None
        for month in sorted(set(months)):
            rank = rank_by_month[month].get(game_id)
            out.append(
                {
                    "game_id": game_id,
                    "month": month,
                    "twitch_avg_viewers": round(avg_twitch[(game_id, month)], 2),
                    "rank": rank,
                    # Positive delta = moved up the leaderboard (lower rank number).
                    "rank_delta": (prev_rank - rank)
                    if prev_rank is not None and rank is not None
                    else None,
                    "reviews_posted": monthly_reviews.get((game_id, month), 0),
                    "est_revenue_snapshot": (latest_estimate.get(game_id) or {}).get(
                        "est_revenue_lifetime_usd"
                    ),
                }
            )
            prev_rank = rank

    return sorted(out, key=lambda r: (r["game_id"], r["month"]))


def gold_revenue_engagement(
    silver_titles: Iterable[dict[str, Any]],
    engagement_daily: Iterable[dict[str, Any]],
    sales_estimates: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Monthly estimated revenue vs. Twitch viewership, Steam subset only.

    All `est_` columns are estimates (SteamSpy owner tiers or Gamalytic
    modeling), never ground truth — kept clearly labeled through to Gold.
    """
    on_steam_ids = {t["game_id"] for t in silver_titles if t.get("on_steam")}
    sales_estimates = list(sales_estimates)

    latest_estimate: dict[str, dict[str, Any]] = {}
    for est in sorted(sales_estimates, key=lambda r: r["snapshot_date"]):
        latest_estimate[est["game_id"]] = est

    out: list[dict[str, Any]] = []
    for row in gold_title_monthly(engagement_daily, sales_estimates):
        game_id = row["game_id"]
        if game_id not in on_steam_ids:
            continue
        est = latest_estimate.get(game_id) or {}
        revenue = est.get("est_revenue_lifetime_usd")
        viewers = row.get("twitch_avg_viewers")
        out.append(
            {
                "game_id": game_id,
                "month": row["month"],
                "est_revenue_lifetime_usd": revenue,
                "est_units_lifetime": est.get("est_units_lifetime"),
                "reviews_posted": row.get("reviews_posted", 0),
                "twitch_avg_viewers": viewers,
                "revenue_per_avg_viewer": round(revenue / viewers, 2)
                if revenue and viewers
                else None,
            }
        )
    return out
