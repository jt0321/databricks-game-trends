"""Local CLI: backfill historical engagement signals to `HISTORY_START`.

Unlike the snapshot extractors (`extract_engagement.py`), these sources
genuinely have history — the extractor's job is to fetch it once (re-runs are
cheap and idempotent) rather than to poll forward. Each writes one bronze
JSONL row **per entity per day**, so the bronze contract stays uniform
("one row = one entity + date fact") regardless of whether the source
returned a whole date range in a single API call:

* ``steam_reviews.jsonl`` — every Steam review since `HISTORY_START`, paged
  backwards from most recent (entity_key = appid). No credentials needed.
* ``wikipedia_pageviews.jsonl`` — daily pageviews per seeded Wikipedia
  article since `HISTORY_START` (entity_key = article title). No credentials.
* ``google_trends.jsonl`` — weekly relative search interest per seeded term,
  rescaled against a fixed anchor term so batches are comparable over time,
  then expanded to one row per day in the week (entity_key = trends term).
  Uses the unofficial `pytrends` package (``pip install pytrends`` /
  declared as a project dependency) since Google Trends has no official API.

Run with ``uv run game-trends-backfill --help`` or:

    uv run python -m game_trends.extract_backfill --sources reviews wikipedia
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

try:
    import requests  # type: ignore
except ImportError:  # pragma: no cover
    requests = None  # the CLI requires it; tests don't

from .extract_steam import write_jsonl
from .schema import HISTORY_START
from .transforms import load_seed_titles

STEAM_REVIEWS_API = "https://store.steampowered.com/appreviews/{appid}"
WIKIMEDIA_PAGEVIEWS_API = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
    "en.wikipedia/all-access/user/{article}/daily/{start}/{end}"
)

DEFAULT_UA = os.environ.get("GAME_TRENDS_UA", "game-trend-lakehouse/0.1")
DEFAULT_OUT_DIR = Path(os.environ.get("GAME_TRENDS_OUT_DIR", "data/raw"))
DEFAULT_SEED = Path("data/seed/seed_titles.csv")

# A large, stable-volume anchor term. Every Trends batch queries it alongside
# the real terms so the 0-100 relative scores can be rescaled to a comparable
# footing across batches (see docs/data_sources.md).
TRENDS_ANCHOR_TERM = "video game"


def _history_start_date() -> date:
    return date.fromisoformat(HISTORY_START)


def _daterange(start: date, end: date) -> Iterator[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def _session() -> "requests.Session":
    if requests is None:  # pragma: no cover
        raise RuntimeError("The 'requests' package is required for extract_backfill")
    s = requests.Session()
    s.headers.update({"User-Agent": DEFAULT_UA, "Accept": "application/json"})
    return s


def _envelope(source: str, entity_key: str, extract_date: date, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": source,
        "entity_key": entity_key,
        "extract_date": extract_date.isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "payload": json.dumps(payload, ensure_ascii=False),
    }


# ---------------------------------------------------------------------------
# Steam reviews
# ---------------------------------------------------------------------------


def fetch_review_page(
    session: "requests.Session", appid: int, cursor: str = "*"
) -> dict[str, Any]:
    resp = session.get(
        STEAM_REVIEWS_API.format(appid=appid),
        params={
            "json": 1,
            "filter": "recent",
            "language": "all",
            "num_per_page": 100,
            "cursor": cursor,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json() or {}


def backfill_steam_reviews(
    session: "requests.Session", seed_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Page backwards through reviews until crossing `HISTORY_START`.

    One bronze row is written per fetched page (not per day) — the page
    itself is the natural unit here since a page can span many days and a
    single day can span many pages for a heavily-reviewed title. Filtering to
    `HISTORY_START` and deduping happens downstream in
    `transforms.build_reviews_silver`.
    """
    cutoff = _history_start_date()
    steam_titles = [s for s in seed_rows if s["steam_appid"] is not None]
    rows: list[dict[str, Any]] = []
    print(f"[backfill] Steam reviews for {len(steam_titles)} titles ...", file=sys.stderr)

    for seed in steam_titles:
        appid = seed["steam_appid"]
        cursor = "*"
        pages = 0
        while True:
            try:
                page = fetch_review_page(session, appid, cursor)
            except Exception as exc:
                print(f"[backfill]   ! appid={appid} page fetch failed: {exc}", file=sys.stderr)
                break
            reviews = page.get("reviews") or []
            if not reviews:
                break
            oldest_ts = min(r.get("timestamp_created", 2**31) for r in reviews)
            oldest_date = datetime.fromtimestamp(oldest_ts, tz=timezone.utc).date()
            rows.append(_envelope("steam_reviews", str(appid), oldest_date, page))
            pages += 1
            next_cursor = page.get("cursor")
            if not next_cursor or next_cursor == cursor or oldest_date < cutoff:
                break
            cursor = next_cursor
            time.sleep(1.0)
        print(f"[backfill]   appid={appid}: {pages} pages", file=sys.stderr)
        time.sleep(1.0)

    return rows


# ---------------------------------------------------------------------------
# Wikipedia pageviews
# ---------------------------------------------------------------------------


def fetch_wikipedia_pageviews(
    session: "requests.Session", article: str, start: date, end: date
) -> dict[str, Any]:
    resp = session.get(
        WIKIMEDIA_PAGEVIEWS_API.format(
            article=article.replace(" ", "_"),
            start=start.strftime("%Y%m%d"),
            end=end.strftime("%Y%m%d"),
        ),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json() or {}


def backfill_wikipedia(
    session: "requests.Session", seed_rows: list[dict[str, Any]], end: date | None = None
) -> list[dict[str, Any]]:
    start = _history_start_date()
    end = end or date.today()
    articles = sorted({s["wikipedia_article"] for s in seed_rows if s.get("wikipedia_article")})
    rows: list[dict[str, Any]] = []
    print(f"[backfill] Wikipedia pageviews for {len(articles)} articles ...", file=sys.stderr)

    for article in articles:
        try:
            body = fetch_wikipedia_pageviews(session, article, start, end)
        except Exception as exc:
            print(f"[backfill]   ! article={article!r} failed: {exc}", file=sys.stderr)
            time.sleep(0.3)
            continue
        for item in body.get("items", []):
            ts = item.get("timestamp")  # "YYYYMMDD00"
            views = item.get("views")
            if not isinstance(ts, str) or len(ts) < 8 or not isinstance(views, int):
                continue
            day = datetime.strptime(ts[:8], "%Y%m%d").date()
            rows.append(_envelope("wikipedia_pageviews", article, day, {"views": views}))
        time.sleep(0.3)

    return rows


# ---------------------------------------------------------------------------
# Google Trends
# ---------------------------------------------------------------------------


def backfill_google_trends(
    seed_rows: list[dict[str, Any]], end: date | None = None
) -> list[dict[str, Any]]:
    """Backfill weekly relative search interest, rescaled against a fixed
    anchor term, expanded to one bronze row per day.

    Google Trends returns values 0-100 *relative to the other terms in the
    same request* — incomparable across separate batches/terms otherwise.
    Including `TRENDS_ANCHOR_TERM` in every request and rescaling
    ``term / anchor * 100`` makes the resulting `trends_index` comparable
    across terms and over time, assuming the anchor's own absolute search
    volume is roughly stable (a reasonable approximation for a generic,
    high-volume term).
    """
    try:
        from pytrends.request import TrendReq  # type: ignore
    except ImportError:
        print(
            "[backfill] pytrends not installed — skipping Google Trends "
            "(pip install pytrends, or add it to your uv environment).",
            file=sys.stderr,
        )
        return []

    start = _history_start_date()
    end = end or date.today()
    terms = sorted({s["trends_term"] for s in seed_rows if s.get("trends_term")})
    pytrends = TrendReq(hl="en-US")
    rows: list[dict[str, Any]] = []
    print(f"[backfill] Google Trends for {len(terms)} terms ...", file=sys.stderr)

    timeframe = f"{start.isoformat()} {end.isoformat()}"
    for term in terms:
        try:
            pytrends.build_payload([term, TRENDS_ANCHOR_TERM], timeframe=timeframe)
            df = pytrends.interest_over_time()
        except Exception as exc:
            print(f"[backfill]   ! term={term!r} failed: {exc}", file=sys.stderr)
            time.sleep(1.0)
            continue
        if df is None or df.empty:
            time.sleep(1.0)
            continue
        for week_start, record in df.iterrows():
            anchor_val = record.get(TRENDS_ANCHOR_TERM)
            term_val = record.get(term)
            if not anchor_val or term_val is None:
                continue
            index = round(float(term_val) / float(anchor_val) * 100, 2)
            week_monday = week_start.date()
            for offset in range(7):
                day = week_monday + timedelta(days=offset)
                if day > end:
                    break
                rows.append(_envelope("google_trends", term, day, {"trends_index": index}))
        time.sleep(1.0)

    return rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

_SOURCES = ("reviews", "wikipedia", "trends")


def run_backfill(seed_path: Path, out_dir: Path, sources: list[str]) -> list[Path]:
    seed_rows = load_seed_titles(seed_path)
    written: list[Path] = []

    if "reviews" in sources or "wikipedia" in sources:
        session = _session()
        if "reviews" in sources:
            rows = backfill_steam_reviews(session, seed_rows)
            if rows:
                path = out_dir / "steam_reviews.jsonl"
                write_jsonl(rows, path)
                print(f"[backfill] Wrote {len(rows)} rows to {path}", file=sys.stderr)
                written.append(path)
        if "wikipedia" in sources:
            rows = backfill_wikipedia(session, seed_rows)
            if rows:
                path = out_dir / "wikipedia_pageviews.jsonl"
                write_jsonl(rows, path)
                print(f"[backfill] Wrote {len(rows)} rows to {path}", file=sys.stderr)
                written.append(path)

    if "trends" in sources:
        rows = backfill_google_trends(seed_rows)
        if rows:
            path = out_dir / "google_trends.jsonl"
            write_jsonl(rows, path)
            print(f"[backfill] Wrote {len(rows)} rows to {path}", file=sys.stderr)
            written.append(path)

    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=f"Backfill historical engagement signals to {HISTORY_START}."
    )
    parser.add_argument("--seed", type=Path, default=DEFAULT_SEED, help="Seed crosswalk CSV.")
    parser.add_argument(
        "--out-dir", type=Path, default=DEFAULT_OUT_DIR, help=f"Output dir (default {DEFAULT_OUT_DIR})."
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=_SOURCES,
        default=list(_SOURCES),
        help="Which sources to backfill (default: all).",
    )
    args = parser.parse_args(argv)
    run_backfill(seed_path=args.seed, out_dir=args.out_dir, sources=args.sources)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
