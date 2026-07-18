"""Canonical schemas for the Game Trend Lakehouse.

These are deliberately defined as plain Python so they can be imported by both
the local CLI / tests and the Databricks notebooks (where they are wrapped in
PySpark StructType objects).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Backfillable sources are loaded from this date forward; snapshot-only
# sources (Twitch, Steam CCU, Reddit) simply accrue history from whenever the
# pipeline first runs. See docs/multi_platform_plan.md.
HISTORY_START = "2020-01-01"


# ---------------------------------------------------------------------------
# Seed crosswalk (data/seed/seed_titles.csv)
# ---------------------------------------------------------------------------

# The curated title universe. `game_id` is the canonical key everywhere; the
# remaining columns map each title to its ID in every tracked source. A blank
# `steam_appid` marks an off-Steam title.
SEED_COLUMNS: list[tuple[str, str]] = [
    ("game_id", "string"),
    ("name", "string"),
    ("steam_appid", "long"),
    ("twitch_category", "string"),
    ("wikipedia_article", "string"),
    ("subreddit", "string"),
    ("trends_term", "string"),
    ("storefronts", "array<string>"),
]


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------

# `entity_key` is the source-native ID (steam appid, Twitch category name,
# wikipedia article, ...). `extract_date` is the date the payload *describes*,
# which differs from `ingested_at` for backfills. Both are nullable so rows
# from the original Steam-only extractor remain valid.
BRONZE_COLUMNS: list[tuple[str, str]] = [
    ("source", "string"),
    ("entity_key", "string"),
    ("extract_date", "date"),
    ("ingested_at", "timestamp"),
    ("batch_id", "string"),
    ("payload", "string"),
]


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------

# Keyed by `game_id`, not `steam_appid`: the dimension covers off-Steam titles,
# for which `steam_appid` is NULL and Steam-derived metadata columns stay NULL
# until another source (e.g. IGDB) fills them.
SILVER_COLUMNS: list[tuple[str, str]] = [
    ("game_id", "string"),
    ("name", "string"),
    ("steam_appid", "long"),
    ("on_steam", "boolean"),
    ("storefronts", "array<string>"),
    ("release_year", "int"),
    ("is_free", "boolean"),
    ("price_cents", "int"),
    ("supports_windows", "boolean"),
    ("supports_mac", "boolean"),
    ("supports_linux", "boolean"),
    ("primary_genre", "string"),
    ("genre_list", "array<string>"),
    ("twitch_category", "string"),
    ("wikipedia_article", "string"),
    ("subreddit", "string"),
    ("trends_term", "string"),
    ("source", "string"),
    ("ingested_at", "timestamp"),
]


@dataclass(frozen=True)
class SilverTitle:
    """One canonical row in `silver_titles`."""

    game_id: str
    name: str
    steam_appid: int | None
    on_steam: bool
    storefronts: list[str]
    release_year: int | None
    is_free: bool | None
    price_cents: int | None
    supports_windows: bool | None
    supports_mac: bool | None
    supports_linux: bool | None
    primary_genre: str | None
    genre_list: list[str] = field(default_factory=list)
    twitch_category: str | None = None
    wikipedia_article: str | None = None
    subreddit: str | None = None
    trends_term: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "game_id": self.game_id,
            "name": self.name,
            "steam_appid": self.steam_appid,
            "on_steam": self.on_steam,
            "storefronts": list(self.storefronts),
            "release_year": self.release_year,
            "is_free": self.is_free,
            "price_cents": self.price_cents,
            "supports_windows": self.supports_windows,
            "supports_mac": self.supports_mac,
            "supports_linux": self.supports_linux,
            "primary_genre": self.primary_genre,
            "genre_list": list(self.genre_list),
            "twitch_category": self.twitch_category,
            "wikipedia_article": self.wikipedia_article,
            "subreddit": self.subreddit,
            "trends_term": self.trends_term,
        }


# ---------------------------------------------------------------------------
# Silver — engagement fact table
# ---------------------------------------------------------------------------

# Grain: game_id x date. Wide and nullable by design: each source's job fills
# only its own columns (off-Steam titles never get Steam columns; days before
# a source's first poll or backfill stay NULL). MERGE on (game_id, date).
SILVER_ENGAGEMENT_COLUMNS: list[tuple[str, str]] = [
    ("game_id", "string"),
    ("date", "date"),
    ("twitch_avg_viewers", "double"),
    ("twitch_peak_viewers", "long"),
    ("twitch_avg_channels", "double"),
    ("wiki_pageviews", "long"),
    ("trends_index", "double"),
    ("reddit_posts", "long"),
    ("reddit_subscribers", "long"),
    ("steam_ccu_peak", "long"),
    ("reviews_posted", "long"),
    ("reviews_positive_share", "double"),
    ("ingested_at", "timestamp"),
]


# ---------------------------------------------------------------------------
# Silver — review-level and sales-estimate tables (Steam subset)
# ---------------------------------------------------------------------------

# Grain: one row per Steam review. Kept separate from the daily engagement
# table because playtime-at-review cohorts can't be pre-aggregated away.
# MERGE / dedupe on review_id; filtered to review_ts >= HISTORY_START.
SILVER_REVIEWS_COLUMNS: list[tuple[str, str]] = [
    ("game_id", "string"),
    ("review_id", "string"),
    ("review_ts", "long"),
    ("voted_up", "boolean"),
    ("playtime_at_review_min", "long"),
    ("language", "string"),
    ("received_free", "boolean"),
]

# Grain: game_id x snapshot_date x est_source. Append-only — the history of
# estimates is itself the time series. `est_source` is "steamspy" (owner-tier
# bounds) or "gamalytic" (revenue/unit estimates); columns the source doesn't
# produce stay NULL.
SILVER_SALES_ESTIMATES_COLUMNS: list[tuple[str, str]] = [
    ("game_id", "string"),
    ("snapshot_date", "date"),
    ("est_owners_low", "long"),
    ("est_owners_high", "long"),
    ("est_units_lifetime", "long"),
    ("est_revenue_lifetime_usd", "double"),
    ("est_source", "string"),
]


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------

GOLD_PLATFORM_COLUMNS: list[tuple[str, str]] = [
    ("release_year", "int"),
    ("platform", "string"),
    ("title_count", "long"),
    ("free_pct", "double"),
    ("avg_price_cents", "double"),
]

GOLD_GENRE_COLUMNS: list[tuple[str, str]] = [
    ("release_year", "int"),
    ("primary_genre", "string"),
    ("title_count", "long"),
    ("release_share", "double"),
    ("avg_price_cents", "double"),
]

# Grain: game_id x week (Monday date). Each raw signal is indexed to that
# title's own trailing baseline (100 = its own recent normal) so Twitch,
# Wikipedia, Trends, and Reddit become comparable and composable.
GOLD_ATTENTION_COLUMNS: list[tuple[str, str]] = [
    ("game_id", "string"),
    ("week", "string"),
    ("twitch_idx", "double"),
    ("wiki_idx", "double"),
    ("trends_idx", "double"),
    ("reddit_idx", "double"),
    ("attention_index", "double"),
    ("attention_wow_delta", "double"),
    ("twitch_avg_viewers", "double"),
    ("wiki_pageviews", "long"),
]

# Grain: week x cohort ("on_steam" | "off_steam") — the direct answer to
# "how much PC gaming attention would a Steam-only catalog miss."
GOLD_STEAM_VS_OFFSTEAM_COLUMNS: list[tuple[str, str]] = [
    ("week", "string"),
    ("cohort", "string"),
    ("title_count", "long"),
    ("twitch_viewer_share", "double"),
    ("wiki_pageview_share", "double"),
    ("top20_attention_titles", "long"),
]

# Grain: game_id x month. Backs a "top movers" leaderboard.
GOLD_TITLE_MONTHLY_COLUMNS: list[tuple[str, str]] = [
    ("game_id", "string"),
    ("month", "string"),
    ("twitch_avg_viewers", "double"),
    ("rank", "int"),
    ("rank_delta", "int"),
    ("reviews_posted", "long"),
    ("est_revenue_snapshot", "double"),
]

# Grain: game_id x month, Steam subset only. All est_ columns are estimates —
# clearly labeled so Genie/dashboards never present them as ground truth.
GOLD_REVENUE_ENGAGEMENT_COLUMNS: list[tuple[str, str]] = [
    ("game_id", "string"),
    ("month", "string"),
    ("est_revenue_lifetime_usd", "double"),
    ("est_units_lifetime", "long"),
    ("reviews_posted", "long"),
    ("twitch_avg_viewers", "double"),
    ("revenue_per_avg_viewer", "double"),
]


def spark_struct(columns: list[tuple[str, str]]):  # pragma: no cover - Databricks-only
    """Build a PySpark StructType from one of the column lists above.

    Imported lazily so this module stays usable without PySpark installed.
    """
    from pyspark.sql import types as T  # type: ignore

    type_map = {
        "string": T.StringType(),
        "long": T.LongType(),
        "int": T.IntegerType(),
        "double": T.DoubleType(),
        "boolean": T.BooleanType(),
        "timestamp": T.TimestampType(),
        "date": T.DateType(),
        "array<string>": T.ArrayType(T.StringType()),
    }
    return T.StructType(
        [T.StructField(name, type_map[t], nullable=True) for name, t in columns]
    )
