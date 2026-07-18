"""Smoke tests on the canonical schema definitions."""

from __future__ import annotations

from game_trends.schema import (
    BRONZE_COLUMNS,
    GOLD_ATTENTION_COLUMNS,
    GOLD_GENRE_COLUMNS,
    GOLD_PLATFORM_COLUMNS,
    GOLD_REVENUE_ENGAGEMENT_COLUMNS,
    GOLD_STEAM_VS_OFFSTEAM_COLUMNS,
    GOLD_TITLE_MONTHLY_COLUMNS,
    HISTORY_START,
    SEED_COLUMNS,
    SILVER_COLUMNS,
    SILVER_ENGAGEMENT_COLUMNS,
    SILVER_REVIEWS_COLUMNS,
    SILVER_SALES_ESTIMATES_COLUMNS,
    SilverTitle,
)


def test_bronze_has_provenance_columns():
    names = {c for c, _ in BRONZE_COLUMNS}
    assert {"source", "ingested_at", "batch_id", "payload"} <= names


def test_silver_contains_expected_columns():
    names = {c for c, _ in SILVER_COLUMNS}
    expected = {
        "game_id",
        "name",
        "steam_appid",
        "on_steam",
        "storefronts",
        "release_year",
        "is_free",
        "price_cents",
        "supports_windows",
        "supports_mac",
        "supports_linux",
        "primary_genre",
        "genre_list",
        "twitch_category",
        "wikipedia_article",
        "subreddit",
        "trends_term",
    }
    assert expected <= names


def test_seed_columns_cover_crosswalk():
    names = {c for c, _ in SEED_COLUMNS}
    assert {
        "game_id",
        "name",
        "steam_appid",
        "twitch_category",
        "wikipedia_article",
        "subreddit",
        "trends_term",
        "storefronts",
    } == names


def test_gold_platform_columns():
    names = {c for c, _ in GOLD_PLATFORM_COLUMNS}
    assert names == {
        "release_year",
        "platform",
        "title_count",
        "free_pct",
        "avg_price_cents",
    }


def test_gold_genre_columns():
    names = {c for c, _ in GOLD_GENRE_COLUMNS}
    assert names == {
        "release_year",
        "primary_genre",
        "title_count",
        "release_share",
        "avg_price_cents",
    }


def test_silver_title_dataclass_roundtrip():
    t = SilverTitle(
        game_id="half-life-2",
        name="Half-Life 2",
        steam_appid=220,
        on_steam=True,
        storefronts=["steam"],
        release_year=2004,
        is_free=False,
        price_cents=999,
        supports_windows=True,
        supports_mac=True,
        supports_linux=True,
        primary_genre="Action",
        genre_list=["Action"],
    )
    d = t.to_dict()
    assert d["game_id"] == "half-life-2"
    assert d["steam_appid"] == 220
    assert d["genre_list"] == ["Action"]
    # The dataclass field set matches the schema column list (minus provenance).
    schema_names = {c for c, _ in SILVER_COLUMNS} - {"source", "ingested_at"}
    assert schema_names == set(d.keys())


def test_history_start_is_pinned_to_2020():
    assert HISTORY_START == "2020-01-01"


def test_bronze_has_entity_key_and_extract_date():
    names = {c for c, _ in BRONZE_COLUMNS}
    assert {"entity_key", "extract_date"} <= names


def test_silver_engagement_columns_keyed_by_game_id_and_date():
    names = {c for c, _ in SILVER_ENGAGEMENT_COLUMNS}
    assert {"game_id", "date"} <= names
    assert {
        "twitch_avg_viewers",
        "twitch_peak_viewers",
        "twitch_avg_channels",
        "wiki_pageviews",
        "trends_index",
        "reddit_posts",
        "reddit_subscribers",
        "steam_ccu_peak",
        "reviews_posted",
        "reviews_positive_share",
    } <= names


def test_silver_reviews_columns():
    names = {c for c, _ in SILVER_REVIEWS_COLUMNS}
    assert names == {
        "game_id",
        "review_id",
        "review_ts",
        "voted_up",
        "playtime_at_review_min",
        "language",
        "received_free",
    }


def test_silver_sales_estimates_columns():
    names = {c for c, _ in SILVER_SALES_ESTIMATES_COLUMNS}
    assert names == {
        "game_id",
        "snapshot_date",
        "est_owners_low",
        "est_owners_high",
        "est_units_lifetime",
        "est_revenue_lifetime_usd",
        "est_source",
    }


def test_gold_attention_columns():
    names = {c for c, _ in GOLD_ATTENTION_COLUMNS}
    assert {"game_id", "week", "attention_index", "attention_wow_delta"} <= names


def test_gold_steam_vs_offsteam_columns():
    names = {c for c, _ in GOLD_STEAM_VS_OFFSTEAM_COLUMNS}
    assert names == {
        "week",
        "cohort",
        "title_count",
        "twitch_viewer_share",
        "wiki_pageview_share",
        "top20_attention_titles",
    }


def test_gold_title_monthly_columns():
    names = {c for c, _ in GOLD_TITLE_MONTHLY_COLUMNS}
    assert {"game_id", "month", "rank", "rank_delta"} <= names


def test_gold_revenue_engagement_columns():
    names = {c for c, _ in GOLD_REVENUE_ENGAGEMENT_COLUMNS}
    assert {"est_revenue_lifetime_usd", "revenue_per_avg_viewer"} <= names


def test_silver_title_supports_offsteam():
    t = SilverTitle(
        game_id="fortnite",
        name="Fortnite",
        steam_appid=None,
        on_steam=False,
        storefronts=["epic"],
        release_year=None,
        is_free=None,
        price_cents=None,
        supports_windows=True,
        supports_mac=None,
        supports_linux=None,
        primary_genre=None,
        twitch_category="Fortnite",
        wikipedia_article="Fortnite",
        subreddit="FortNiteBR",
        trends_term="Fortnite",
    )
    d = t.to_dict()
    assert d["steam_appid"] is None
    assert d["on_steam"] is False
    assert d["storefronts"] == ["epic"]
