"""Smoke tests on the canonical schema definitions."""

from __future__ import annotations

from game_trends.schema import (
    BRONZE_COLUMNS,
    GOLD_GENRE_COLUMNS,
    GOLD_PLATFORM_COLUMNS,
    SILVER_COLUMNS,
    SilverTitle,
)


def test_bronze_has_provenance_columns():
    names = {c for c, _ in BRONZE_COLUMNS}
    assert {"source", "ingested_at", "batch_id", "payload"} <= names


def test_silver_contains_expected_columns():
    names = {c for c, _ in SILVER_COLUMNS}
    expected = {
        "appid",
        "name",
        "release_year",
        "is_free",
        "price_cents",
        "supports_windows",
        "supports_mac",
        "supports_linux",
        "primary_genre",
        "genre_list",
    }
    assert expected <= names


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
        appid=220,
        name="Half-Life 2",
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
    assert d["appid"] == 220
    assert d["genre_list"] == ["Action"]
    # The dataclass field set matches the schema column list (minus provenance).
    schema_names = {c for c, _ in SILVER_COLUMNS} - {"source", "ingested_at"}
    assert schema_names == set(d.keys())
