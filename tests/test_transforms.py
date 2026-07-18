"""Tests for the pure-Python silver/gold transforms."""

from __future__ import annotations

import json

import pytest

from pathlib import Path

from game_trends.transforms import (
    build_silver_titles,
    dedupe_silver,
    gold_genre_trends,
    gold_platform_trends,
    load_seed_titles,
    parse_release_year,
    slugify,
    steam_payload_to_silver,
)

SEED_CSV = Path(__file__).resolve().parent.parent / "data" / "seed" / "seed_titles.csv"


def _wrap(appid: int, data: dict) -> str:
    """Wrap a Steam `data` block in the envelope the API actually returns."""
    return json.dumps({str(appid): {"success": True, "data": data}})


@pytest.fixture
def sample_payload() -> str:
    return _wrap(
        220,
        {
            "steam_appid": 220,
            "name": "Half-Life 2",
            "is_free": False,
            "price_overview": {"final": 999, "currency": "USD"},
            "platforms": {"windows": True, "mac": True, "linux": True},
            "genres": [{"id": "1", "description": "Action"}],
            "release_date": {"coming_soon": False, "date": "16 Nov, 2004"},
        },
    )


# ---------------------------------------------------------------------------
# parse_release_year
# ---------------------------------------------------------------------------

class TestParseReleaseYear:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ({"date": "16 Nov, 2004"}, 2004),
            ({"date": "Mar 10, 2017"}, 2017),
            ({"date": "2020-05-01"}, 2020),
            ({"date": "Q4 2023"}, 2023),
            ({"date": "2019"}, 2019),
            ("10 Mar, 2017", 2017),
            (2015, 2015),
        ],
    )
    def test_parses_common_formats(self, raw, expected):
        assert parse_release_year(raw) == expected

    @pytest.mark.parametrize("raw", [None, "", "soon", {"date": ""}, {"date": "TBA"}])
    def test_returns_none_for_unparseable(self, raw):
        assert parse_release_year(raw) is None


# ---------------------------------------------------------------------------
# steam_payload_to_silver
# ---------------------------------------------------------------------------

class TestSteamPayloadToSilver:
    def test_happy_path(self, sample_payload):
        row = steam_payload_to_silver(sample_payload)
        assert row is not None
        assert row["appid"] == 220
        assert row["name"] == "Half-Life 2"
        assert row["release_year"] == 2004
        assert row["is_free"] is False
        assert row["price_cents"] == 999
        assert row["supports_windows"] and row["supports_mac"] and row["supports_linux"]
        assert row["primary_genre"] == "Action"
        assert row["genre_list"] == ["Action"]

    def test_free_to_play_has_zero_price(self):
        payload = _wrap(
            570,
            {
                "steam_appid": 570,
                "name": "Dota 2",
                "is_free": True,
                "platforms": {"windows": True, "mac": True, "linux": True},
                "genres": [{"description": "MOBA"}, {"description": "Free To Play"}],
                "release_date": {"date": "9 Jul, 2013"},
            },
        )
        row = steam_payload_to_silver(payload)
        assert row["is_free"] is True
        assert row["price_cents"] == 0
        assert row["primary_genre"] == "MOBA"
        assert row["genre_list"] == ["MOBA", "Free To Play"]

    def test_missing_genres_fallback_to_unknown(self):
        payload = _wrap(
            1,
            {
                "steam_appid": 1,
                "name": "Empty",
                "is_free": False,
                "platforms": {"windows": True},
                "genres": [],
                "release_date": {"date": "2022"},
            },
        )
        row = steam_payload_to_silver(payload)
        assert row["primary_genre"] == "Unknown"
        assert row["genre_list"] == ["Unknown"]

    def test_unsuccessful_payload_returns_none(self):
        payload = json.dumps({"99999": {"success": False}})
        assert steam_payload_to_silver(payload) is None

    def test_malformed_json_returns_none(self):
        assert steam_payload_to_silver("{not json") is None

    def test_missing_required_fields_returns_none(self):
        # Missing `name`
        payload = _wrap(7, {"steam_appid": 7, "is_free": True, "platforms": {}})
        assert steam_payload_to_silver(payload) is None

    def test_accepts_dict_input(self, sample_payload):
        row = steam_payload_to_silver(json.loads(sample_payload))
        assert row is not None and row["appid"] == 220


# ---------------------------------------------------------------------------
# dedupe_silver
# ---------------------------------------------------------------------------

def test_dedupe_silver_keeps_last():
    rows = [
        {"appid": 1, "name": "old"},
        {"appid": 2, "name": "two"},
        {"appid": 1, "name": "new"},
    ]
    out = dedupe_silver(rows)
    assert {r["appid"] for r in out} == {1, 2}
    assert next(r for r in out if r["appid"] == 1)["name"] == "new"


def test_dedupe_silver_drops_rows_without_appid():
    out = dedupe_silver([{"name": "no id"}, {"appid": 1, "name": "ok"}])
    assert out == [{"appid": 1, "name": "ok"}]


# ---------------------------------------------------------------------------
# Seed crosswalk & canonical titles
# ---------------------------------------------------------------------------

class TestSlugify:
    @pytest.mark.parametrize(
        "name,expected",
        [
            ("Half-Life 2", "half-life-2"),
            ("Baldur's Gate 3", "baldur-s-gate-3"),
            ("PUBG: BATTLEGROUNDS", "pubg-battlegrounds"),
            ("  Rust  ", "rust"),
            ("???", "unknown"),
        ],
    )
    def test_slugs(self, name, expected):
        assert slugify(name) == expected


class TestLoadSeedTitles:
    def test_parses_and_normalizes(self, tmp_path):
        csv_path = tmp_path / "seed.csv"
        csv_path.write_text(
            "game_id,name,steam_appid,twitch_category,wikipedia_article,subreddit,trends_term,storefronts\n"
            "fortnite,Fortnite,,Fortnite,Fortnite,FortNiteBR,Fortnite,epic\n"
            "dota-2,Dota 2,570,Dota 2,Dota_2,DotA2,Dota 2,steam\n"
            ",Missing Id,1,x,y,z,w,steam\n"
        )
        rows = load_seed_titles(csv_path)
        assert [r["game_id"] for r in rows] == ["fortnite", "dota-2"]
        fortnite, dota = rows
        assert fortnite["steam_appid"] is None
        assert fortnite["storefronts"] == ["epic"]
        assert dota["steam_appid"] == 570

    def test_real_seed_csv_is_valid(self):
        rows = load_seed_titles(SEED_CSV)
        assert len(rows) >= 30
        ids = [r["game_id"] for r in rows]
        assert len(ids) == len(set(ids)), "duplicate game_id in seed CSV"
        appids = [r["steam_appid"] for r in rows if r["steam_appid"] is not None]
        assert len(appids) == len(set(appids)), "duplicate steam_appid in seed CSV"
        assert all(r["storefronts"] for r in rows), "every seed row needs storefronts"
        offsteam = [r for r in rows if r["steam_appid"] is None]
        assert len(offsteam) >= 5, "seed should cover off-Steam titles"
        assert all(r["game_id"] == slugify(r["game_id"]) for r in rows), (
            "game_id must be slug-shaped"
        )


class TestBuildSilverTitles:
    SEED = [
        {
            "game_id": "fortnite",
            "name": "Fortnite",
            "steam_appid": None,
            "twitch_category": "Fortnite",
            "wikipedia_article": "Fortnite",
            "subreddit": "FortNiteBR",
            "trends_term": "Fortnite",
            "storefronts": ["epic"],
        },
        {
            "game_id": "dota-2",
            "name": "Dota 2",
            "steam_appid": 570,
            "twitch_category": "Dota 2",
            "wikipedia_article": "Dota_2",
            "subreddit": "DotA2",
            "trends_term": "Dota 2",
            "storefronts": ["steam"],
        },
    ]

    STEAM_DOTA = {
        "appid": 570,
        "name": "Dota 2",
        "release_year": 2013,
        "is_free": True,
        "price_cents": 0,
        "supports_windows": True,
        "supports_mac": True,
        "supports_linux": True,
        "primary_genre": "MOBA",
        "genre_list": ["MOBA"],
    }

    def test_seeded_steam_title_gets_seed_identity(self):
        out = build_silver_titles([self.STEAM_DOTA], self.SEED)
        dota = next(r for r in out if r["game_id"] == "dota-2")
        assert dota["steam_appid"] == 570
        assert dota["on_steam"] is True
        assert dota["release_year"] == 2013
        assert dota["subreddit"] == "DotA2"

    def test_offsteam_seed_title_gets_stub_row(self):
        out = build_silver_titles([self.STEAM_DOTA], self.SEED)
        fortnite = next(r for r in out if r["game_id"] == "fortnite")
        assert fortnite["steam_appid"] is None
        assert fortnite["on_steam"] is False
        assert fortnite["storefronts"] == ["epic"]
        assert fortnite["release_year"] is None
        assert fortnite["supports_windows"] is True  # PC-only universe
        assert fortnite["wikipedia_article"] == "Fortnite"

    def test_seeded_steam_title_without_payload_still_present(self):
        out = build_silver_titles([], self.SEED)
        dota = next(r for r in out if r["game_id"] == "dota-2")
        assert dota["on_steam"] is True
        assert dota["steam_appid"] == 570
        assert dota["release_year"] is None

    def test_unseeded_steam_title_gets_slug_and_name_defaults(self):
        row = dict(self.STEAM_DOTA, appid=999, name="Some Indie Game")
        out = build_silver_titles([row], self.SEED)
        indie = next(r for r in out if r["game_id"] == "some-indie-game")
        assert indie["on_steam"] is True
        assert indie["storefronts"] == ["steam"]
        assert indie["twitch_category"] == "Some Indie Game"
        assert indie["trends_term"] == "Some Indie Game"
        assert indie["wikipedia_article"] is None

    def test_output_sorted_and_unique_by_game_id(self):
        out = build_silver_titles([self.STEAM_DOTA], self.SEED)
        ids = [r["game_id"] for r in out]
        assert ids == sorted(ids)
        assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# Gold aggregations
# ---------------------------------------------------------------------------

SILVER_FIXTURE = [
    {
        "appid": 1,
        "name": "A",
        "release_year": 2020,
        "is_free": True,
        "price_cents": 0,
        "supports_windows": True,
        "supports_mac": False,
        "supports_linux": True,
        "primary_genre": "Action",
    },
    {
        "appid": 2,
        "name": "B",
        "release_year": 2020,
        "is_free": False,
        "price_cents": 1999,
        "supports_windows": True,
        "supports_mac": True,
        "supports_linux": False,
        "primary_genre": "Action",
    },
    {
        "appid": 3,
        "name": "C",
        "release_year": 2020,
        "is_free": False,
        "price_cents": 999,
        "supports_windows": True,
        "supports_mac": False,
        "supports_linux": False,
        "primary_genre": "Indie",
    },
    {
        "appid": 4,
        "name": "D",
        "release_year": 2021,
        "is_free": False,
        "price_cents": 2999,
        "supports_windows": True,
        "supports_mac": True,
        "supports_linux": True,
        "primary_genre": "RPG",
    },
]


class TestGoldPlatformTrends:
    def test_counts_each_platform_independently(self):
        out = gold_platform_trends(SILVER_FIXTURE)
        by_key = {(r["release_year"], r["platform"]): r for r in out}

        # 2020: all 3 support windows, 1 mac, 1 linux
        assert by_key[(2020, "windows")]["title_count"] == 3
        assert by_key[(2020, "mac")]["title_count"] == 1
        assert by_key[(2020, "linux")]["title_count"] == 1

        # 2021: D supports all 3
        assert by_key[(2021, "windows")]["title_count"] == 1
        assert by_key[(2021, "linux")]["title_count"] == 1

    def test_free_pct_and_avg_price(self):
        out = gold_platform_trends(SILVER_FIXTURE)
        windows_2020 = next(r for r in out if r["release_year"] == 2020 and r["platform"] == "windows")
        # 1 free out of 3 titles
        assert windows_2020["free_pct"] == pytest.approx(1 / 3, rel=1e-3)
        # avg of 0, 1999, 999
        assert windows_2020["avg_price_cents"] == pytest.approx((0 + 1999 + 999) / 3, rel=1e-3)

    def test_skips_rows_without_release_year(self):
        rows = SILVER_FIXTURE + [{"appid": 99, "release_year": None, "supports_windows": True}]
        out = gold_platform_trends(rows)
        years = {r["release_year"] for r in out}
        assert None not in years


class TestGoldGenreTrends:
    def test_shares_sum_to_one_per_year(self):
        out = gold_genre_trends(SILVER_FIXTURE)
        for year in {r["release_year"] for r in out}:
            year_rows = [r for r in out if r["release_year"] == year]
            assert sum(r["release_share"] for r in year_rows) == pytest.approx(1.0, abs=1e-3)

    def test_counts_per_genre(self):
        out = gold_genre_trends(SILVER_FIXTURE)
        by_key = {(r["release_year"], r["primary_genre"]): r for r in out}
        assert by_key[(2020, "Action")]["title_count"] == 2
        assert by_key[(2020, "Indie")]["title_count"] == 1
        assert by_key[(2021, "RPG")]["title_count"] == 1
        assert by_key[(2020, "Action")]["release_share"] == pytest.approx(2 / 3, rel=1e-3)

    def test_empty_input_returns_empty(self):
        assert gold_genre_trends([]) == []
