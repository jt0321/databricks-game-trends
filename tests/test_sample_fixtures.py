"""End-to-end check that the hand-written sample data flows through the
transforms exactly as documented in `data/sample/expected_*.json`.

If this test starts failing, either the transforms changed (update the
expected files) or the fixtures drifted (fix the JSONL).
"""

from __future__ import annotations

import json
from pathlib import Path

from game_trends.transforms import (
    gold_genre_trends,
    gold_platform_trends,
    steam_payload_to_silver,
)


SAMPLE = Path(__file__).resolve().parent.parent / "data" / "sample"


def _load_jsonl(path: Path) -> list[dict]:
    with path.open() as f:
        return [json.loads(line) for line in f if line.strip()]


def _silver_from_sample() -> list[dict]:
    bronze = _load_jsonl(SAMPLE / "steam_apps.jsonl")
    rows = []
    for b in bronze:
        row = steam_payload_to_silver(b["payload"])
        if row is not None:
            rows.append(row)
    return rows


def test_sample_silver_matches_expected():
    expected = json.loads((SAMPLE / "expected_silver.json").read_text())
    actual = _silver_from_sample()
    # Compare as appid → row (order-insensitive)
    assert {r["appid"]: r for r in actual} == {r["appid"]: r for r in expected}


def test_sample_gold_platform_matches_expected():
    silver = _silver_from_sample()
    expected = json.loads((SAMPLE / "expected_gold_platform.json").read_text())
    actual = gold_platform_trends(silver)
    key = lambda r: (r["release_year"], r["platform"])  # noqa: E731
    assert sorted(actual, key=key) == sorted(expected, key=key)


def test_sample_gold_genre_matches_expected():
    silver = _silver_from_sample()
    expected = json.loads((SAMPLE / "expected_gold_genre.json").read_text())
    actual = gold_genre_trends(silver)
    key = lambda r: (r["release_year"], r["primary_genre"])  # noqa: E731
    assert sorted(actual, key=key) == sorted(expected, key=key)
