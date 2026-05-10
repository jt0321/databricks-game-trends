"""Canonical schemas for the Game Trend Lakehouse.

These are deliberately defined as plain Python so they can be imported by both
the local CLI / tests and the Databricks notebooks (where they are wrapped in
PySpark StructType objects).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------

BRONZE_COLUMNS: list[tuple[str, str]] = [
    ("source", "string"),
    ("ingested_at", "timestamp"),
    ("batch_id", "string"),
    ("payload", "string"),
]


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------

SILVER_COLUMNS: list[tuple[str, str]] = [
    ("appid", "long"),
    ("name", "string"),
    ("release_year", "int"),
    ("is_free", "boolean"),
    ("price_cents", "int"),
    ("supports_windows", "boolean"),
    ("supports_mac", "boolean"),
    ("supports_linux", "boolean"),
    ("primary_genre", "string"),
    ("genre_list", "array<string>"),
    ("source", "string"),
    ("ingested_at", "timestamp"),
]


@dataclass(frozen=True)
class SilverTitle:
    """One canonical row in `silver_titles`."""

    appid: int
    name: str
    release_year: int | None
    is_free: bool
    price_cents: int
    supports_windows: bool
    supports_mac: bool
    supports_linux: bool
    primary_genre: str
    genre_list: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "appid": self.appid,
            "name": self.name,
            "release_year": self.release_year,
            "is_free": self.is_free,
            "price_cents": self.price_cents,
            "supports_windows": self.supports_windows,
            "supports_mac": self.supports_mac,
            "supports_linux": self.supports_linux,
            "primary_genre": self.primary_genre,
            "genre_list": list(self.genre_list),
        }


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
        "array<string>": T.ArrayType(T.StringType()),
    }
    return T.StructType(
        [T.StructField(name, type_map[t], nullable=True) for name, t in columns]
    )
