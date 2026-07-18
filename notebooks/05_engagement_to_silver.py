# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Engagement snapshots → `silver_engagement_daily`
# MAGIC
# MAGIC Rolls every bronze snapshot source — `twitch_helix`, `steam_ccu`,
# MAGIC `wikipedia_pageviews`, `google_trends`, `reddit` — up to one row per
# MAGIC `game_id` × `date` and MERGEs them into `silver_engagement_daily`, the
# MAGIC wide cross-platform engagement fact table. (`steam_reviews` is handled by
# MAGIC notebook `06`, since review-level dedup has to happen before daily
# MAGIC aggregation — see `reviews_daily_from_silver`.)
# MAGIC
# MAGIC The table is **column-owned by source**: this notebook writes only the
# MAGIC columns these five sources produce; the review job (`06`) fills
# MAGIC `reviews_posted` / `reviews_positive_share` via the same (game_id, date)
# MAGIC MERGE pattern without touching these. Off-Steam titles simply never
# MAGIC receive Steam columns, and days before a source's first poll/backfill
# MAGIC stay NULL for that source's columns.
# MAGIC
# MAGIC Re-running is idempotent: the rollup recomputes each (game_id, date) from
# MAGIC the full bronze history for these five sources.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "game_trends")
dbutils.widgets.text("seed_path", "../data/seed/seed_titles.csv")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SEED_PATH = dbutils.widgets.get("seed_path")
BRONZE = f"{CATALOG}.{SCHEMA}.bronze_game_events"
SILVER_ENGAGEMENT = f"{CATALOG}.{SCHEMA}.silver_engagement_daily"

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos")  # safe no-op if not a Repo

from game_trends.transforms import build_engagement_daily, load_seed_titles  # noqa: E402
from game_trends.schema import SILVER_ENGAGEMENT_COLUMNS, spark_struct  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Roll snapshots up to game_id × date
# MAGIC
# MAGIC Snapshot volume is small (seed universe × polls/day), so the rollup runs
# MAGIC driver-side with the same pure-Python function the tests exercise.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F

SNAPSHOT_SOURCES = ["twitch_helix", "steam_ccu", "wikipedia_pageviews", "google_trends", "reddit"]

bronze_rows = [
    r.asDict()
    for r in (
        spark.table(BRONZE)
        .filter(F.col("source").isin(*SNAPSHOT_SOURCES))
        .select("source", "entity_key", "extract_date", "payload")
        .collect()
    )
]

seed_rows = load_seed_titles(SEED_PATH)
daily_rows = build_engagement_daily(bronze_rows, seed_rows)

now = datetime.now(timezone.utc)
for r in daily_rows:
    r["ingested_at"] = now

print(f"{len(bronze_rows)} snapshots → {len(daily_rows)} daily rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE into `silver_engagement_daily`

# COMMAND ----------

from datetime import date as _date

for r in daily_rows:
    r["date"] = _date.fromisoformat(r["date"])

daily_df = spark.createDataFrame(daily_rows, schema=spark_struct(SILVER_ENGAGEMENT_COLUMNS))

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {SILVER_ENGAGEMENT} (
        game_id STRING,
        date DATE,
        twitch_avg_viewers DOUBLE,
        twitch_peak_viewers LONG,
        twitch_avg_channels DOUBLE,
        wiki_pageviews LONG,
        trends_index DOUBLE,
        reddit_posts LONG,
        reddit_subscribers LONG,
        steam_ccu_peak LONG,
        reviews_posted LONG,
        reviews_positive_share DOUBLE,
        ingested_at TIMESTAMP
    ) USING DELTA
    """
)

daily_df.createOrReplaceTempView("_engagement_batch")

# Update ONLY the columns this job owns (reviews_posted / reviews_positive_share
# belong to notebook 06) — other sources' columns are never touched, so job
# order across sources doesn't matter.
spark.sql(
    f"""
    MERGE INTO {SILVER_ENGAGEMENT} t
    USING _engagement_batch s
    ON t.game_id = s.game_id AND t.date = s.date
    WHEN MATCHED THEN UPDATE SET
        t.twitch_avg_viewers = coalesce(s.twitch_avg_viewers, t.twitch_avg_viewers),
        t.twitch_peak_viewers = coalesce(s.twitch_peak_viewers, t.twitch_peak_viewers),
        t.twitch_avg_channels = coalesce(s.twitch_avg_channels, t.twitch_avg_channels),
        t.wiki_pageviews = coalesce(s.wiki_pageviews, t.wiki_pageviews),
        t.trends_index = coalesce(s.trends_index, t.trends_index),
        t.reddit_posts = coalesce(s.reddit_posts, t.reddit_posts),
        t.reddit_subscribers = coalesce(s.reddit_subscribers, t.reddit_subscribers),
        t.steam_ccu_peak = coalesce(s.steam_ccu_peak, t.steam_ccu_peak),
        t.ingested_at = s.ingested_at
    WHEN NOT MATCHED THEN INSERT *
    """
)

print(f"Merged into {SILVER_ENGAGEMENT}")
display(
    spark.sql(
        f"""
        SELECT date,
               COUNT(*) AS titles,
               SUM(CASE WHEN twitch_avg_viewers IS NOT NULL THEN 1 ELSE 0 END) AS with_twitch,
               SUM(CASE WHEN steam_ccu_peak IS NOT NULL THEN 1 ELSE 0 END) AS with_ccu,
               SUM(CASE WHEN wiki_pageviews IS NOT NULL THEN 1 ELSE 0 END)   AS with_wiki,
               SUM(CASE WHEN trends_index IS NOT NULL THEN 1 ELSE 0 END)    AS with_trends,
               SUM(CASE WHEN reddit_subscribers IS NOT NULL THEN 1 ELSE 0 END) AS with_reddit
        FROM {SILVER_ENGAGEMENT}
        GROUP BY date ORDER BY date DESC LIMIT 14
        """
    )
)
