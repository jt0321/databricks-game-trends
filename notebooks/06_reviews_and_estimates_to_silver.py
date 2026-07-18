# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Reviews & sales estimates → Silver
# MAGIC
# MAGIC Two independent jobs sharing a notebook because both are Steam-subset,
# MAGIC append-oriented, and small relative to the engagement snapshot volume:
# MAGIC
# MAGIC 1. **`silver_reviews`** — bronze `steam_reviews` pages deduped by
# MAGIC    `review_id` and filtered to `HISTORY_START`, then rolled up into the
# MAGIC    `reviews_posted` / `reviews_positive_share` columns of
# MAGIC    `silver_engagement_daily` (this job owns those two columns; see `05`
# MAGIC    for the rest).
# MAGIC 2. **`silver_sales_estimates`** — bronze `steamspy_snapshot` /
# MAGIC    `gamalytic` rows parsed into estimate rows. Append-only: every
# MAGIC    snapshot is kept, since the history of estimates is itself the
# MAGIC    time series Gold reads the *latest* value from.
# MAGIC
# MAGIC Re-running is idempotent: reviews dedupe on `review_id`, and sales
# MAGIC estimates MERGE on `(game_id, snapshot_date, est_source)`.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "game_trends")
dbutils.widgets.text("seed_path", "../data/seed/seed_titles.csv")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SEED_PATH = dbutils.widgets.get("seed_path")
BRONZE = f"{CATALOG}.{SCHEMA}.bronze_game_events"
SILVER_REVIEWS = f"{CATALOG}.{SCHEMA}.silver_reviews"
SILVER_ENGAGEMENT = f"{CATALOG}.{SCHEMA}.silver_engagement_daily"
SILVER_ESTIMATES = f"{CATALOG}.{SCHEMA}.silver_sales_estimates"

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos")  # safe no-op if not a Repo

from game_trends.transforms import (  # noqa: E402
    build_reviews_silver,
    build_sales_estimates,
    load_seed_titles,
    reviews_daily_from_silver,
)
from game_trends.schema import (  # noqa: E402
    SILVER_REVIEWS_COLUMNS,
    SILVER_SALES_ESTIMATES_COLUMNS,
    spark_struct,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reviews: bronze → `silver_reviews`

# COMMAND ----------

from pyspark.sql import functions as F

review_bronze_rows = [
    r.asDict()
    for r in (
        spark.table(BRONZE)
        .filter(F.col("source") == "steam_reviews")
        .select("source", "entity_key", "extract_date", "payload")
        .collect()
    )
]

seed_rows = load_seed_titles(SEED_PATH)
review_rows = build_reviews_silver(review_bronze_rows, seed_rows)
print(f"{len(review_bronze_rows)} bronze pages → {len(review_rows)} deduped reviews")

reviews_df = spark.createDataFrame(review_rows, schema=spark_struct(SILVER_REVIEWS_COLUMNS))

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {SILVER_REVIEWS} (
        game_id STRING,
        review_id STRING,
        review_ts LONG,
        voted_up BOOLEAN,
        playtime_at_review_min LONG,
        language STRING,
        received_free BOOLEAN
    ) USING DELTA
    """
)

reviews_df.createOrReplaceTempView("_reviews_batch")

spark.sql(
    f"""
    MERGE INTO {SILVER_REVIEWS} t
    USING _reviews_batch s
    ON t.review_id = s.review_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

print(f"Merged into {SILVER_REVIEWS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reviews → daily rollup → `silver_engagement_daily`
# MAGIC
# MAGIC Runs on the full, already-deduped `silver_reviews` table (not raw
# MAGIC bronze pages), so overlapping backfill pagination never double-counts.

# COMMAND ----------

all_reviews = [r.asDict() for r in spark.table(SILVER_REVIEWS).collect()]
daily_review_rows = reviews_daily_from_silver(all_reviews)

for r in daily_review_rows:
    from datetime import date as _date
    r["date"] = _date.fromisoformat(r["date"])

if daily_review_rows:
    daily_reviews_df = spark.createDataFrame(
        daily_review_rows, schema="game_id STRING, date DATE, reviews_posted LONG, reviews_positive_share DOUBLE"
    )
    daily_reviews_df.createOrReplaceTempView("_reviews_daily_batch")

    spark.sql(
        f"""
        MERGE INTO {SILVER_ENGAGEMENT} t
        USING _reviews_daily_batch s
        ON t.game_id = s.game_id AND t.date = s.date
        WHEN MATCHED THEN UPDATE SET
            t.reviews_posted = s.reviews_posted,
            t.reviews_positive_share = s.reviews_positive_share
        WHEN NOT MATCHED THEN INSERT (game_id, date, reviews_posted, reviews_positive_share)
            VALUES (s.game_id, s.date, s.reviews_posted, s.reviews_positive_share)
        """
    )
    print(f"Merged {len(daily_review_rows)} daily review rollups into {SILVER_ENGAGEMENT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales estimates: bronze → `silver_sales_estimates`

# COMMAND ----------

estimates_bronze_rows = [
    r.asDict()
    for r in (
        spark.table(BRONZE)
        .filter(F.col("source").isin("steamspy_snapshot", "gamalytic"))
        .select("source", "entity_key", "extract_date", "payload")
        .collect()
    )
]

estimate_rows = build_sales_estimates(estimates_bronze_rows, seed_rows)
print(f"{len(estimates_bronze_rows)} bronze snapshots → {len(estimate_rows)} estimate rows")

for r in estimate_rows:
    from datetime import date as _date
    r["snapshot_date"] = _date.fromisoformat(r["snapshot_date"])

estimates_df = spark.createDataFrame(estimate_rows, schema=spark_struct(SILVER_SALES_ESTIMATES_COLUMNS))

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {SILVER_ESTIMATES} (
        game_id STRING,
        snapshot_date DATE,
        est_owners_low LONG,
        est_owners_high LONG,
        est_units_lifetime LONG,
        est_revenue_lifetime_usd DOUBLE,
        est_source STRING
    ) USING DELTA
    """
)

estimates_df.createOrReplaceTempView("_estimates_batch")

spark.sql(
    f"""
    MERGE INTO {SILVER_ESTIMATES} t
    USING _estimates_batch s
    ON t.game_id = s.game_id AND t.snapshot_date = s.snapshot_date AND t.est_source = s.est_source
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

print(f"Merged into {SILVER_ESTIMATES}")
display(spark.sql(f"SELECT est_source, COUNT(*) AS snapshots FROM {SILVER_ESTIMATES} GROUP BY est_source"))
