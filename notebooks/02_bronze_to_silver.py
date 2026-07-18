# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze → Silver
# MAGIC
# MAGIC Parses raw Steam Store payloads from `bronze_game_events`, attaches the
# MAGIC canonical `game_id` identity from the seed crosswalk
# MAGIC (`data/seed/seed_titles.csv`), and MERGEs the result into `silver_titles`
# MAGIC on `game_id`.
# MAGIC
# MAGIC `silver_titles` is the **cross-platform title dimension**: every seeded
# MAGIC title gets a row even without Steam data (off-Steam titles have
# MAGIC `steam_appid IS NULL`), and Steam titles outside the seed flow through
# MAGIC with a slugified `game_id`.
# MAGIC
# MAGIC Re-running this notebook is idempotent — Silver always reflects the
# MAGIC latest payload per title.
# MAGIC
# MAGIC > **Migration note**: the table key changed from `appid` to `game_id`.
# MAGIC > If you have a pre-existing `silver_titles` from the Steam-only schema,
# MAGIC > drop it once before running: `DROP TABLE <catalog>.<schema>.silver_titles`.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "game_trends")
dbutils.widgets.text("seed_path", "../data/seed/seed_titles.csv")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SEED_PATH = dbutils.widgets.get("seed_path")
BRONZE = f"{CATALOG}.{SCHEMA}.bronze_game_events"
SILVER = f"{CATALOG}.{SCHEMA}.silver_titles"

# COMMAND ----------

# Repos puts `src/` on the path automatically when using "Files in Repos".
# If you copy this notebook outside a Repo, %pip install -e the project first.
import sys
sys.path.insert(0, "/Workspace/Repos")  # safe no-op if not a Repo

from game_trends.transforms import (  # noqa: E402
    build_silver_titles,
    load_seed_titles,
    steam_payload_to_silver,
)
from game_trends.schema import SILVER_COLUMNS, spark_struct  # noqa: E402

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

bronze_df = (
    spark.table(BRONZE)
    .filter(F.col("source") == "steam_store")
    .select("payload", "ingested_at", "source")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Steam payloads via mapInPandas
# MAGIC
# MAGIC We keep the transform logic in `game_trends.transforms` so it's covered
# MAGIC by the local pytest suite. Spark just provides the parallelism.

# COMMAND ----------

steam_schema = T.StructType(
    [
        T.StructField("appid", T.LongType()),
        T.StructField("name", T.StringType()),
        T.StructField("release_year", T.IntegerType()),
        T.StructField("is_free", T.BooleanType()),
        T.StructField("price_cents", T.IntegerType()),
        T.StructField("supports_windows", T.BooleanType()),
        T.StructField("supports_mac", T.BooleanType()),
        T.StructField("supports_linux", T.BooleanType()),
        T.StructField("primary_genre", T.StringType()),
        T.StructField("genre_list", T.ArrayType(T.StringType())),
        T.StructField("ingested_at", T.TimestampType()),
    ]
)


def to_steam_rows(iter_pdf):
    import pandas as pd
    for pdf in iter_pdf:
        rows = []
        for _, r in pdf.iterrows():
            row = steam_payload_to_silver(r["payload"])
            if row is None:
                continue
            row["ingested_at"] = r["ingested_at"]
            rows.append(row)
        yield pd.DataFrame(rows, columns=[f.name for f in steam_schema.fields])


steam_df = bronze_df.mapInPandas(to_steam_rows, schema=steam_schema).dropna(subset=["appid"])

# Keep the latest ingested row per appid
window_spec = (
    F.row_number()
    .over(F.expr("PARTITION BY appid ORDER BY ingested_at DESC"))  # type: ignore[arg-type]
)
steam_df = steam_df.withColumn("_rn", window_spec).filter(F.col("_rn") == 1).drop("_rn")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attach canonical identity from the seed crosswalk
# MAGIC
# MAGIC The tracked universe is a curated seed list (a few hundred titles at
# MAGIC most), so identity assembly runs driver-side with the same pure-Python
# MAGIC function the tests exercise.

# COMMAND ----------

from datetime import datetime, timezone

seed_rows = load_seed_titles(SEED_PATH)
steam_rows = [r.asDict() for r in steam_df.drop("ingested_at").collect()]

silver_rows = build_silver_titles(steam_rows, seed_rows)
now = datetime.now(timezone.utc)
for r in silver_rows:
    r["source"] = "steam_store" if r["steam_appid"] is not None else "seed_titles"
    r["ingested_at"] = now

silver_df = spark.createDataFrame(silver_rows, schema=spark_struct(SILVER_COLUMNS))
display(silver_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE into Silver

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {SILVER} (
        game_id STRING,
        name STRING,
        steam_appid LONG,
        on_steam BOOLEAN,
        storefronts ARRAY<STRING>,
        release_year INT,
        is_free BOOLEAN,
        price_cents INT,
        supports_windows BOOLEAN,
        supports_mac BOOLEAN,
        supports_linux BOOLEAN,
        primary_genre STRING,
        genre_list ARRAY<STRING>,
        twitch_category STRING,
        wikipedia_article STRING,
        subreddit STRING,
        trends_term STRING,
        source STRING,
        ingested_at TIMESTAMP
    ) USING DELTA
    """
)

silver_df.createOrReplaceTempView("_silver_batch")

# Identity and crosswalk columns are seed-owned (batch wins); Steam-derived
# metadata coalesces so a seed stub with NULL metadata never clobbers a row a
# previous batch populated from a real payload.
spark.sql(
    f"""
    MERGE INTO {SILVER} t
    USING _silver_batch s
    ON t.game_id = s.game_id
    WHEN MATCHED THEN UPDATE SET
        t.name = s.name,
        t.steam_appid = coalesce(s.steam_appid, t.steam_appid),
        t.on_steam = s.on_steam OR t.on_steam,
        t.storefronts = s.storefronts,
        t.release_year = coalesce(s.release_year, t.release_year),
        t.is_free = coalesce(s.is_free, t.is_free),
        t.price_cents = coalesce(s.price_cents, t.price_cents),
        t.supports_windows = coalesce(s.supports_windows, t.supports_windows),
        t.supports_mac = coalesce(s.supports_mac, t.supports_mac),
        t.supports_linux = coalesce(s.supports_linux, t.supports_linux),
        t.primary_genre = coalesce(s.primary_genre, t.primary_genre),
        t.genre_list = CASE WHEN size(s.genre_list) > 0 THEN s.genre_list ELSE t.genre_list END,
        t.twitch_category = s.twitch_category,
        t.wikipedia_article = s.wikipedia_article,
        t.subreddit = s.subreddit,
        t.trends_term = s.trends_term,
        t.source = s.source,
        t.ingested_at = s.ingested_at
    WHEN NOT MATCHED THEN INSERT *
    """
)

print(f"Merged into {SILVER}")
display(spark.sql(f"SELECT on_steam, COUNT(*) AS titles FROM {SILVER} GROUP BY on_steam"))
