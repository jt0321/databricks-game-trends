# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest Steam / SteamSpy → Bronze
# MAGIC
# MAGIC Reads raw JSONL produced by the local extractor (or by a Workflow task
# MAGIC that writes to a Unity Catalog Volume) and appends it to
# MAGIC `bronze_game_events` as immutable rows tagged with `source`,
# MAGIC `ingested_at`, and `batch_id`.
# MAGIC
# MAGIC **Inputs**
# MAGIC - `/Volumes/<catalog>/<schema>/landing/steam_apps.jsonl`
# MAGIC - `/Volumes/<catalog>/<schema>/landing/steamspy_top.jsonl`
# MAGIC
# MAGIC **Output**
# MAGIC - Delta table `${catalog}.${schema}.bronze_game_events`

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "game_trends")
dbutils.widgets.text(
    "landing_path",
    "/Volumes/main/game_trends/landing",
    "Folder containing *.jsonl raw files",
)

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LANDING = dbutils.widgets.get("landing_path")
BRONZE = f"{CATALOG}.{SCHEMA}.bronze_game_events"

# COMMAND ----------

import uuid
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

batch_id = str(uuid.uuid4())
print(f"batch_id = {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read raw JSONL
# MAGIC
# MAGIC Each line is already shaped as `{source, ingested_at, payload}` by the
# MAGIC local extractor, so Bronze is essentially a structured append.

# COMMAND ----------

raw_schema = T.StructType(
    [
        T.StructField("source", T.StringType()),
        T.StructField("ingested_at", T.StringType()),  # parsed below
        T.StructField("payload", T.StringType()),
    ]
)

raw_df = (
    spark.read.schema(raw_schema)
    .json(f"{LANDING}/*.jsonl")
    .withColumn("ingested_at", F.to_timestamp("ingested_at"))
    .withColumn("batch_id", F.lit(batch_id))
    .select("source", "ingested_at", "batch_id", "payload")
)

display(raw_df.limit(5))
print(f"Rows to append: {raw_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append to Bronze (create if missing)

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {BRONZE} (
        source STRING,
        ingested_at TIMESTAMP,
        batch_id STRING,
        payload STRING
    ) USING DELTA
    """
)

(
    raw_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(BRONZE)
)

print(f"Appended batch {batch_id} to {BRONZE}")
display(spark.sql(f"SELECT source, COUNT(*) AS n FROM {BRONZE} GROUP BY source"))
