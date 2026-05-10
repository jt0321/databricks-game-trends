# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze → Silver
# MAGIC
# MAGIC Parses raw Steam Store payloads from `bronze_game_events`, applies the
# MAGIC pure-Python transform in `game_trends.transforms.steam_payload_to_silver`,
# MAGIC and MERGEs the result into `silver_titles` on `appid`.
# MAGIC
# MAGIC Re-running this notebook is idempotent — Silver always reflects the
# MAGIC latest payload per `appid`.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "game_trends")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
BRONZE = f"{CATALOG}.{SCHEMA}.bronze_game_events"
SILVER = f"{CATALOG}.{SCHEMA}.silver_titles"

# COMMAND ----------

# Repos puts `src/` on the path automatically when using "Files in Repos".
# If you copy this notebook outside a Repo, %pip install -e the project first.
import sys
sys.path.insert(0, "/Workspace/Repos")  # safe no-op if not a Repo

from game_trends.transforms import steam_payload_to_silver  # noqa: E402
from game_trends.schema import SILVER_COLUMNS  # noqa: E402

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
# MAGIC ## Apply the pure-Python transform via mapInPandas
# MAGIC
# MAGIC We keep the transform logic in `game_trends.transforms` so it's covered
# MAGIC by the local pytest suite. Spark just provides the parallelism.

# COMMAND ----------

silver_schema = T.StructType(
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
        T.StructField("source", T.StringType()),
        T.StructField("ingested_at", T.TimestampType()),
    ]
)


def to_silver(iter_pdf):
    import pandas as pd
    for pdf in iter_pdf:
        rows = []
        for _, r in pdf.iterrows():
            row = steam_payload_to_silver(r["payload"])
            if row is None:
                continue
            row["source"] = r["source"]
            row["ingested_at"] = r["ingested_at"]
            rows.append(row)
        yield pd.DataFrame(rows, columns=[c for c, _ in SILVER_COLUMNS])


silver_df = bronze_df.mapInPandas(to_silver, schema=silver_schema).dropna(subset=["appid"])

# Keep the latest ingested row per appid
window_spec = (
    F.row_number()
    .over(F.expr("PARTITION BY appid ORDER BY ingested_at DESC"))  # type: ignore[arg-type]
)
silver_df = silver_df.withColumn("_rn", window_spec).filter(F.col("_rn") == 1).drop("_rn")

display(silver_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE into Silver

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {SILVER} (
        appid LONG,
        name STRING,
        release_year INT,
        is_free BOOLEAN,
        price_cents INT,
        supports_windows BOOLEAN,
        supports_mac BOOLEAN,
        supports_linux BOOLEAN,
        primary_genre STRING,
        genre_list ARRAY<STRING>,
        source STRING,
        ingested_at TIMESTAMP
    ) USING DELTA
    """
)

silver_df.createOrReplaceTempView("_silver_batch")

spark.sql(
    f"""
    MERGE INTO {SILVER} t
    USING _silver_batch s
    ON t.appid = s.appid
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

print(f"Merged into {SILVER}")
display(spark.sql(f"SELECT COUNT(*) AS rows FROM {SILVER}"))
