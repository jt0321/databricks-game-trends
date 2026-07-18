# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Cross-platform Gold: attention, cohort split, leaderboard, revenue
# MAGIC
# MAGIC Builds the four Gold tables that answer the project's original
# MAGIC question — how much PC gaming attention (and, for the Steam subset,
# MAGIC revenue) sits outside the Steam catalog:
# MAGIC
# MAGIC | Table | Grain | Answers |
# MAGIC | --- | --- | --- |
# MAGIC | `gold_attention_trends` | game_id × week | Is this title trending, relative to its own history? |
# MAGIC | `gold_steam_vs_offsteam` | week × cohort | What share of attention goes to games not on Steam? |
# MAGIC | `gold_title_monthly` | game_id × month | Leaderboard: rank and rank movement |
# MAGIC | `gold_revenue_engagement` | game_id × month (Steam only) | Does estimated revenue track viewership? |
# MAGIC
# MAGIC All four are pure-Python aggregations (`game_trends.transforms`) run
# MAGIC driver-side over the Silver tables — the curated universe is small
# MAGIC enough (a few hundred titles × a few years of daily rows) that this is
# MAGIC comfortably within driver memory, and it keeps the exact same logic
# MAGIC covered by the local pytest suite. Each table is a `CREATE OR REPLACE`
# MAGIC rebuild from Silver, same as `03_gold_metrics.py`.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "game_trends")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SILVER_TITLES = f"{CATALOG}.{SCHEMA}.silver_titles"
SILVER_ENGAGEMENT = f"{CATALOG}.{SCHEMA}.silver_engagement_daily"
SILVER_ESTIMATES = f"{CATALOG}.{SCHEMA}.silver_sales_estimates"
GOLD_ATTENTION = f"{CATALOG}.{SCHEMA}.gold_attention_trends"
GOLD_COHORT = f"{CATALOG}.{SCHEMA}.gold_steam_vs_offsteam"
GOLD_TITLE_MONTHLY = f"{CATALOG}.{SCHEMA}.gold_title_monthly"
GOLD_REVENUE = f"{CATALOG}.{SCHEMA}.gold_revenue_engagement"

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos")  # safe no-op if not a Repo

from game_trends.transforms import (  # noqa: E402
    gold_attention_trends,
    gold_revenue_engagement,
    gold_steam_vs_offsteam,
    gold_title_monthly,
)
from game_trends.schema import (  # noqa: E402
    GOLD_ATTENTION_COLUMNS,
    GOLD_REVENUE_ENGAGEMENT_COLUMNS,
    GOLD_STEAM_VS_OFFSTEAM_COLUMNS,
    GOLD_TITLE_MONTHLY_COLUMNS,
    spark_struct,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver driver-side

# COMMAND ----------

titles = [r.asDict() for r in spark.table(SILVER_TITLES).collect()]
engagement_daily = [r.asDict() for r in spark.table(SILVER_ENGAGEMENT).collect()]
for r in engagement_daily:
    r["date"] = r["date"].isoformat() if r["date"] else None

try:
    sales_estimates = [r.asDict() for r in spark.table(SILVER_ESTIMATES).collect()]
    for r in sales_estimates:
        r["snapshot_date"] = r["snapshot_date"].isoformat() if r["snapshot_date"] else None
except Exception:
    sales_estimates = []  # table not yet created (no estimate sources run yet)

print(f"{len(titles)} titles, {len(engagement_daily)} daily engagement rows, {len(sales_estimates)} estimate snapshots")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_attention_trends

# COMMAND ----------

attention_rows = gold_attention_trends(engagement_daily)
attention_df = spark.createDataFrame(attention_rows, schema=spark_struct(GOLD_ATTENTION_COLUMNS))
attention_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_ATTENTION)
display(spark.table(GOLD_ATTENTION).orderBy("game_id", "week").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_steam_vs_offsteam

# COMMAND ----------

cohort_rows = gold_steam_vs_offsteam(titles, engagement_daily)
cohort_df = spark.createDataFrame(cohort_rows, schema=spark_struct(GOLD_STEAM_VS_OFFSTEAM_COLUMNS))
cohort_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_COHORT)
display(spark.table(GOLD_COHORT).orderBy("week", "cohort"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_title_monthly

# COMMAND ----------

from pyspark.sql import functions as F

monthly_rows = gold_title_monthly(engagement_daily, sales_estimates)
monthly_df = spark.createDataFrame(monthly_rows, schema=spark_struct(GOLD_TITLE_MONTHLY_COLUMNS))
monthly_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_TITLE_MONTHLY)
display(spark.table(GOLD_TITLE_MONTHLY).orderBy("month", F.asc_nulls_last("rank")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_revenue_engagement
# MAGIC
# MAGIC Steam subset only. Every `est_` column is an estimate (SteamSpy owner
# MAGIC tiers or Gamalytic modeling), never ground truth.

# COMMAND ----------

revenue_rows = gold_revenue_engagement(titles, engagement_daily, sales_estimates)
revenue_df = spark.createDataFrame(revenue_rows, schema=spark_struct(GOLD_REVENUE_ENGAGEMENT_COLUMNS))
revenue_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_REVENUE)
display(spark.table(GOLD_REVENUE).orderBy(F.desc_nulls_last("revenue_per_avg_viewer")).limit(20))
