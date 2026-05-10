# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver → Gold
# MAGIC
# MAGIC Builds two business-friendly Gold tables from `silver_titles`:
# MAGIC
# MAGIC | Table | Grain |
# MAGIC | --- | --- |
# MAGIC | `gold_platform_trends` | release_year × platform (windows/mac/linux) |
# MAGIC | `gold_genre_trends` | release_year × primary_genre |
# MAGIC
# MAGIC Both are rebuilt as `CREATE OR REPLACE TABLE` for simplicity — the
# MAGIC source-of-truth is Silver, so the rebuild is cheap and unambiguous.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "game_trends")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SILVER = f"{CATALOG}.{SCHEMA}.silver_titles"
GOLD_PLATFORM = f"{CATALOG}.{SCHEMA}.gold_platform_trends"
GOLD_GENRE = f"{CATALOG}.{SCHEMA}.gold_genre_trends"

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_platform_trends
# MAGIC
# MAGIC A title can support multiple platforms, so we UNION ALL the three flags
# MAGIC and aggregate. This mirrors `transforms.gold_platform_trends`.

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {GOLD_PLATFORM}
    USING DELTA
    PARTITIONED BY (release_year)
    AS
    WITH unpivoted AS (
        SELECT release_year, 'windows' AS platform, is_free, price_cents
        FROM {SILVER} WHERE supports_windows AND release_year IS NOT NULL
        UNION ALL
        SELECT release_year, 'mac', is_free, price_cents
        FROM {SILVER} WHERE supports_mac AND release_year IS NOT NULL
        UNION ALL
        SELECT release_year, 'linux', is_free, price_cents
        FROM {SILVER} WHERE supports_linux AND release_year IS NOT NULL
    )
    SELECT
        release_year,
        platform,
        COUNT(*)                                              AS title_count,
        ROUND(AVG(CASE WHEN is_free THEN 1.0 ELSE 0.0 END), 4) AS free_pct,
        ROUND(AVG(price_cents), 2)                             AS avg_price_cents
    FROM unpivoted
    GROUP BY release_year, platform
    """
)

display(spark.table(GOLD_PLATFORM).orderBy("release_year", "platform"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_genre_trends

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {GOLD_GENRE}
    USING DELTA
    PARTITIONED BY (release_year)
    AS
    WITH yearly_totals AS (
        SELECT release_year, COUNT(*) AS total FROM {SILVER}
        WHERE release_year IS NOT NULL
        GROUP BY release_year
    ),
    by_genre AS (
        SELECT
            release_year,
            COALESCE(primary_genre, 'Unknown') AS primary_genre,
            COUNT(*)                  AS title_count,
            ROUND(AVG(price_cents), 2) AS avg_price_cents
        FROM {SILVER}
        WHERE release_year IS NOT NULL
        GROUP BY release_year, COALESCE(primary_genre, 'Unknown')
    )
    SELECT
        g.release_year,
        g.primary_genre,
        g.title_count,
        ROUND(g.title_count / y.total, 4) AS release_share,
        g.avg_price_cents
    FROM by_genre g
    JOIN yearly_totals y USING (release_year)
    """
)

from pyspark.sql import functions as F  # noqa: E402

display(spark.table(GOLD_GENRE).orderBy("release_year", F.desc("release_share")))
