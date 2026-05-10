-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 — Genie Demo Questions
-- MAGIC
-- MAGIC Each section pairs a natural-language question (what a stakeholder
-- MAGIC would ask Genie) with the SQL we expect Genie to generate against the
-- MAGIC gold tables. Use this file to:
-- MAGIC
-- MAGIC 1. **Seed a Genie space** — add the questions as starter prompts.
-- MAGIC 2. **Evaluate Genie** — paste each NL question in, compare the SQL it
-- MAGIC    produces against the reference below.
-- MAGIC 3. **Back a dashboard** — every query stands alone as a tile.
-- MAGIC
-- MAGIC Replace `main.game_trends` with your catalog/schema if different.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q1. What were the top 5 genres by number of releases last year?

SELECT primary_genre, title_count
FROM main.game_trends.gold_genre_trends
WHERE release_year = (SELECT MAX(release_year) FROM main.game_trends.gold_genre_trends)
ORDER BY title_count DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q2. Compare average price of free vs paid titles by year.

SELECT
    release_year,
    ROUND(AVG(CASE WHEN is_free THEN price_cents END) / 100.0, 2)        AS avg_price_free_usd,
    ROUND(AVG(CASE WHEN NOT is_free THEN price_cents END) / 100.0, 2)    AS avg_price_paid_usd,
    COUNT(*)                                                             AS titles
FROM main.game_trends.silver_titles
WHERE release_year IS NOT NULL
GROUP BY release_year
ORDER BY release_year;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q3. Which platforms saw the largest growth in release share between 2018 and 2024?

WITH share_by_year AS (
    SELECT
        release_year,
        platform,
        title_count * 1.0 / SUM(title_count) OVER (PARTITION BY release_year) AS share
    FROM main.game_trends.gold_platform_trends
)
SELECT
    platform,
    ROUND(MAX(CASE WHEN release_year = 2024 THEN share END) -
          MAX(CASE WHEN release_year = 2018 THEN share END), 4) AS share_delta
FROM share_by_year
WHERE release_year IN (2018, 2024)
GROUP BY platform
ORDER BY share_delta DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q4. Show me the genre mix for free-to-play titles only.

SELECT
    primary_genre,
    COUNT(*)                                          AS titles,
    ROUND(COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (), 4)  AS share
FROM main.game_trends.silver_titles
WHERE is_free
GROUP BY primary_genre
ORDER BY titles DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q5. Which release year had the most Linux-supporting titles?

SELECT release_year, title_count
FROM main.game_trends.gold_platform_trends
WHERE platform = 'linux'
ORDER BY title_count DESC
LIMIT 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q6. (Bonus) Year-over-year release count for the action genre.

SELECT
    release_year,
    title_count,
    title_count - LAG(title_count) OVER (ORDER BY release_year) AS yoy_change
FROM main.game_trends.gold_genre_trends
WHERE primary_genre = 'Action'
ORDER BY release_year;
