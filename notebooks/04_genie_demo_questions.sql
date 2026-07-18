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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q7. What share of PC gaming Twitch hours went to games not sold on
-- MAGIC Steam, by quarter?
-- MAGIC
-- MAGIC `gold_steam_vs_offsteam` is weekly; quarter-bucket it for the trend.

SELECT
    date_trunc('QUARTER', to_date(week)) AS quarter,
    cohort,
    ROUND(AVG(twitch_viewer_share), 4) AS avg_twitch_viewer_share
FROM main.game_trends.gold_steam_vs_offsteam
GROUP BY date_trunc('QUARTER', to_date(week)), cohort
ORDER BY quarter, cohort;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q8. Which non-Steam games had more Twitch viewership than the top
-- MAGIC Steam release this month?

WITH ranked AS (
    SELECT m.*, t.on_steam
    FROM main.game_trends.gold_title_monthly m
    JOIN main.game_trends.silver_titles t USING (game_id)
    WHERE m.month = (SELECT MAX(month) FROM main.game_trends.gold_title_monthly)
),
top_steam AS (
    SELECT MIN(rank) AS best_steam_rank
    FROM ranked WHERE on_steam
)
SELECT game_id, twitch_avg_viewers, rank
FROM ranked, top_steam
WHERE NOT on_steam AND rank < best_steam_rank
ORDER BY rank;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q9. Show the weekly attention index for a title since 2024.
-- MAGIC (Genie substitutes `game_id` — this reference uses `counter-strike-2`.)

SELECT week, attention_index, twitch_idx, wiki_idx, trends_idx, reddit_idx
FROM main.game_trends.gold_attention_trends
WHERE game_id = 'counter-strike-2' AND week >= '2024-01-01'
ORDER BY week;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q10. Which titles' attention index spiked the most last month?

SELECT game_id, week, attention_index, attention_wow_delta
FROM main.game_trends.gold_attention_trends
WHERE week >= date_sub(current_date(), 35)
ORDER BY attention_wow_delta DESC NULLS LAST
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q11. For Steam games, how does estimated revenue per average Twitch
-- MAGIC viewer differ between free-to-play and paid titles?

SELECT
    t.is_free,
    ROUND(AVG(r.revenue_per_avg_viewer), 2) AS avg_revenue_per_viewer,
    COUNT(DISTINCT r.game_id) AS titles
FROM main.game_trends.gold_revenue_engagement r
JOIN main.game_trends.silver_titles t USING (game_id)
WHERE r.revenue_per_avg_viewer IS NOT NULL
GROUP BY t.is_free;
