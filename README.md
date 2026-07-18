# Game Trend Lakehouse

An end-to-end **medallion lakehouse** on Databricks for analyzing the PC gaming market — **on and off Steam**. It ingests Steam/SteamSpy metadata alongside cross-platform engagement signals (Twitch, Wikipedia, Google Trends, Reddit) and Steam-side revenue estimates into Delta tables (Bronze → Silver → Gold), runs as a Workflow / Lakeflow-style pipeline, and exposes the result to Databricks SQL dashboards and **Genie** natural-language queries.

The pipeline is fully reproducible with **no paid credentials** — every data source is public and free (some require a free app registration).

---

## 1. Project Goal

Track and analyze trends in the PC gaming market — including titles that live entirely outside Steam (Fortnite, Roblox, Minecraft, League of Legends, …) — using engagement signals that are tracked per-game rather than per-storefront. Useful questions the lakehouse answers:

- Which genres are gaining release share year-over-year?
- How does the price distribution differ between indie and AAA-adjacent titles?
- What share of PC gaming attention (Twitch viewers, Wikipedia views) goes to games not sold on Steam?
- Which titles are trending relative to their own recent history, regardless of platform?
- For Steam titles, does estimated revenue track Twitch viewership?
- How is Linux / SteamOS support changing over time?

---

## 2. Architecture

```
   Public APIs                  Landing             Bronze              Silver                    Gold                    Serve
   ─────────────                ────────            ──────              ──────                    ─────                   ─────
   Steam Store / SteamSpy  ─┐                                          silver_titles          gold_platform_trends
   Twitch / Steam CCU       │                                          silver_engagement_daily gold_genre_trends
   Steam Reviews (≥2020)    ├──►  data/raw/*.jsonl ──►  bronze_game_events ──►  silver_reviews  ──►  gold_attention_trends   ──►  Dashboards
   Wikipedia / Trends       │     (or cloud volume)     (raw Delta append)     silver_sales_estimates gold_steam_vs_offsteam      Genie (NL → SQL)
   Reddit / Gamalytic      ─┘                                                                        gold_title_monthly
                                                                                                       gold_revenue_engagement
```

See [`docs/architecture.md`](docs/architecture.md) for the full Mermaid diagram and layer contracts, and [`docs/multi_platform_plan.md`](docs/multi_platform_plan.md) for the design rationale.

---

## 3. Data Sources

| Source                | Auth               | Used for                                         |
| --------------------- | ------------------ | ------------------------------------------------ |
| Steam Store API       | None (public)      | Title metadata, genres, price, OS                |
| SteamSpy API          | None (public)      | Ownership tiers, tags, playtime, owner-tier estimates |
| Steam CCU API         | None (public)      | Concurrent players per title (snapshot polls)    |
| Steam Reviews API     | None (public)      | Review velocity + playtime-at-review, backfilled to 2020 |
| Wikimedia Pageviews   | None (public)      | Daily article views, backfilled to 2020 — covers off-Steam titles |
| Google Trends         | None (`pytrends`, optional dep) | Anchor-rescaled relative search interest, backfilled to 2020 |
| Twitch Helix          | Free app token     | Viewers + channel counts per game — the cross-storefront signal covering off-Steam titles |
| Reddit                | Free app token     | Subreddit subscribers + post volume              |
| Gamalytic              | Free API key       | Steam revenue/unit estimates                     |
| Seed crosswalk CSV    | n/a (in repo)      | Curated title universe + per-source ID mapping   |

The tracked universe is defined by [`data/seed/seed_titles.csv`](data/seed/seed_titles.csv) — a curated crosswalk that includes titles **outside Steam** (Fortnite, Roblox, Minecraft, League of Legends, …).

Full details, rate-limit etiquette, and env vars in [`docs/data_sources.md`](docs/data_sources.md).

---

## 4. Setup

### Local (for transforms, tests, sample extraction)

```bash
uv run pytest                          # runs tests on pure-Python transforms (automatically installs dependencies)
uv run game-trends-extract             # pulls a tiny sample from Steam/SteamSpy to data/raw/
uv run game-trends-extract-engagement  # snapshots Twitch, Steam CCU, and Reddit for the seed universe
uv run game-trends-extract-estimates   # snapshots SteamSpy owner tiers + Gamalytic revenue estimates
uv run game-trends-backfill            # backfills Steam reviews, Wikipedia pageviews, Google Trends to 2020
```

Every extractor degrades gracefully without credentials — it runs the
credential-free sources and skips the rest with a notice. See
`.env.example` for the full list of optional free-tier keys
(`TWITCH_CLIENT_ID`/`SECRET`, `REDDIT_CLIENT_ID`/`SECRET`,
`GAMALYTIC_API_KEY`) and `--skip-*` flags. Google Trends backfill additionally
needs the optional `pytrends` dependency: `uv sync --extra trends`.

Python 3.10+ and [uv](https://github.com/astral-sh/uv) are required. Spark is **not** needed locally — Spark code lives in the Databricks notebooks; local logic is pure Python so tests stay fast and dependency-light.

### Databricks

1. Import this repo as a **Git folder** (Repos) in your workspace.
2. Attach the notebooks under `notebooks/` to a serverless or shared cluster (DBR 14.3+).
3. Run in order: `01_ingest_steam` → `02_bronze_to_silver` → `05_engagement_to_silver` + `06_reviews_and_estimates_to_silver` (either order) → `03_gold_metrics` + `07_gold_engagement_metrics` (either order).
4. Open `notebooks/04_genie_demo_questions.sql` in Databricks SQL or attach the gold tables to a Genie space.

### Deploying as a job/pipeline

The skeleton bundle in `resources/databricks_asset_bundle.yml` shows how to wire the original three notebooks into a Databricks Asset Bundle / Lakeflow-style declarative pipeline; extend it with `05`–`07` before deploying the full pipeline. It is a *placeholder* — fill in your workspace host and catalog before `databricks bundle deploy`.

---

## 5. Databricks Features Used

- **Medallion architecture** — Bronze (raw Delta), Silver (cleaned & deduped), Gold (business metrics).
- **Delta Lake** — schema enforcement, MERGE/upsert, time travel-friendly append patterns.
- **Notebooks** — `# COMMAND ----------` cell markers for clean Repos round-trips.
- **Workflows / Lakeflow** — declarative pipeline YAML skeleton.
- **Databricks SQL** — gold metrics modeled as `gold_*` tables that back dashboards directly.
- **Genie** — `04_genie_demo_questions.sql` pairs natural-language questions with reference SQL, both for seeding a Genie space and for evaluating its NL→SQL output.
- **Unity Catalog-ready** — three-level naming (`catalog.schema.table`) used throughout, with sensible defaults.

---

## 6. Example Genie Questions

Once the gold tables are registered, paste these into a Genie space as starter prompts:

1. *"What were the top 5 genres by number of releases last year?"*
2. *"Compare average price of indie vs non-indie titles by year."*
3. *"Which platforms saw the largest growth in release share between 2018 and 2024?"*
4. *"Show me the genre mix for free-to-play titles only."*
5. *"Which release year had the most Linux-supporting titles?"*
6. *"Year-over-year release count for the action genre."*
7. *"What share of PC gaming Twitch hours went to games not sold on Steam, by quarter?"*
8. *"Which non-Steam games had more Twitch viewership than the top Steam release this month?"*
9. *"Show the weekly attention index for Counter-Strike 2 since 2024."*
10. *"Which titles' attention index spiked the most last month?"*
11. *"For Steam games, how does estimated revenue per average Twitch viewer differ between free-to-play and paid titles?"*

The reference SQL each question should resolve to is in [`notebooks/04_genie_demo_questions.sql`](notebooks/04_genie_demo_questions.sql) so you can evaluate Genie's NL→SQL accuracy.

---

## 7. Why Game Trend Data

Steam is one of the largest and most openly queryable catalogs of software releases anywhere — tens of thousands of titles with structured metadata for genres, platforms, pricing, release dates, ownership tiers, and player time. That makes it a genuinely useful substrate for several kinds of analysis:

- **Catalog & market analysis** — track how genre mix, pricing, and platform support shift year over year.
- **Personal gaming data** — overlay your own library, wishlist, or playtime against the broader release landscape.
- **Streaming / community enrichment** — the schema is structured so that Twitch viewers, Reddit mentions, or community ratings can be merged in later without reshaping Bronze or Silver.
- **AI agent inputs** — the Gold tables and Genie space form a clean tool surface for agents that need to reason about games (recommendation flows, "what should I play next" assistants, market-research helpers).

The project is designed to be extended: new sources land in Bronze as additional `source` values, schema-aware transforms move them through Silver, and new business questions become new Gold tables.

---

## 8. Repository Layout

```
.
├── README.md
├── LICENSE
├── pyproject.toml
├── uv.lock
├── .env.example
├── .gitignore
├── docs/
│   ├── architecture.md
│   ├── data_sources.md
│   └── multi_platform_plan.md
├── notebooks/
│   ├── 01_ingest_steam.py
│   ├── 02_bronze_to_silver.py
│   ├── 03_gold_metrics.py
│   ├── 04_genie_demo_questions.sql
│   ├── 05_engagement_to_silver.py
│   ├── 06_reviews_and_estimates_to_silver.py
│   └── 07_gold_engagement_metrics.py
├── src/game_trends/
│   ├── __init__.py
│   ├── extract_steam.py        # local CLI extractor (Steam metadata)
│   ├── extract_engagement.py   # local CLI extractor (Twitch + Steam CCU + Reddit snapshots)
│   ├── extract_estimates.py    # local CLI extractor (SteamSpy + Gamalytic sales estimates)
│   ├── extract_backfill.py     # local CLI extractor (reviews / Wikipedia / Trends backfill to 2020)
│   ├── schema.py               # canonical schemas
│   └── transforms.py           # pure-Python silver/gold logic
├── tests/
│   ├── test_transforms.py
│   └── test_schema.py
├── data/seed/                  # curated title universe + ID crosswalk
├── data/sample/                # tiny hand-written fixtures
├── resources/
│   └── databricks_asset_bundle.yml
└── scripts/
    └── extract_sample.sh
```

---

## 9. License

MIT. See [`LICENSE`](LICENSE).
