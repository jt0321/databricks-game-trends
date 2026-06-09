# Game Trend Lakehouse

An end-to-end **medallion lakehouse** on Databricks for analyzing the PC gaming market. It ingests public Steam and SteamSpy data into Delta tables (Bronze → Silver → Gold), runs as a Workflow / Lakeflow-style pipeline, and exposes the result to Databricks SQL dashboards and **Genie** natural-language queries.

The pipeline is fully reproducible with **no paid credentials** — every data source is public and free.

---

## 1. Project Goal

Track and analyze trends in the PC gaming market — platforms, genres, release cadence, pricing — using public Steam metadata. Useful questions the lakehouse answers:

- Which genres are gaining release share year-over-year?
- How does the price distribution differ between indie and AAA-adjacent titles?
- Which platforms / OS targets dominate new releases?
- How is Linux / SteamOS support changing over time?
- What does the long tail of "owned but unplayed" look like?

---

## 2. Architecture

```
   Public APIs / Files          Landing             Bronze              Silver             Gold                Serve
   ─────────────────────        ────────            ──────              ──────             ─────               ─────
   Steam Store API   ─┐                                                                    gold_platform_trends   Dashboards
   SteamSpy API       ├──►  data/raw/*.jsonl  ──►  bronze_game_events ──►  silver_titles ──►                  ──►
   (optional) Twitch  ─┘    (or cloud volume)      (raw Delta append)      (typed,          gold_genre_trends     Genie (NL → SQL)
                                                                            deduped,
                                                                            enriched)
```

See [`docs/architecture.md`](docs/architecture.md) for the full Mermaid diagram and layer contracts.

---

## 3. Data Sources

| Source                | Auth          | Used for                           |
| --------------------- | ------------- | ---------------------------------- |
| Steam Store API       | None (public) | Title metadata, genres, price, OS  |
| SteamSpy API          | None (public) | Ownership tiers, tags, playtime    |
| Twitch Helix *(opt.)* | App token     | Streaming/viewer enrichment (docs only) |

Full details, rate-limit etiquette, and env vars in [`docs/data_sources.md`](docs/data_sources.md).

---

## 4. Setup

### Local (for transforms, tests, sample extraction)

```bash
uv run pytest               # runs tests on pure-Python transforms (automatically installs dependencies)
uv run game-trends-extract  # pulls a tiny sample from Steam/SteamSpy to data/raw/
```

Python 3.10+ and [uv](https://github.com/astral-sh/uv) are required. Spark is **not** needed locally — Spark code lives in the Databricks notebooks; local logic is pure Python so tests stay fast and dependency-light.

### Databricks

1. Import this repo as a **Git folder** (Repos) in your workspace.
2. Attach the notebooks under `notebooks/` to a serverless or shared cluster (DBR 14.3+).
3. Run `01_ingest_steam.py` → `02_bronze_to_silver.py` → `03_gold_metrics.py` in order.
4. Open `notebooks/04_genie_demo_questions.sql` in Databricks SQL or attach the gold tables to a Genie space.

### Deploying as a job/pipeline

The skeleton bundle in `resources/databricks_asset_bundle.yml` shows how to wire the three notebooks into a Databricks Asset Bundle / Lakeflow-style declarative pipeline. It is a *placeholder* — fill in your workspace host and catalog before `databricks bundle deploy`.

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
│   └── data_sources.md
├── notebooks/
│   ├── 01_ingest_steam.py
│   ├── 02_bronze_to_silver.py
│   ├── 03_gold_metrics.py
│   └── 04_genie_demo_questions.sql
├── src/game_trends/
│   ├── __init__.py
│   ├── extract_steam.py        # local CLI extractor
│   ├── schema.py               # canonical schemas
│   └── transforms.py           # pure-Python silver/gold logic
├── tests/
│   ├── test_transforms.py
│   └── test_schema.py
├── data/sample/                # tiny hand-written fixtures
├── resources/
│   └── databricks_asset_bundle.yml
└── scripts/
    └── extract_sample.sh
```

---

## 9. License

MIT. See [`LICENSE`](LICENSE).
