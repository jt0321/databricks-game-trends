# Game Trend Lakehouse

A portfolio-quality Databricks project that builds an end-to-end **medallion lakehouse** over public gaming datasets. It demonstrates ingestion from open APIs, Delta-based Bronze/Silver/Gold layers, Databricks Workflows / Lakeflow-style pipelines, SQL dashboarding, and **Genie** natural-language questions.

The goal is to look like a real, deployable data product — not a toy notebook — while remaining fully reproducible with **no paid credentials**.

---

## 1. Project Goal

Track and analyze trends in the PC gaming market (platforms, genres, release cadence, popularity) using public Steam metadata. The lakehouse answers questions like:

- Which genres are gaining release share year-over-year?
- How does the price distribution differ between indie and AAA-adjacent titles?
- Which platforms / OS targets dominate new releases?
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
make install         # creates a venv and installs requirements
make extract-sample  # pulls a tiny sample from Steam/SteamSpy to data/raw/
make test            # runs pytest on pure-Python transforms
```

Python 3.10+ required. Spark is **not** needed locally — Spark code lives in the Databricks notebooks; local logic is pure Python so tests stay fast and dependency-light.

### Databricks

1. Import this repo as a **Git folder** (Repos) in your workspace.
2. Attach the notebooks under `notebooks/` to a serverless or shared cluster (DBR 14.3+).
3. Run `01_ingest_steam.py` → `02_bronze_to_silver.py` → `03_gold_metrics.py` in order.
4. Open `notebooks/04_genie_demo_questions.sql` in Databricks SQL or attach the gold tables to a Genie space.

### Deploying as a job/pipeline

The skeleton bundle in `resources/databricks_asset_bundle.yml` shows how to wire the three notebooks into a Databricks Asset Bundle / Lakeflow-style declarative pipeline. It is a *placeholder* — fill in your workspace host and catalog before `databricks bundle deploy`.

---

## 5. Databricks Features Showcased

- **Medallion architecture** — Bronze (raw Delta), Silver (cleaned & deduped), Gold (business metrics).
- **Delta Lake** — schema enforcement, MERGE/upsert, time travel-friendly append patterns.
- **Notebooks** — `# COMMAND ----------` cell markers for clean Repos round-trips.
- **Workflows / Lakeflow** — declarative pipeline YAML skeleton.
- **Databricks SQL** — gold metrics modeled as `gold_*` tables ready to back dashboards.
- **Genie** — `04_genie_demo_questions.sql` shows natural-language questions paired with SQL the LLM should generate; useful for Genie space configuration and evaluation.
- **Unity Catalog-ready** — three-level naming (`catalog.schema.table`) used throughout, with sensible defaults.

---

## 6. Genie Demo Questions

Once the gold tables are registered, paste these into a Genie space:

1. *"What were the top 5 genres by number of releases last year?"*
2. *"Compare average price of indie vs non-indie titles by year."*
3. *"Which platforms saw the largest growth in release share between 2018 and 2024?"*
4. *"Show me the genre mix for free-to-play titles only."*
5. *"Which release year had the most Linux-supporting titles?"*

The SQL Genie should generate is in [`notebooks/04_genie_demo_questions.sql`](notebooks/04_genie_demo_questions.sql) so you can evaluate accuracy.

---

## 7. Portfolio Value

This repo is built to demonstrate, in one place:

- **Data Engineering**: API ingestion with throttling, idempotent Bronze landing, schema-enforced Silver, business-friendly Gold.
- **SQL & Performance**: well-shaped aggregate tables, partition-friendly date columns, examples of windowed analytics.
- **Cloud / Databricks**: notebooks, jobs/pipelines-as-code, Unity Catalog naming, Genie integration.
- **Software engineering**: typed pure-Python transforms, unit tests, Makefile, asset bundle skeleton, `.env.example`, no secrets.
- **AI/Agents adjacent**: Genie NL→SQL evaluation set, structured for future agent workflows (e.g., an "ingest planner" agent).

---

## 8. Repository Layout

```
.
├── README.md
├── LICENSE
├── Makefile
├── requirements.txt
├── pyproject.toml
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
