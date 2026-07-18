# Multi-Platform Tracking Plan

Extends the lakehouse beyond the Steam catalog so that popular PC titles that
live **outside Steam** — Fortnite, Roblox, Minecraft, League of Legends,
Valorant, and similar — are tracked with the same pipeline. The unlock is that
attention/engagement signals (Twitch, Wikipedia, Google Trends, Reddit) are
**per-game, not per-storefront**, so they form a common ruler across Steam and
non-Steam titles. Steam-only metrics (CCU, reviews, sales estimates) then
enrich the Steam subset.

## Status

Sequencing steps 1-3 and 5 below are **implemented** — the seed crosswalk,
cross-platform `silver_titles`, `silver_engagement_daily` covering all five
snapshot-shaped sources (Twitch, CCU, Wikipedia, Trends, Reddit), the three
2020+ backfills, `silver_reviews`, `silver_sales_estimates`, and all four new
Gold tables (`gold_attention_trends`, `gold_steam_vs_offsteam`,
`gold_title_monthly`, `gold_revenue_engagement`). See
`src/game_trends/transforms.py`, `extract_engagement.py`,
`extract_backfill.py`, `extract_estimates.py`, and notebooks `05`-`07`.

**Deliberately deferred** (step 4's remaining half): IGDB metadata
enrichment and Steam global achievement percentages. Both are secondary,
metadata-polish sources — the seed CSV's manually curated `storefronts`
column already covers what Gold needs for the Steam-vs-off-Steam split, and
achievement completion isn't wired into any Gold table's grain. Worth
revisiting if the seed universe grows past hand-curation size (IGDB) or if a
"how far do players get" metric becomes a real question (achievements).

## Scope decisions

- **PC only.** Consoles are out: Sony/Microsoft publish no player data, and
  viewership was never per-platform to begin with.
- **History starts 2020-01-01** (`HISTORY_START`). Backfillable sources are
  loaded from this date; snapshot-only sources accumulate forward from first
  pipeline run.
- **Free tier / free signup only.** Twitch dev app (also unlocks IGDB),
  Gamalytic free API tier, Reddit app — all free. No paid data.
- **Curated title universe, not the full catalog.** Cross-source tracking
  requires ID mapping per title, so the universe is a seed list (~100–500
  titles: Steam top sellers + top Twitch categories + notable off-Steam PC
  titles), maintained as a versioned CSV in-repo.

## Sources

`source` values in Bronze. **B** = backfillable to 2020, **F** = forward-only
snapshots (history accrues as the pipeline runs).

| source | B/F | What it provides | Coverage | Auth | Status |
| --- | --- | --- | --- | --- | --- |
| `steam_store` *(existing)* | F | Title metadata, genres, price, OS | Steam only | none | ✅ |
| `steamspy` *(existing)* | F | Owner tiers, tags, avg/median playtime (bulk top-100 list) | Steam only | none | ✅ |
| `igdb` | F | Cross-store metadata: platforms, storefronts, release dates, genres | **All PC titles** | Twitch dev app | ⏳ deferred |
| `seed_titles` | — | Curated crosswalk CSV: `game_id` ↔ per-source IDs | All tracked titles | n/a (in repo) | ✅ |
| `twitch_helix` | F | Viewers **and channel counts** per game (poll snapshots) | **All titles** | Twitch dev app | ✅ |
| `wikipedia_pageviews` | **B** | Daily article views (official REST API, data since 2015) | **All titles** | none | ✅ |
| `google_trends` | **B** | Weekly relative search interest (2004+) | **All titles** | none (pytrends, optional dep) | ✅ |
| `reddit` | F | Subreddit subscribers + same-day post count (capped page) | **All titles** | Reddit app | ✅ |
| `steam_reviews` | **B** | Every review with timestamp, vote, and author playtime-at-review (2013+; filtered to ≥ 2020) | Steam only | none | ✅ |
| `steam_ccu` | F | Official current concurrent players | Steam only | none | ✅ |
| `steam_achievements` | F | Global achievement completion % | Steam only | none | ⏳ deferred |
| `steamspy_snapshot` | F | Per-title owner-tier estimate (`appdetails`, distinct from the bulk `steamspy` source) | Steam only | none | ✅ |
| `gamalytic` | **B** (lifetime) | Sales/revenue estimates: lifetime totals any age, time series accruing | Steam only | free API key | ✅ |

Deliberately excluded: SteamCharts/SteamDB/SullyGnome backfills (no APIs,
scraping-only, terms-of-service friction — revisit only if a 2020–now CCU
backfill becomes a hard requirement), console anything, Pushshift/Arctic Shift
Reddit dumps (bulk-heavy; forward-only Reddit polling is enough).

### Source caveats

- **Google Trends is a relative index** (0–100 within each request). Every
  batch queries a fixed anchor term (`TRENDS_ANCHOR_TERM = "video game"` in
  `extract_backfill.py`) and rescales `term / anchor * 100` against it before
  landing, or values would be incomparable across batches.
- **Twitch daily rollups are snapshot averages.** Fidelity depends on poll
  cadence (target: hourly job; minimum viable: a few polls/day). Store raw
  snapshots in Bronze; compute avg/peak at Silver.
- **Reddit post volume** is a capped single page of `/new` (100 posts)
  counted against the current UTC day — undercounts extremely high-volume
  subreddits rather than paginating exhaustively, same tradeoff as the
  Twitch stream-page cap. Forward-accumulating like Twitch/CCU.
- **Gamalytic accuracy**: estimates, ~77% within ±30%. Always labeled
  `est_` in Silver/Gold.

## Bronze

Keep the single append-only envelope table, with two added columns:

### `bronze_game_events` (extended)

| column | type | notes |
| --- | --- | --- |
| `source` | STRING | one of the source values above |
| `entity_key` | STRING | **new** — source-native ID (`appid`, Twitch `game_id`, wiki article, subreddit, trends term) |
| `extract_date` | DATE | **new** — the date the payload *describes* (≠ `ingested_at` for backfills) |
| `ingested_at` | TIMESTAMP | as today |
| `batch_id` | STRING | as today; backfill batches prefixed `backfill-` |
| `payload` | STRING | raw JSON, unexploded |

`extract_date` is what makes backfills coexist with pollers: a 2021 Wikipedia
pageview row lands with `extract_date = 2021-…` and `ingested_at = now`.
Partition by `source`; high-volume sources (`steam_reviews`,
`wikipedia_pageviews`, `twitch_helix`) additionally benefit from clustering on
`extract_date`.

## Silver

Four tables. `game_id` is a stable human-readable slug from the seed CSV
(e.g. `fortnite`, `baldurs-gate-3`) — the join key everywhere.

### `silver_titles` (reworked: canonical cross-platform dimension)

One row per tracked title. Today's table is keyed on `appid`; the key becomes
`game_id` and `steam_appid` becomes a **nullable** column — NULL is the
signature of an off-Steam title.

| column | type | notes |
| --- | --- | --- |
| `game_id` | STRING | PK, from seed CSV |
| `name` | STRING | |
| `steam_appid` | LONG | nullable — NULL ⇒ off-Steam |
| `on_steam` | BOOLEAN | derived; the headline cohort split |
| `storefronts` | ARRAY<STRING> | from IGDB (`steam`, `epic`, `riot`, `standalone`, …) |
| `release_year` | INT | |
| `is_free` | BOOLEAN | |
| `price_cents` | INT | nullable off-Steam |
| `primary_genre`, `genre_list` | STRING, ARRAY<STRING> | IGDB for off-Steam, Steam Store otherwise |
| `supports_windows/mac/linux` | BOOLEAN | |
| `twitch_game_id` | STRING | crosswalk |
| `wikipedia_article` | STRING | crosswalk |
| `subreddit` | STRING | crosswalk |
| `trends_term` | STRING | crosswalk |
| `ingested_at` | TIMESTAMP | |

Write: MERGE on `game_id` (metadata sources layered: seed CSV ⊕ IGDB ⊕ Steam
Store, later sources fill NULLs only).

### `silver_engagement_daily` (new: the core fact table)

Grain: `game_id` × `date`. Wide nullable columns — Genie-friendly, and NULLs
fall out naturally (off-Steam titles have NULL Steam columns; backfilled
history has NULL Twitch columns before first poll).

| column | type | source |
| --- | --- | --- |
| `game_id` | STRING | PK part |
| `date` | DATE | PK part |
| `twitch_avg_viewers` | DOUBLE | twitch_helix |
| `twitch_peak_viewers` | LONG | twitch_helix |
| `twitch_avg_channels` | DOUBLE | twitch_helix |
| `wiki_pageviews` | LONG | wikipedia_pageviews |
| `trends_index` | DOUBLE | google_trends (anchor-rescaled; weekly value repeated daily) |
| `reddit_posts` | LONG | reddit |
| `reddit_subscribers` | LONG | reddit (snapshot carried on poll days) |
| `steam_ccu_peak` | LONG | steam_ccu (max of day's polls) |
| `reviews_posted` | LONG | steam_reviews |
| `reviews_positive_share` | DOUBLE | steam_reviews |

Write: MERGE on (`game_id`, `date`) — each source's job updates only its own
columns, so pollers and backfills never clobber each other.

### `silver_reviews` (new: review-level, Steam subset)

Grain: one row per review. Needed because playtime-at-review cohorts can't be
pre-aggregated into the daily table.

`game_id`, `review_id`, `review_ts`, `voted_up` BOOLEAN,
`playtime_at_review_min` LONG, `language`, `received_free` BOOLEAN.
Filter: `review_ts >= HISTORY_START`. MERGE on `review_id`.

### `silver_sales_estimates` (new: Steam subset)

Grain: `game_id` × `snapshot_date`. `est_owners_low/high` (SteamSpy tier
bounds), `est_units_lifetime`, `est_revenue_lifetime_usd`, `est_source`
(`steamspy` | `gamalytic`). Append per snapshot — the history of estimates is
itself the time series.

## Gold

Rebuilt as `CREATE OR REPLACE` from Silver, as today.

### `gold_attention_trends` — *what's trending, cross-platform*

Grain: `game_id` × `week`. Each signal indexed to that title's trailing
13-week baseline (`100` = its own recent normal), so Twitch viewers, wiki
views, and trends become comparable and composable:

`twitch_idx`, `wiki_idx`, `trends_idx`, `reddit_idx`,
`attention_index` (mean of available idx columns), `attention_wow_delta`,
plus raw `twitch_avg_viewers` and `wiki_pageviews` for absolute scale.

### `gold_steam_vs_offsteam` — *the original question, answered directly*

Grain: `week` × `cohort` (`on_steam` / `off_steam`): `title_count`,
`twitch_viewer_share`, `wiki_pageview_share`, `top20_attention_titles`
(count of cohort titles in the week's top-20 attention_index). Shows how much
of PC gaming attention the Steam catalog alone would miss.

### `gold_title_monthly` — *leaderboard*

Grain: `game_id` × `month`: rank per signal, rank deltas vs prior month,
`reviews_posted`, `est_revenue_snapshot`. Backs the "top movers" dashboard.

### `gold_genre_trends` *(existing — engagement extension deferred)*

Originally planned: add `twitch_avg_viewers_sum` / `wiki_pageviews_sum` per
`release_year` × `primary_genre`. **Deferred alongside IGDB** — off-Steam
titles have no `release_year` or `primary_genre` without IGDB metadata (the
seed CSV doesn't carry genre), so this extension isn't meaningful until IGDB
lands. `gold_genre_trends` is unchanged for now.

### `gold_revenue_engagement` — *Steam subset*

Grain: `game_id` × `month`: `est_revenue_lifetime_usd` (latest snapshot),
`est_units_lifetime`, `reviews_posted`, `twitch_avg_viewers`,
`revenue_per_avg_viewer` — do viewership spikes coincide with estimated sales
movement? All `est_` columns clearly estimate-labeled for Genie.

## Pipeline shape (as built)

CLI extractors write JSONL to `data/raw/`; `01_ingest_steam.py` appends
whatever's there to Bronze regardless of source (the landing contract is
source-agnostic), so no new ingest notebook was needed for the new sources.

```
CLI (one-time-ish, re-runnable):
  uv run game-trends-backfill              steam_reviews / wikipedia_pageviews /
                                            google_trends (≥ HISTORY_START)   → Bronze

CLI (scheduled — hourly for twitch/ccu, daily for the rest):
  uv run game-trends-extract               steam_store, steamspy (bulk)      → Bronze
  uv run game-trends-extract-engagement    twitch_helix, steam_ccu, reddit   → Bronze
  uv run game-trends-extract-estimates     steamspy_snapshot, gamalytic      → Bronze

Notebooks:
  01_ingest_steam                 land data/raw/*.jsonl                     → Bronze
  02_bronze_to_silver              MERGE                                     → silver_titles
  05_engagement_to_silver          MERGE (twitch/ccu/wiki/trends/reddit cols) → silver_engagement_daily
  06_reviews_and_estimates_to_silver  dedupe/MERGE + daily rollup            → silver_reviews,
                                                                                silver_sales_estimates,
                                                                                silver_engagement_daily (reviews cols)
  03_gold_metrics                  CREATE OR REPLACE                        → gold_platform_trends,
                                                                                gold_genre_trends
  07_gold_engagement_metrics       CREATE OR REPLACE                        → gold_attention_trends,
                                                                                gold_steam_vs_offsteam,
                                                                                gold_title_monthly,
                                                                                gold_revenue_engagement
  04_genie_demo_questions          Q1-Q11
```

`05` and `06` own disjoint columns of `silver_engagement_daily` and can run
in either order. `07` should run after both.

Backfill jobs are idempotent and re-runnable (MERGE on natural keys), so
"one-time" really means "run until the 2020→now window is filled, re-run
freely."

## New Genie starter questions

1. *"Which non-Steam games had more Twitch viewership than the top Steam
   release this month?"*
2. *"Show weekly attention index for Fortnite vs Counter-Strike 2 since
   2024."*
3. *"What share of PC gaming Twitch hours went to games not sold on Steam,
   by quarter?"*
4. *"Which titles' Wikipedia views spiked the most last month?"*
5. *"For Steam games, how does estimated revenue per average Twitch viewer
   differ between free-to-play and paid titles?"*

## Sequencing

1. ✅ **Seed CSV + `silver_titles` rework** — everything joins through
   `game_id`; nothing else can land first.
2. ✅ **Twitch Helix promoted from optional to core** + `silver_engagement_daily`
   with Twitch + Steam CCU columns — first cross-platform Gold becomes
   possible.
3. ✅ **Backfills**: steam_reviews, wikipedia_pageviews, google_trends — Gold
   gets 2020+ depth immediately.
4. ⏳ **IGDB + achievements** — dimension quality, remaining signals.
   Reddit and Gamalytic (originally grouped here) are done; IGDB and
   achievement percentages are deferred — see Status above.
5. ✅ **Gold rebuild + Genie question refresh** — `gold_attention_trends`,
   `gold_steam_vs_offsteam`, `gold_title_monthly`, `gold_revenue_engagement`,
   and `notebooks/04_genie_demo_questions.sql` Q7-Q11.
