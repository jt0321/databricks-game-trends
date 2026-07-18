# Multi-Platform Tracking Plan

Extends the lakehouse beyond the Steam catalog so that popular PC titles that
live **outside Steam** — Fortnite, Roblox, Minecraft, League of Legends,
Valorant, and similar — are tracked with the same pipeline. The unlock is that
attention/engagement signals (Twitch, Wikipedia, Google Trends, Reddit) are
**per-game, not per-storefront**, so they form a common ruler across Steam and
non-Steam titles. Steam-only metrics (CCU, reviews, sales estimates) then
enrich the Steam subset.

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

| source | B/F | What it provides | Coverage | Auth |
| --- | --- | --- | --- | --- |
| `steam_store` *(existing)* | F | Title metadata, genres, price, OS | Steam only | none |
| `steamspy` *(existing)* | F | Owner tiers, tags, avg/median playtime | Steam only | none |
| `igdb` | F | Cross-store metadata: platforms, storefronts, release dates, genres | **All PC titles** | Twitch dev app |
| `seed_titles` | — | Curated crosswalk CSV: `game_id` ↔ per-source IDs | All tracked titles | n/a (in repo) |
| `twitch_helix` | F | Viewers **and channel counts** per game (poll snapshots) | **All titles** | Twitch dev app |
| `wikipedia_pageviews` | **B** | Daily article views (official REST API, data since 2015) | **All titles** | none |
| `google_trends` | **B** | Weekly relative search interest (2004+) | **All titles** | none (pytrends) |
| `reddit` | F | Subreddit subscribers + recent post volume | **All titles** | Reddit app |
| `steam_reviews` | **B** | Every review with timestamp, vote, and author playtime-at-review (2013+; filtered to ≥ 2020) | Steam only | none |
| `steam_ccu` | F | Official current concurrent players | Steam only | none |
| `steam_achievements` | F | Global achievement completion % | Steam only | none |
| `gamalytic` | **B** (lifetime) | Sales/revenue estimates: lifetime totals any age, time series accruing | Steam only | free API key |

Deliberately excluded: SteamCharts/SteamDB/SullyGnome backfills (no APIs,
scraping-only, terms-of-service friction — revisit only if a 2020–now CCU
backfill becomes a hard requirement), console anything, Pushshift/Arctic Shift
Reddit dumps (bulk-heavy; forward-only Reddit polling is enough).

### Source caveats

- **Google Trends is a relative index** (0–100 within each request). Every
  batch must include a fixed anchor term (e.g. `"Minecraft"`) and be rescaled
  against it before landing, or values are incomparable across batches.
- **Twitch daily rollups are snapshot averages.** Fidelity depends on poll
  cadence (target: hourly job; minimum viable: a few polls/day). Store raw
  snapshots in Bronze; compute avg/peak at Silver.
- **Reddit post volume** is paginate-backwards-limited (~1000 posts); treat as
  forward-accumulating like Twitch.
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

### `gold_genre_trends` *(existing, extended)*

Add engagement: `twitch_avg_viewers_sum`, `wiki_pageviews_sum` per
`release_year` × `primary_genre`, alongside the existing release-count
columns. Off-Steam titles now contribute via IGDB genres.

### `gold_revenue_engagement` — *Steam subset*

Grain: `game_id` × `month`: `est_revenue_lifetime_usd` (latest snapshot),
`est_units_lifetime`, `reviews_posted`, `twitch_avg_viewers`,
`revenue_per_avg_viewer` — do viewership spikes coincide with estimated sales
movement? All `est_` columns clearly estimate-labeled for Genie.

## Pipeline shape

```
00_backfill_history      one-time-ish: steam_reviews (≥2020), wikipedia_pageviews (≥2020),
                         google_trends (≥2020), gamalytic lifetime      → Bronze
01_ingest_snapshots      scheduled (hourly for twitch/ccu; daily rest):
                         steam_store, steamspy, igdb, twitch_helix,
                         steam_ccu, steam_achievements, reddit, gamalytic → Bronze
02_bronze_to_silver      MERGE into silver_titles / silver_engagement_daily /
                         silver_reviews / silver_sales_estimates
03_gold_metrics          CREATE OR REPLACE the five gold tables
04_genie_demo_questions  updated question set (below)
```

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

1. **Seed CSV + `silver_titles` rework** — everything joins through
   `game_id`; nothing else can land first.
2. **Twitch Helix promoted from optional to core** + `silver_engagement_daily`
   with Twitch + Steam CCU columns — first cross-platform Gold becomes
   possible.
3. **Backfills**: steam_reviews, wikipedia_pageviews, google_trends — Gold
   gets 2020+ depth immediately.
4. **IGDB + Reddit + Gamalytic + achievements** — dimension quality, sales
   estimates, remaining signals.
5. **Gold rebuild + Genie question refresh.**
