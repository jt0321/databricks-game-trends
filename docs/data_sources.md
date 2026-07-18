# Data Sources

All sources are **public and free** (some need a free app registration). The
full multi-platform source roadmap — including backfillable history and the
sources not yet wired in — is in
[`multi_platform_plan.md`](multi_platform_plan.md); this file documents what
the extractors use today.

## 1. Steam Store API

- **Base URL**: `https://store.steampowered.com/api/`
- **Endpoints used**:
  - `appdetails?appids=<id>&cc=us&l=english` — full metadata for a single title (name, genres, platforms, price, release date, is_free).
  - `appdetails?appids=<id1>,<id2>` — small batches (Steam quietly rate-limits these; treat as 1 ID per call for safety).
- **Auth**: none.
- **Format**: JSON. Note the envelope shape `{ "<appid>": { "success": bool, "data": {...} } }`.
- **Rate limit**: undocumented. Community consensus is roughly **~200 requests / 5 minutes per IP**. The extractor in this repo defaults to a conservative **1.5 s sleep between requests** and caps sample size at 50 titles.

## 2. SteamSpy API

- **Base URL**: `https://steamspy.com/api.php`
- **Endpoints used**:
  - `?request=top100in2weeks` — top 100 most-played titles in the last two weeks (great seed list for the sample extract).
  - `?request=appdetails&appid=<id>` — ownership tiers, tags, average playtime.
- **Auth**: none.
- **Format**: JSON dict keyed by `appid`.
- **Rate limit**: documented at **1 request/sec** for `appdetails`; the bulk "all" endpoints are heavier and best avoided unless you need a full refresh. The extractor sleeps 1.2 s between calls.

## 3. Steam CCU (official)

- **Endpoint**: `https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid=<id>`
- **Auth**: none.
- **Format**: `{ "response": { "player_count": <int>, "result": 1 } }`.
- **Semantics**: current-moment concurrent players only — **no history**. The
  pipeline accrues its own history by polling; `silver_engagement_daily`
  keeps the daily peak across polls.
- Used by `uv run game-trends-extract-engagement` (works with zero
  credentials via `--skip-twitch`). The extractor sleeps 1 s between calls.

## 4. Twitch Helix — *core engagement source*

- **Base URL**: `https://api.twitch.tv/helix/`
- **Why core**: the only free, official, cross-storefront demand signal — it
  covers Fortnite, Roblox, Minecraft, and League of Legends exactly as well
  as any Steam title, which is what makes `silver_engagement_daily`
  comparable across the whole seed universe.
- **Auth**: register a free Twitch developer app for a **Client ID** +
  **Client Secret**, exchanged for an app access token via the OAuth
  client-credentials flow. Missing credentials don't break the extractor —
  the Twitch half is skipped with a notice.
- **Endpoints used**:
  - `games?name=<category>` — resolve seed `twitch_category` names to Twitch
    game IDs (batched, up to 100 names per call).
  - `streams?game_id=<id>&first=100` — paged up to 5 pages per category;
    each snapshot stores summed viewers, channel count, and the top single
    stream. Viewer mass concentrates in top streams, so the cap loses little.
- **Semantics**: live snapshots only — Twitch offers **no historical
  endpoints**. History accrues from polling, ideally a few times per day.
- **Rate limit**: 800 points/minute per app token (Helix point system); the
  extractor sleeps 0.5 s between categories.
- **Env vars**: `TWITCH_CLIENT_ID`, `TWITCH_CLIENT_SECRET`.

## 5. Steam reviews (official) — *backfillable to `HISTORY_START`*

- **Endpoint**: `https://store.steampowered.com/appreviews/<appid>`
- **Auth**: none.
- **Format**: `{"success": 1, "reviews": [{"recommendationid", "timestamp_created", "voted_up", "author": {"playtime_at_review"}, ...}], "cursor": ...}`.
- **Semantics**: full history back to ~2013 — the one Steam-side source that
  is genuinely backfillable, not just a snapshot. `uv run game-trends-backfill
  --sources reviews` pages backwards with `filter=recent` until crossing
  `HISTORY_START` (2020-01-01), then `transforms.build_reviews_silver` dedupes
  by `review_id` (backfill pagination can overlap across re-runs) and filters
  the cutoff.
- **Why it matters**: review velocity and playtime-at-review are the best
  free proxies for sales and engagement depth — the same signal underlying
  third-party revenue estimators like Gamalytic.
- **Rate limit**: undocumented; the extractor sleeps 1 s between page fetches
  and between titles.

## 6. Wikimedia Pageviews API — *backfillable to `HISTORY_START`*

- **Endpoint**: `https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/user/<article>/daily/<start>/<end>`
- **Auth**: none.
- **Format**: `{"items": [{"timestamp": "YYYYMMDD00", "views": <int>}, ...]}`.
- **Semantics**: official daily pageviews back to July 2015 — comfortably
  covers `HISTORY_START`. `uv run game-trends-backfill --sources wikipedia`
  unpacks the date-range response into one bronze row per day per article, so
  the bronze contract ("one row = one entity + date fact") stays uniform.
- **Rate limit**: generous for this volume; the extractor sleeps 0.3 s
  between articles.

## 7. Google Trends (unofficial `pytrends`) — *backfillable to `HISTORY_START`*

- **Auth**: none — Trends has no official API or key. `game-trends-backfill
  --sources trends` uses the community `pytrends` package
  (`uv sync --extra trends`); the extractor degrades gracefully with a notice
  if it isn't installed.
- **Semantics**: search interest is a **relative index (0-100) within a
  single request**, not an absolute number — incomparable across separate
  batches or terms on its own. The extractor always queries a fixed anchor
  term (`TRENDS_ANCHOR_TERM = "video game"`) alongside the real term and
  rescales `term_value / anchor_value * 100`, so the resulting `trends_index`
  is comparable across terms and over time (assuming the anchor's own
  absolute volume is roughly stable — a reasonable approximation for a
  generic, high-volume term). Trends returns weekly points; the extractor
  expands each week to one bronze row per day.
- **Fragility**: unofficial and rate-limit-sensitive. Treat `google_trends`
  as the most likely source to need retry/backoff tuning in production.

## 8. Reddit (engagement extension)

- **Base URL**: `https://oauth.reddit.com`
- **Auth**: register a free "script" app at
  `https://www.reddit.com/prefs/apps`, exchanged for an OAuth token via the
  client-credentials flow (same pattern as Twitch). Missing credentials skip
  the source with a notice.
- **Endpoints used**:
  - `/r/<subreddit>/about` — subscriber count.
  - `/r/<subreddit>/new?limit=100` — a capped page of newest posts, counted
    against the current UTC day for a same-day post-volume estimate. Like the
    Twitch stream-page cap, this undercounts extremely high-volume
    subreddits rather than paginating exhaustively.
- **Semantics**: live snapshot only, no history — accrues forward like
  Twitch/CCU.
- **Env vars**: `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`.

## 9. Gamalytic (Steam revenue/unit estimates)

- **Endpoint**: `https://api.gamalytic.com/game/<appid>`
- **Auth**: free tier signup at `https://gamalytic.com/pricing`
  (~2500 requests/day), passed as an `api-key` header.
- **Semantics**: lifetime revenue/unit estimates, not day-by-day history —
  `uv run game-trends-extract-estimates` takes one snapshot per run, and the
  *history of snapshots* in `silver_sales_estimates` becomes the time series.
  Estimates, not ground truth (~77% within ±30% margin per Gamalytic's own
  published accuracy) — always flows through Gold with an `est_` prefix.
- **Env vars**: `GAMALYTIC_API_KEY`.

SteamSpy `appdetails` (source #2 above) doubles as a second, credential-free
estimate source: its `owners` field is a coarse ownership-tier range parsed
into `est_owners_low` / `est_owners_high`, distinguished from Gamalytic by
`est_source`.

## Environment variables

The local extractor reads only optional, **non-secret** settings:

| Variable               | Purpose                                              | Default                          |
| ---------------------- | ---------------------------------------------------- | -------------------------------- |
| `GAME_TRENDS_UA`       | `User-Agent` header sent to public APIs (etiquette). | `game-trend-lakehouse/0.1`       |
| `GAME_TRENDS_SAMPLE_N` | Number of titles to pull in `uv run game-trends-extract`. | `25`                             |
| `GAME_TRENDS_OUT_DIR`  | Where raw JSONL is written.                          | `data/raw`                       |
| `TWITCH_CLIENT_ID`     | Twitch app client id (engagement extractor).         | unset — Twitch half skipped      |
| `TWITCH_CLIENT_SECRET` | Twitch app client secret (engagement extractor).     | unset — Twitch half skipped      |
| `REDDIT_CLIENT_ID`     | Reddit app client id (engagement extractor).         | unset — Reddit half skipped      |
| `REDDIT_CLIENT_SECRET` | Reddit app client secret (engagement extractor).     | unset — Reddit half skipped      |
| `GAMALYTIC_API_KEY`    | Gamalytic API key (estimates extractor).             | unset — Gamalytic half skipped   |

See `.env.example` for the local template. **No real secrets are ever committed.**

## Rate-limit etiquette

- Send a descriptive `User-Agent` so API operators can contact you if needed.
- Sleep between requests even when the spec doesn't require it — these are free community APIs.
- Cap batch sizes via `GAME_TRENDS_SAMPLE_N`. Mirroring the entire Steam catalog is rarely the goal; pulling a focused slice keeps iteration fast and stays well inside rate limits.
- Cache aggressively. Re-running `uv run game-trends-extract` should not re-hit the API if `data/raw/` already has fresh files (the CLI skips when the output file is < 24 h old).
