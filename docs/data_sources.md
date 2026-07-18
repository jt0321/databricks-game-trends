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

## Environment variables

The local extractor reads only optional, **non-secret** settings:

| Variable               | Purpose                                              | Default                          |
| ---------------------- | ---------------------------------------------------- | -------------------------------- |
| `GAME_TRENDS_UA`       | `User-Agent` header sent to public APIs (etiquette). | `game-trend-lakehouse/0.1`       |
| `GAME_TRENDS_SAMPLE_N` | Number of titles to pull in `uv run game-trends-extract`. | `25`                             |
| `GAME_TRENDS_OUT_DIR`  | Where raw JSONL is written.                          | `data/raw`                       |
| `TWITCH_CLIENT_ID`     | Twitch app client id (engagement extractor).         | unset — Twitch half skipped      |
| `TWITCH_CLIENT_SECRET` | Twitch app client secret (engagement extractor).     | unset — Twitch half skipped      |

See `.env.example` for the local template. **No real secrets are ever committed.**

## Rate-limit etiquette

- Send a descriptive `User-Agent` so API operators can contact you if needed.
- Sleep between requests even when the spec doesn't require it — these are free community APIs.
- Cap batch sizes via `GAME_TRENDS_SAMPLE_N`. Mirroring the entire Steam catalog is rarely the goal; pulling a focused slice keeps iteration fast and stays well inside rate limits.
- Cache aggressively. Re-running `uv run game-trends-extract` should not re-hit the API if `data/raw/` already has fresh files (the CLI skips when the output file is < 24 h old).
