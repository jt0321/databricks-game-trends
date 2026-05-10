# Data Sources

All primary sources are **public and free**. Twitch is documented as an optional extension but not required.

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
- **Rate limit**: documented at **1 request/sec** for `appdetails`; "all" endpoints are heavier — avoid in a portfolio extract. The extractor sleeps 1.2 s between calls.

## 3. Twitch Helix — *optional extension only*

- **Base URL**: `https://api.twitch.tv/helix/`
- **Why optional**: requires registering a Twitch developer app to obtain a **Client ID** + **Client Secret**, then exchanging them for an **app access token** via OAuth client-credentials flow. This is free but not zero-friction, so it is **not** part of the default pipeline.
- **Useful endpoints** *(if you enable it)*:
  - `games?name=<title>` — resolve a Steam title to a Twitch `game_id`.
  - `streams?game_id=<id>&first=100` — current viewer counts.
- **Rate limit**: 800 points/minute per app token (Helix point system).
- **Env vars** (only needed if extending):
  - `TWITCH_CLIENT_ID`
  - `TWITCH_CLIENT_SECRET`

## Environment variables

The local extractor reads only optional, **non-secret** settings:

| Variable               | Purpose                                              | Default                          |
| ---------------------- | ---------------------------------------------------- | -------------------------------- |
| `GAME_TRENDS_UA`       | `User-Agent` header sent to public APIs (etiquette). | `game-trends-portfolio/0.1`      |
| `GAME_TRENDS_SAMPLE_N` | Number of titles to pull in `make extract-sample`.   | `25`                             |
| `GAME_TRENDS_OUT_DIR`  | Where raw JSONL is written.                          | `data/raw`                       |
| `TWITCH_CLIENT_ID`     | *(optional extension)* Twitch app client id.         | unset                            |
| `TWITCH_CLIENT_SECRET` | *(optional extension)* Twitch app client secret.     | unset                            |

See `.env.example` for the local template. **No real secrets are ever committed.**

## Rate-limit etiquette

- Send a descriptive `User-Agent` so API operators can contact you if needed.
- Sleep between requests even when the spec doesn't require it — portfolio extractors should never look like a scraper.
- Cap sample sizes (`GAME_TRENDS_SAMPLE_N`) — the goal is to *demonstrate* the pipeline, not to mirror Steam.
- Cache aggressively. Re-running `make extract-sample` should not re-hit the API if `data/raw/` already has fresh files (the CLI skips when the output file is < 24 h old).
