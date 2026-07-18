"""Local CLI: periodic sales-estimate snapshots (SteamSpy + Gamalytic).

Distinct from `extract_backfill.py`: these sources model current lifetime
totals rather than day-by-day history, so the extractor takes one snapshot
per title per run and the *history of snapshots* becomes the time series
(see `transforms.build_sales_estimates`, `silver_sales_estimates`).

* ``steamspy_estimates.jsonl`` — SteamSpy `appdetails` owner-tier estimate
  per seeded Steam title. No credentials needed.
* ``gamalytic_estimates.jsonl`` — Gamalytic lifetime revenue/unit estimate
  per seeded Steam title. Needs a free API key
  (https://gamalytic.com/pricing — free tier, ~2500 requests/day).

Run with ``uv run game-trends-extract-estimates`` or:

    uv run python -m game_trends.extract_estimates --skip-gamalytic
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import requests  # type: ignore
except ImportError:  # pragma: no cover
    requests = None  # the CLI requires it; tests don't

from .extract_steam import write_jsonl
from .transforms import load_seed_titles

STEAMSPY_API = "https://steamspy.com/api.php"
GAMALYTIC_API = "https://api.gamalytic.com/game/{appid}"

DEFAULT_UA = os.environ.get("GAME_TRENDS_UA", "game-trend-lakehouse/0.1")
DEFAULT_OUT_DIR = Path(os.environ.get("GAME_TRENDS_OUT_DIR", "data/raw"))
DEFAULT_SEED = Path("data/seed/seed_titles.csv")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _envelope(source: str, entity_key: str, payload: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    return {
        "source": source,
        "entity_key": entity_key,
        "extract_date": now.date().isoformat(),
        "ingested_at": now.isoformat(),
        "payload": json.dumps(payload, ensure_ascii=False),
    }


def _session() -> "requests.Session":
    if requests is None:  # pragma: no cover
        raise RuntimeError("The 'requests' package is required for extract_estimates")
    s = requests.Session()
    s.headers.update({"User-Agent": DEFAULT_UA, "Accept": "application/json"})
    return s


def extract_steamspy_estimates(
    session: "requests.Session", seed_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    steam_titles = [s for s in seed_rows if s["steam_appid"] is not None]
    rows: list[dict[str, Any]] = []
    print(f"[estimates] SteamSpy owner tiers for {len(steam_titles)} titles ...", file=sys.stderr)
    for seed in steam_titles:
        appid = seed["steam_appid"]
        try:
            resp = session.get(
                STEAMSPY_API, params={"request": "appdetails", "appid": appid}, timeout=30
            )
            resp.raise_for_status()
            payload = resp.json() or {}
        except Exception as exc:
            print(f"[estimates]   ! appid={appid} failed: {exc}", file=sys.stderr)
            time.sleep(1.2)
            continue
        rows.append(_envelope("steamspy_snapshot", str(appid), payload))
        time.sleep(1.2)  # SteamSpy's documented appdetails rate limit
    return rows


def extract_gamalytic_estimates(
    session: "requests.Session", seed_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    api_key = os.environ.get("GAMALYTIC_API_KEY")
    if not api_key:
        print(
            "[estimates] GAMALYTIC_API_KEY unset — skipping Gamalytic "
            "(free tier signup: https://gamalytic.com/pricing).",
            file=sys.stderr,
        )
        return []

    steam_titles = [s for s in seed_rows if s["steam_appid"] is not None]
    rows: list[dict[str, Any]] = []
    print(f"[estimates] Gamalytic estimates for {len(steam_titles)} titles ...", file=sys.stderr)
    for seed in steam_titles:
        appid = seed["steam_appid"]
        try:
            resp = session.get(
                GAMALYTIC_API.format(appid=appid),
                headers={"api-key": api_key},
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json() or {}
        except Exception as exc:
            print(f"[estimates]   ! appid={appid} failed: {exc}", file=sys.stderr)
            time.sleep(0.5)
            continue
        rows.append(_envelope("gamalytic", str(appid), payload))
        time.sleep(0.5)
    return rows


def extract(
    seed_path: Path, out_dir: Path, skip_steamspy: bool = False, skip_gamalytic: bool = False
) -> list[Path]:
    seed_rows = load_seed_titles(seed_path)
    session = _session()
    written: list[Path] = []

    if not skip_steamspy:
        rows = extract_steamspy_estimates(session, seed_rows)
        if rows:
            path = out_dir / "steamspy_estimates.jsonl"
            write_jsonl(rows, path)
            print(f"[estimates] Wrote {len(rows)} rows to {path}", file=sys.stderr)
            written.append(path)

    if not skip_gamalytic:
        rows = extract_gamalytic_estimates(session, seed_rows)
        if rows:
            path = out_dir / "gamalytic_estimates.jsonl"
            write_jsonl(rows, path)
            print(f"[estimates] Wrote {len(rows)} rows to {path}", file=sys.stderr)
            written.append(path)

    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Snapshot SteamSpy owner-tier and Gamalytic revenue estimates."
    )
    parser.add_argument("--seed", type=Path, default=DEFAULT_SEED, help="Seed crosswalk CSV.")
    parser.add_argument(
        "--out-dir", type=Path, default=DEFAULT_OUT_DIR, help=f"Output dir (default {DEFAULT_OUT_DIR})."
    )
    parser.add_argument("--skip-steamspy", action="store_true")
    parser.add_argument("--skip-gamalytic", action="store_true", help="Skip even with a key set.")
    args = parser.parse_args(argv)
    extract(
        seed_path=args.seed,
        out_dir=args.out_dir,
        skip_steamspy=args.skip_steamspy,
        skip_gamalytic=args.skip_gamalytic,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
