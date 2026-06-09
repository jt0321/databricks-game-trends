#!/usr/bin/env bash
# Tiny wrapper around the CLI extractor. Equivalent to `uv run game-trends-extract`.

set -euo pipefail

LIMIT="${GAME_TRENDS_SAMPLE_N:-25}"
OUT="${GAME_TRENDS_OUT_DIR:-data/raw}"

cd "$(dirname "$0")/.."
python -m game_trends.extract_steam --limit "$LIMIT" --out-dir "$OUT"
