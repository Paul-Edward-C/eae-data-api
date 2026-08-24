#!/usr/bin/env bash
# Refresh the chart packs served by the API from the chartsite build.
set -euo pipefail
SRC="${1:-/Users/paul/Documents/ghost/chartsite/data/packs}"
DST="$(cd "$(dirname "$0")" && pwd)/packs"
[[ -d "$SRC" ]] || { echo "no such source: $SRC" >&2; exit 1; }
mkdir -p "$DST"
rsync -a --delete --include='*.json' --exclude='*' "$SRC/" "$DST/"
echo "synced $(ls "$DST" | wc -l | tr -d ' ') packs from $SRC"
