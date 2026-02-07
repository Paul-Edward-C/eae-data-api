# Changelog

## 2026-02-07 — Repo migration and automation

### Repo relocation
- Moved active repo from `~/Documents/ghost/data_api` (iCloud) to `~/data_api` (local)
- Re-initialized git repo from remote (`github.com/Paul-Edward-C/eae-data-api.git`)
- Old iCloud copy at `~/Documents/ghost/data_api` still exists (won't delete due to iCloud lock) — no longer active

### Cleanup
- Deleted unused `json_data/` directory (legacy JSON output, no longer used)
- Deleted unused `cache/` directory (old caching layer, replaced by SQLite)

### Verification
- Confirmed `sqlite_data_access.py` reads DB from `~/.local/data_api/data.db` (correct path)
- Confirmed API still works after migration

### New: automatic parquet file watcher
- Added `watch_parquet.py` — monitors parquet source directories for changes and triggers incremental DB updates via `update_db.py`
- Added `launchd` plist (`~/Library/LaunchAgents/com.eae.watch-parquet.plist`) for auto-start on login
- Added `watchdog` to `requirements.txt`
