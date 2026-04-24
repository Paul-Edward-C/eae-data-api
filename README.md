# East Asia Econ Data API

REST API serving economic time series data, built with FastAPI, backed by SQLite, deployed on Railway.

## Overview

Data coverage:
- **China (cn)** - Macroeconomic, trade, prices, industry data
- **Japan (jp)** - GDP, CPI, labor, financial markets data
- **Korea (kr)** - Economic indicators and forecasts
- **Taiwan (tw)** - Trade, production, prices data
- **Regional (region)** - Cross-country comparisons and aggregates

## Setup

```bash
pip install -r requirements.txt
```

## Setting up a second Mac (local update pipeline)

Everything below runs on your local machine and keeps Cloudflare R2 (what the Railway API serves) fresh. Do this once per device.

Assumes the second Mac is logged in as user `paul` and has anaconda at `/Users/paul/opt/anaconda3`. If not, edit paths in `deploy/launchd/*.plist` before loading.

### 1. Clone the repo and install deps

```bash
git clone git@github.com:Paul-Edward-C/eae-data-api.git ~/data_api
cd ~/data_api
pip install -r requirements.txt
```

### 2. Create the local data directory

The SQLite DB lives **outside iCloud** (iCloud doesn't support symlinks and will corrupt the file).

```bash
mkdir -p ~/.local/data_api
```

### 3. Copy R2 credentials

`r2_keys.txt` is git-ignored (contains secrets). Copy it from the other Mac via AirDrop/scp to:

```
~/data_api/r2_keys.txt
```

Format (two lines):
```
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
```

### 4. Parquet source files

`update_db.py` reads from `~/Documents/DATA/{cn,jp,kr,tw,region}/{country}_input/`. Because `~/Documents` is iCloud-synced, these should appear automatically on the second Mac — wait until iCloud finishes syncing before the first DB build.

> iCloud sync can produce "Resource deadlock avoided" errors while files are downloading. If you see these, wait and retry.

### 5. Seed the local database

Pick one:

**Fastest** — download the pre-built DB from R2 (~2 GB compressed, ~7 GB decompressed):

```bash
cd ~/data_api
set -a; source r2_keys.txt; set +a
python -c "
import boto3, gzip, shutil
from botocore.config import Config
c = boto3.client('s3',
    endpoint_url='https://fd2c6c5f2d6d8bc9ca228f83b5671df3.r2.cloudflarestorage.com',
    aws_access_key_id='$R2_ACCESS_KEY_ID',
    aws_secret_access_key='$R2_SECRET_ACCESS_KEY',
    region_name='auto',
    config=Config(signature_version='s3v4'))
c.download_file('eae-data-api', 'data.db.gz', '/Users/paul/.local/data_api/data.db.gz')
with gzip.open('/Users/paul/.local/data_api/data.db.gz','rb') as fi, open('/Users/paul/.local/data_api/data.db','wb') as fo:
    shutil.copyfileobj(fi, fo)
print('DB ready')
"
```

**Slow** — rebuild locally from parquets (~2–3 hours):

```bash
python update_db.py --rebuild
```

### 6. Install the launchd agents

Two agents ship with the repo under `deploy/launchd/`:

| Agent | What it does | Schedule |
|---|---|---|
| `com.eae.watch-parquet` | Watches parquet dirs, runs `build_database()` after a 30 s debounce | Always on (KeepAlive) |
| `com.eae.upload-r2` | Runs `update_db.py --upload` — rebuilds + compresses + pushes to R2 | Daily at 02:00 |

Install:

```bash
cp ~/data_api/deploy/launchd/com.eae.watch-parquet.plist ~/Library/LaunchAgents/
cp ~/data_api/deploy/launchd/com.eae.upload-r2.plist     ~/Library/LaunchAgents/

launchctl load ~/Library/LaunchAgents/com.eae.watch-parquet.plist
launchctl load ~/Library/LaunchAgents/com.eae.upload-r2.plist

launchctl list | grep eae     # both should be listed
```

### 7. Verify

```bash
# Watcher: touch a parquet, confirm a rebuild fires within ~30s
tail -f ~/.local/data_api/watch_parquet.log

# Upload job: trigger it manually once (takes 20–40 min)
launchctl start com.eae.upload-r2
tail -f ~/.local/data_api/upload.log
```

### Everyday ops on this machine

```bash
launchctl list | grep eae                                              # status
launchctl start com.eae.upload-r2                                      # force an upload now
launchctl unload ~/Library/LaunchAgents/com.eae.upload-r2.plist        # disable a job
launchctl load   ~/Library/LaunchAgents/com.eae.upload-r2.plist        # re-enable
tail -f ~/.local/data_api/{watch_parquet,upload}.log                   # logs
```

If you edit a `.plist` on disk, `unload` then `load` for changes to take effect.

## Updating Data

### 1. Update source parquet files

Data is sourced from parquet files in these directories:

| Country  | Path                                              |
|----------|---------------------------------------------------|
| China    | `/Users/paul/Documents/DATA/cn/cn_input/`         |
| Japan    | `/Users/paul/Documents/DATA/jp/jp_input/`         |
| Korea    | `/Users/paul/Documents/DATA/kr/kr_input/`         |
| Taiwan   | `/Users/paul/Documents/DATA/tw/tw_input/`         |
| Regional | `/Users/paul/Documents/DATA/region/region_input/` |

Files follow the naming convention `{name}_{frequency}.parquet` where frequency is `m` (monthly), `q` (quarterly), or `a` (annual).

Files matching `latest`, `recent`, `hist`, or `history` are automatically excluded.

### 2. Run `update_db.py`

```bash
# Rebuild database and upload to R2
python update_db.py --upload

# Rebuild specific countries only
python update_db.py --country cn jp --upload

# Rebuild locally without uploading
python update_db.py

# Upload existing compressed DB without rebuilding
python update_db.py --upload-only
```

This script:
- Reads all parquet files from the configured country directories
- Builds a new SQLite database (`data.db`) with indexes and full-text search
- Pre-computes stats for the `/stats` endpoint
- Compresses to `data.db.gz` and uploads to Cloudflare R2 (if `--upload` is used)

### 3. The API picks up the new data

- On Railway, the API auto-downloads `data.db.gz` from R2 on startup if no local copy exists.
- You can trigger a refresh without redeploying via the admin endpoint: `POST /admin/refresh-db` (requires admin API key), which deletes the local DB and re-downloads from R2.

### Typical workflow

```
Edit parquet files → python update_db.py --upload → Redeploy or POST /admin/refresh-db
```

## Running Locally

```bash
uvicorn api:app --reload --port 8000
```

## API Endpoints

### Public (no auth required)

| Endpoint | Description |
|----------|-------------|
| `GET /` | API info |
| `GET /health` | Health check |
| `GET /stats` | API statistics |
| `GET /search?q={pattern}` | Search series by name |
| `GET /countries` | List countries |
| `GET /frequencies` | List frequencies |
| `GET /info/{series_name}` | Series metadata |
| `GET /docs` | Swagger UI |

### Authenticated (API key or Ghost token)

| Endpoint | Description |
|----------|-------------|
| `GET /series?columns={col1;col2}` | Multi-series data fetch |
| `GET /series/{series_name}` | Single series data |
| `POST /keys/provision` | Issue API key for Ghost member |
| `GET /keys/me` | Current user's key info and usage |
| `GET /usage` | Check rate limit status |

### Admin only

| Endpoint | Description |
|----------|-------------|
| `POST /admin/refresh-db` | Re-download database from R2 |
| `POST /admin/reload-hidden-series` | Reload hidden series config |

### Column name separator

Column names contain commas (e.g., "Japan, JGB, 10Y"), so use **semicolons (;)** to separate multiple columns:

```
columns=Japan, JGB, 10Y;Japan, JGB, 20Y
```

### Example requests

```bash
# Search for columns
curl "http://localhost:8000/search?q=JGB&freq=m&limit=10"

# Get data (requires API key)
curl -H "X-API-Key: your-key" \
  "http://localhost:8000/series?columns=Japan, JGB, 10Y;Japan, JGB, 20Y&freq=m&start=2020-01-01"

# Get CSV format
curl -H "X-API-Key: your-key" \
  "http://localhost:8000/series?columns=Japan, JGB, 10Y&freq=m&format=csv"
```

## Authentication

### Ghost Members

Ghost blog members authenticate using their member JWT token:

```bash
curl -H "Authorization: Bearer <ghost_member_token>" \
  "http://localhost:8000/series?columns=Japan, JGB, 10Y"
```

### Per-user API Keys

Ghost members can provision a personal API key via `POST /keys/provision`, then use it as:

```bash
curl -H "X-API-Key: eae_..." \
  "http://localhost:8000/series?columns=Japan, JGB, 10Y"
```

### Rate limits (monthly)

| Tier | Who | Limit |
|------|-----|-------|
| Free | Free Ghost members | 10 requests/month |
| Daily | Daily tier subscribers | 30 requests/month |
| Premium | Daily+Data, East Asia, admin | Unlimited |

## Response Formats

The `/series` endpoint supports three output formats:

**records** (default):
```json
{"data": [{"Date": "2024-01-01", "Japan, JGB, 10Y": 0.65}]}
```

**columns**:
```json
{"data": {"Date": ["2024-01-01"], "Japan, JGB, 10Y": [0.65]}}
```

**csv**:
```
Date,Japan, JGB, 10Y
2024-01-01,0.65
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `GHOST_URL` | Ghost blog URL | For Ghost auth |
| `GHOST_ADMIN_KEY` | Ghost Admin API key (`id:secret` format) | For Ghost auth |
| `R2_ACCESS_KEY_ID` | Cloudflare R2 access key | For DB sync |
| `R2_SECRET_ACCESS_KEY` | Cloudflare R2 secret key | For DB sync |
| `S3_BUCKET` | R2 bucket name (default: `eae-data-api`) | Optional |
| `API_KEYS` | Comma-separated legacy admin API keys | Optional |
| `ALLOWED_ORIGINS` | Comma-separated CORS origins (default: `*`) | Optional |
| `RAILWAY_VOLUME_MOUNT_PATH` | Persistent storage path on Railway | On Railway |

## Deployment (Railway)

Configured via `railway.json` and `Procfile`. The start command is:

```
uvicorn api:app --host 0.0.0.0 --port $PORT
```

Set the environment variables above in the Railway dashboard. The database persists on a Railway volume at `$RAILWAY_VOLUME_MOUNT_PATH/data.db`.
