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
