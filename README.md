# East Asia Econ Data API

Unified data access layer for economic data using DuckDB.

## Overview

This API provides access to economic time series data for:
- **China (cn)** - Macroeconomic, trade, prices, industry data
- **Japan (jp)** - GDP, CPI, labor, financial markets data
- **Korea (kr)** - Economic indicators and forecasts
- **Taiwan (tw)** - Trade, production, prices data
- **Regional (region)** - Cross-country comparisons and aggregates

## Setup

```bash
pip install -r requirements.txt
```

## Local Usage

### Direct data access

```python
from data_access import get_series, search_columns, list_columns

# Search for columns
cols = search_columns('JGB', freq='m')

# Get data
df = get_series(['Japan, JGB, 10Y', 'Japan, JGB, 20Y'], freq='m', start_date='2020-01-01')

# List all columns for a country
jp_cols = list_columns(freq='m', country='jp', prefix='Japan')
```

### Create animated charts

```python
from chart_helper import create_line_animation

create_line_animation(
    title='Japan, JGB yields',
    columns=['Japan, JGB, 10Y', 'Japan, JGB, 20Y', 'Japan, JGB, 30Y'],
    labels=['10Y', '20Y', '30Y'],
    freq='m',
    start_date='2000-01-01',
    country='jp',
    foot_label='EAE, MOF'
)
```

## API Server

### Start server

```bash
cd /Users/paul/Documents/DATA/tools/data_api
uvicorn api:app --reload --port 8000
```

### Endpoints

| Endpoint | Description | Auth Required |
|----------|-------------|---------------|
| `GET /` | API info | No |
| `GET /health` | Health check | No |
| `GET /stats` | API statistics | No |
| `GET /search?q={pattern}` | Search columns | No |
| `GET /series?columns={cols}` | Get data | Yes |
| `GET /columns` | List columns | No |
| `GET /info/{column}` | Column metadata | Yes |
| `GET /usage` | Check rate limit status | Yes |
| `GET /countries` | List countries | No |
| `GET /frequencies` | List frequencies | No |
| `GET /docs` | Swagger UI | No |
| `POST /admin/rebuild-index` | Rebuild index | Admin |

### Example requests

```bash
# Health check
curl "http://localhost:8000/health"

# Search for columns
curl "http://localhost:8000/search?q=JGB&freq=m&limit=10"

# Get data (requires API key) - use semicolons to separate columns
curl -H "X-API-Key: demo-key-123" \
  "http://localhost:8000/series?columns=Japan, JGB, 10Y;Japan, JGB, 20Y&freq=m&start=2020-01-01"

# Get single column
curl -H "X-API-Key: demo-key-123" \
  "http://localhost:8000/series?columns=Japan, JGB, 10Y&freq=m&start=2024-01-01"

# List columns
curl "http://localhost:8000/columns?freq=m&country=jp&prefix=Japan&limit=20"

# Get column info
curl "http://localhost:8000/info/Japan, JGB, 10Y"

# Get stats
curl "http://localhost:8000/stats"

# Get CSV format
curl -H "X-API-Key: demo-key-123" \
  "http://localhost:8000/series?columns=Japan, JGB, 10Y&freq=m&start=2024-01-01&format=csv"
```

### Important: Column Name Separator

Column names contain commas (e.g., "Japan, JGB, 10Y"), so use **semicolons (;)** to separate multiple columns:

```
# Correct - multiple columns
columns=Japan, JGB, 10Y;Japan, JGB, 20Y

# Single column (no separator needed)
columns=Japan, JGB, 10Y
```

## Authentication

The API supports two authentication methods:

### Ghost Members (recommended for public deployment)

Ghost blog members can authenticate using their member JWT token:

```bash
curl -H "Authorization: Bearer <ghost_member_token>" \
  "http://localhost:8000/series?columns=Japan, JGB, 10Y&freq=m"
```

Rate limits are based on Ghost membership tier:
| Ghost Tier | API Tier | Requests/Day |
|------------|----------|--------------|
| Free member | free | 10 |
| Daily | daily | 100 |
| Daily + Data | premium | 10,000 |
| East Asia | premium | 10,000 |
| Comped | premium | 10,000 |
| API Key | premium | 10,000 |

### API Keys (admin/legacy)

For admin access or backwards compatibility:

```bash
curl -H "X-API-Key: demo-key-123" \
  "http://localhost:8000/series?columns=Japan, JGB, 10Y&freq=m"
```

Set API keys via environment variable:
```bash
export API_KEYS="key1,key2,key3"
```

Default demo key: `demo-key-123`

### Check Usage

Authenticated users can check their rate limit status:

```bash
curl -H "X-API-Key: demo-key-123" "http://localhost:8000/usage"
```

## Deployment

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `GHOST_URL` | Your Ghost blog URL (e.g., https://yourblog.com) | For Ghost auth |
| `GHOST_ADMIN_KEY` | Ghost Admin API key (format: `id:secret`) | For Ghost auth |
| `API_KEYS` | Comma-separated admin API keys | Optional |
| `ALLOWED_ORIGINS` | Comma-separated CORS origins (default: `*`) | Optional |

### Railway/Render/Fly.io

1. Create `Procfile`:
   ```
   web: uvicorn api:app --host 0.0.0.0 --port $PORT
   ```

2. Set environment variables (see above)

3. Deploy

### Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
```

## Data Structure

Data is stored in parquet files organized by:
- Country: `cn`, `jp`, `kr`, `tw`, `region`
- Frequency: `_d` (daily), `_w` (weekly), `_m` (monthly), `_q` (quarterly), `_a` (annual)

Example: `/Users/paul/Documents/DATA/jp/jp_input/jp_jgb_m.parquet`

## Column Index

On first use, the system builds an index of all columns across parquet files. This is cached at:
```
/Users/paul/Documents/DATA/tools/data_api/cache/column_index.json
```

To rebuild:
```python
from data_access import DataAccess
da = DataAccess()
da.rebuild_index()
```

Or via API:
```bash
curl -X POST -H "X-API-Key: demo-key-123" "http://localhost:8000/admin/rebuild-index"
```

## Response Formats

The `/series` endpoint supports three output formats:

**records** (default):
```json
{
  "data": [
    {"Date": "2024-01-01", "Japan, JGB, 10Y": 0.65},
    {"Date": "2024-02-01", "Japan, JGB, 10Y": 0.72}
  ]
}
```

**columns**:
```json
{
  "data": {
    "Date": ["2024-01-01", "2024-02-01"],
    "Japan, JGB, 10Y": [0.65, 0.72]
  }
}
```

**csv**:
```
Date,Japan, JGB, 10Y
2024-01-01,0.65
2024-02-01,0.72
```
