"""
FastAPI Server for Economic Data API
=====================================
Provides REST API access to economic data with Ghost membership integration.
"""

from fastapi import FastAPI, HTTPException, Query, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import List, Optional
from collections import defaultdict
from datetime import datetime, timedelta
from functools import lru_cache
import hashlib
import pandas as pd
import jwt
import requests
import os

from data_access import DataAccess

# =============================================================================
# Data Cache (speeds up repeated queries)
# =============================================================================

_series_cache = {}
_cache_ttl = 300  # 5 minutes

def get_cached_series(cache_key: str):
    """Get cached series if not expired."""
    if cache_key in _series_cache:
        data, timestamp = _series_cache[cache_key]
        if (datetime.now() - timestamp).seconds < _cache_ttl:
            return data
        del _series_cache[cache_key]
    return None

def set_cached_series(cache_key: str, data):
    """Cache series data with timestamp."""
    # Limit cache size
    if len(_series_cache) > 100:
        # Remove oldest entries
        oldest = sorted(_series_cache.items(), key=lambda x: x[1][1])[:20]
        for key, _ in oldest:
            del _series_cache[key]
    _series_cache[cache_key] = (data, datetime.now())

# =============================================================================
# Configuration
# =============================================================================

# Ghost configuration (set via environment variables)
GHOST_URL = os.environ.get('GHOST_URL', '')
GHOST_ADMIN_KEY = os.environ.get('GHOST_ADMIN_KEY', '')

# Legacy API keys (for backwards compatibility / admin access)
API_KEYS = os.environ.get('API_KEYS', 'demo-key-123').split(',')

# Tier limits (requests per day)
# Maps to Ghost Pro tiers:
#   - free: Free members (no paid tier)
#   - daily: "Daily" tier subscribers
#   - premium: "Daily + Data" and "East Asia" tier subscribers, plus admin API key users
TIER_LIMITS = {
    'free': 10,        # Free Ghost members
    'daily': 100,      # "Daily" tier subscribers
    'premium': 10000,  # "Daily + Data", "East Asia" tiers, and admin
}

# =============================================================================
# Initialize App
# =============================================================================

app = FastAPI(
    title="East Asia Econ Data API",
    description="""
API for accessing economic time series data for China, Japan, Korea, Taiwan, and regional aggregates.

## Authentication

**For Ghost Members:** Use your member token in the Authorization header:
```
Authorization: Bearer <your_ghost_member_token>
```

**For Admin/Legacy:** Use API key in X-API-Key header:
```
X-API-Key: <your_api_key>
```

## Rate Limits

- Free: 10 requests/day
- Daily: 100 requests/day
- Premium: 10,000 requests/day (Daily + Data, East Asia tiers)
""",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS - configure for your Ghost domain in production
ALLOWED_ORIGINS = os.environ.get('ALLOWED_ORIGINS', '*').split(',')
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data access instance
da = DataAccess()

# Simple in-memory rate limiter (use Redis for production at scale)
request_counts = defaultdict(lambda: {'count': 0, 'reset': datetime.now()})

# =============================================================================
# Ghost Members Integration
# =============================================================================

def get_ghost_member(token: str) -> Optional[dict]:
    """Verify Ghost member token and return member info with tier."""
    if not GHOST_URL or not GHOST_ADMIN_KEY:
        return None

    try:
        # Decode the member JWT (Ghost uses signed JWTs for members)
        # We decode without verification first to get the email
        decoded = jwt.decode(token, options={"verify_signature": False})
        email = decoded.get('sub') or decoded.get('email')

        if not email:
            return None

        # Create Ghost Admin API token
        key_id, key_secret = GHOST_ADMIN_KEY.split(':')
        iat = int(datetime.now().timestamp())
        header = {'alg': 'HS256', 'typ': 'JWT', 'kid': key_id}
        payload = {'iat': iat, 'exp': iat + 300, 'aud': '/admin/'}
        admin_token = jwt.encode(
            payload,
            bytes.fromhex(key_secret),
            algorithm='HS256',
            headers=header
        )

        # Fetch member from Ghost Admin API
        response = requests.get(
            f"{GHOST_URL}/ghost/api/admin/members/?filter=email:'{email}'",
            headers={"Authorization": f"Ghost {admin_token}"},
            timeout=10
        )

        if response.ok:
            members = response.json().get('members', [])
            if members:
                member = members[0]

                # Determine tier from Ghost tiers/products
                # Maps to: "Daily + Data" or "East Asia" → premium, "Daily" → daily, else → free
                tier_names = [t.get('name', '').lower() for t in member.get('tiers', [])]

                if any('east asia' in t or 'data' in t for t in tier_names):
                    # "Daily + Data" or "East Asia" tiers get premium
                    tier = 'premium'
                elif any('daily' in t for t in tier_names):
                    # "Daily" tier
                    tier = 'daily'
                elif member.get('status') == 'comped':
                    # Comped members get premium access
                    tier = 'premium'
                else:
                    tier = 'free'

                return {
                    'email': email,
                    'name': member.get('name', ''),
                    'tier': tier,
                    'uuid': member.get('uuid'),
                    'auth_type': 'ghost'
                }

        return None

    except jwt.DecodeError:
        return None
    except Exception as e:
        print(f"Ghost auth error: {e}")
        return None


# =============================================================================
# Authentication Dependencies
# =============================================================================

async def get_current_user(
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None)
) -> dict:
    """
    Authenticate user via Ghost member token OR legacy API key.
    Returns user info with tier.
    """
    # Try Ghost member token first
    if authorization and authorization.startswith('Bearer '):
        token = authorization.replace('Bearer ', '')
        member = get_ghost_member(token)
        if member:
            return member

    # Fall back to API key (admin access = premium tier)
    if x_api_key and x_api_key in API_KEYS:
        return {
            'email': 'api_key_user',
            'name': 'API Key User',
            'tier': 'premium',
            'uuid': None,
            'auth_type': 'api_key'
        }

    # No valid auth
    raise HTTPException(
        status_code=401,
        detail="Authentication required. Provide Ghost member token (Authorization: Bearer <token>) or API key (X-API-Key: <key>)"
    )


async def get_optional_user(
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None)
) -> Optional[dict]:
    """Optional authentication - returns None if not authenticated."""
    try:
        return await get_current_user(authorization, x_api_key)
    except HTTPException:
        return None


# =============================================================================
# Rate Limiting
# =============================================================================

def check_rate_limit(user: dict) -> dict:
    """
    Check if user has exceeded their tier's rate limit.
    Returns remaining requests info.
    """
    identifier = user.get('email', 'anonymous')
    tier = user.get('tier', 'free')
    limit = TIER_LIMITS.get(tier, TIER_LIMITS['free'])

    now = datetime.now()
    user_data = request_counts[identifier]

    # Reset daily at midnight
    if now - user_data['reset'] > timedelta(days=1):
        user_data['count'] = 0
        user_data['reset'] = now

    if user_data['count'] >= limit:
        reset_time = user_data['reset'] + timedelta(days=1)
        raise HTTPException(
            status_code=429,
            detail={
                "error": "Rate limit exceeded",
                "tier": tier,
                "limit": limit,
                "reset_at": reset_time.isoformat()
            }
        )

    user_data['count'] += 1

    return {
        'used': user_data['count'],
        'limit': limit,
        'remaining': limit - user_data['count'],
        'tier': tier
    }


# =============================================================================
# Public Routes (no auth required)
# =============================================================================

@app.get("/")
async def root():
    """API information."""
    return {
        "name": "East Asia Econ Data API",
        "version": "2.0.0",
        "description": "Economic time series data for China, Japan, Korea, Taiwan, and regional aggregates",
        "authentication": {
            "ghost_members": "Authorization: Bearer <member_token>",
            "api_key": "X-API-Key: <api_key>"
        },
        "tier_limits": TIER_LIMITS,
        "endpoints": {
            "search": "/search?q={pattern}&freq={m|q|a}",
            "series": "/series?columns={col1;col2}&freq={m|q|a}&start={date}&end={date}",
            "columns": "/columns?freq={m|q|a}&country={cn|jp|kr|tw|region}",
            "info": "/info/{column_name}",
            "countries": "/countries",
            "frequencies": "/frequencies",
            "health": "/health",
            "stats": "/stats",
            "usage": "/usage"
        },
        "note": "Use semicolons (;) to separate multiple columns in /series endpoint",
        "docs": "/docs"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "East Asia Econ Data API", "version": "2.0.0"}


@app.get("/countries")
async def list_countries():
    """List available countries/regions."""
    return {
        "countries": [
            {"code": "cn", "name": "China"},
            {"code": "jp", "name": "Japan"},
            {"code": "kr", "name": "Korea"},
            {"code": "tw", "name": "Taiwan"},
            {"code": "region", "name": "Regional/Cross-country"}
        ]
    }


@app.get("/frequencies")
async def list_frequencies():
    """List available data frequencies."""
    return {
        "frequencies": [
            {"code": "d", "name": "Daily"},
            {"code": "w", "name": "Weekly"},
            {"code": "m", "name": "Monthly"},
            {"code": "q", "name": "Quarterly"},
            {"code": "a", "name": "Annual"}
        ]
    }


@app.get("/search")
async def search_columns(
    q: str = Query(..., description="Search pattern (case-insensitive)"),
    freq: str = Query('m', description="Frequency: m (monthly), q (quarterly), a (annual)"),
    country: Optional[str] = Query(None, description="Country filter: cn, jp, kr, tw, region"),
    limit: int = Query(50, description="Maximum results", le=200),
    user: Optional[dict] = Depends(get_optional_user)
):
    """
    Search for columns matching a pattern.

    No authentication required, but authenticated users get higher limits.
    """
    # Unauthenticated users get limited results
    if user is None:
        limit = min(limit, 20)

    try:
        results = da.search_columns(q, freq=freq, country=country, limit=limit)
        return {
            "query": q,
            "freq": freq,
            "country": country,
            "count": len(results),
            "results": results,
            "authenticated": user is not None
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/columns")
async def list_columns(
    freq: str = Query('m', description="Frequency: m, q, a"),
    country: Optional[str] = Query(None, description="Country filter"),
    prefix: Optional[str] = Query(None, description="Column name prefix filter"),
    limit: int = Query(100, description="Maximum results", le=1000),
    user: Optional[dict] = Depends(get_optional_user)
):
    """List available columns."""
    # Unauthenticated users get limited results
    if user is None:
        limit = min(limit, 50)

    try:
        results = da.list_columns(freq=freq, country=country, prefix=prefix)
        return {
            "freq": freq,
            "country": country,
            "prefix": prefix,
            "count": len(results),
            "results": results[:limit],
            "authenticated": user is not None
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats(user: Optional[dict] = Depends(get_optional_user)):
    """Get API statistics."""
    try:
        if da._column_index is None:
            da._load_or_build_column_index()

        total_columns = len(da._column_index)

        # Count columns by country
        country_counts = {}
        for col, info in da._column_index.items():
            for country in info.get('countries', []):
                country_counts[country] = country_counts.get(country, 0) + 1

        # Count columns by frequency
        freq_counts = {'m': 0, 'q': 0, 'a': 0, 'd': 0, 'w': 0}
        for col, info in da._column_index.items():
            for f in info.get('files', []):
                for freq in freq_counts.keys():
                    if f'_{freq}.' in f:
                        freq_counts[freq] += 1
                        break

        return {
            "total_columns": total_columns,
            "columns_by_country": country_counts,
            "columns_by_frequency": freq_counts,
            "countries_available": list(da.data_roots.keys())
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Protected Routes (auth required)
# =============================================================================

@app.get("/series")
async def get_series(
    columns: str = Query(..., description="Semicolon-separated column names"),
    freq: str = Query('m', description="Frequency: m, q, a"),
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD). Defaults to 5 years ago."),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    country: Optional[str] = Query(None, description="Country filter"),
    format: str = Query('records', description="Output format: records, columns, or csv"),
    full_history: bool = Query(False, description="Set to true to get full history instead of last 5 years"),
    user: dict = Depends(get_current_user)
):
    """
    Get time series data for specified columns.

    **Requires authentication.** Use semicolons (;) to separate multiple columns.

    By default, returns last 5 years of data. Use full_history=true for complete history.

    Example: /series?columns=Japan, JGB, 10Y;Japan, JGB, 20Y&freq=m&start=2020-01-01
    """
    # Check rate limit
    rate_info = check_rate_limit(user)

    # Default to last 5 years if no start date and not requesting full history
    if start is None and not full_history:
        start = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')

    try:
        # Parse columns - use semicolon as separator
        col_list = [c.strip() for c in columns.split(';')]

        # Check cache first
        cache_key = hashlib.md5(f"{columns}:{freq}:{start}:{end}:{country}".encode()).hexdigest()
        cached = get_cached_series(cache_key)

        if cached is not None:
            df = cached
        else:
            df = da.get_series(
                columns=col_list,
                freq=freq,
                start_date=start,
                end_date=end,
                country=country
            )
            # Cache the result
            if not df.empty:
                set_cached_series(cache_key, df.copy())

        if df.empty:
            return {
                "columns": col_list,
                "freq": freq,
                "count": 0,
                "data": [],
                "rate_limit": rate_info
            }

        # Reset index for output
        df = df.reset_index()
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

        if format == 'csv':
            return JSONResponse(
                content=df.to_csv(index=False),
                media_type="text/csv"
            )
        elif format == 'columns':
            return {
                "columns": col_list,
                "freq": freq,
                "count": len(df),
                "data": df.to_dict(orient='list'),
                "rate_limit": rate_info
            }
        else:  # records
            return {
                "columns": col_list,
                "freq": freq,
                "count": len(df),
                "data": df.to_dict(orient='records'),
                "rate_limit": rate_info
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/info/{column_name:path}")
async def get_column_info(
    column_name: str,
    user: dict = Depends(get_current_user)
):
    """Get metadata about a specific column."""
    # Check rate limit
    rate_info = check_rate_limit(user)

    try:
        info = da.get_column_info(column_name)
        if not info:
            raise HTTPException(status_code=404, detail=f"Column not found: {column_name}")

        # Add latest dates for each frequency
        latest = {}
        for freq in ['m', 'q', 'a']:
            freq_key = f'_{freq}'
            if any(freq_key in f for f in info.get('files', [])):
                latest[freq] = da.get_latest_date(column_name, freq)

        return {
            "column": column_name,
            "files": info.get('files', []),
            "countries": info.get('countries', []),
            "latest_dates": latest,
            "rate_limit": rate_info
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/usage")
async def get_usage(user: dict = Depends(get_current_user)):
    """Get current user's API usage and rate limit status."""
    identifier = user.get('email', 'anonymous')
    tier = user.get('tier', 'free')
    limit = TIER_LIMITS.get(tier, TIER_LIMITS['free'])

    user_data = request_counts.get(identifier, {'count': 0, 'reset': datetime.now()})

    return {
        "user": {
            "email": user.get('email'),
            "name": user.get('name'),
            "tier": tier,
            "auth_type": user.get('auth_type')
        },
        "usage": {
            "requests_used": user_data['count'],
            "requests_limit": limit,
            "requests_remaining": max(0, limit - user_data['count']),
            "reset_at": (user_data['reset'] + timedelta(days=1)).isoformat()
        },
        "tier_limits": TIER_LIMITS
    }


# =============================================================================
# Admin Routes
# =============================================================================

@app.post("/admin/rebuild-index")
async def rebuild_index(user: dict = Depends(get_current_user)):
    """Rebuild the column index (premium/admin only)."""
    if user.get('tier') != 'premium':
        raise HTTPException(status_code=403, detail="Premium access required")

    try:
        da.rebuild_index()
        return {"status": "success", "message": "Column index rebuilt"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Run
# =============================================================================

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
