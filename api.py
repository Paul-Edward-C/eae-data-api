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
import json_data_access as jda  # Fast JSON-based access
import api_keys

# SQLite access is optional (only available if data.db exists)
try:
    import sqlite_data_access as sda
    SQLITE_AVAILABLE = True
except Exception:
    SQLITE_AVAILABLE = False
    sda = None

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

# Tier limits (series lookups per month)
# Maps to Ghost Pro tiers:
#   - free: Free members (no paid tier)
#   - daily: "Daily" tier subscribers
#   - premium: "Daily + Data" and "East Asia" tier subscribers, plus admin API key users
TIER_LIMITS = {
    'free': 10,        # Free Ghost members — 10/month
    'daily': 30,       # "Daily" tier subscribers — 30/month
    'premium': None,   # "Daily + Data", "East Asia" tiers, and admin — unlimited
}

# =============================================================================
# Initialize App
# =============================================================================

app = FastAPI(
    title="East Asia Econ Data API",
    description="""
API for accessing economic time series data for China, Japan, Korea, Taiwan, and regional aggregates.

## Authentication

**Per-user API key (recommended):** Get your key at /api-keys/ on eastasiaecon.com, then:
```
X-API-Key: eae_your_key_here
```

**Ghost member token:** Use your member JWT:
```
Authorization: Bearer <your_ghost_member_token>
```

## Rate Limits (monthly)

- Free members: 10 series lookups/month
- Daily subscribers: 30 series lookups/month
- Premium (Daily + Data, East Asia): Unlimited
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

# Initialize API keys database
api_keys.init_db()

# Simple in-memory rate limiter (legacy fallback for Ghost JWT auth)
request_counts = defaultdict(lambda: {'count': 0, 'reset': datetime.now()})

# =============================================================================
# Ghost Members Integration
# =============================================================================

def get_ghost_member(token: str) -> Optional[dict]:
    """Verify Ghost member token and return member info with tier."""
    if not GHOST_URL or not GHOST_ADMIN_KEY:
        return None

    try:
        decoded = jwt.decode(token, options={"verify_signature": False})
        email = decoded.get('sub') or decoded.get('email')

        if not email:
            return None

        member = _lookup_ghost_member_by_email(email)
        if member:
            member['auth_type'] = 'ghost'
            return member

        return None

    except jwt.DecodeError:
        return None
    except Exception as e:
        print(f"Ghost auth error: {e}")
        return None


# =============================================================================
# Ghost Admin API Helpers
# =============================================================================

def _get_ghost_admin_token() -> Optional[str]:
    """Create a Ghost Admin API JWT for server-to-server calls."""
    if not GHOST_URL or not GHOST_ADMIN_KEY:
        return None
    try:
        key_id, key_secret = GHOST_ADMIN_KEY.split(':')
        iat = int(datetime.now().timestamp())
        header = {'alg': 'HS256', 'typ': 'JWT', 'kid': key_id}
        payload = {'iat': iat, 'exp': iat + 300, 'aud': '/admin/'}
        return jwt.encode(
            payload,
            bytes.fromhex(key_secret),
            algorithm='HS256',
            headers=header
        )
    except Exception:
        return None


def _determine_tier(member: dict) -> str:
    """Determine API tier from Ghost member data."""
    tier_names = [t.get('name', '').lower() for t in member.get('tiers', [])]
    if any('east asia' in t or 'data' in t for t in tier_names):
        return 'premium'
    elif any('daily' in t for t in tier_names):
        return 'daily'
    elif member.get('status') == 'comped':
        return 'premium'
    return 'free'


def _lookup_ghost_member_by_email(email: str) -> Optional[dict]:
    """
    Look up a Ghost member by email via the Admin API.
    Returns dict with email, name, tier, uuid — or None if not found.
    """
    admin_token = _get_ghost_admin_token()
    if not admin_token:
        return None

    try:
        response = requests.get(
            f"{GHOST_URL}/ghost/api/admin/members/?filter=email:'{email}'",
            headers={"Authorization": f"Ghost {admin_token}"},
            timeout=10
        )
        if response.ok:
            members = response.json().get('members', [])
            if members:
                member = members[0]
                return {
                    'email': member.get('email', email),
                    'name': member.get('name', ''),
                    'tier': _determine_tier(member),
                    'uuid': member.get('uuid'),
                }
        return None
    except Exception as e:
        print(f"Ghost lookup error: {e}")
        return None


# =============================================================================
# Authentication Dependencies
# =============================================================================

async def get_current_user(
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None)
) -> dict:
    """
    Authenticate user via per-user API key, Ghost member token, or legacy API key.
    Returns user info with tier.
    """
    # 1. Try per-user API key (eae_... keys stored in keys.db)
    if x_api_key and x_api_key.startswith('eae_'):
        key_info = api_keys.get_key_info(x_api_key)
        if key_info:
            return {
                'email': key_info['email'],
                'name': key_info['name'],
                'tier': key_info['tier'],
                'uuid': key_info.get('ghost_uuid'),
                'auth_type': 'user_api_key',
                'api_key': key_info['api_key']
            }
        raise HTTPException(status_code=401, detail="Invalid API key")

    # 2. Try Ghost member token
    if authorization and authorization.startswith('Bearer '):
        token = authorization.replace('Bearer ', '')
        member = get_ghost_member(token)
        if member:
            return member

    # 3. Fall back to legacy env-var API key (admin access = premium tier)
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
        detail="Authentication required. Provide API key (X-API-Key: eae_...) or Ghost member token (Authorization: Bearer <token>)"
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
    Check if user has exceeded their tier's monthly rate limit.
    Uses SQLite tracking for per-user API keys, in-memory for legacy auth.
    """
    tier = user.get('tier', 'free')

    # Per-user API keys: use SQLite monthly tracking
    if user.get('auth_type') == 'user_api_key' and user.get('api_key'):
        usage = api_keys.check_and_increment_usage(user['api_key'], tier)

        if usage['limit'] is not None and usage['used'] > usage['limit']:
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "Monthly rate limit exceeded",
                    "tier": tier,
                    "limit": usage['limit'],
                    "used": usage['used'],
                    "month": usage['month'],
                    "upgrade": "Upgrade your Ghost membership for higher limits"
                }
            )

        return {
            'used': usage['used'],
            'limit': usage['limit'],
            'remaining': usage['remaining'],
            'tier': tier,
            'period': 'monthly',
            'month': usage['month']
        }

    # Legacy auth (Ghost JWT / env-var keys): use in-memory daily tracking
    identifier = user.get('email', 'anonymous')
    limit = TIER_LIMITS.get(tier, TIER_LIMITS['free'])

    now = datetime.now()
    user_data = request_counts[identifier]

    if now - user_data['reset'] > timedelta(days=1):
        user_data['count'] = 0
        user_data['reset'] = now

    if limit is not None and user_data['count'] >= limit:
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
        'remaining': (limit - user_data['count']) if limit is not None else None,
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
            "per_user_key": "X-API-Key: eae_... (get yours at /api-keys/)",
            "ghost_members": "Authorization: Bearer <member_token>",
            "legacy_api_key": "X-API-Key: <admin_key>"
        },
        "tier_limits": TIER_LIMITS,
        "endpoints": {
            "v2_search": "/v2/search?q={pattern}&freq={m|q|a}&country={cn|jp|kr|tw|region} (recommended)",
            "v2_series": "/v2/series/{series_name}?freq={m|q|a}&start={date}&end={date} (recommended, fast)",
            "v2_info": "/v2/info/{series_name}",
            "v2_stats": "/v2/stats",
            "search": "/search?q={pattern}&freq={m|q|a} (legacy)",
            "series": "/series?columns={col1;col2}&freq={m|q|a} (legacy, slower)",
            "countries": "/countries",
            "frequencies": "/frequencies",
            "health": "/health",
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
    tier = user.get('tier', 'free')
    limit = TIER_LIMITS.get(tier, TIER_LIMITS['free'])

    # Per-user API key: read from SQLite
    if user.get('auth_type') == 'user_api_key' and user.get('api_key'):
        usage = api_keys.get_usage(user['api_key'])
        return {
            "user": {
                "email": user.get('email'),
                "name": user.get('name'),
                "tier": tier,
                "auth_type": user.get('auth_type')
            },
            "usage": {
                "requests_used": usage['used'],
                "requests_limit": limit,
                "requests_remaining": max(0, limit - usage['used']) if limit is not None else None,
                "period": "monthly",
                "month": usage['month']
            },
            "tier_limits": TIER_LIMITS
        }

    # Legacy auth
    identifier = user.get('email', 'anonymous')
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
            "requests_remaining": max(0, limit - user_data['count']) if limit is not None else None,
            "reset_at": (user_data['reset'] + timedelta(days=1)).isoformat()
        },
        "tier_limits": TIER_LIMITS
    }


# =============================================================================
# API Key Management Routes
# =============================================================================

@app.post("/keys/provision")
async def provision_api_key(
    email: str = Query(..., description="Ghost member email address")
):
    """
    Provision an API key for a Ghost member.
    Validates email against Ghost Admin API before issuing a key.
    Idempotent: returns existing key if already provisioned.
    """
    # Validate against Ghost
    ghost_member = _lookup_ghost_member_by_email(email)
    if not ghost_member:
        raise HTTPException(
            status_code=403,
            detail="Email not found in Ghost membership. You must be a registered member."
        )

    # Provision (or return existing) key
    key_info = api_keys.provision_key(
        email=ghost_member['email'],
        name=ghost_member.get('name', ''),
        tier=ghost_member['tier'],
        ghost_uuid=ghost_member.get('uuid')
    )

    # Get current usage
    usage = api_keys.get_usage(key_info['api_key'])
    limit = TIER_LIMITS.get(key_info['tier'], TIER_LIMITS['free'])

    return {
        "api_key": key_info['api_key'],
        "email": key_info['email'],
        "name": key_info['name'],
        "tier": key_info['tier'],
        "created_at": key_info['created_at'],
        "usage": {
            "used": usage['used'],
            "limit": limit,
            "remaining": max(0, limit - usage['used']) if limit is not None else None,
            "month": usage['month']
        }
    }


@app.get("/keys/me")
async def get_my_key(user: dict = Depends(get_current_user)):
    """
    Get the current user's API key info and usage.
    Requires authentication with a per-user API key.
    """
    if user.get('auth_type') != 'user_api_key':
        raise HTTPException(
            status_code=400,
            detail="This endpoint requires a per-user API key (eae_...). Use POST /keys/provision to get one."
        )

    usage = api_keys.get_usage(user['api_key'])
    limit = TIER_LIMITS.get(user['tier'], TIER_LIMITS['free'])

    return {
        "api_key": user['api_key'],
        "email": user['email'],
        "name": user['name'],
        "tier": user['tier'],
        "usage": {
            "used": usage['used'],
            "limit": limit,
            "remaining": max(0, limit - usage['used']) if limit is not None else None,
            "month": usage['month']
        }
    }


@app.post("/keys/regenerate")
async def regenerate_api_key(
    email: str = Query(..., description="Ghost member email address")
):
    """
    Generate a new API key, invalidating the old one.
    Validates email against Ghost Admin API.
    """
    # Validate against Ghost
    ghost_member = _lookup_ghost_member_by_email(email)
    if not ghost_member:
        raise HTTPException(
            status_code=403,
            detail="Email not found in Ghost membership."
        )

    key_info = api_keys.regenerate_key(email)
    if not key_info:
        raise HTTPException(
            status_code=404,
            detail="No existing API key found for this email. Use POST /keys/provision first."
        )

    return {
        "api_key": key_info['api_key'],
        "email": key_info['email'],
        "name": key_info['name'],
        "tier": key_info['tier'],
        "created_at": key_info['created_at'],
        "message": "New key generated. Your old key has been deactivated."
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
# Fast JSON-Based Endpoints (recommended for speed)
# =============================================================================

@app.get("/v2/search")
async def search_series_v2(
    q: str = Query(..., description="Search pattern (case-insensitive)"),
    freq: Optional[str] = Query(None, description="Frequency filter: m, q, a"),
    country: Optional[str] = Query(None, description="Country filter: cn, jp, kr, tw, region"),
    limit: int = Query(50, description="Maximum results", le=200),
    user: Optional[dict] = Depends(get_optional_user)
):
    """
    Search for series (v2 - uses fast JSON index).

    No authentication required for search.
    """
    if user is None:
        limit = min(limit, 20)

    try:
        results = jda.search_series(q, freq=freq, country=country, limit=limit)
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


@app.get("/v2/series/{series_name:path}")
async def get_series_v2(
    series_name: str,
    freq: Optional[str] = Query(None, description="Frequency: m, q, a (optional, uses index default)"),
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD) - filters returned data"),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD) - filters returned data"),
    user: dict = Depends(get_current_user)
):
    """
    Get data for a single series (v2 - fast JSON-based).

    **Requires authentication.**

    This endpoint is much faster than /series as it fetches pre-generated JSON files.
    """
    rate_info = check_rate_limit(user)

    try:
        data = jda.get_series_data(series_name, freq=freq)

        if data is None:
            raise HTTPException(status_code=404, detail=f"Series not found: {series_name}")

        # Filter by date range if specified
        if start or end:
            filtered_data = []
            for row in data['data']:
                date = row['Date']
                if start and date < start:
                    continue
                if end and date > end:
                    continue
                filtered_data.append(row)
            data['data'] = filtered_data
            data['count'] = len(filtered_data)
            if filtered_data:
                data['min_date'] = filtered_data[0]['Date']
                data['max_date'] = filtered_data[-1]['Date']

        data['rate_limit'] = rate_info
        return data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/v2/info/{series_name:path}")
async def get_series_info_v2(
    series_name: str,
    user: Optional[dict] = Depends(get_optional_user)
):
    """
    Get metadata about a series (v2 - fast, no data download).

    No authentication required.
    """
    try:
        info = jda.get_series_info(series_name)

        if info is None:
            raise HTTPException(status_code=404, detail=f"Series not found: {series_name}")

        return {
            "series_name": series_name,
            **info
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/v2/stats")
async def get_stats_v2():
    """Get statistics about available data (v2)."""
    try:
        return jda.get_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# v3 SQLite-Based Endpoints (fastest, recommended)
# =============================================================================

@app.get("/v3/search")
async def search_series_v3(
    q: str = Query(..., description="Search pattern (case-insensitive)"),
    freq: Optional[str] = Query(None, description="Frequency filter: m, q, a"),
    country: Optional[str] = Query(None, description="Country filter: cn, jp, kr, tw, region"),
    limit: int = Query(50, description="Maximum results", le=200),
    user: Optional[dict] = Depends(get_optional_user)
):
    """
    Search for series (v3 - SQLite, fastest).

    No authentication required for search.
    Returns series metadata without data.
    """
    if not SQLITE_AVAILABLE:
        raise HTTPException(status_code=503, detail="SQLite database not available. Use /search instead.")

    if user is None:
        limit = min(limit, 20)

    try:
        results = sda.search_series(q, freq=freq, country=country, limit=limit)
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


@app.get("/v3/series/{series_name:path}")
async def get_series_v3(
    series_name: str,
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    user: dict = Depends(get_current_user)
):
    """
    Get data for a single series (v3 - SQLite, fastest).

    **Requires authentication.**
    """
    if not SQLITE_AVAILABLE:
        raise HTTPException(status_code=503, detail="SQLite database not available. Use /series instead.")

    rate_info = check_rate_limit(user)

    try:
        data = sda.get_series_data(series_name, start=start, end=end)

        if data is None:
            raise HTTPException(status_code=404, detail=f"Series not found: {series_name}")

        data['rate_limit'] = rate_info
        return data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/v3/info/{series_name:path}")
async def get_series_info_v3(
    series_name: str,
    user: Optional[dict] = Depends(get_optional_user)
):
    """
    Get metadata about a series (v3 - SQLite).

    No authentication required.
    """
    if not SQLITE_AVAILABLE:
        raise HTTPException(status_code=503, detail="SQLite database not available.")

    try:
        info = sda.get_series_info(series_name)

        if info is None:
            raise HTTPException(status_code=404, detail=f"Series not found: {series_name}")

        return info
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/v3/stats")
async def get_stats_v3():
    """Get statistics about available data (v3 - SQLite)."""
    if not SQLITE_AVAILABLE:
        raise HTTPException(status_code=503, detail="SQLite database not available. Use /stats instead.")

    try:
        return sda.get_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/v3/debug")
async def debug_v3():
    """Debug endpoint for SQLite database status."""
    import subprocess
    from pathlib import Path

    result = {
        "sqlite_available": SQLITE_AVAILABLE,
        "db_path": str(sda.DB_PATH) if SQLITE_AVAILABLE else None,
        "db_exists": sda.DB_PATH.exists() if SQLITE_AVAILABLE else False,
        "db_url": sda.DB_URL if SQLITE_AVAILABLE else None,
    }

    if SQLITE_AVAILABLE:
        # Check if curl is available
        try:
            curl_result = subprocess.run(['curl', '--version'], capture_output=True, text=True, timeout=5)
            result["curl_available"] = curl_result.returncode == 0
        except Exception as e:
            result["curl_available"] = False
            result["curl_error"] = str(e)

        # Check if gunzip is available
        try:
            gunzip_result = subprocess.run(['gunzip', '--version'], capture_output=True, text=True, timeout=5)
            result["gunzip_available"] = gunzip_result.returncode == 0
        except Exception as e:
            result["gunzip_available"] = False
            result["gunzip_error"] = str(e)

        # Try to download and see what happens
        if not sda.DB_PATH.exists():
            try:
                import requests
                response = requests.head(sda.DB_URL, allow_redirects=True, timeout=10)
                result["url_accessible"] = response.status_code == 200
                result["url_status"] = response.status_code
                result["content_length"] = response.headers.get('content-length')
            except Exception as e:
                result["url_accessible"] = False
                result["url_error"] = str(e)

    return result


# =============================================================================
# Run
# =============================================================================

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
