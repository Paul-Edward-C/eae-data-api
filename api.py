"""
FastAPI Server for Economic Data API
=====================================
Provides REST API access to economic data with Ghost membership integration.
SQLite-only backend.
"""

from fastapi import FastAPI, HTTPException, Query, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from collections import defaultdict
from datetime import datetime, timedelta
import json
import jwt
import requests
import os
from pathlib import Path

import sqlite_data_access as sda
import api_keys

# =============================================================================
# Series Visibility Filter (admin vs public)
# =============================================================================

_HIDDEN_SERIES_PATH = Path(__file__).parent / 'hidden_series.json'
_hidden_patterns = []


def _load_hidden_patterns():
    """Load hidden series patterns from config file."""
    global _hidden_patterns
    try:
        if _HIDDEN_SERIES_PATH.exists():
            with open(_HIDDEN_SERIES_PATH) as f:
                config = json.load(f)
            _hidden_patterns = [p.lower() for p in config.get('hidden_patterns', [])]
        else:
            _hidden_patterns = []
    except Exception as e:
        print(f"Warning: Could not load hidden_series.json: {e}")
        _hidden_patterns = []


_load_hidden_patterns()


def is_hidden_series(series_name: str) -> bool:
    """Check if a series should be hidden from public users."""
    if not _hidden_patterns:
        return False
    name_lower = series_name.lower()
    return any(p in name_lower for p in _hidden_patterns)


def is_admin_user(user: Optional[dict]) -> bool:
    """Check if user is admin (legacy env-var API key)."""
    if user is None:
        return False
    return user.get('auth_type') == 'api_key'


# =============================================================================
# Configuration
# =============================================================================

# Ghost configuration (set via environment variables)
GHOST_URL = os.environ.get('GHOST_URL', '')
GHOST_ADMIN_KEY = os.environ.get('GHOST_ADMIN_KEY', '')

# Legacy API keys (for backwards compatibility / admin access)
API_KEYS = os.environ.get('API_KEYS', 'demo-key-123').split(',')

# Tier limits (series lookups per month)
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
    version="3.0.0",
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

# Initialize API keys database
api_keys.init_db()

# Warm SQLite connection + stats cache at startup (avoids 40s cold hit on first request)
try:
    sda.get_stats()
    print(f"Database ready: {sda.DB_PATH}")
except Exception as e:
    print(f"Warning: could not warm database at startup: {e}")

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
        "version": "3.0.0",
        "description": "Economic time series data for China, Japan, Korea, Taiwan, and regional aggregates",
        "authentication": {
            "per_user_key": "X-API-Key: eae_... (get yours at /api-keys/)",
            "ghost_members": "Authorization: Bearer <member_token>",
            "legacy_api_key": "X-API-Key: <admin_key>"
        },
        "tier_limits": TIER_LIMITS,
        "endpoints": {
            "search": "/search?q={pattern}&freq={m|q|a}&country={cn|jp|kr|tw|region}",
            "series": "/series/{series_name}?freq={m|q|a}&start={date}&end={date}",
            "multi_series": "/series?columns={name1;name2;name3}&freq={m|q|a}&start={date}&end={date}",
            "info": "/info/{series_name}",
            "stats": "/stats",
            "countries": "/countries",
            "frequencies": "/frequencies",
            "health": "/health",
            "usage": "/usage",
            "debug": "/debug"
        },
        "note": "Use semicolons (;) to separate multiple columns in /series?columns= endpoint",
        "docs": "/docs"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "East Asia Econ Data API", "version": "3.0.0"}


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


# =============================================================================
# Data Routes — dual-decorated for root + /v3 backward compat
# =============================================================================

@app.get("/search")
@app.get("/v3/search")
async def search_series(
    q: str = Query(..., description="Search pattern (case-insensitive)"),
    freq: Optional[str] = Query(None, description="Frequency filter: m, q, a"),
    country: Optional[str] = Query(None, description="Country filter: cn, jp, kr, tw, region"),
    limit: int = Query(50, description="Maximum results", le=200),
    user: Optional[dict] = Depends(get_optional_user)
):
    """
    Search for series by name.

    No authentication required for search.
    Returns series metadata without data.
    """
    if user is None:
        limit = min(limit, 20)

    try:
        fetch_limit = limit if is_admin_user(user) else limit * 3
        results = sda.search_series(q, freq=freq, country=country, limit=fetch_limit)

        if not is_admin_user(user):
            results = [r for r in results if not is_hidden_series(r['name'])]
        results = results[:limit]

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


# Multi-series endpoint — registered BEFORE /series/{name} so FastAPI matches query-param version first
@app.get("/series")
@app.get("/v3/series")
async def get_multi_series(
    columns: str = Query(..., description="Semicolon-separated series names"),
    freq: Optional[str] = Query(None, description="Frequency: m, q, a"),
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    user: dict = Depends(get_current_user)
):
    """
    Get data for multiple series in one call.

    **Requires authentication.** Use semicolons (;) to separate series names.

    Example: /series?columns=Japan, CPI;Japan, PPI&freq=m
    """
    names = [n.strip() for n in columns.split(';')]
    if not is_admin_user(user):
        names = [n for n in names if not is_hidden_series(n)]

    rate_info = check_rate_limit(user)

    try:
        result = sda.get_multi_series_data(names, freq=freq, start=start, end=end)

        return {
            "columns": names,
            "freq": freq,
            "count": len(result['series']),
            "series": result['series'],
            "not_found": result['not_found'],
            "rate_limit": rate_info
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/series/{series_name:path}")
@app.get("/v3/series/{series_name:path}")
async def get_series(
    series_name: str,
    freq: Optional[str] = Query(None, description="Frequency: m, q, a (defaults to m)"),
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    user: dict = Depends(get_current_user)
):
    """
    Get data for a single series.

    **Requires authentication.**
    """
    if not is_admin_user(user) and is_hidden_series(series_name):
        raise HTTPException(status_code=404, detail=f"Series not found: {series_name}")

    rate_info = check_rate_limit(user)

    try:
        data = sda.get_series_data(series_name, freq=freq, start=start, end=end)

        if data is None:
            raise HTTPException(status_code=404, detail=f"Series not found: {series_name}")

        data['rate_limit'] = rate_info
        return data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/info/{series_name:path}")
@app.get("/v3/info/{series_name:path}")
async def get_series_info(
    series_name: str,
    user: Optional[dict] = Depends(get_optional_user)
):
    """
    Get metadata about a series.

    No authentication required.
    """
    if not is_admin_user(user) and is_hidden_series(series_name):
        raise HTTPException(status_code=404, detail=f"Series not found: {series_name}")

    try:
        info = sda.get_series_info(series_name)

        if info is None:
            raise HTTPException(status_code=404, detail=f"Series not found: {series_name}")

        return info
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
@app.get("/v3/stats")
async def get_stats():
    """Get statistics about available data."""
    try:
        return sda.get_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debug")
@app.get("/v3/debug")
async def debug():
    """Debug endpoint for database status."""
    result = {
        "db_path": str(sda.DB_PATH),
        "db_exists": sda.DB_PATH.exists(),
        "r2_bucket": sda.R2_BUCKET,
        "r2_key": sda.R2_DB_KEY,
    }

    if sda.DB_PATH.exists():
        result["db_size_mb"] = round(sda.DB_PATH.stat().st_size / 1024 / 1024, 1)

    return result


# =============================================================================
# Usage & API Key Routes (unchanged)
# =============================================================================

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

@app.post("/admin/reload-hidden-series")
async def reload_hidden_series(user: dict = Depends(get_current_user)):
    """Reload hidden series config (admin only)."""
    if not is_admin_user(user):
        raise HTTPException(status_code=403, detail="Admin access required")

    _load_hidden_patterns()
    return {
        "status": "success",
        "hidden_patterns_count": len(_hidden_patterns),
        "patterns": _hidden_patterns
    }


@app.post("/admin/refresh-db")
async def refresh_database(user: dict = Depends(get_current_user)):
    """Delete local data.db and re-download from R2 (admin only)."""
    if not is_admin_user(user):
        raise HTTPException(status_code=403, detail="Admin access required")

    success = sda.refresh_database()
    if success:
        return {"status": "success", "message": "Database refreshed from R2"}
    else:
        raise HTTPException(status_code=500, detail="Failed to refresh database from R2")


# =============================================================================
# Run
# =============================================================================

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
