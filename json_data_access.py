"""
JSON Data Access Layer
======================
Fast data access using pre-generated JSON files from R2.
"""

import os
import json
import boto3
from botocore.config import Config
from typing import Optional, List, Dict
from datetime import datetime

# R2 Configuration (uses same env vars as data_access.py)
R2_BUCKET = os.environ.get('S3_BUCKET', 'eae-data-api')
R2_ENDPOINT = os.environ.get('R2_ENDPOINT', 'fd2c6c5f2d6d8bc9ca228f83b5671df3.r2.cloudflarestorage.com')
JSON_PREFIX = "json"

# Cache for index
_index_cache = None
_index_loaded_at = None
_INDEX_TTL = 3600  # Reload index every hour

# Lazy-initialized S3 client
_s3_client = None


def _get_s3_client():
    """Get or create S3/R2 client."""
    global _s3_client
    if _s3_client is not None:
        return _s3_client

    access_key = os.environ.get('R2_ACCESS_KEY_ID') or os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('R2_SECRET_ACCESS_KEY') or os.environ.get('AWS_SECRET_ACCESS_KEY')

    _s3_client = boto3.client(
        's3',
        endpoint_url=f'https://{R2_ENDPOINT}',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='auto',
        config=Config(signature_version='s3v4')
    )
    return _s3_client


def _get_r2_json(key: str) -> dict:
    """Fetch and parse a JSON file from R2."""
    client = _get_s3_client()
    response = client.get_object(Bucket=R2_BUCKET, Key=key)
    return json.loads(response['Body'].read())


def load_index() -> Dict:
    """Load the series index from R2."""
    global _index_cache, _index_loaded_at

    # Check if we need to reload
    if _index_cache is not None and _index_loaded_at is not None:
        if (datetime.now() - _index_loaded_at).seconds < _INDEX_TTL:
            return _index_cache

    try:
        _index_cache = _get_r2_json(f"{JSON_PREFIX}/_index.json")
        _index_loaded_at = datetime.now()
        print(f"Loaded index with {len(_index_cache):,} series")
        return _index_cache
    except Exception as e:
        print(f"Error loading index: {e}")
        if _index_cache is not None:
            return _index_cache
        return {}


def search_series(query: str, freq: Optional[str] = None, country: Optional[str] = None, limit: int = 50) -> List[Dict]:
    """Search for series by name. Returns list of dicts with name and frequency info."""
    index = load_index()
    query_lower = query.lower()

    results = []
    for name, info in index.items():
        frequencies = info.get('frequencies', {})

        # Check country filter
        if country and info.get('country') != country:
            continue

        # Check frequency filter
        if freq and freq not in frequencies:
            continue

        # Check name match
        if query_lower in name.lower():
            results.append({
                'name': name,
                'country': info.get('country'),
                'frequencies': list(frequencies.keys())
            })

            if len(results) >= limit:
                break

    return results


def get_series_data(name: str, freq: Optional[str] = None) -> Optional[Dict]:
    """
    Get data for a single series.
    Returns the pre-made JSON data directly.
    If freq is not specified, defaults to 'm', then falls back to first available.
    """
    index = load_index()

    if name not in index:
        return None

    info = index[name]
    frequencies = info.get('frequencies', {})

    if not frequencies:
        return None

    # Resolve frequency: use requested, default to 'm', or first available
    if freq and freq in frequencies:
        freq_info = frequencies[freq]
    elif freq:
        return None  # Requested freq not available
    elif 'm' in frequencies:
        freq_info = frequencies['m']
    else:
        freq_info = next(iter(frequencies.values()))

    # Fetch the JSON file from R2
    try:
        return _get_r2_json(f"{JSON_PREFIX}/{freq_info['file']}")
    except Exception as e:
        print(f"Error fetching series {name}: {e}")
        return None


def get_series_info(name: str) -> Optional[Dict]:
    """Get metadata about a series without fetching the data."""
    index = load_index()

    if name not in index:
        return None

    info = index[name]
    return {
        'country': info.get('country'),
        'frequencies': info.get('frequencies', {})
    }


def list_countries() -> List[str]:
    """List available countries."""
    return ['cn', 'jp', 'kr', 'tw', 'region']


def list_frequencies() -> List[str]:
    """List available frequencies."""
    return ['m', 'q', 'a']


def get_stats() -> Dict:
    """Get statistics about available data."""
    index = load_index()

    # Count by country and frequency
    by_country = {}
    by_freq = {}
    total_series_freq = 0

    for name, info in index.items():
        country = info.get('country', 'unknown')
        by_country[country] = by_country.get(country, 0) + 1

        for freq in info.get('frequencies', {}):
            by_freq[freq] = by_freq.get(freq, 0) + 1
            total_series_freq += 1

    return {
        'total_series': len(index),
        'total_series_freq': total_series_freq,
        'by_country': by_country,
        'by_frequency': by_freq
    }
