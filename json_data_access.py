"""
JSON Data Access Layer
======================
Fast data access using pre-generated JSON files from R2.
"""

import os
import json
import requests
from functools import lru_cache
from typing import Optional, List, Dict
from datetime import datetime

# R2 Configuration
R2_ACCOUNT_ID = "5765013aee7bba62569f876c28e33be2"
R2_BUCKET = "eae-data"
R2_PUBLIC_URL = f"https://pub-12b43d8daa8540218b86ca90ec0e9ae7.r2.dev"
JSON_PREFIX = "json"

# Cache for index
_index_cache = None
_index_loaded_at = None
_INDEX_TTL = 3600  # Reload index every hour


def get_r2_url(path: str) -> str:
    """Get public R2 URL for a file."""
    return f"{R2_PUBLIC_URL}/{JSON_PREFIX}/{path}"


def load_index() -> Dict:
    """Load the series index from R2."""
    global _index_cache, _index_loaded_at

    # Check if we need to reload
    if _index_cache is not None and _index_loaded_at is not None:
        if (datetime.now() - _index_loaded_at).seconds < _INDEX_TTL:
            return _index_cache

    try:
        url = get_r2_url("_index.json")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        _index_cache = response.json()
        _index_loaded_at = datetime.now()
        print(f"Loaded index with {len(_index_cache):,} series")
        return _index_cache
    except Exception as e:
        print(f"Error loading index: {e}")
        if _index_cache is not None:
            return _index_cache
        return {}


def search_series(query: str, freq: Optional[str] = None, country: Optional[str] = None, limit: int = 50) -> List[str]:
    """Search for series by name."""
    index = load_index()
    query_lower = query.lower()

    results = []
    for name, info in index.items():
        # Check frequency filter
        if freq and info.get('frequency') != freq:
            continue

        # Check country filter
        if country and info.get('country') != country:
            continue

        # Check name match
        if query_lower in name.lower():
            results.append(name)

            if len(results) >= limit:
                break

    return results


def get_series_data(name: str, freq: Optional[str] = None) -> Optional[Dict]:
    """
    Get data for a single series.
    Returns the pre-made JSON data directly.
    """
    index = load_index()

    if name not in index:
        return None

    info = index[name]

    # Check frequency
    if freq and info.get('frequency') != freq:
        return None

    # Fetch the JSON file
    try:
        url = get_r2_url(info['file'])
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching series {name}: {e}")
        return None


def get_series_info(name: str) -> Optional[Dict]:
    """Get metadata about a series without fetching the data."""
    index = load_index()

    if name not in index:
        return None

    return index[name]


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

    for name, info in index.items():
        country = info.get('country', 'unknown')
        freq = info.get('frequency', 'unknown')

        by_country[country] = by_country.get(country, 0) + 1
        by_freq[freq] = by_freq.get(freq, 0) + 1

    return {
        'total_series': len(index),
        'by_country': by_country,
        'by_frequency': by_freq
    }
