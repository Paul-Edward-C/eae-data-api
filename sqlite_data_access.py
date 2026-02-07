"""
SQLite Data Access Layer
========================
Fast data access using local SQLite database.
Downloads database from R2 if not present.
"""

import sqlite3
import os
from typing import Optional, List, Dict
from pathlib import Path

# Database path — use Railway volume if available so DB persists across deploys
_volume = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH')
DB_PATH = Path(_volume) / 'data.db' if _volume else Path.home() / '.local' / 'data_api' / 'data.db'

# R2 download configuration
R2_BUCKET = os.environ.get('S3_BUCKET', 'eae-data-api')
R2_ENDPOINT = os.environ.get('R2_ENDPOINT', 'fd2c6c5f2d6d8bc9ca228f83b5671df3.r2.cloudflarestorage.com')
R2_DB_KEY = 'data.db.gz'

# Connection pool (reuse connections)
_connection = None
_stats_cache = None


def download_database():
    """Download and decompress database from R2 using boto3."""
    import gzip
    import shutil
    import traceback
    import boto3
    from botocore.config import Config

    if DB_PATH.exists():
        print(f"Database already exists: {DB_PATH}")
        return True

    gz_path = DB_PATH.parent / 'data.db.gz'

    try:
        access_key = os.environ.get('R2_ACCESS_KEY_ID') or os.environ.get('AWS_ACCESS_KEY_ID')
        secret_key = os.environ.get('R2_SECRET_ACCESS_KEY') or os.environ.get('AWS_SECRET_ACCESS_KEY')

        client = boto3.client(
            's3',
            endpoint_url=f'https://{R2_ENDPOINT}',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='auto',
            config=Config(signature_version='s3v4')
        )

        print(f"Downloading database from R2 ({R2_BUCKET}/{R2_DB_KEY})...")
        client.download_file(R2_BUCKET, R2_DB_KEY, str(gz_path))

        print(f"Download complete: {gz_path.stat().st_size / 1024 / 1024:.0f}MB")
        print("Decompressing...")

        with gzip.open(gz_path, 'rb') as f_in:
            with open(DB_PATH, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Remove compressed file
        gz_path.unlink()

        print(f"Database ready: {DB_PATH} ({DB_PATH.stat().st_size / 1024 / 1024:.0f}MB)")
        return True

    except Exception as e:
        print(f"Error downloading database: {e}")
        traceback.print_exc()
        if gz_path.exists():
            gz_path.unlink()
        if DB_PATH.exists():
            DB_PATH.unlink()
        return False


def refresh_database():
    """Delete existing database and re-download from R2. Returns True on success."""
    global _connection, _stats_cache
    _stats_cache = None
    # Close existing connection
    if _connection is not None:
        try:
            _connection.close()
        except Exception:
            pass
        _connection = None

    # Delete existing database
    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f"Deleted existing database: {DB_PATH}")

    return download_database()


def get_connection():
    """Get database connection (reuses existing connection)."""
    global _connection
    if _connection is None:
        if not DB_PATH.exists():
            if not download_database():
                raise RuntimeError("Could not download database")

        _connection = sqlite3.connect(DB_PATH, check_same_thread=False)
        _connection.row_factory = sqlite3.Row
    return _connection


def search_series(query: str, freq: Optional[str] = None, country: Optional[str] = None, limit: int = 50) -> List[Dict]:
    """
    Search for series by name. Groups results by series name and returns
    available frequencies for each.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Build query to get matching series grouped by name
    try:
        # Try FTS first
        fts_query = query.replace('"', '').replace("'", "")
        sql = '''
            SELECT s.name, s.country, s.frequency
            FROM series s
            JOIN series_fts fts ON s.id = fts.rowid
            WHERE series_fts MATCH ?
        '''
        params = [f'"{fts_query}"']

        if freq:
            sql += ' AND s.frequency = ?'
            params.append(freq)
        if country:
            sql += ' AND s.country = ?'
            params.append(country)

        sql += ' ORDER BY s.name'
        cursor.execute(sql, params)
        rows = cursor.fetchall()

        if not rows:
            raise sqlite3.OperationalError("No FTS results")

    except sqlite3.OperationalError:
        # Fall back to LIKE
        sql = 'SELECT name, country, frequency FROM series WHERE name LIKE ?'
        params = [f'%{query}%']

        if freq:
            sql += ' AND frequency = ?'
            params.append(freq)
        if country:
            sql += ' AND country = ?'
            params.append(country)

        sql += ' ORDER BY name'
        cursor.execute(sql, params)
        rows = cursor.fetchall()

    # Group by name, collect frequencies
    seen = {}
    results = []
    for row in rows:
        name = row['name']
        if name not in seen:
            seen[name] = {
                'name': name,
                'country': row['country'],
                'frequencies': []
            }
            results.append(seen[name])
        seen[name]['frequencies'].append(row['frequency'])

        if len(results) >= limit and name != rows[-1]['name']:
            break

    # Sort frequencies consistently
    for r in results:
        r['frequencies'] = sorted(set(r['frequencies']))

    return results[:limit]


def get_series_data(name: str, freq: Optional[str] = None, start: Optional[str] = None, end: Optional[str] = None) -> Optional[Dict]:
    """
    Get data for a single series.
    If freq not specified, defaults to 'm', then falls back to first available.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Find the right series row
    if freq:
        cursor.execute(
            'SELECT id, name, country, frequency, min_date, max_date, count FROM series WHERE name = ? AND frequency = ?',
            (name, freq))
    else:
        # Try monthly first, then any
        cursor.execute(
            'SELECT id, name, country, frequency, min_date, max_date, count FROM series WHERE name = ? AND frequency = ?',
            (name, 'm'))

    series = cursor.fetchone()

    if not series and not freq:
        # Fall back to any frequency
        cursor.execute(
            'SELECT id, name, country, frequency, min_date, max_date, count FROM series WHERE name = ? ORDER BY frequency',
            (name,))
        series = cursor.fetchone()

    if not series:
        return None

    series_dict = dict(series)
    series_id = series_dict['id']

    # Build data query
    sql = 'SELECT date, value FROM data WHERE series_id = ?'
    params = [series_id]

    if start:
        sql += ' AND date >= ?'
        params.append(start)
    if end:
        sql += ' AND date <= ?'
        params.append(end)

    sql += ' ORDER BY date'

    cursor.execute(sql, params)
    data_rows = cursor.fetchall()

    return {
        'series_name': series_dict['name'],
        'country': series_dict['country'],
        'frequency': series_dict['frequency'],
        'count': len(data_rows),
        'min_date': data_rows[0]['date'] if data_rows else series_dict['min_date'],
        'max_date': data_rows[-1]['date'] if data_rows else series_dict['max_date'],
        'data': [{'Date': row['date'], 'value': row['value']} for row in data_rows]
    }


def get_series_info(name: str) -> Optional[Dict]:
    """Get metadata about a series — returns all available frequencies."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT name, country, frequency, min_date, max_date, count
        FROM series WHERE name = ?
        ORDER BY frequency
    ''', (name,))

    rows = cursor.fetchall()
    if not rows:
        return None

    frequencies = {}
    country = None
    for row in rows:
        country = row['country']
        frequencies[row['frequency']] = {
            'count': row['count'],
            'min_date': row['min_date'],
            'max_date': row['max_date']
        }

    return {
        'series_name': name,
        'country': country,
        'frequencies': frequencies
    }


def get_stats() -> Dict:
    """Get statistics from pre-computed stats table. Cached in memory after first call."""
    global _stats_cache
    if _stats_cache is not None:
        return _stats_cache

    import json as _json
    conn = get_connection()
    cursor = conn.cursor()

    # Try pre-computed stats table first (instant)
    try:
        cursor.execute('SELECT key, value FROM stats')
        raw = {row['key']: row['value'] for row in cursor.fetchall()}
        if raw:
            _stats_cache = {
                'total_series': int(raw['total_series']),
                'total_series_freq': int(raw['total_series_freq']),
                'total_data_points': int(raw['total_data_points']),
                'by_country': _json.loads(raw['by_country']),
                'by_frequency': _json.loads(raw['by_frequency'])
            }
            return _stats_cache
    except Exception:
        pass

    # Fallback: compute live (slow, for old databases without stats table)
    cursor.execute('''
        SELECT
            COUNT(DISTINCT name) as total_names,
            COUNT(*) as total_series,
            SUM(count) as total_data_points
        FROM series
    ''')
    row = cursor.fetchone()

    cursor.execute('SELECT country, COUNT(DISTINCT name) as count FROM series GROUP BY country')
    by_country = {r['country']: r['count'] for r in cursor.fetchall()}

    cursor.execute('SELECT frequency, COUNT(*) as count FROM series GROUP BY frequency')
    by_freq = {r['frequency']: r['count'] for r in cursor.fetchall()}

    _stats_cache = {
        'total_series': row['total_names'],
        'total_series_freq': row['total_series'],
        'total_data_points': row['total_data_points'] or 0,
        'by_country': by_country,
        'by_frequency': by_freq
    }
    return _stats_cache


def get_multi_series_data(names: List[str], freq: Optional[str] = None, start: Optional[str] = None, end: Optional[str] = None) -> Dict:
    """
    Get data for multiple series in one call.
    Returns dict with found series and list of not-found names.
    """
    series = {}
    not_found = []

    for name in names:
        data = get_series_data(name, freq=freq, start=start, end=end)
        if data is not None:
            series[name] = data
        else:
            not_found.append(name)

    return {
        'series': series,
        'not_found': not_found
    }


def list_countries() -> List[str]:
    """List available countries."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT country FROM series ORDER BY country')
    return [row[0] for row in cursor.fetchall()]


def list_frequencies() -> List[str]:
    """List available frequencies."""
    return ['a', 'm', 'q']
