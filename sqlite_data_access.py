"""
SQLite Data Access Layer
========================
Fast data access using local SQLite database.
Downloads database from R2 if not present.
"""

import sqlite3
import os
import threading
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

# One lock around download / swap / connect. Without it, /admin/refresh-db deleted data.db and spent minutes
# downloading, and any request in that window saw no file, started a second download into the same paths, or
# connected to a half-written file and kept that connection for the life of the process.
_db_lock = threading.RLock()


def check_database(path) -> Optional[str]:
    """None if the SQLite file at `path` is sound, else a description of the problem.
    Runs quick_check (b-tree structure) and the FTS5 index's own integrity check."""
    try:
        conn = sqlite3.connect(path)
        try:
            res = [r[0] for r in conn.execute('PRAGMA quick_check(5)').fetchall()]
            if res != ['ok']:
                return 'quick_check: ' + '; '.join(res)
            conn.execute("INSERT INTO series_fts(series_fts) VALUES('integrity-check')")
            conn.execute('SELECT COUNT(*) FROM series').fetchone()
        finally:
            conn.close()
    except sqlite3.DatabaseError as e:
        return f'{type(e).__name__}: {e}'
    return None


def download_database(replace=False):
    """Download and decompress the database from R2 using boto3.

    Decompresses to a temporary file, checks it, and only then renames it over DB_PATH, so a reader never sees
    a partial file and a bad upload never replaces a good database. replace=False returns early if DB_PATH
    exists. Caller must hold _db_lock."""
    import gzip
    import shutil
    import traceback
    import boto3
    from botocore.config import Config

    if DB_PATH.exists() and not replace:
        print(f"Database already exists: {DB_PATH}")
        return True

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    gz_path = DB_PATH.parent / 'data.db.gz.download'
    tmp_path = DB_PATH.parent / 'data.db.download'

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
            with open(tmp_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        gz_path.unlink()

        problem = check_database(tmp_path)
        if problem:
            print(f"Downloaded database failed its integrity check, keeping the current one: {problem}")
            tmp_path.unlink()
            return False

        for suffix in ('-wal', '-shm'):          # stale WAL files would be applied to the new file
            p = DB_PATH.with_name(DB_PATH.name + suffix)
            if p.exists():
                p.unlink()
        os.replace(tmp_path, DB_PATH)
        print(f"Database ready: {DB_PATH} ({DB_PATH.stat().st_size / 1024 / 1024:.0f}MB)")
        return True

    except Exception as e:
        print(f"Error downloading database: {e}")
        traceback.print_exc()
        for p in (gz_path, tmp_path):
            if p.exists():
                p.unlink()
        return False


def refresh_database():
    """Re-download the database from R2 and swap it in. Returns True on success.

    The current connection keeps serving while the new copy downloads and is checked; the rename leaves the old
    file open for it (same inode). Only after a good swap is the connection dropped, so the next request opens the
    new file. The old connection is not closed here, because another thread may be mid-query on it; it closes when
    garbage-collected. If the new copy fails its check, the old database stays in place.
    """
    global _connection, _stats_cache
    with _db_lock:
        ok = download_database(replace=True)
        if ok:
            _connection = None
            _stats_cache = None
        return ok


def get_connection():
    """Get database connection (reuses existing connection)."""
    global _connection
    if _connection is not None:
        return _connection
    with _db_lock:
        if _connection is None:
            if not DB_PATH.exists():
                if not download_database():
                    raise RuntimeError("Could not download database")

            conn = sqlite3.connect(DB_PATH, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            _connection = conn
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

    except sqlite3.DatabaseError as e:
        # Fall back to LIKE: no FTS hits, or a damaged FTS index (DatabaseError, 'disk image is malformed'),
        # which used to surface as an HTTP 500 for every search
        if not (isinstance(e, sqlite3.OperationalError) and str(e) == 'No FTS results'):
            print(f"FTS search failed, falling back to LIKE: {type(e).__name__}: {e}")
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
