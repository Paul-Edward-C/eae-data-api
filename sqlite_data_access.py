"""
SQLite Data Access Layer
========================
Fast data access using local SQLite database.
"""

import sqlite3
from typing import Optional, List, Dict
from pathlib import Path

# Database path
DB_PATH = Path(__file__).parent / 'data.db'

# Connection pool (reuse connections)
_connection = None


def get_connection():
    """Get database connection (reuses existing connection)."""
    global _connection
    if _connection is None:
        _connection = sqlite3.connect(DB_PATH, check_same_thread=False)
        _connection.row_factory = sqlite3.Row
    return _connection


def search_series(query: str, freq: Optional[str] = None, country: Optional[str] = None, limit: int = 50) -> List[Dict]:
    """Search for series by name using full-text search."""
    conn = get_connection()
    cursor = conn.cursor()

    # Try FTS first for speed
    try:
        if freq or country:
            # FTS with filters - need to join
            sql = '''
                SELECT s.name, s.country, s.frequency, s.min_date, s.max_date, s.count
                FROM series s
                JOIN series_fts fts ON s.id = fts.rowid
                WHERE series_fts MATCH ?
            '''
            # Escape special FTS characters and create query
            fts_query = query.replace('"', '').replace("'", "")
            params = [f'"{fts_query}"']

            if freq:
                sql += ' AND s.frequency = ?'
                params.append(freq)

            if country:
                sql += ' AND s.country = ?'
                params.append(country)

            sql += ' ORDER BY s.count DESC LIMIT ?'
            params.append(limit)
        else:
            # Pure FTS query
            sql = '''
                SELECT s.name, s.country, s.frequency, s.min_date, s.max_date, s.count
                FROM series s
                JOIN series_fts fts ON s.id = fts.rowid
                WHERE series_fts MATCH ?
                ORDER BY s.count DESC
                LIMIT ?
            '''
            fts_query = query.replace('"', '').replace("'", "")
            params = [f'"{fts_query}"', limit]

        cursor.execute(sql, params)
        results = cursor.fetchall()

        # If no results, fall back to LIKE
        if not results:
            raise sqlite3.OperationalError("No FTS results, falling back to LIKE")

        return [dict(r) for r in results]

    except sqlite3.OperationalError:
        # Fall back to LIKE query
        sql = '''
            SELECT s.name, s.country, s.frequency, s.min_date, s.max_date, s.count
            FROM series s
            WHERE s.name LIKE ?
        '''
        params = [f'%{query}%']

        if freq:
            sql += ' AND s.frequency = ?'
            params.append(freq)

        if country:
            sql += ' AND s.country = ?'
            params.append(country)

        sql += ' ORDER BY s.count DESC LIMIT ?'
        params.append(limit)

        cursor.execute(sql, params)
        results = cursor.fetchall()

        return [dict(r) for r in results]


def get_series_data(name: str, start: Optional[str] = None, end: Optional[str] = None) -> Optional[Dict]:
    """Get data for a single series."""
    conn = get_connection()
    cursor = conn.cursor()

    # Get series metadata
    cursor.execute('''
        SELECT id, name, country, frequency, min_date, max_date, count
        FROM series WHERE name = ?
    ''', (name,))

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
    """Get metadata about a series without fetching the data."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT name, country, frequency, min_date, max_date, count
        FROM series WHERE name = ?
    ''', (name,))

    series = cursor.fetchone()
    if not series:
        return None

    return dict(series)


def get_stats() -> Dict:
    """Get statistics about available data."""
    conn = get_connection()
    cursor = conn.cursor()

    # Total series
    cursor.execute('SELECT COUNT(*) FROM series')
    total = cursor.fetchone()[0]

    # By country
    cursor.execute('''
        SELECT country, COUNT(*) as count
        FROM series GROUP BY country
    ''')
    by_country = {row['country']: row['count'] for row in cursor.fetchall()}

    # By frequency
    cursor.execute('''
        SELECT frequency, COUNT(*) as count
        FROM series GROUP BY frequency
    ''')
    by_freq = {row['frequency']: row['count'] for row in cursor.fetchall()}

    # Total data points
    cursor.execute('SELECT COUNT(*) FROM data')
    total_data_points = cursor.fetchone()[0]

    return {
        'total_series': total,
        'total_data_points': total_data_points,
        'by_country': by_country,
        'by_frequency': by_freq
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
