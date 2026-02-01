#!/usr/bin/env python3
"""
Create SQLite Database from Parquet Files
==========================================
Creates a SQLite database with all series data.
This is fast to query and can be deployed as a single file.
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

# Configuration
LOCAL_DATA_ROOTS = {
    'cn': '/Users/paul/Documents/DATA/cn/cn_input',
    'jp': '/Users/paul/Documents/DATA/jp/jp_input',
    'kr': '/Users/paul/Documents/DATA/kr/kr_input',
    'tw': '/Users/paul/Documents/DATA/tw/tw_input',
    'region': '/Users/paul/Documents/DATA/region/region_input',
}

OUTPUT_DB = Path('/Users/paul/Documents/DATA/tools/data_api/data.db')
EXCLUDED_PATTERNS = ['latest', 'recent', 'hist', 'history']
VALID_SUFFIXES = ['_m.parquet', '_q.parquet', '_a.parquet']


def get_frequency(filename):
    """Extract frequency from filename."""
    for suffix in VALID_SUFFIXES:
        if filename.endswith(suffix):
            return suffix.split('.')[0][-1]
    return None


def is_excluded(filename):
    """Check if file should be excluded."""
    name_lower = filename.lower()
    return any(pattern in name_lower for pattern in EXCLUDED_PATTERNS)


def create_tables(conn):
    """Create database tables."""
    cursor = conn.cursor()

    # Series metadata table (unique on name + frequency)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS series (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            country TEXT NOT NULL,
            frequency TEXT NOT NULL,
            min_date TEXT,
            max_date TEXT,
            count INTEGER,
            UNIQUE(name, frequency)
        )
    ''')

    # Data table (long format)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data (
            series_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            value REAL,
            FOREIGN KEY (series_id) REFERENCES series(id)
        )
    ''')

    conn.commit()


def create_indexes(conn):
    """Create indexes for fast queries."""
    cursor = conn.cursor()
    print("Creating indexes...")

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_series_name ON series(name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_series_country ON series(country)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_series_freq ON series(frequency)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_series ON data(series_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_date ON data(series_id, date)')

    # Full-text search index for series names
    cursor.execute('CREATE VIRTUAL TABLE IF NOT EXISTS series_fts USING fts5(name, content=series, content_rowid=id)')
    cursor.execute('INSERT INTO series_fts(series_fts) VALUES("rebuild")')

    conn.commit()
    print("Indexes created")


def load_parquet_file(conn, filepath, country, frequency):
    """Load a single parquet file into the database."""
    try:
        df = pd.read_parquet(filepath)

        # Reset index if Date is index
        if df.index.name == 'Date':
            df = df.reset_index()

        if 'Date' not in df.columns:
            return 0

        # Convert dates to string
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

        value_cols = [c for c in df.columns if c != 'Date']
        if not value_cols:
            return 0

        cursor = conn.cursor()
        series_count = 0

        for col in value_cols:
            series_df = df[['Date', col]].dropna(subset=[col])
            if series_df.empty:
                continue

            min_date = series_df['Date'].min()
            max_date = series_df['Date'].max()
            count = len(series_df)

            # Insert series metadata
            try:
                cursor.execute('''
                    INSERT INTO series (name, country, frequency, min_date, max_date, count)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (col, country, frequency, min_date, max_date, count))
                series_id = cursor.lastrowid
            except sqlite3.IntegrityError:
                # Series already exists for this frequency (duplicate across files)
                cursor.execute('SELECT id FROM series WHERE name = ? AND frequency = ?', (col, frequency))
                result = cursor.fetchone()
                if result:
                    series_id = result[0]
                else:
                    continue

            # Insert data
            data_rows = [(series_id, d, float(v))
                        for d, v in zip(series_df['Date'], series_df[col])]
            cursor.executemany(
                'INSERT INTO data (series_id, date, value) VALUES (?, ?, ?)',
                data_rows
            )

            series_count += 1

        conn.commit()
        return series_count

    except Exception as e:
        print(f"  Error loading {filepath}: {e}")
        conn.rollback()
        return 0


def main():
    print("Creating SQLite database...")
    print(f"Output: {OUTPUT_DB}")

    # Remove existing database
    if OUTPUT_DB.exists():
        OUTPUT_DB.unlink()

    conn = sqlite3.connect(OUTPUT_DB)
    create_tables(conn)

    total_series = 0
    total_files = 0

    for country, base_path in LOCAL_DATA_ROOTS.items():
        path = Path(base_path)
        if not path.exists():
            print(f"Skipping {country} - path not found")
            continue

        print(f"\n{country.upper()}...")
        country_series = 0

        parquet_files = sorted(path.glob('*.parquet'))
        for filepath in parquet_files:
            if is_excluded(filepath.name):
                continue

            freq = get_frequency(filepath.name)
            if not freq:
                continue

            count = load_parquet_file(conn, filepath, country, freq)
            if count > 0:
                print(f"  {filepath.name}: {count} series")
                country_series += count
                total_files += 1

        print(f"  Total: {country_series} series")
        total_series += country_series

    create_indexes(conn)

    # Get final stats
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM series')
    series_count = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM data')
    data_count = cursor.fetchone()[0]

    conn.close()

    print(f"\n{'='*50}")
    print(f"COMPLETE:")
    print(f"  Files processed: {total_files}")
    print(f"  Series: {series_count:,}")
    print(f"  Data points: {data_count:,}")
    print(f"  Database size: {OUTPUT_DB.stat().st_size / 1024 / 1024:.1f} MB")


if __name__ == '__main__':
    main()
