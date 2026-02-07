#!/usr/bin/env python3
"""
Update SQLite Database
======================
Creates/updates a SQLite database from local parquet files and optionally
uploads the compressed database to R2.

Supports incremental updates: only parquet files that have changed (by mtime)
are reprocessed. Use --rebuild for a full rebuild.

Usage:
    python update_db.py                  # Incremental update (all countries)
    python update_db.py --country jp     # Incremental update (Japan only)
    python update_db.py --rebuild        # Full rebuild (old behavior)
    python update_db.py --upload         # Build + compress + upload to R2
    python update_db.py --upload-only    # Upload existing .gz to R2
"""

import argparse
import gzip
import json
import os
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Configuration
LOCAL_DATA_ROOTS = {
    'cn': '/Users/paul/Documents/DATA/cn/cn_input',
    'jp': '/Users/paul/Documents/DATA/jp/jp_input',
    'kr': '/Users/paul/Documents/DATA/kr/kr_input',
    'tw': '/Users/paul/Documents/DATA/tw/tw_input',
    'region': '/Users/paul/Documents/DATA/region/region_input',
}

# Store .db files outside iCloud to avoid sync issues with large binary files.
# Symlinks in the project directory point here.
OUTPUT_DB = Path('/Users/paul/.local/data_api/data.db')
EXCLUDED_PATTERNS = ['latest', 'recent', 'hist', 'history']
VALID_SUFFIXES = ['_m.parquet', '_q.parquet', '_a.parquet']

# R2 upload config
R2_BUCKET = os.environ.get('S3_BUCKET', 'eae-data-api')
R2_ENDPOINT = os.environ.get('R2_ENDPOINT', 'fd2c6c5f2d6d8bc9ca228f83b5671df3.r2.cloudflarestorage.com')
R2_DB_KEY = 'data.db.gz'


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

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS series (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            country TEXT NOT NULL,
            frequency TEXT NOT NULL,
            source_file TEXT,
            min_date TEXT,
            max_date TEXT,
            count INTEGER,
            UNIQUE(name, frequency)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data (
            series_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            value REAL,
            FOREIGN KEY (series_id) REFERENCES series(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_files (
            filepath TEXT PRIMARY KEY,
            country TEXT NOT NULL,
            frequency TEXT NOT NULL,
            mtime REAL NOT NULL,
            series_count INTEGER,
            last_processed TEXT
        )
    ''')

    conn.commit()


def ensure_source_file_column(conn):
    """Add source_file column to series table if it doesn't exist."""
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(series)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'source_file' not in columns:
        cursor.execute('ALTER TABLE series ADD COLUMN source_file TEXT')
        conn.commit()


def create_indexes(conn):
    """Create indexes for fast queries."""
    cursor = conn.cursor()
    print("Creating indexes...")

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_series_name ON series(name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_series_country ON series(country)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_series_freq ON series(frequency)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_series_source ON series(source_file)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_series ON data(series_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_date ON data(series_id, date)')

    # Full-text search index for series names
    cursor.execute('CREATE VIRTUAL TABLE IF NOT EXISTS series_fts USING fts5(name, content=series, content_rowid=id)')
    cursor.execute('INSERT INTO series_fts(series_fts) VALUES("rebuild")')

    conn.commit()
    print("Indexes created")


def create_stats_table(conn):
    """Pre-compute stats into a small table for instant lookups."""
    cursor = conn.cursor()
    print("Pre-computing stats...")

    cursor.execute('DROP TABLE IF EXISTS stats')
    cursor.execute('''
        CREATE TABLE stats (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    ''')

    cursor.execute('SELECT COUNT(DISTINCT name) FROM series')
    total_series = cursor.fetchone()[0]

    cursor.execute('SELECT COUNT(*) FROM series')
    total_series_freq = cursor.fetchone()[0]

    cursor.execute('SELECT SUM(count) FROM series')
    total_data_points = cursor.fetchone()[0] or 0

    cursor.execute('SELECT country, COUNT(DISTINCT name) as count FROM series GROUP BY country')
    by_country = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.execute('SELECT frequency, COUNT(*) as count FROM series GROUP BY frequency')
    by_freq = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.execute("INSERT INTO stats VALUES ('total_series', ?)", (str(total_series),))
    cursor.execute("INSERT INTO stats VALUES ('total_series_freq', ?)", (str(total_series_freq),))
    cursor.execute("INSERT INTO stats VALUES ('total_data_points', ?)", (str(total_data_points),))
    cursor.execute("INSERT INTO stats VALUES ('by_country', ?)", (json.dumps(by_country),))
    cursor.execute("INSERT INTO stats VALUES ('by_frequency', ?)", (json.dumps(by_freq),))

    conn.commit()
    print(f"Stats: {total_series:,} series, {total_data_points:,} data points")


def load_parquet_file(conn, filepath, country, frequency, source_file):
    """Load a single parquet file into the database.

    Uses batch operations for efficiency:
    - Vectorized metadata computation
    - executemany for series upserts
    - Bulk DELETE of old data via source_file
    - Melt + executemany for data inserts
    """
    try:
        df = pd.read_parquet(filepath)

        if df.index.name == 'Date':
            df = df.reset_index()

        if 'Date' not in df.columns:
            return 0

        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

        value_cols = [c for c in df.columns if c != 'Date']
        if not value_cols:
            return 0

        cursor = conn.cursor()

        # Phase 1: Melt to long format (vectorized, avoids per-column loop)
        df_long = df.melt(id_vars=['Date'], value_vars=value_cols,
                          var_name='name', value_name='value')
        df_long = df_long.dropna(subset=['value'])

        if df_long.empty:
            return 0

        # Phase 2: Compute series metadata from melted data
        meta = df_long.groupby('name', sort=False).agg(
            min_date=('Date', 'min'),
            max_date=('Date', 'max'),
            count=('Date', 'size')
        )
        valid_cols = list(meta.index)

        series_rows = [
            (name, country, frequency, source_file,
             row['min_date'], row['max_date'], int(row['count']))
            for name, row in meta.iterrows()
        ]

        # Phase 3: Batch upsert all series
        cursor.executemany('''
            INSERT INTO series (name, country, frequency, source_file, min_date, max_date, count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name, frequency) DO UPDATE SET
                country = excluded.country,
                source_file = excluded.source_file,
                min_date = excluded.min_date,
                max_date = excluded.max_date,
                count = excluded.count
        ''', series_rows)

        # Phase 4: Get series IDs via source_file lookup
        cursor.execute(
            'SELECT name, id FROM series WHERE source_file = ? AND frequency = ?',
            (source_file, frequency))
        name_to_id = dict(cursor.fetchall())

        # Phase 5: Bulk delete old data for all series from this file
        series_ids = list(name_to_id.values())
        for i in range(0, len(series_ids), 500):
            batch = series_ids[i:i+500]
            cursor.execute(
                f'DELETE FROM data WHERE series_id IN ({",".join("?" * len(batch))})',
                batch)

        # Phase 6: Map series names to IDs and bulk insert data
        df_long['series_id'] = df_long['name'].map(name_to_id).astype(int)
        df_insert = df_long[['series_id', 'Date', 'value']].rename(
            columns={'Date': 'date'})
        df_insert.to_sql('data', conn, if_exists='append', index=False)

        conn.commit()
        return len(valid_cols)

    except Exception as e:
        print(f"  Error loading {filepath}: {e}")
        conn.rollback()
        return 0


def get_parquet_files(countries):
    """Collect all valid parquet files for the given countries."""
    files = []
    for country in countries:
        base_path = LOCAL_DATA_ROOTS.get(country)
        if not base_path:
            print(f"Unknown country: {country}")
            continue

        path = Path(base_path)
        if not path.exists():
            print(f"Skipping {country} - path not found: {path}")
            continue

        for filepath in sorted(path.glob('*.parquet')):
            if is_excluded(filepath.name):
                continue
            freq = get_frequency(filepath.name)
            if not freq:
                continue
            files.append((filepath, country, freq))

    return files


def detect_changed_files(conn, parquet_files, countries):
    """Compare file mtimes against processed_files table.

    Returns (changed, unchanged, removed) where:
    - changed: list of (filepath, country, freq) that need reprocessing
    - unchanged: count of files that haven't changed
    - removed: list of filepaths in DB but no longer on disk (scoped to given countries)
    """
    cursor = conn.cursor()

    # Build lookup of current files
    current_files = {}
    for filepath, country, freq in parquet_files:
        current_files[str(filepath)] = (filepath, country, freq)

    # Get previously processed files — only for the countries being updated
    placeholders = ','.join('?' * len(countries))
    cursor.execute(
        f'SELECT filepath, mtime FROM processed_files WHERE country IN ({placeholders})',
        countries)
    processed = {row[0]: row[1] for row in cursor.fetchall()}

    changed = []
    unchanged = 0

    for fpath_str, (filepath, country, freq) in current_files.items():
        current_mtime = filepath.stat().st_mtime
        prev_mtime = processed.get(fpath_str)

        if prev_mtime is None or current_mtime != prev_mtime:
            changed.append((filepath, country, freq))
        else:
            unchanged += 1

    # Files in DB but no longer on disk
    removed = [fp for fp in processed if fp not in current_files]

    return changed, unchanged, removed


def remove_file_data(conn, filepath_str):
    """Remove all series and data rows that came from a specific source file."""
    cursor = conn.cursor()
    cursor.execute('SELECT id FROM series WHERE source_file = ?', (filepath_str,))
    series_ids = [row[0] for row in cursor.fetchall()]

    if series_ids:
        placeholders = ','.join('?' * len(series_ids))
        cursor.execute(f'DELETE FROM data WHERE series_id IN ({placeholders})', series_ids)
        cursor.execute(f'DELETE FROM series WHERE id IN ({placeholders})', series_ids)

    cursor.execute('DELETE FROM processed_files WHERE filepath = ?', (filepath_str,))
    conn.commit()
    return len(series_ids)


def needs_migration(conn):
    """Check if the database needs a full rebuild for migration.

    Returns True if:
    - processed_files table doesn't exist
    - series table lacks source_file column
    - processed_files table is empty but series table has data
    """
    cursor = conn.cursor()

    # Check if processed_files table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='processed_files'")
    if not cursor.fetchone():
        return True

    # Check if series has source_file column
    cursor.execute("PRAGMA table_info(series)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'source_file' not in columns:
        return True

    # Check if tracking data exists
    cursor.execute('SELECT COUNT(*) FROM processed_files')
    tracked = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM series')
    series_count = cursor.fetchone()[0]

    if tracked == 0 and series_count > 0:
        return True

    return False


def incremental_update(conn, countries):
    """Perform an incremental update: only reprocess changed files."""
    parquet_files = get_parquet_files(countries)
    if not parquet_files:
        print("No parquet files found")
        return

    changed, unchanged, removed = detect_changed_files(conn, parquet_files, countries)

    print(f"Files scanned: {len(parquet_files)}")
    print(f"  Changed/new: {len(changed)}")
    print(f"  Unchanged:   {unchanged}")
    print(f"  Removed:     {len(removed)}")

    if not changed and not removed:
        print("\nNothing to update.")
        return

    # Remove data for files that no longer exist
    for fp in removed:
        count = remove_file_data(conn, fp)
        print(f"  Removed {count} series from deleted file: {Path(fp).name}")

    # Process changed files
    total_series = 0
    for filepath, country, freq in changed:
        source_file = str(filepath)

        # Remove old data from this file before loading new data
        remove_file_data(conn, source_file)

        count = load_parquet_file(conn, filepath, country, freq, source_file)
        if count > 0:
            print(f"  {filepath.name}: {count} series")
            total_series += count

            # Record in processed_files
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO processed_files (filepath, country, frequency, mtime, series_count, last_processed)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(filepath) DO UPDATE SET
                    mtime = excluded.mtime,
                    series_count = excluded.series_count,
                    last_processed = excluded.last_processed
            ''', (source_file, country, freq, filepath.stat().st_mtime, count,
                  datetime.now(timezone.utc).isoformat()))
            conn.commit()

    # Rebuild FTS and stats
    create_indexes(conn)
    create_stats_table(conn)

    print(f"\nIncremental update complete: {total_series} series from {len(changed)} files")


def full_rebuild(conn, countries):
    """Full rebuild: drop all data and reload everything."""
    cursor = conn.cursor()

    print("Full rebuild: clearing all data...")
    cursor.execute('DROP TABLE IF EXISTS data')
    cursor.execute('DROP TABLE IF EXISTS series')
    cursor.execute('DROP TABLE IF EXISTS processed_files')
    cursor.execute('DROP TABLE IF EXISTS stats')
    cursor.execute('DROP TABLE IF EXISTS series_fts')
    conn.commit()

    # Aggressive pragmas for bulk loading (safe since we rebuild from scratch)
    conn.execute('PRAGMA journal_mode=OFF')
    conn.execute('PRAGMA synchronous=OFF')
    conn.execute('PRAGMA cache_size=-512000')  # 512MB cache

    create_tables(conn)

    parquet_files = get_parquet_files(countries)
    if not parquet_files:
        print("No parquet files found")
        return

    total_series = 0
    total_files = 0
    current_country = None

    for filepath, country, freq in parquet_files:
        if country != current_country:
            if current_country is not None:
                print()
            print(f"{country.upper()}...")
            current_country = country

        source_file = str(filepath)
        count = load_parquet_file(conn, filepath, country, freq, source_file)
        if count > 0:
            print(f"  {filepath.name}: {count} series")
            total_series += count
            total_files += 1

            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO processed_files (filepath, country, frequency, mtime, series_count, last_processed)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (source_file, country, freq, filepath.stat().st_mtime, count,
                  datetime.now(timezone.utc).isoformat()))
            conn.commit()

    create_indexes(conn)
    create_stats_table(conn)

    # Restore safe pragmas for normal operation
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')

    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM series')
    series_count = cursor.fetchone()[0]
    cursor.execute('SELECT SUM(count) FROM series')
    data_count = cursor.fetchone()[0] or 0

    print("Running VACUUM to reclaim space...")
    conn.execute('VACUUM')

    print(f"\n{'='*50}")
    print(f"COMPLETE:")
    print(f"  Files processed: {total_files}")
    print(f"  Series: {series_count:,}")
    print(f"  Data points: {data_count:,}")
    print(f"  Database size: {OUTPUT_DB.stat().st_size / 1024 / 1024:.1f} MB")


def build_database(countries=None, rebuild=False):
    """Build or update the SQLite database from parquet files."""
    if countries is None:
        countries = list(LOCAL_DATA_ROOTS.keys())

    print(f"Output: {OUTPUT_DB}")
    print(f"Countries: {', '.join(countries)}")

    db_exists = OUTPUT_DB.exists() and OUTPUT_DB.stat().st_size > 0

    if rebuild or not db_exists:
        if db_exists:
            OUTPUT_DB.unlink()
        print("Starting full rebuild...")
        conn = sqlite3.connect(OUTPUT_DB)
        conn.execute('PRAGMA journal_mode=WAL')
        create_tables(conn)
        full_rebuild(conn, countries)
        conn.close()
    else:
        conn = sqlite3.connect(OUTPUT_DB)
        conn.execute('PRAGMA journal_mode=WAL')

        # Check if migration is needed (first incremental run on old DB)
        if needs_migration(conn):
            print("Migration needed: no tracking data found.")
            print("Performing one-time full rebuild to populate tracking...")
            ensure_source_file_column(conn)
            full_rebuild(conn, countries)
        else:
            ensure_source_file_column(conn)
            print("Starting incremental update...")
            incremental_update(conn, countries)

        conn.close()

    return OUTPUT_DB


def compress_and_upload(db_path):
    """Gzip the database and upload to R2."""
    import boto3
    from botocore.config import Config

    gz_path = db_path.parent / 'data.db.gz'

    print(f"\nCompressing {db_path.name}...")
    with open(db_path, 'rb') as f_in:
        with gzip.open(gz_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    original_mb = db_path.stat().st_size / 1024 / 1024
    compressed_mb = gz_path.stat().st_size / 1024 / 1024
    print(f"  {original_mb:.1f} MB -> {compressed_mb:.1f} MB ({compressed_mb/original_mb*100:.0f}%)")

    access_key = os.environ.get('R2_ACCESS_KEY_ID') or os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('R2_SECRET_ACCESS_KEY') or os.environ.get('AWS_SECRET_ACCESS_KEY')

    if not access_key or not secret_key:
        print("\nError: R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY must be set for upload")
        sys.exit(1)

    client = boto3.client(
        's3',
        endpoint_url=f'https://{R2_ENDPOINT}',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='auto',
        config=Config(signature_version='s3v4')
    )

    print(f"Uploading to R2 ({R2_BUCKET}/{R2_DB_KEY})...")
    client.upload_file(str(gz_path), R2_BUCKET, R2_DB_KEY)
    print("Upload complete")


def upload_only():
    """Upload existing data.db.gz to R2 without rebuilding."""
    import boto3
    from botocore.config import Config

    gz_path = OUTPUT_DB.parent / 'data.db.gz'

    if not gz_path.exists():
        print(f"Error: {gz_path} not found. Run without --upload-only first to build and compress.")
        sys.exit(1)

    print(f"Uploading existing {gz_path.name} ({gz_path.stat().st_size / 1024 / 1024:.1f} MB)")

    access_key = os.environ.get('R2_ACCESS_KEY_ID') or os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('R2_SECRET_ACCESS_KEY') or os.environ.get('AWS_SECRET_ACCESS_KEY')

    if not access_key or not secret_key:
        print("Error: R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY must be set for upload")
        sys.exit(1)

    client = boto3.client(
        's3',
        endpoint_url=f'https://{R2_ENDPOINT}',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='auto',
        config=Config(signature_version='s3v4')
    )

    print(f"Uploading to R2 ({R2_BUCKET}/{R2_DB_KEY})...")
    client.upload_file(str(gz_path), R2_BUCKET, R2_DB_KEY)
    print("Upload complete")


def main():
    parser = argparse.ArgumentParser(
        description='Build/update SQLite database from parquet files and optionally upload to R2',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python update_db.py                  # Incremental update (all countries)
  python update_db.py --country jp     # Incremental update (Japan only)
  python update_db.py --rebuild        # Full rebuild (delete + reload everything)
  python update_db.py --upload         # Build + compress + upload to R2
  python update_db.py --upload-only    # Upload existing data.db.gz to R2
  python update_db.py --country cn jp  # Incremental update for China and Japan
        """
    )

    parser.add_argument(
        '--country', '-c',
        nargs='+',
        choices=list(LOCAL_DATA_ROOTS.keys()),
        help='Countries to update (default: all)'
    )

    parser.add_argument(
        '--rebuild',
        action='store_true',
        help='Full rebuild: delete database and reload all files'
    )

    parser.add_argument(
        '--upload', '-u',
        action='store_true',
        help='Compress and upload to R2 after building'
    )

    parser.add_argument(
        '--upload-only',
        action='store_true',
        help='Upload existing data.db.gz to R2 (skip rebuild and compress)'
    )

    args = parser.parse_args()

    if args.upload_only:
        upload_only()
    else:
        db_path = build_database(countries=args.country, rebuild=args.rebuild)
        if args.upload:
            compress_and_upload(db_path)


if __name__ == '__main__':
    main()
