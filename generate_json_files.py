#!/usr/bin/env python3
"""
Generate JSON Files for Each Series
====================================
Creates individual JSON files for each data series.
These are uploaded to R2 and served directly by the API.
"""

import os
import json
import hashlib
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

OUTPUT_DIR = Path('/Users/paul/Documents/DATA/tools/data_api/json_data')
INDEX_FILE = OUTPUT_DIR / '_index.json'

EXCLUDED_PATTERNS = ['latest', 'recent', 'hist', 'history']
VALID_SUFFIXES = ['_m.parquet', '_q.parquet', '_a.parquet']  # Skip daily/weekly for now


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


def safe_filename(series_name):
    """Create a safe filename from series name."""
    # Create hash for unique filename
    hash_suffix = hashlib.md5(series_name.encode()).hexdigest()[:8]
    # Clean the name for readability
    safe = series_name.replace(',', '').replace('%', 'pct').replace(' ', '_')
    safe = ''.join(c if c.isalnum() or c == '_' else '_' for c in safe)
    safe = safe[:50]  # Limit length
    return f"{safe}_{hash_suffix}"


def generate_json_for_file(filepath, country, frequency, output_dir, index):
    """Generate JSON files for all series in a parquet file."""
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
        count = 0

        for col in value_cols:
            # Get non-null data
            series_df = df[['Date', col]].dropna(subset=[col])
            if series_df.empty:
                continue

            # Create JSON data
            data = {
                'series_name': col,
                'country': country,
                'frequency': frequency,
                'count': len(series_df),
                'min_date': series_df['Date'].min(),
                'max_date': series_df['Date'].max(),
                'data': series_df.rename(columns={col: 'value'}).to_dict('records')
            }

            # Save to file
            filename = safe_filename(col)
            filepath_out = output_dir / country / frequency / f"{filename}.json"
            filepath_out.parent.mkdir(parents=True, exist_ok=True)

            with open(filepath_out, 'w') as f:
                json.dump(data, f, separators=(',', ':'))

            # Add to index (nest by frequency)
            if col not in index:
                index[col] = {
                    'country': country,
                    'frequencies': {}
                }
            index[col]['frequencies'][frequency] = {
                'file': f"{country}/{frequency}/{filename}.json",
                'count': len(series_df),
                'min_date': data['min_date'],
                'max_date': data['max_date']
            }

            count += 1

        return count

    except Exception as e:
        print(f"  Error: {filepath} - {e}")
        return 0


def main():
    print("Generating JSON files...")
    print(f"Output directory: {OUTPUT_DIR}")

    # Clean output directory
    if OUTPUT_DIR.exists():
        import shutil
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True)

    index = {}
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

            count = generate_json_for_file(filepath, country, freq, OUTPUT_DIR, index)
            if count > 0:
                print(f"  {filepath.name}: {count} series")
                country_series += count
                total_files += 1

        print(f"  Total: {country_series} series")
        total_series += country_series

    # Save index
    with open(INDEX_FILE, 'w') as f:
        json.dump(index, f, separators=(',', ':'))

    print(f"\n{'='*50}")
    print(f"COMPLETE:")
    print(f"  Files processed: {total_files}")
    print(f"  Series generated: {total_series}")
    print(f"  Index saved: {INDEX_FILE}")
    print(f"  Output size: {sum(f.stat().st_size for f in OUTPUT_DIR.rglob('*.json')) / 1024 / 1024:.1f} MB")


if __name__ == '__main__':
    main()
