#!/usr/bin/env python3
"""
Sync Parquet Files to S3
========================
Syncs parquet data files to S3 for the deployed API.
Excludes files with 'latest', 'recent', 'hist', 'history' in names.

Usage:
    python sync_to_s3.py                    # Sync all countries
    python sync_to_s3.py --country cn       # Sync only China
    python sync_to_s3.py --country cn jp    # Sync China and Japan
    python sync_to_s3.py --dry-run          # Preview without uploading
    python sync_to_s3.py --file cn_cpi_m    # Sync specific file(s)

Environment Variables:
    S3_BUCKET: Target S3 bucket (default: eae-data-api)
    AWS_PROFILE: AWS profile to use (optional)
"""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

# Configuration
S3_BUCKET = "eae-data-api"  # Override with --bucket or S3_BUCKET env var

LOCAL_DATA_ROOTS = {
    'cn': '/Users/paul/Documents/DATA/cn/cn_input',
    'jp': '/Users/paul/Documents/DATA/jp/jp_input',
    'kr': '/Users/paul/Documents/DATA/kr/kr_input',
    'tw': '/Users/paul/Documents/DATA/tw/tw_input',
    'region': '/Users/paul/Documents/DATA/region/region_input',
}

# Files containing these patterns will be excluded
EXCLUDED_PATTERNS = ['latest', 'recent', 'hist', 'history']

# Valid frequency suffixes
VALID_SUFFIXES = ['_d.parquet', '_w.parquet', '_m.parquet', '_q.parquet', '_a.parquet']


def is_excluded(filename: str) -> bool:
    """Check if file should be excluded."""
    name_lower = filename.lower()
    return any(pattern in name_lower for pattern in EXCLUDED_PATTERNS)


def is_valid_data_file(filename: str) -> bool:
    """Check if file is a valid data parquet file."""
    return any(filename.endswith(suffix) for suffix in VALID_SUFFIXES)


def get_files_to_sync(country: str, specific_files: List[str] = None) -> List[Path]:
    """Get list of parquet files to sync for a country."""
    base_path = Path(LOCAL_DATA_ROOTS[country])
    if not base_path.exists():
        print(f"Warning: Path does not exist: {base_path}")
        return []

    files = []
    for f in base_path.glob('*.parquet'):
        # Skip excluded files
        if is_excluded(f.name):
            continue

        # Only include valid frequency files
        if not is_valid_data_file(f.name):
            continue

        # Filter by specific files if provided
        if specific_files:
            if not any(sf in f.name for sf in specific_files):
                continue

        files.append(f)

    return sorted(files)


def sync_file(local_path: Path, country: str, bucket: str, dry_run: bool = False) -> bool:
    """Sync a single file to S3."""
    s3_path = f"s3://{bucket}/{country}/{local_path.name}"

    cmd = ['aws', 's3', 'cp', str(local_path), s3_path]

    if dry_run:
        print(f"  [DRY RUN] {local_path.name} -> {s3_path}")
        return True

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  ✓ {local_path.name}")
            return True
        else:
            print(f"  ✗ {local_path.name}: {result.stderr.strip()}")
            return False
    except Exception as e:
        print(f"  ✗ {local_path.name}: {e}")
        return False


def sync_country(country: str, bucket: str, dry_run: bool = False,
                 specific_files: List[str] = None) -> tuple:
    """Sync all files for a country. Returns (success_count, fail_count)."""
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Syncing {country.upper()}...")

    files = get_files_to_sync(country, specific_files)

    if not files:
        print(f"  No files to sync")
        return 0, 0

    print(f"  Found {len(files)} files")

    success = 0
    failed = 0

    for f in files:
        if sync_file(f, country, bucket, dry_run):
            success += 1
        else:
            failed += 1

    return success, failed


def main():
    import os

    parser = argparse.ArgumentParser(
        description='Sync parquet files to S3 for the EAE Data API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python sync_to_s3.py                     # Sync all countries
  python sync_to_s3.py --country cn jp     # Sync China and Japan only
  python sync_to_s3.py --dry-run           # Preview what would be synced
  python sync_to_s3.py --file cn_cpi       # Sync files matching 'cn_cpi'
  python sync_to_s3.py --list              # List files that would be synced
        """
    )

    parser.add_argument(
        '--country', '-c',
        nargs='+',
        choices=list(LOCAL_DATA_ROOTS.keys()),
        help='Countries to sync (default: all)'
    )

    parser.add_argument(
        '--bucket', '-b',
        default=os.environ.get('S3_BUCKET', S3_BUCKET),
        help=f'S3 bucket name (default: {S3_BUCKET})'
    )

    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Preview without uploading'
    )

    parser.add_argument(
        '--file', '-f',
        nargs='+',
        help='Sync only files matching these patterns'
    )

    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List files that would be synced and exit'
    )

    args = parser.parse_args()

    countries = args.country or list(LOCAL_DATA_ROOTS.keys())
    bucket = args.bucket

    print(f"S3 Bucket: {bucket}")
    print(f"Countries: {', '.join(countries)}")
    print(f"Excluded patterns: {', '.join(EXCLUDED_PATTERNS)}")

    if args.file:
        print(f"File filter: {', '.join(args.file)}")

    # List mode
    if args.list:
        print("\nFiles to sync:")
        for country in countries:
            files = get_files_to_sync(country, args.file)
            if files:
                print(f"\n{country.upper()} ({len(files)} files):")
                for f in files:
                    print(f"  {f.name}")
        return

    # Sync mode
    total_success = 0
    total_failed = 0

    for country in countries:
        success, failed = sync_country(
            country,
            bucket,
            dry_run=args.dry_run,
            specific_files=args.file
        )
        total_success += success
        total_failed += failed

    # Summary
    print(f"\n{'=' * 40}")
    if args.dry_run:
        print(f"DRY RUN COMPLETE: {total_success} files would be synced")
    else:
        print(f"SYNC COMPLETE: {total_success} succeeded, {total_failed} failed")

    if total_failed > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
