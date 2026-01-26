"""
Data Access Layer for Economic Data
====================================
Provides unified access to parquet files via DuckDB.
Supports both local files and S3 storage.

Environment Variables:
- DATA_SOURCE: 'local' or 's3' (default: auto-detect)
- S3_BUCKET: S3 bucket name (required for S3 mode)
- AWS_ACCESS_KEY_ID: AWS credentials (optional if using IAM roles)
- AWS_SECRET_ACCESS_KEY: AWS credentials (optional if using IAM roles)
- AWS_REGION: AWS region (default: us-east-1)
"""

import duckdb
import pandas as pd
from pathlib import Path
from typing import List, Optional, Union
import json
import os
import re

# =============================================================================
# Configuration
# =============================================================================

# Files containing these patterns in their names will be excluded
EXCLUDED_PATTERNS = ['latest', 'recent', 'hist', 'history']

# Local paths (used for local development)
LOCAL_DATA_ROOTS = {
    'cn': '/Users/paul/Documents/DATA/cn/cn_input',
    'jp': '/Users/paul/Documents/DATA/jp/jp_input',
    'kr': '/Users/paul/Documents/DATA/kr/kr_input',
    'tw': '/Users/paul/Documents/DATA/tw/tw_input',
    'region': '/Users/paul/Documents/DATA/region/region_input',
}

# S3 paths (used for deployed API)
def get_s3_data_roots(bucket: str) -> dict:
    """Generate S3 paths for each country."""
    return {
        'cn': f's3://{bucket}/cn',
        'jp': f's3://{bucket}/jp',
        'kr': f's3://{bucket}/kr',
        'tw': f's3://{bucket}/tw',
        'region': f's3://{bucket}/region',
    }

# Cache directory
CACHE_DIR = Path(os.environ.get('CACHE_DIR', '/Users/paul/Documents/DATA/tools/data_api/cache'))
COLUMN_INDEX_PATH = CACHE_DIR / 'column_index.json'


def is_excluded_file(filename: str) -> bool:
    """Check if a file should be excluded based on naming patterns."""
    name_lower = filename.lower()
    return any(p in name_lower for p in EXCLUDED_PATTERNS)


class DataAccess:
    """Main data access class using DuckDB."""

    def __init__(self, data_roots: dict = None, use_s3: bool = None):
        """
        Initialize DataAccess.

        Parameters:
        -----------
        data_roots : dict, optional
            Custom data root paths. If not provided, auto-detects based on environment.
        use_s3 : bool, optional
            Force S3 mode (True) or local mode (False). If None, auto-detects.
        """
        # Determine data source
        if use_s3 is None:
            data_source = os.environ.get('DATA_SOURCE', 'auto')
            if data_source == 's3':
                use_s3 = True
            elif data_source == 'local':
                use_s3 = False
            else:
                # Auto-detect: use S3 if bucket is configured, otherwise local
                use_s3 = bool(os.environ.get('S3_BUCKET'))

        self.use_s3 = use_s3

        if data_roots:
            self.data_roots = data_roots
        elif use_s3:
            bucket = os.environ.get('S3_BUCKET', 'eae-data-api')
            self.data_roots = get_s3_data_roots(bucket)
        else:
            self.data_roots = LOCAL_DATA_ROOTS

        # Initialize DuckDB connection
        self.conn = duckdb.connect(':memory:')

        # Configure S3 access if needed
        if self.use_s3:
            self._configure_s3()

        self._column_index = None

    def _configure_s3(self):
        """Configure DuckDB for S3 access."""
        # Install and load httpfs extension for S3 support
        self.conn.execute("INSTALL httpfs;")
        self.conn.execute("LOAD httpfs;")

        # Configure AWS credentials if provided
        aws_key = os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret = os.environ.get('AWS_SECRET_ACCESS_KEY')
        aws_region = os.environ.get('AWS_REGION', 'us-east-1')

        if aws_key and aws_secret:
            self.conn.execute(f"SET s3_access_key_id='{aws_key}';")
            self.conn.execute(f"SET s3_secret_access_key='{aws_secret}';")

        self.conn.execute(f"SET s3_region='{aws_region}';")

    def _get_parquet_files(self, freq: str, country: str = None) -> List[str]:
        """
        Get list of parquet files matching frequency and country.
        Excludes files with 'latest', 'recent', 'hist', 'history' in names.
        """
        suffix = f'_{freq}.parquet'
        files = []

        countries = [country] if country else self.data_roots.keys()

        for ctry in countries:
            if ctry not in self.data_roots:
                continue

            base_path = self.data_roots[ctry]

            if self.use_s3:
                # For S3, we need to list files using DuckDB's glob
                try:
                    pattern = f"{base_path}/*{suffix}"
                    result = self.conn.execute(f"SELECT * FROM glob('{pattern}')").fetchall()
                    for row in result:
                        filepath = row[0]
                        filename = filepath.split('/')[-1]
                        if not is_excluded_file(filename):
                            files.append(filepath)
                except Exception:
                    # Glob might not work for all S3 setups, fall back to pattern
                    files.append(f"{base_path}/*{suffix}")
            else:
                # Local filesystem
                path = Path(base_path)
                if path.exists():
                    for f in path.glob(f'*{suffix}'):
                        if not is_excluded_file(f.name):
                            files.append(str(f))

        return files

    def _get_parquet_pattern(self, freq: str, country: str = None) -> Union[str, List[str]]:
        """Get glob pattern for parquet files (legacy method for compatibility)."""
        suffix = f'_{freq}.parquet'
        if country and country in self.data_roots:
            return f"{self.data_roots[country]}/*{suffix}"
        # All countries
        patterns = [f"{path}/*{suffix}" for path in self.data_roots.values()]
        return patterns

    def get_series(
        self,
        columns: Union[str, List[str]],
        freq: str = 'm',
        start_date: str = None,
        end_date: str = None,
        country: str = None
    ) -> pd.DataFrame:
        """
        Get time series data for specified columns.

        Parameters:
        -----------
        columns : str or list
            Column name(s) to retrieve
        freq : str
            Frequency: 'm' (monthly), 'q' (quarterly), 'a' (annual), 'd' (daily), 'w' (weekly)
        start_date : str
            Start date filter (YYYY-MM-DD)
        end_date : str
            End date filter (YYYY-MM-DD)
        country : str
            Filter to specific country (cn, jp, kr, tw, region)

        Returns:
        --------
        pd.DataFrame with Date index and requested columns
        """
        if isinstance(columns, str):
            columns = [columns]

        # Get parquet files (excluding 'latest', 'recent', etc.)
        parquet_files = self._get_parquet_files(freq, country)

        if not parquet_files:
            return pd.DataFrame()

        # Build query
        col_list = ', '.join([f'"{col}"' for col in columns])

        where_clauses = []
        if start_date:
            where_clauses.append(f'"Date" >= \'{start_date}\'')
        if end_date:
            where_clauses.append(f'"Date" <= \'{end_date}\'')

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        # Query each file and combine
        dfs = []
        for filepath in parquet_files:
            try:
                query = f"""
                    SELECT "Date", {col_list}
                    FROM read_parquet('{filepath}', union_by_name=true)
                    {where_sql}
                    ORDER BY "Date"
                """
                df = self.conn.execute(query).df()
                # Drop rows where all value columns are null
                value_cols = [c for c in df.columns if c != 'Date']
                df = df.dropna(subset=value_cols, how='all')
                if not df.empty:
                    dfs.append(df)
            except duckdb.CatalogException:
                # Column doesn't exist in this file, skip
                continue
            except Exception as e:
                # Other errors - skip but could log
                continue

        if not dfs:
            return pd.DataFrame()

        # Combine and deduplicate
        result = pd.concat(dfs, ignore_index=True)
        result = result.drop_duplicates(subset=['Date']).sort_values('Date')
        result = result.set_index('Date')

        return result

    def search_columns(
        self,
        pattern: str,
        freq: str = 'm',
        country: str = None,
        limit: int = 50
    ) -> List[str]:
        """
        Search for columns matching a pattern.

        Parameters:
        -----------
        pattern : str
            Search pattern (case-insensitive substring match)
        freq : str
            Frequency to search in
        country : str
            Filter to specific country
        limit : int
            Maximum results to return

        Returns:
        --------
        List of matching column names
        """
        # Use column index if available
        if self._column_index is None:
            self._load_or_build_column_index()

        freq_key = f'_{freq}'
        pattern_lower = pattern.lower()

        matches = []
        for col_name, info in self._column_index.items():
            if pattern_lower in col_name.lower():
                # Check frequency
                if any(freq_key in f for f in info.get('files', [])):
                    if country is None or country in info.get('countries', []):
                        matches.append(col_name)
                        if len(matches) >= limit:
                            break

        return matches

    def list_columns(
        self,
        freq: str = 'm',
        country: str = None,
        prefix: str = None
    ) -> List[str]:
        """
        List available columns.

        Parameters:
        -----------
        freq : str
            Frequency filter
        country : str
            Country filter
        prefix : str
            Column name prefix filter

        Returns:
        --------
        List of column names
        """
        if self._column_index is None:
            self._load_or_build_column_index()

        freq_key = f'_{freq}'
        columns = []

        for col_name, info in self._column_index.items():
            if any(freq_key in f for f in info.get('files', [])):
                if country is None or country in info.get('countries', []):
                    if prefix is None or col_name.startswith(prefix):
                        columns.append(col_name)

        return sorted(columns)

    def get_column_info(self, column: str) -> dict:
        """Get metadata about a column (which files contain it, date range, etc.)."""
        if self._column_index is None:
            self._load_or_build_column_index()

        return self._column_index.get(column, {})

    def _load_or_build_column_index(self):
        """Load column index from cache or build it."""
        if COLUMN_INDEX_PATH.exists():
            with open(COLUMN_INDEX_PATH, 'r') as f:
                self._column_index = json.load(f)
        else:
            self._build_column_index()

    def _build_column_index(self):
        """Build index of all columns across all parquet files."""
        print("Building column index (this may take a moment)...")

        self._column_index = {}

        for country, base_path in self.data_roots.items():
            if self.use_s3:
                # For S3, list files using glob
                try:
                    for freq in ['d', 'w', 'm', 'q', 'a']:
                        pattern = f"{base_path}/*_{freq}.parquet"
                        try:
                            result = self.conn.execute(f"SELECT * FROM glob('{pattern}')").fetchall()
                            for row in result:
                                filepath = row[0]
                                filename = filepath.split('/')[-1]

                                # Skip excluded files
                                if is_excluded_file(filename):
                                    continue

                                self._index_parquet_file(filepath, filename, country)
                        except Exception:
                            continue
                except Exception as e:
                    print(f"Error listing S3 files for {country}: {e}")
            else:
                # Local filesystem
                path = Path(base_path)
                if not path.exists():
                    continue

                for parquet_file in path.glob('*.parquet'):
                    # Skip excluded files
                    if is_excluded_file(parquet_file.name):
                        continue

                    self._index_parquet_file(str(parquet_file), parquet_file.name, country)

        # Convert sets to lists for JSON serialization
        for col in self._column_index:
            self._column_index[col]['countries'] = list(self._column_index[col]['countries'])

        # Save to cache
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(COLUMN_INDEX_PATH, 'w') as f:
            json.dump(self._column_index, f)

        print(f"Column index built: {len(self._column_index)} columns indexed")

    def _index_parquet_file(self, filepath: str, filename: str, country: str):
        """Index columns from a single parquet file."""
        try:
            # Get columns without loading data
            schema = self.conn.execute(f"DESCRIBE SELECT * FROM '{filepath}'").df()
            columns = schema['column_name'].tolist()

            for col in columns:
                if col == 'Date':
                    continue

                if col not in self._column_index:
                    self._column_index[col] = {
                        'files': [],
                        'countries': set()
                    }

                self._column_index[col]['files'].append(filename)
                self._column_index[col]['countries'].add(country)

        except Exception as e:
            print(f"Error reading {filepath}: {e}")

    def rebuild_index(self):
        """Force rebuild of column index."""
        if COLUMN_INDEX_PATH.exists():
            COLUMN_INDEX_PATH.unlink()
        self._column_index = None
        self._load_or_build_column_index()

    def get_latest_date(self, column: str, freq: str = 'm') -> Optional[str]:
        """Get the latest date with data for a column."""
        try:
            df = self.get_series(column, freq=freq)
            if not df.empty:
                # Get last non-null value
                last_valid = df[column].last_valid_index()
                if last_valid is not None:
                    return str(last_valid.date())
        except:
            pass
        return None


# =============================================================================
# Convenience functions for direct imports
# =============================================================================

_default_instance = None

def get_data_access() -> DataAccess:
    """Get or create default DataAccess instance."""
    global _default_instance
    if _default_instance is None:
        _default_instance = DataAccess()
    return _default_instance


def get_series(columns, freq='m', start_date=None, end_date=None, country=None):
    """Get time series data. See DataAccess.get_series for details."""
    return get_data_access().get_series(columns, freq, start_date, end_date, country)


def search_columns(pattern, freq='m', country=None, limit=50):
    """Search for columns. See DataAccess.search_columns for details."""
    return get_data_access().search_columns(pattern, freq, country, limit)


def list_columns(freq='m', country=None, prefix=None):
    """List available columns. See DataAccess.list_columns for details."""
    return get_data_access().list_columns(freq, country, prefix)


# =============================================================================
# CLI for testing
# =============================================================================

if __name__ == '__main__':
    import sys

    print(f"Data source: {'S3' if os.environ.get('S3_BUCKET') else 'Local'}")

    da = DataAccess()
    print(f"Data roots: {list(da.data_roots.keys())}")
    print(f"Using S3: {da.use_s3}")

    # Search for JGB columns
    print("\nSearching for JGB columns...")
    jgb_cols = da.search_columns('JGB', freq='m')
    print(f"Found {len(jgb_cols)} columns:")
    for col in jgb_cols[:10]:
        print(f"  - {col}")

    # Get data
    if jgb_cols:
        print(f"\nGetting data for: {jgb_cols[0]}")
        df = da.get_series(jgb_cols[0], freq='m', start_date='2020-01-01')
        print(df.tail())
