#!/usr/bin/env python3
"""
Parquet File Watcher
====================
Watches parquet source directories for changes and triggers incremental
database updates via update_db.py.

Uses a 30-second debounce: after the last file change, waits 30 seconds
before running the update (to batch multiple file writes together).

Usage:
    python watch_parquet.py          # Run in foreground
    python watch_parquet.py --test   # Run once and exit (for testing)
"""

import argparse
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

# Import config and update function from update_db
sys.path.insert(0, str(Path(__file__).parent))
from update_db import LOCAL_DATA_ROOTS, EXCLUDED_PATTERNS, VALID_SUFFIXES, build_database

DEBOUNCE_SECONDS = 30


def is_relevant_parquet(filepath):
    """Check if a file path is a parquet file we care about."""
    name = Path(filepath).name.lower()
    if not any(name.endswith(suffix) for suffix in VALID_SUFFIXES):
        return False
    if any(pattern in name for pattern in EXCLUDED_PATTERNS):
        return False
    return True


def log(msg):
    """Print a timestamped log message."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)


class ParquetHandler(FileSystemEventHandler):
    """Handles file system events for parquet files with debouncing."""

    def __init__(self):
        super().__init__()
        self._timer = None
        self._lock = threading.Lock()
        self._changed_countries = set()
        self._updating = False

    def _on_relevant_change(self, path):
        """Called when a relevant parquet file changes."""
        # Determine which country this file belongs to
        path_str = str(path)
        country = None
        for c, root in LOCAL_DATA_ROOTS.items():
            if path_str.startswith(root):
                country = c
                break

        if not country:
            return

        with self._lock:
            if self._updating:
                log(f"  Update in progress, queuing: {Path(path).name}")
                self._changed_countries.add(country)
                return

            self._changed_countries.add(country)
            # Reset debounce timer
            if self._timer is not None:
                self._timer.cancel()
            countries_so_far = ', '.join(sorted(self._changed_countries))
            log(f"  Change detected: {Path(path).name} ({country}) — waiting {DEBOUNCE_SECONDS}s for more changes...")
            self._timer = threading.Timer(DEBOUNCE_SECONDS, self._run_update)
            self._timer.daemon = True
            self._timer.start()

    def _run_update(self):
        """Run the incremental update after debounce period."""
        with self._lock:
            countries = list(self._changed_countries)
            self._changed_countries.clear()
            self._timer = None
            self._updating = True

        log(f"Running incremental update for: {', '.join(sorted(countries))}")
        start = time.time()
        try:
            build_database(countries=countries, rebuild=False)
            elapsed = time.time() - start
            log(f"Update complete ({elapsed:.1f}s)")
        except Exception as e:
            log(f"Update failed: {e}")
        finally:
            with self._lock:
                self._updating = False
                # If new changes arrived during update, schedule another run
                if self._changed_countries:
                    log("New changes detected during update, scheduling follow-up...")
                    self._timer = threading.Timer(DEBOUNCE_SECONDS, self._run_update)
                    self._timer.daemon = True
                    self._timer.start()

    def on_created(self, event):
        if not event.is_directory and is_relevant_parquet(event.src_path):
            self._on_relevant_change(event.src_path)

    def on_modified(self, event):
        if not event.is_directory and is_relevant_parquet(event.src_path):
            self._on_relevant_change(event.src_path)

    def on_deleted(self, event):
        if not event.is_directory and is_relevant_parquet(event.src_path):
            self._on_relevant_change(event.src_path)


def main():
    parser = argparse.ArgumentParser(description='Watch parquet directories for changes')
    parser.add_argument('--test', action='store_true',
                        help='Run a single update check and exit')
    args = parser.parse_args()

    if args.test:
        log("Test mode: running incremental update for all countries")
        build_database(rebuild=False)
        return

    # Verify watched directories exist
    watched = []
    for country, root in LOCAL_DATA_ROOTS.items():
        p = Path(root)
        if p.exists():
            watched.append((country, root))
        else:
            log(f"Warning: {country} directory not found: {root}")

    if not watched:
        log("Error: no directories to watch")
        sys.exit(1)

    handler = ParquetHandler()
    observer = Observer()

    for country, root in watched:
        observer.schedule(handler, root, recursive=False)
        log(f"Watching: {root} ({country})")

    log(f"Debounce: {DEBOUNCE_SECONDS}s")
    log("Ready. Press Ctrl+C to stop.")

    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log("Shutting down...")
        observer.stop()
    observer.join()


if __name__ == '__main__':
    main()
