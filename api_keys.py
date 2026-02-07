"""
API Key Management
==================
Per-user API keys stored in SQLite, linked to Ghost membership tiers.
Uses a separate keys.db (not the large data.db).
"""

import sqlite3
import secrets
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Database path — configurable via env var for Railway persistent volume
KEYS_DB_PATH = Path(os.environ.get('KEYS_DB_PATH', Path(__file__).parent / 'keys.db'))


def _get_conn():
    """Get a SQLite connection with WAL mode for concurrent reads."""
    conn = sqlite3.connect(str(KEYS_DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create tables if they don't exist."""
    conn = _get_conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS api_keys (
                api_key     TEXT PRIMARY KEY,
                email       TEXT NOT NULL,
                name        TEXT NOT NULL DEFAULT '',
                tier        TEXT NOT NULL DEFAULT 'free',
                ghost_uuid  TEXT,
                created_at  TEXT NOT NULL,
                tier_checked_at TEXT NOT NULL,
                is_active   INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS usage (
                api_key TEXT NOT NULL,
                month   TEXT NOT NULL,  -- YYYY-MM
                count   INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (api_key, month),
                FOREIGN KEY (api_key) REFERENCES api_keys(api_key)
            );

            CREATE INDEX IF NOT EXISTS idx_api_keys_email ON api_keys(email);
        """)
        conn.commit()
    finally:
        conn.close()


def generate_key() -> str:
    """Generate a new API key: eae_ + 32 hex chars."""
    return 'eae_' + secrets.token_hex(16)


def provision_key(email: str, name: str = '', tier: str = 'free', ghost_uuid: str = None) -> dict:
    """
    Provision a key for a user. Idempotent: returns existing active key
    or creates a new one. Updates tier if it has changed.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        # Check for existing active key
        row = conn.execute(
            "SELECT * FROM api_keys WHERE email = ? AND is_active = 1",
            (email,)
        ).fetchone()

        if row:
            # Update tier and name if changed
            if row['tier'] != tier or row['name'] != name:
                conn.execute(
                    "UPDATE api_keys SET tier = ?, name = ?, tier_checked_at = ? WHERE api_key = ?",
                    (tier, name, now, row['api_key'])
                )
                conn.commit()
            return dict(row) | {'tier': tier, 'name': name, 'tier_checked_at': now}

        # Create new key
        api_key = generate_key()
        conn.execute(
            """INSERT INTO api_keys (api_key, email, name, tier, ghost_uuid, created_at, tier_checked_at, is_active)
               VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
            (api_key, email, name, tier, ghost_uuid, now, now)
        )
        conn.commit()

        return {
            'api_key': api_key,
            'email': email,
            'name': name,
            'tier': tier,
            'ghost_uuid': ghost_uuid,
            'created_at': now,
            'tier_checked_at': now,
            'is_active': 1
        }
    finally:
        conn.close()


def get_key_info(api_key: str) -> Optional[dict]:
    """Look up a key. Returns user info dict or None."""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM api_keys WHERE api_key = ? AND is_active = 1",
            (api_key,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def check_and_increment_usage(api_key: str, tier: str) -> dict:
    """
    Increment monthly usage counter and check against tier limit.
    Returns {used, limit, remaining}.
    """
    TIER_LIMITS = {
        'free': 10,
        'daily': 30,
        'premium': None,  # unlimited
    }

    month = datetime.now(timezone.utc).strftime('%Y-%m')
    limit = TIER_LIMITS.get(tier)
    conn = _get_conn()
    try:
        # Upsert usage row
        conn.execute(
            """INSERT INTO usage (api_key, month, count) VALUES (?, ?, 1)
               ON CONFLICT(api_key, month) DO UPDATE SET count = count + 1""",
            (api_key, month)
        )
        conn.commit()

        row = conn.execute(
            "SELECT count FROM usage WHERE api_key = ? AND month = ?",
            (api_key, month)
        ).fetchone()
        used = row['count'] if row else 1

        if limit is not None:
            remaining = max(0, limit - used)
        else:
            remaining = None  # unlimited

        return {
            'used': used,
            'limit': limit,
            'remaining': remaining,
            'month': month
        }
    finally:
        conn.close()


def get_usage(api_key: str) -> dict:
    """Get current month's usage without incrementing."""
    month = datetime.now(timezone.utc).strftime('%Y-%m')
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT count FROM usage WHERE api_key = ? AND month = ?",
            (api_key, month)
        ).fetchone()
        return {
            'used': row['count'] if row else 0,
            'month': month
        }
    finally:
        conn.close()


def regenerate_key(email: str) -> Optional[dict]:
    """
    Generate a new key for a user, deactivating the old one.
    Returns the new key info or None if no existing key found.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        # Find existing active key to preserve tier info
        old = conn.execute(
            "SELECT * FROM api_keys WHERE email = ? AND is_active = 1",
            (email,)
        ).fetchone()

        if not old:
            return None

        # Deactivate old key
        conn.execute(
            "UPDATE api_keys SET is_active = 0 WHERE email = ? AND is_active = 1",
            (email,)
        )

        # Create new key
        new_key = generate_key()
        conn.execute(
            """INSERT INTO api_keys (api_key, email, name, tier, ghost_uuid, created_at, tier_checked_at, is_active)
               VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
            (new_key, old['email'], old['name'], old['tier'], old['ghost_uuid'], now, now)
        )
        conn.commit()

        return {
            'api_key': new_key,
            'email': old['email'],
            'name': old['name'],
            'tier': old['tier'],
            'ghost_uuid': old['ghost_uuid'],
            'created_at': now,
            'tier_checked_at': now,
            'is_active': 1
        }
    finally:
        conn.close()


def update_tier(email: str, tier: str):
    """Update the tier for a user's active key."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        conn.execute(
            "UPDATE api_keys SET tier = ?, tier_checked_at = ? WHERE email = ? AND is_active = 1",
            (tier, now, email)
        )
        conn.commit()
    finally:
        conn.close()
