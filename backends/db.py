"""
PostgreSQL database connection and helpers for external_api_proxy.
Uses DATABASE_URL from environment with connection pooling.
"""
import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError(
        "DATABASE_URL environment variable is required for PostgreSQL. "
        "Example: postgresql://user:password@localhost:5432/nswpsn"
    )

# Connection pool: min 2, max 20 connections shared across threads
_pool = pool.ThreadedConnectionPool(2, 20, DATABASE_URL)


def get_conn():
    """Get a PostgreSQL connection from the pool. Caller MUST call conn.close() to return it."""
    conn = _pool.getconn()
    conn.autocommit = False
    return conn


def get_conn_dict():
    """Get connection with RealDictCursor from pool. Caller MUST call conn.close() to return it."""
    conn = _pool.getconn()
    conn.autocommit = False
    # Attach cursor factory so callers using conn.cursor() get dict rows
    conn.cursor_factory = RealDictCursor
    return conn


# Override close to return connection to pool instead of destroying it
_original_close = psycopg2.extensions.connection.close


def _return_to_pool(conn):
    """Return connection to pool instead of closing it."""
    try:
        _pool.putconn(conn)
    except Exception:
        # If pool rejects it (e.g. broken conn), actually close it
        try:
            _original_close(conn)
        except Exception:
            pass


# Monkey-patch so existing conn.close() calls return to pool
psycopg2.extensions.connection.close = _return_to_pool
