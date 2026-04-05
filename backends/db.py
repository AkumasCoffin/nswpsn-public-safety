"""
PostgreSQL database connection and helpers for external_api_proxy.
Uses DATABASE_URL from environment.
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError(
        "DATABASE_URL environment variable is required for PostgreSQL. "
        "Example: postgresql://user:password@localhost:5432/nswpsn"
    )


def get_conn():
    """Get a new PostgreSQL connection."""
    return psycopg2.connect(DATABASE_URL)


def get_conn_dict():
    """Get connection with RealDictCursor for row['col'] access (like sqlite3.Row)."""
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
