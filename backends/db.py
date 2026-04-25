"""
PostgreSQL database connection and helpers for external_api_proxy.
Uses DATABASE_URL from environment with connection pooling.

Connections returned from get_conn() / get_conn_dict() are lightweight
wrappers whose .close() returns the underlying connection to the pool
instead of actually closing it. The wrapper proxies every other attribute
to the real connection, so existing `conn.cursor()`, `conn.commit()`, etc.
calls work unchanged.

(Older versions of this module monkey-patched
`psycopg2.extensions.connection.close`, but that C-extension type is
immutable on Python 3.13+ and raises TypeError at import time.)
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


class _PooledConn:
    """Proxy around a pooled psycopg2 connection.

    `.close()` returns the underlying connection to the pool. Every other
    attribute/method is forwarded to the wrapped connection, so existing
    call sites that use `conn.cursor()`, `conn.commit()`, `conn.rollback()`,
    `conn.autocommit = ...`, etc. keep working.
    """
    __slots__ = ('_conn', '_closed')

    def __init__(self, conn):
        object.__setattr__(self, '_conn', conn)
        object.__setattr__(self, '_closed', False)

    def close(self):
        if self._closed:
            return
        object.__setattr__(self, '_closed', True)
        try:
            _pool.putconn(self._conn)
        except Exception:
            # Pool rejected it — actually close the underlying connection.
            try:
                self._conn.close()
            except Exception:
                pass

    def __getattr__(self, name):
        # __getattr__ only fires for attributes not found on the wrapper.
        return getattr(self._conn, name)

    def __setattr__(self, name, value):
        # Route attribute writes (e.g. `conn.autocommit = False`) to the
        # underlying connection; internal state uses object.__setattr__.
        if name in _PooledConn.__slots__:
            object.__setattr__(self, name, value)
        else:
            setattr(self._conn, name, value)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


def get_conn():
    """Get a PostgreSQL connection from the pool. Caller MUST call conn.close() to return it.

    Always resets cursor_factory to the default tuple cursor. Without this, a
    connection previously leased via get_conn_dict() would come back with
    RealDictCursor still set, and tuple-unpacking the rows would silently bind
    variables to the column *names* instead of values.
    """
    conn = _pool.getconn()
    conn.autocommit = False
    conn.cursor_factory = None  # restore default tuple cursor
    return _PooledConn(conn)


def get_conn_dict():
    """Get connection with RealDictCursor from pool. Caller MUST call conn.close() to return it."""
    conn = _pool.getconn()
    conn.autocommit = False
    conn.cursor_factory = RealDictCursor
    return _PooledConn(conn)
