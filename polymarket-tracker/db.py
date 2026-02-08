"""
Shared SQLite database module for all Polymarket Copy Trader components.

Provides:
- A single WAL-mode connection pool (one writer at a time)
- A global write lock shared across app.py and fetcher.py
- Context-manager helpers that guarantee connections are closed
"""
import sqlite3
import threading
from contextlib import contextmanager

DATABASE = 'polymarket_trades.db'

# ONE global write lock shared by every module that imports db.py
_write_lock = threading.Lock()


def get_db():
    """Open a new SQLite connection with WAL mode and generous busy_timeout."""
    conn = sqlite3.connect(
        DATABASE,
        timeout=120,               # wait up to 120s for the lock
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=120000')  # 120 seconds
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA wal_autocheckpoint=1000')
    return conn


@contextmanager
def get_conn():
    """Context manager that yields a read-only connection and auto-closes it."""
    conn = get_db()
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_write_conn():
    """
    Context manager that:
    1. Acquires the global write lock
    2. Opens a connection
    3. Yields it
    4. Commits on success, rolls back on exception
    5. Closes the connection and releases the lock
    """
    with _write_lock:
        conn = get_db()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def db_write(fn):
    """Execute a callable under the global write lock. Legacy helper."""
    with _write_lock:
        return fn()
