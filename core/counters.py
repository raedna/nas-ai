"""
core/counters.py — durable event counters (SPEED/quality bookkeeping).
Read them in the SQL Inspector:  SELECT * FROM runtime_counters;
"""
from core.db import execute, fetchall

_READY = False


def ensure_table():
    global _READY
    if _READY:
        return
    execute("""
        CREATE TABLE IF NOT EXISTS runtime_counters (
            name TEXT PRIMARY KEY,
            count BIGINT NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """, ())
    _READY = True


def bump(name: str, n: int = 1) -> None:
    try:
        ensure_table()
        execute("""
            INSERT INTO runtime_counters (name, count) VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE
            SET count = runtime_counters.count + EXCLUDED.count,
                updated_at = NOW()
        """, (name, n))
    except Exception:
        pass  # counters must never break a turn


def snapshot():
    ensure_table()
    return fetchall("SELECT name, count, updated_at FROM runtime_counters "
                    "ORDER BY name", ())
