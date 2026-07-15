"""
core/sql_snippets.py — saved SQL statements for the Inspector (user library).
"""
from core.db import execute, fetchall

_READY = False


def ensure_table():
    global _READY
    if _READY:
        return
    execute("""
        CREATE TABLE IF NOT EXISTS sql_snippets (
            id SERIAL PRIMARY KEY,
            label TEXT NOT NULL,
            sql TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """, ())
    _READY = True


def save_snippet(label: str, sql: str) -> None:
    ensure_table()
    execute("INSERT INTO sql_snippets (label, sql) VALUES (%s, %s)",
            (str(label)[:120] or str(sql)[:60], str(sql)))


def list_snippets():
    ensure_table()
    return fetchall(
        "SELECT id, label, sql FROM sql_snippets ORDER BY id DESC LIMIT 50", ())


def delete_snippet(snippet_id: int) -> None:
    execute("DELETE FROM sql_snippets WHERE id = %s", (snippet_id,))
