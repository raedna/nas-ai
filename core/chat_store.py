"""
core/chat_store.py — persistent chat sessions (Memory M1).

Sessions and messages live in the same PostgreSQL as everything else.
Every turn is written as it happens — a crash or restart loses nothing.
This is also the substrate for later memory phases: remembered facts cite
(session, date); feedback attaches to message rows.
"""
import json

from core.db import execute, fetchall

_READY = False


def ensure_chat_tables():
    global _READY
    if _READY:
        return
    execute("""
        CREATE TABLE IF NOT EXISTS chat_sessions (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL DEFAULT 'New chat',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """, ())
    execute("""
        CREATE TABLE IF NOT EXISTS chat_messages (
            id SERIAL PRIMARY KEY,
            session_id INT NOT NULL REFERENCES chat_sessions(id) ON DELETE CASCADE,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            collection TEXT,
            answer_payload JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """, ())
    execute("""
        CREATE INDEX IF NOT EXISTS chat_messages_session
        ON chat_messages (session_id, id)
    """, ())
    _READY = True


def create_session(title: str = "New chat") -> int:
    ensure_chat_tables()
    # INSERT..RETURNING needs a COMMIT — fetchall() is the read helper and
    # rolls back on connection return (the row vanished, FK violations
    # followed). Raw connection with explicit commit.
    from core.db import get_conn
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO chat_sessions (title) VALUES (%s) RETURNING id",
                (str(title)[:120],))
            sid = cur.fetchone()[0]
        conn.commit()
    return int(sid)


def list_sessions(limit: int = 30):
    """Most recent first: [{id, title, updated_at, n_messages}]."""
    ensure_chat_tables()
    return fetchall("""
        SELECT s.id, s.title, s.updated_at,
               (SELECT count(*) FROM chat_messages m
                WHERE m.session_id = s.id) AS n_messages
        FROM chat_sessions s
        ORDER BY s.updated_at DESC
        LIMIT %s
    """, (limit,))


def get_messages(session_id: int):
    """Full transcript, oldest first."""
    ensure_chat_tables()
    return fetchall("""
        SELECT role, content, collection, answer_payload, created_at
        FROM chat_messages WHERE session_id = %s ORDER BY id
    """, (session_id,))


def add_message(session_id: int, role: str, content: str,
                collection: str = None, answer_payload: dict = None):
    ensure_chat_tables()
    _payload = None
    if answer_payload:
        try:
            _payload = json.dumps(answer_payload, default=str)
        except Exception:
            _payload = None
    execute("""
        INSERT INTO chat_messages (session_id, role, content, collection, answer_payload)
        VALUES (%s, %s, %s, %s, %s::jsonb)
    """, (session_id, str(role), str(content), collection, _payload))
    execute("UPDATE chat_sessions SET updated_at = NOW() WHERE id = %s",
            (session_id,))


def set_title_from_first_question(session_id: int, question: str):
    """Title = first question, truncated — set once, only while still default."""
    execute("""
        UPDATE chat_sessions SET title = %s
        WHERE id = %s AND title = 'New chat'
    """, (str(question)[:80], session_id))


def delete_session(session_id: int):
    ensure_chat_tables()
    execute("DELETE FROM chat_sessions WHERE id = %s", (session_id,))
