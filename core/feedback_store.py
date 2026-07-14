"""
core/feedback_store.py — answer feedback capture (Memory M4a).

Capture only: every thumbs verdict is stored with enough context (question,
answer, collection, method) to design the CONSUMPTION mechanism later
against real accumulated data — ranking priors are guesses until there is
a feedback corpus to measure them on.
"""
from core.db import execute, fetchall

_READY = False


def ensure_feedback_table():
    global _READY
    if _READY:
        return
    execute("""
        CREATE TABLE IF NOT EXISTS answer_feedback (
            id SERIAL PRIMARY KEY,
            session_id INT,
            question TEXT NOT NULL,
            answer TEXT NOT NULL,
            collection TEXT,
            method TEXT,
            verdict TEXT NOT NULL CHECK (verdict IN ('up', 'down')),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """, ())
    _READY = True


def record_feedback(question: str, answer: str, verdict: str,
                    collection: str = None, method: str = None,
                    session_id=None) -> None:
    ensure_feedback_table()
    execute("""
        INSERT INTO answer_feedback
            (session_id, question, answer, collection, method, verdict)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (session_id, str(question)[:1000], str(answer)[:3000],
          collection, method, verdict))
    print(f"[FEEDBACK] {verdict}: {str(question)[:60]}")


def feedback_prior(question: str):
    """Net verdict per collection for THIS question (normalized exact match).
    M4 v1 consumption: +1 per up, -1 per down. Exact matching transfers no
    feedback to questions it was not given on; similarity matching waits for
    a corpus to calibrate against."""
    ensure_feedback_table()
    import re
    qn = re.sub(r"[^a-z0-9]+", " ", str(question).lower()).strip()
    rows = fetchall("""
        SELECT collection,
               SUM(CASE verdict WHEN 'up' THEN 1 ELSE -1 END) AS net
        FROM answer_feedback
        WHERE regexp_replace(lower(question), '[^a-z0-9]+', ' ', 'g') = %s
        AND collection IS NOT NULL
        GROUP BY collection
    """, (qn,))
    return {r["collection"]: int(r["net"]) for r in rows}


def list_feedback(limit: int = 100):
    ensure_feedback_table()
    return fetchall("""
        SELECT id, question, verdict, collection, method, created_at
        FROM answer_feedback ORDER BY id DESC LIMIT %s
    """, (limit,))


def delete_feedback(feedback_id: int) -> None:
    """Mis-clicks change arbitration now — they need an undo."""
    execute("DELETE FROM answer_feedback WHERE id = %s", (feedback_id,))


def feedback_stats():
    ensure_feedback_table()
    return fetchall("""
        SELECT collection, verdict, count(*) AS n
        FROM answer_feedback GROUP BY collection, verdict
        ORDER BY collection, verdict
    """, ())
