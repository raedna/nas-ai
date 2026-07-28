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
    # Source-anchored votes: the vote judges the ANSWER, and the answer came
    # from a specific entry. Legacy rows (NULL) keep collection-level effect.
    execute("ALTER TABLE answer_feedback "
            "ADD COLUMN IF NOT EXISTS source_entry TEXT", ())
    _READY = True


def record_feedback(question: str, answer: str, verdict: str,
                    collection: str = None, method: str = None,
                    session_id=None, source_entry: str = None) -> None:
    ensure_feedback_table()
    execute("""
        INSERT INTO answer_feedback
            (session_id, question, answer, collection, method, verdict,
             source_entry)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (session_id, str(question)[:1000], str(answer)[:3000],
          collection, method, verdict,
          (str(source_entry)[:500] if source_entry else None)))
    print(f"[FEEDBACK] {verdict}: {str(question)[:60]} "
          f"(source: {source_entry})")


def feedback_prior(question: str):
    """Net verdicts for THIS question (normalized exact match), keyed by
    collection -> {source_entry_or_None: net}. A vote judges the ANSWER it
    was given on: source-anchored votes count only while the collection is
    again answering from that same entry (a thumbs-down on the wrong doc
    must not keep penalizing the collection after the pipeline learns to
    answer from the right one). Legacy rows (source NULL) keep the old
    collection-level effect. Similarity matching waits for a corpus."""
    ensure_feedback_table()
    import re
    qn = re.sub(r"[^a-z0-9]+", " ", str(question).lower()).strip()
    rows = fetchall("""
        SELECT collection, source_entry,
               SUM(CASE verdict WHEN 'up' THEN 1 ELSE -1 END) AS net
        FROM answer_feedback
        WHERE regexp_replace(lower(question), '[^a-z0-9]+', ' ', 'g') = %s
        AND collection IS NOT NULL
        GROUP BY collection, source_entry
    """, (qn,))
    out = {}
    for r in rows:
        out.setdefault(r["collection"], {})[r["source_entry"]] = int(r["net"])
    return out


def list_feedback(limit: int = 100):
    ensure_feedback_table()
    return fetchall("""
        SELECT id, question, verdict, collection, method, created_at
        FROM answer_feedback ORDER BY id DESC LIMIT %s
    """, (limit,))


def delete_feedback(feedback_id: int) -> None:
    """Mis-clicks change arbitration now — they need an undo."""
    execute("DELETE FROM answer_feedback WHERE id = %s", (feedback_id,))


def verified_answer(question: str):
    """M4b: the newest 👍-verified answer for THIS question (normalized
    exact match), unless its source collection re-ingested since the
    verdict — data changing retires stale verdicts at query time, no
    invalidation machinery. Returns {answer, collection, verified_at} | None."""
    ensure_feedback_table()
    import re
    qn = re.sub(r"[^a-z0-9]+", " ", str(question).lower()).strip()
    rows = fetchall("""
        SELECT f.answer, f.collection, f.created_at
        FROM answer_feedback f
        WHERE regexp_replace(lower(f.question), '[^a-z0-9]+', ' ', 'g') = %s
        AND f.verdict = 'up'
        AND NOT EXISTS (
            SELECT 1 FROM answer_feedback d
            WHERE regexp_replace(lower(d.question), '[^a-z0-9]+', ' ', 'g') = %s
            AND d.verdict = 'down' AND d.created_at > f.created_at)
        AND (f.collection IS NULL OR NOT EXISTS (
            SELECT 1 FROM files i
            WHERE i.collection_name = f.collection
            AND i.updated_at > f.created_at))
        ORDER BY f.created_at DESC LIMIT 1
    """, (qn, qn))
    if not rows:
        return None
    return {"answer": rows[0]["answer"], "collection": rows[0]["collection"],
            "verified_at": rows[0]["created_at"]}


def latest_verdict(question: str):
    """'up' | 'down' | None — the newest verdict on this exact question."""
    ensure_feedback_table()
    import re
    qn = re.sub(r"[^a-z0-9]+", " ", str(question).lower()).strip()
    rows = fetchall("""
        SELECT verdict FROM answer_feedback
        WHERE regexp_replace(lower(question), '[^a-z0-9]+', ' ', 'g') = %s
        ORDER BY created_at DESC LIMIT 1
    """, (qn,))
    return rows[0]["verdict"] if rows else None


def feedback_stats():
    ensure_feedback_table()
    return fetchall("""
        SELECT collection, verdict, count(*) AS n
        FROM answer_feedback GROUP BY collection, verdict
        ORDER BY collection, verdict
    """, ())
