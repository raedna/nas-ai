"""
core/memory_store.py — the `memory` collection (Memory M2).

Remembered facts are ORDINARY CHUNKS in an ordinary collection: they get
embeddings, tsvector indexing, vocab, routing, BM25+vector retrieval and
arbitration for free — no parallel answer path, no new guards. Provenance
(session, date, the question being discussed) lives in the payload and in
the visible text, so a memory can never masquerade as corpus data.

Capture triggers (config system.json -> memory.triggers) are matched
deterministically in chat_turn — no LLM decides what enters memory.
"""
import uuid
from datetime import datetime

from core.db import execute, fetchall, upsert_chunk, upsert_collection

MEMORY_COLLECTION = "memory"


def _triggers():
    """Config-driven capture prefixes; defaults are a starting vocabulary."""
    try:
        from core.system_config import load_system_config
        t = load_system_config().get("memory", {}).get("triggers")
        if isinstance(t, list) and t:
            return [str(x).lower() for x in t]
    except Exception:
        pass
    # Bare "remember " is deliberately absent: it captures recall questions
    # ("remember the file we discussed?"). Add it via config if wanted.
    return ["remember that ", "remember: ", "note that ",
            "keep in mind that ", "keep in mind "]


def _fillers():
    try:
        from core.system_config import load_system_config
        f = load_system_config().get("memory", {}).get("filler_words")
        if isinstance(f, list) and f:
            return [str(x).lower() for x in f]
    except Exception:
        pass
    return ["ok", "okay", "please", "also", "hey", "and", "so", "now"]


def match_memory_command(question: str):
    """Deterministic capture gate: returns the FACT text if the message is a
    memory command, else None. Leading filler words ('ok, remember that...')
    are stripped before the prefix match; recall QUESTIONS ('do you remember
    X?') never match because 'do you' is not a filler. Longest trigger wins
    ('remember that' before 'remember')."""
    import re as _re
    q = str(question or "").strip()
    ql = q.lower()
    _fill = set(_fillers())
    while True:
        m = _re.match(r"^([a-z']+)[,\s]+", ql)
        if m and m.group(1) in _fill:
            q = q[m.end():]
            ql = ql[m.end():]
        else:
            break
    for trig in sorted(_triggers(), key=len, reverse=True):
        if ql.startswith(trig):
            fact = q[len(trig):].strip()
            return fact if fact else None
    return None


def ensure_memory_collection():
    rows = fetchall("SELECT 1 FROM collections WHERE name = %s",
                    (MEMORY_COLLECTION,))
    if not rows:
        upsert_collection(MEMORY_COLLECTION, {
            "path": "",
            "source_label": "User memory",
            "notes": "Facts the user asked NAS-AI to remember (chat capture).",
        })


def remember(fact: str, session_id=None, context_question: str = None,
             origin: str = "chat_command") -> str:
    """Store one fact as a memory chunk. Returns the chunk id."""
    from core.embedder import embed_text

    ensure_memory_collection()
    told_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    chunk_id = f"memory:{uuid.uuid4().hex[:12]}"

    # The provenance is IN the text — any surface that renders this chunk
    # (answer, compose block, related section) shows it unconditionally.
    nlp_text = f"{fact}\n\n(User note, {told_at})"

    upsert_chunk({
        "id": chunk_id,
        "collection_name": MEMORY_COLLECTION,
        "source_file": "user_memory",
        "source_type": "memory",
        "doc_type": "memory_note",
        "identifier": chunk_id.split(":", 1)[1],
        "identifier_namespace": "memory",
        "primary_name": fact[:80],
        "description": fact,
        "nlp_text": nlp_text,
        "embedding": embed_text(nlp_text),
        "embedding_model": None,
        "embedded_at": None,
        "payload": {
            "doc_type": "memory_note",
            # identifier/source_file mirrored INTO the payload — the NER
            # cross-link scanner (and other payload-keyed consumers) read
            # them from there, not from the table columns.
            "identifier": chunk_id.split(":", 1)[1],
            "source_file": "user_memory",
            "source": "user",
            "origin": origin,
            "told_at": told_at,
            "session_id": session_id,
            "context_question": context_question,
            "text": nlp_text,
        },
    })
    # Refresh the memory collection's vocabulary so Tier 1.3 ownership and
    # spell-correction know its words immediately.
    try:
        from core.vocab import build_collection_vocab
        build_collection_vocab(MEMORY_COLLECTION)
    except Exception as e:
        print(f"[MEMORY] vocab refresh failed: {e}")
    # Cross-link the note to any records it MENTIONS (gsact.txt -> the recon
    # record) via the existing identifier scanner — integration without
    # touching the corpus: data stays data, recollection stays recollection.
    try:
        from core.ner_cross_linker import run_identifier_ner
        run_identifier_ner(MEMORY_COLLECTION)
        # Memory edges are AUTO-CONFIRMED at filename confidence: the note is
        # the user's own assertion and a filename match is unambiguous —
        # requiring KG review here would hide the note from the very answers
        # it annotates. Low-confidence (code-term) edges still await review.
        execute("""UPDATE cross_links SET status = 'confirmed'
                   WHERE source_collection = %s
                   AND status IN ('candidate', 'pending_review')
                   AND confidence >= 0.85""", (MEMORY_COLLECTION,))
    except Exception as e:
        print(f"[MEMORY] cross-link scan failed: {e}")
    print(f"[MEMORY] saved ({origin}): {fact[:80]}")
    return chunk_id


def list_memories(limit: int = 100):
    return fetchall("""
        SELECT identifier, primary_name, nlp_text,
               payload->>'told_at' AS told_at,
               payload->>'origin' AS origin
        FROM chunks WHERE collection_name = %s
        ORDER BY created_at DESC LIMIT %s
    """, (MEMORY_COLLECTION, limit))


def forget(identifier: str) -> int:
    """Delete one memory by identifier; returns rows removed."""
    rows = fetchall(
        "SELECT count(*) AS n FROM chunks WHERE collection_name = %s AND identifier = %s",
        (MEMORY_COLLECTION, identifier))
    execute("DELETE FROM chunks WHERE collection_name = %s AND identifier = %s",
            (MEMORY_COLLECTION, identifier))
    # A forgotten note's edges must go with it — dangling confirmed links
    # point the related-section builder at a chunk that no longer exists.
    execute("""DELETE FROM cross_links
               WHERE (source_collection = %s AND source_identifier = %s)
               OR (target_collection = %s AND target_identifier = %s)""",
            (MEMORY_COLLECTION, identifier, MEMORY_COLLECTION, identifier))
    return rows[0]["n"] if rows else 0
