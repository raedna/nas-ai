"""
core/vocab.py — per-collection vocabulary + trigram spell-correction (VOCAB-01).

The vocabulary is the collection's OWN lexicon: distinct lexemes from the
nlp_text tsvector index (ts_stat), stored with document counts. Query-time,
a token unknown to the vocabulary is corrected to its nearest lexeme by
trigram similarity ('brodcaster' -> 'broadcast') — deterministic, grounded
in the data, no external dictionary. Tokens with no close neighbour stay
unchanged (downstream corpus filtering handles them).

Config (system.json):
    "vocab_correction": {"enabled": true, "min_similarity": 0.5}
"""
import re

from core.db import execute, fetchall


def ensure_vocab_table():
    execute("""
        CREATE TABLE IF NOT EXISTS collection_vocab (
            collection TEXT NOT NULL,
            word TEXT NOT NULL,
            ndoc INT NOT NULL DEFAULT 1,
            PRIMARY KEY (collection, word)
        )
    """, ())
    execute("""
        CREATE INDEX IF NOT EXISTS collection_vocab_word_trgm
        ON collection_vocab USING gin (word gin_trgm_ops)
    """, ())


def build_collection_vocab(collection: str) -> int:
    """(Re)build the vocabulary for one collection from its tsvector index.
    Collection name is sanitized (internal names only) because ts_stat takes
    a query string, not parameters."""
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", str(collection)):
        raise ValueError(f"invalid collection name: {collection!r}")
    ensure_vocab_table()
    execute("DELETE FROM collection_vocab WHERE collection = %s", (collection,))
    execute(f"""
        INSERT INTO collection_vocab (collection, word, ndoc)
        SELECT %s, word, ndoc
        FROM ts_stat('SELECT nlp_text_tsv FROM chunks
                      WHERE collection_name = ''{collection}''')
        WHERE length(word) >= 3
        ON CONFLICT (collection, word) DO UPDATE SET ndoc = EXCLUDED.ndoc
    """, (collection,))
    rows = fetchall(
        "SELECT count(*) AS n FROM collection_vocab WHERE collection = %s",
        (collection,))
    n = rows[0]["n"] if rows else 0
    print(f"[VOCAB] {collection}: {n} lexemes")
    return n


def _cfg():
    try:
        from core.system_config import load_system_config
        c = load_system_config().get("vocab_correction", {})
        return bool(c.get("enabled", True)), float(c.get("min_similarity", 0.75))
    except Exception:
        return True, 0.75


def correct_word(word: str, collection: str = None):
    """Return (corrected_word, was_corrected). A word already in the
    vocabulary is kept. An unknown word: pg_trgm fetches candidate lexemes
    (loose recall), difflib sequence ratio scores them (typo-robust — raw
    trigram overlap scores 'brodcaster'/'broadcast' at only ~0.4 because a
    scrambled letter destroys three trigrams; sequence ratio gives 0.84).
    Best candidate wins at ratio >= config min_similarity (default 0.75);
    ties broken by document frequency, then alphabetically."""
    enabled, min_sim = _cfg()
    w = str(word or "").lower()
    if not enabled or len(w) < 3:
        return w, False
    try:
        # Stopwords are absent from the vocabulary BY DESIGN (tsvector drops
        # them) — never "correct" them ('again' -> 'gain'). An empty tsvector
        # means the word carries no lexical signal at all.
        _ts = fetchall("SELECT to_tsvector('english', %s)::text AS v", (w,))
        _tsv = str(_ts[0]["v"] or "") if _ts else ""
        if not _tsv.strip():
            return w, False
        _m = re.match(r"'([^']+)'", _tsv)
        _lexeme = _m.group(1) if _m else w

        # Known = the word OR ITS LEXEME exists in the vocabulary ('acting'
        # is known via 'act'); membership on surface form alone made every
        # inflected known word look like a typo.
        scope = "AND collection = %s" if collection else ""
        args = (w, _lexeme, collection) if collection else (w, _lexeme)
        known = fetchall(
            f"SELECT 1 FROM collection_vocab WHERE word IN (%s, %s) {scope} LIMIT 1",
            args)
        if known:
            return w, False
        args2 = (w, collection, w) if collection else (w, w)
        rows = fetchall(f"""
            SELECT word, similarity(word, %s) AS sim, ndoc
            FROM collection_vocab
            WHERE {'collection = %s AND' if collection else ''} word %% %s
            ORDER BY sim DESC, ndoc DESC, word ASC
            LIMIT 8
        """, args2)
        if rows:
            from difflib import SequenceMatcher
            scored = sorted(
                ((SequenceMatcher(None, w, r["word"]).ratio(),
                  int(r["ndoc"]), r["word"]) for r in rows),
                key=lambda x: (-x[0], -x[1], x[2]))
            if scored and scored[0][0] >= min_sim:
                return scored[0][2], True
    except Exception:
        pass
    return w, False


def correct_words(words, collection: str = None):
    """Correct a list of tokens; returns (corrected_list, corrections_dict)."""
    out, changed = [], {}
    for w in words:
        c, was = correct_word(w, collection)
        out.append(c)
        if was:
            changed[w] = c
    if changed:
        print(f"[VOCAB] corrected: {changed}")
    return out, changed
