"""core/keyword_extractor.py — ingestion-time 'about' scan.

After a full entry is serialized (every chunk of a doc/ticket), ONE LLM call
decides what the entry IS ABOUT — a 1-2 sentence `about` line plus a
`keywords` list — and what it merely MENTIONS (`related`). The result is
stamped onto every chunk payload of the entry. Query-time code (the reranker
subject guard) then reads these deterministically: the model classifies once
at ingestion with the whole entry in view; gates decide at query time.

Only doc-shaped entries are scanned. entity_row records already carry their
about-ness in structured fields, and per-record LLM calls would be
prohibitive (thousands of FIX tags / BBG fields). kb_docs is mixed per
chunk — table rows inside a doc collection skip, prose entries scan.

Extraction failures leave the payload unannotated; every reader must fall
back to pre-keyword behavior when `about`/`keywords` are absent.
"""
from typing import Dict, List, Optional

# doc_types that are records, not prose — never scanned
_SKIP_DOC_TYPES = {"entity_row"}

# One entry's text is capped before the LLM call; abouts come from the
# opening + structure, not from reading 100KB of log dumps.
_MAX_ENTRY_CHARS = 12000

_SYSTEM = (
    "You are indexing a document for retrieval. Read the ENTIRE document, "
    "then return STRICT JSON: "
    "{\"about\": \"...\", \"keywords\": [...], \"related\": [...]}.\n"
    "- about: 1-2 sentences stating what this document IS ABOUT — its "
    "subject and purpose, not a summary of every detail.\n"
    "- keywords: 5-15 lowercase keywords/phrases naming the document's "
    "SUBJECTS: the systems, entities, processes and error conditions it is "
    "about. Each keyword must answer yes to: 'is this document about "
    "this?'\n"
    "- related: lowercase terms the document MENTIONS as context, cause, "
    "cross-reference or example, but is NOT about. A troubleshooting guide "
    "for system X that says 'check system Y traffic as a possible cause' "
    "is about X; Y goes in related. When unsure which list a term belongs "
    "in, put it in related."
)


def extract_entry(title: str, text: str) -> Optional[Dict]:
    """One LLM call for one full entry. Returns {about, keywords, related}
    with normalized shapes, or None on any failure (caller must treat None
    as 'leave unannotated')."""
    from core.local_llm_client import call_local_llm_json
    try:
        out = call_local_llm_json(
            _SYSTEM,
            f"Document title: {title}\n\n{text[:_MAX_ENTRY_CHARS]}",
            temperature=0.0)
    except Exception as e:
        print(f"[KEYWORDS] extraction failed for {title!r}: "
              f"{type(e).__name__}: {e}")
        return None
    if not isinstance(out, dict):
        return None
    about = str(out.get("about") or "").strip()
    kws = [str(k).strip().lower() for k in (out.get("keywords") or [])
           if str(k).strip()]
    rel = [str(k).strip().lower() for k in (out.get("related") or [])
           if str(k).strip()]
    if not about and not kws:
        return None
    return {"about": about, "keywords": kws, "related": rel}


def about_scan_mode(collection_name: str) -> str:
    """Per-collection declared config (collections.json about_scan):
    'all'  — scan every entry regardless of doc_type (kb_docs: prose
             articles typed entity_row by the tables pipeline),
    'auto' — scan non-entity_row entries only (default),
    'off'  — never scan (record collections: xml, bbg, images).
    Declared over inferred: doc_type is a weak proxy (tracker lesson)."""
    try:
        import json
        from core.paths import COLLECTIONS_PATH
        with open(COLLECTIONS_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f).get(collection_name) or {}
        mode = str(cfg.get("about_scan") or "auto").lower()
        return mode if mode in ("all", "auto", "off") else "auto"
    except Exception:
        return "auto"


def scan_collection(collection_name: str, task_id=None,
                    is_cancelled=None) -> Dict:
    """On-demand / post-ingest 'about' scan over EXISTING chunks (same
    pattern as cross-links + concept vectors). Groups chunks by entry
    (primary_name), one LLM call per entry, updates payload in place — no
    re-embed. Resumable: entries already carrying 'about' are skipped, so
    unchanged files keep their annotations and a stopped run continues."""
    import json as _json
    from core.db import fetchall, execute

    mode = about_scan_mode(collection_name)
    if mode == "off":
        print(f"[KEYWORDS] {collection_name}: about_scan=off — skipping")
        return {"mode": mode, "entries": 0, "scanned": 0, "failed": 0}

    type_clause = ("" if mode == "all"
                   else "AND LOWER(COALESCE(doc_type, '')) "
                        "NOT IN ('entity_row')")
    rows = fetchall(
        f"""
        SELECT primary_name, BOOL_OR(payload ? 'about') AS has_about
        FROM chunks
        WHERE collection_name = %s
          AND COALESCE(primary_name, '') != ''
          {type_clause}
        GROUP BY primary_name
        ORDER BY primary_name
        """, (collection_name,))
    todo = [r["primary_name"] for r in rows if not r["has_about"]]
    print(f"[KEYWORDS] {collection_name} (about_scan={mode}): "
          f"{len(rows)} entries, {len(rows) - len(todo)} annotated, "
          f"{len(todo)} to scan")
    # About = what the entry IS ABOUT. For sectioned entries (tickets), that
    # is the INITIATING issue — resolution/closure chatter would blur it
    # (user decision 2026-08-02). Declared source sections; entries without
    # section metadata scan whole as before.
    try:
        import json as _json2
        from core.paths import SYSTEM_CONFIG_PATH as _SCP2
        with open(_SCP2, "r", encoding="utf-8") as _f2:
            _src_sections = [str(s).lower() for s in _json2.load(_f2).get(
                "about_scan_source_sections", ["issue"])]
    except Exception:
        _src_sections = ["issue"]

    scanned = failed = 0
    for name in todo:
        if is_cancelled and is_cancelled(task_id):
            print(f"[KEYWORDS] {collection_name}: cancelled after {scanned}")
            break
        parts = fetchall(
            "SELECT nlp_text, LOWER(COALESCE(payload->>'section','')) AS sec "
            "FROM chunks WHERE collection_name = %s "
            "AND primary_name = %s ORDER BY id", (collection_name, name))
        _issue_parts = [p for p in parts if p["sec"] in _src_sections]
        full = "\n\n".join(p["nlp_text"] or ""
                           for p in (_issue_parts or parts))
        if not full.strip():
            continue
        res = extract_entry(name, full)
        if not res:
            failed += 1
            continue
        execute(
            "UPDATE chunks SET payload = payload || %s::jsonb "
            "WHERE collection_name = %s AND primary_name = %s",
            (_json.dumps(res), collection_name, name))
        scanned += 1
    print(f"[KEYWORDS] {collection_name}: scanned {scanned}, "
          f"failed {failed}")
    vectors = sync_about_vectors(collection_name)
    return {"mode": mode, "entries": len(rows), "scanned": scanned,
            "failed": failed, "vectors": vectors}


def sync_about_vectors(collection_name: str) -> int:
    """Embed each entry's `about` line into the about_vectors table (used by
    the reranker's about guard for semantic is-about comparison). Idempotent:
    only missing or changed abouts re-embed. Vector size from system.json."""
    import json as _json
    from core.db import fetchall, execute

    try:
        from core.paths import SYSTEM_CONFIG_PATH
        with open(SYSTEM_CONFIG_PATH, "r", encoding="utf-8") as f:
            _dim = int(_json.load(f).get("vector_size", 1024))
    except Exception:
        _dim = 1024
    execute(
        f"""
        CREATE TABLE IF NOT EXISTS about_vectors (
            collection_name TEXT NOT NULL,
            primary_name    TEXT NOT NULL,
            about           TEXT NOT NULL,
            embedding       vector({_dim}),
            updated_at      TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (collection_name, primary_name)
        )
        """, ())

    rows = fetchall(
        """
        SELECT c.primary_name, MAX(c.payload->>'about') AS about,
               MAX(av.about) AS stored_about
        FROM chunks c
        LEFT JOIN about_vectors av
               ON av.collection_name = c.collection_name
              AND av.primary_name = c.primary_name
        WHERE c.collection_name = %s
          AND c.payload ? 'about'
          AND COALESCE(c.payload->>'about', '') != ''
        GROUP BY c.primary_name
        """, (collection_name,))
    todo = [(r["primary_name"], r["about"]) for r in rows
            if r["about"] != r["stored_about"]]
    if not todo:
        print(f"[KEYWORDS] {collection_name}: about_vectors up to date "
              f"({len(rows)} entries)")
        return 0
    from core.embedder import embed_texts
    vecs = embed_texts([a for _, a in todo])
    for (name, about), vec in zip(todo, vecs):
        execute(
            """
            INSERT INTO about_vectors
                (collection_name, primary_name, about, embedding, updated_at)
            VALUES (%s, %s, %s, %s::vector, NOW())
            ON CONFLICT (collection_name, primary_name) DO UPDATE SET
                about = EXCLUDED.about,
                embedding = EXCLUDED.embedding,
                updated_at = NOW()
            """, (collection_name, name, about, _json.dumps(vec)))
    print(f"[KEYWORDS] {collection_name}: embedded {len(todo)} about lines")
    return len(todo)
