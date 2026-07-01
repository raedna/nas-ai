import re
from core.db import fetchall


def _meaningful_context(text, term):
    """CL-02: True only if `term` appears as a whole word inside enough surrounding
    text to be a real discussion, not a bare list entry or passing reference."""
    if not text or not term:
        return False
    pat = re.compile(r'(?<![A-Za-z0-9])' + re.escape(term) + r'(?![A-Za-z0-9])', re.I)
    for m in pat.finditer(text):
        window = text[max(0, m.start() - 60): m.end() + 60]
        others = [w for w in re.findall(r'[A-Za-z0-9]+', window)
                  if w.lower() != term.lower()]
        if len(others) >= 8:
            return True
    return False


def discover_cross_links(source_collection, target_collections=None):
    """
    Scan source_collection's identifiers/names against all (or given)
    target collections. Returns list of candidate link dicts, not yet saved.
    """
    if target_collections is None:
        rows = fetchall(
            "SELECT DISTINCT collection_name FROM chunks WHERE collection_name != %s",
            (source_collection,)
        )
        target_collections = [r["collection_name"] for r in rows]

    # Check if this collection uses chunk-level identifiers (e.g. Obsidian: note_chunk_N)
    # If so, group by source_file and use that as the anchor — one query per note, not per chunk
    _sample = fetchall("""
        SELECT payload->>'identifier' AS identifier
        FROM chunks WHERE collection_name = %s
        AND payload->>'identifier' IS NOT NULL
        LIMIT 5
    """, (source_collection,))

    _is_chunked = any(
        "_chunk_" in (r.get("identifier") or "")
        for r in _sample
    )

    if _is_chunked:
        source_rows = fetchall("""
            SELECT DISTINCT
                payload->>'source_file' AS identifier,
                payload->>'primary_name' AS primary_name,
                payload->>'type' AS type_value,
                payload->>'category' AS category,
                payload->>'reference_identifier' AS ref_id
            FROM chunks
            WHERE collection_name = %s
            AND payload->>'source_file' IS NOT NULL
        """, (source_collection,))
    else:
        source_rows = fetchall("""
            SELECT DISTINCT
                payload->>'identifier' AS identifier,
                payload->>'primary_name' AS primary_name,
                payload->>'type' AS type_value,
                payload->>'category' AS category,
                payload->>'reference_identifier' AS ref_id
            FROM chunks
            WHERE collection_name = %s
            AND payload->>'identifier' IS NOT NULL
        """, (source_collection,))

    candidates = []

    for target in target_collections:
        for src in source_rows:
            src_id = (src.get("identifier") or "").strip()
            src_name = (src.get("primary_name") or "").strip()
            src_type = (src.get("type_value") or "").strip()
            src_cat = (src.get("category") or "").strip()
            src_ref = (src.get("ref_id") or "").strip()
            if not src_id:
                continue
            base = {
                "source_collection": source_collection,
                "source_identifier": src_id,
                "target_collection": target,
            }

            # Strategy 1 — exact identifier: the (normalized) source ID appears in the
            # target's identifier OR aliases. Auto-confirmed (1.0). Skip short numeric
            # IDs (arbitrary sequence numbers across unrelated systems).
            if src_id.isdigit() and len(src_id) < 5:
                exact = []
            else:
                exact = fetchall("""
                    SELECT DISTINCT payload->>'identifier' AS identifier
                    FROM chunks
                    WHERE collection_name = %s
                      AND (lower(payload->>'identifier') = lower(%s)
                           OR payload->'aliases' ? %s)
                """, (target, src_id, src_id))
            for e in exact:
                candidates.append({**base, "target_identifier": e["identifier"],
                                   "match_type": "exact_identifier", "confidence": 1.0})

            # Strategy 2 — structured field reference: a reference_identifier-role field
            # on the source points at a target ID/name. Auto-confirmed (0.95).
            if src_ref and not (src_ref.isdigit() and len(src_ref) < 5):
                refs = fetchall("""
                    SELECT DISTINCT payload->>'identifier' AS identifier
                    FROM chunks
                    WHERE collection_name = %s
                      AND (lower(payload->>'identifier') = lower(%s)
                           OR lower(payload->>'primary_name') = lower(%s))
                """, (target, src_ref, src_ref))
                for r in refs:
                    candidates.append({**base, "target_identifier": r["identifier"],
                                       "match_type": "field_reference", "confidence": 0.95})

            # Strategy 3 — name/trigram: NEVER auto-confirmed. Emit as pending ONLY when
            # corroborated by a shared type or category. The corroboration is pushed into
            # SQL so similarity() runs on the few matching rows, not the whole collection
            # (and we skip entirely when the source has no type/category to corroborate on).
            if not exact and src_name and len(src_name) >= 4 and (src_type or src_cat):
                _sentinel = "__NAS_AI_NO_TYPE_OR_CATEGORY_MATCH__"

                similar = fetchall("""
                    SELECT DISTINCT
                        payload->>'identifier' AS identifier,
                        similarity(payload->>'primary_name', %s) AS sim
                    FROM chunks
                    WHERE collection_name = %s
                      AND payload->>'primary_name' IS NOT NULL
                      AND (payload->>'type' = %s OR payload->>'category' = %s)
                      AND similarity(payload->>'primary_name', %s) > 0.4
                    ORDER BY sim DESC LIMIT 3
                """, (src_name, target, src_type or _sentinel, src_cat or _sentinel, src_name))
                for s in similar:
                    candidates.append({**base, "target_identifier": s["identifier"],
                                       "match_type": "name_similarity",
                                       "confidence": round(min(float(s["sim"]), 0.85), 3)})

            # Mentions: intentionally NOT emitted as cross-links (mostly-rejected noise).
            # Reintroduce later as a separate, opt-in "mentions" hint if needed.

    return candidates