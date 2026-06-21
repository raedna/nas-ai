from core.db import fetchall


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

    source_rows = fetchall("""
        SELECT DISTINCT
            payload->>'identifier' AS identifier,
            payload->>'primary_name' AS primary_name,
            payload->>'type' AS type_value
        FROM chunks
        WHERE collection_name = %s
        AND payload->>'identifier' IS NOT NULL
    """, (source_collection,))

    candidates = []

    for target in target_collections:
        for src in source_rows:
            src_id = (src.get("identifier") or "").strip()
            src_name = (src.get("primary_name") or "").strip()
            if not src_id:
                continue

            # Strategy 1: exact identifier match — skip plain short numeric IDs
            # (these are arbitrary sequence numbers in unrelated systems, e.g.
            # FIX tag "79" vs KB article "79", not real matches)
            if src_id.isdigit() and len(src_id) < 5:
                exact = []
            else:
                exact = fetchall("""
                    SELECT DISTINCT payload->>'identifier' AS identifier
                    FROM chunks
                    WHERE collection_name = %s
                    AND payload->>'identifier' = %s
                """, (target, src_id))

            for e in exact:
                candidates.append({
                    "source_collection": source_collection,
                    "source_identifier": src_id,
                    "target_collection": target,
                    "target_identifier": e["identifier"],
                    "match_type": "exact_identifier",
                    "confidence": 1.0,
                })

            # Strategy 2: name similarity (only if no exact match and name exists)
            if not exact and src_name:
                similar = fetchall("""
                    SELECT DISTINCT
                        payload->>'identifier' AS identifier,
                        similarity(payload->>'primary_name', %s) AS sim
                    FROM chunks
                    WHERE collection_name = %s
                    AND payload->>'primary_name' IS NOT NULL
                    AND similarity(payload->>'primary_name', %s) > 0.3
                    ORDER BY sim DESC
                    LIMIT 3
                """, (src_name, target, src_name))

                for s in similar:
                    candidates.append({
                        "source_collection": source_collection,
                        "source_identifier": src_id,
                        "target_collection": target,
                        "target_identifier": s["identifier"],
                        "match_type": "name_similarity",
                        "confidence": float(s["sim"]),
                    })

            # Strategy 3: mention matching — does source identifier or type
            # appear as a substring in target collection's text content?
            if not exact:
                mention_terms = [src_id]
                if src.get("type_value"):
                    mention_terms.append(src["type_value"])

                from core.query_helpers import load_doc_query_hints
                generic_terms = set(load_doc_query_hints().get("generic_terms", []))

                for term in mention_terms:
                    if not term or len(term) < 4:
                        continue
                    if term.isdigit():
                        continue
                    if term.strip().lower() in generic_terms:
                        continue
                    mentions = fetchall("""
                        SELECT DISTINCT
                            payload->>'identifier' AS identifier,
                            payload->>'primary_name' AS primary_name
                        FROM chunks
                        WHERE collection_name = %s
                        AND payload->>'description' ILIKE %s
                        LIMIT 5
                    """, (target, f"%{term}%"))

                    for m in mentions:
                        candidates.append({
                            "source_collection": source_collection,
                            "source_identifier": src_id,
                            "target_collection": target,
                            "target_identifier": m["identifier"] or m["primary_name"],
                            "match_type": "mention",
                            "confidence": 0.6,
                        })

    return candidates