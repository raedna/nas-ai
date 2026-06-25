from core.db import execute, fetchall


def ensure_cross_links_table():
    try:
        execute("""
            CREATE TABLE IF NOT EXISTS cross_links (
                id SERIAL PRIMARY KEY,
                source_collection TEXT NOT NULL,
                source_identifier TEXT NOT NULL,
                target_collection TEXT NOT NULL,
                target_identifier TEXT NOT NULL,
                match_type TEXT NOT NULL,
                confidence FLOAT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE (source_collection, source_identifier, target_collection, target_identifier, match_type)
            )
        """)
        execute("""
            CREATE INDEX IF NOT EXISTS idx_cross_links_source
            ON cross_links (source_collection, source_identifier)
        """)
        execute("""
            CREATE INDEX IF NOT EXISTS idx_cross_links_target
            ON cross_links (target_collection, target_identifier)
        """)
    except Exception as e:
        print(f"[CROSS LINK DB] Could not create cross_links table: {e}")


def save_cross_link_candidates(candidates):
    """Save candidates to DB. Auto-confirm >0.9, pending_review 0.3-0.9, skip <0.3."""
    # Deduplicate by unique key, keeping highest confidence
    seen = {}
    for c in candidates:
        key = (c["source_collection"], c["source_identifier"],
               c["target_collection"], c["target_identifier"], c["match_type"])
        if key not in seen or c["confidence"] > seen[key]["confidence"]:
            seen[key] = c
    candidates = list(seen.values())

    saved = 0
    skipped = 0

    for c in candidates:
        confidence = c["confidence"]

        if confidence >= 0.9:
            status = "confirmed"
        elif confidence >= 0.3:
            status = "pending_review"
        else:
            skipped += 1
            continue

        try:
            execute("""
                INSERT INTO cross_links
                    (source_collection, source_identifier, target_collection,
                     target_identifier, match_type, confidence, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_collection, source_identifier, target_collection, target_identifier, match_type)
                DO UPDATE SET 
                    confidence = EXCLUDED.confidence, 
                    updated_at = NOW()
                    -- status intentionally not updated to preserve manual confirmations/rejections
            """, (
                c["source_collection"], c["source_identifier"],
                c["target_collection"], c["target_identifier"],
                c["match_type"], confidence, status
            ))
            saved += 1
        except Exception as e:
            print(f"[CROSS LINK DB] Save failed: {e}")

    return {"saved": saved, "skipped": skipped}


def get_cross_links_for_identifier(collection_name, identifier, status="confirmed"):
    """Lookup confirmed links for a given source identifier (used at query time)."""
    rows = fetchall("""
        SELECT target_collection, target_identifier, match_type, confidence
        FROM cross_links
        WHERE source_collection = %s AND source_identifier = %s AND status = %s
        ORDER BY confidence DESC
    """, (collection_name, identifier, status))
    return rows