from core.db import fetchall, execute
import json

def persist_related_titles_as_crosslinks(collection):
    """
    Persist obsidian/doc related_titles wikilinks to cross_links table
    as confirmed links (confidence 1.0, match_type 'wikilink').
    Called after ingestion for doc collections.
    """
    # Get all chunks with related_titles
    rows = fetchall("""
        SELECT DISTINCT
            payload->>'source_file' AS source_file,
            payload->>'related_titles' AS related_titles
        FROM chunks
        WHERE collection_name = %s
        AND payload->>'related_titles' IS NOT NULL
        AND payload->>'related_titles' != '[]'
    """, (collection,))

    saved = 0
    skipped = 0

    for row in rows:
        source_file = row['source_file']
        if not source_file:
            continue

        try:
            titles = json.loads(row['related_titles']) if isinstance(row['related_titles'], str) else row['related_titles']
        except Exception:
            continue

        for title in titles:
            if not title or not isinstance(title, str):
                continue

            # Skip image references (png, jpg etc.) and short titles
            if '.' in title and title.rsplit('.', 1)[-1].lower() in ('png', 'jpg', 'jpeg', 'gif', 'svg', 'pdf'):
                continue
            if len(title.strip()) < 3:
                continue

            # Check if target note exists in same collection
            target = fetchall("""
                SELECT DISTINCT payload->>'source_file' AS sf
                FROM chunks
                WHERE collection_name = %s
                AND (
                    payload->>'source_file' ILIKE %s
                    OR payload->>'note_title' ILIKE %s
                    OR payload->>'primary_name' ILIKE %s
                )
                LIMIT 1
            """, (collection, f"%{title}%", f"%{title}%", f"%{title}%"))

            if not target:
                skipped += 1
                continue

            target_file = target[0]['sf']
            if target_file == source_file:
                skipped += 1
                continue

            try:
                execute("""
                    INSERT INTO cross_links
                        (source_collection, source_identifier, target_collection,
                         target_identifier, match_type, confidence, status)
                    VALUES (%s, %s, %s, %s, 'wikilink', 1.0, 'confirmed')
                    ON CONFLICT (source_collection, source_identifier, target_collection,
                                 target_identifier, match_type)
                    DO UPDATE SET confidence = 1.0, updated_at = NOW()
                """, (collection, source_file, collection, target_file))
                saved += 1
            except Exception as e:
                print(f"[WIKILINK] Error saving {source_file} → {target_file}: {e}")
                skipped += 1

    print(f"[WIKILINK] {collection}: saved {saved}, skipped {skipped}")
    return saved