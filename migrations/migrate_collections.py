"""
Migration: collections.json -> PostgreSQL collections table
===========================================================
Reads config/collections.json and inserts each collection
into the PostgreSQL collections table.

Run from project root with nas-ai conda env active:
    python migrations/migrate_collections.py

Safe to run multiple times -- uses INSERT ... ON CONFLICT DO UPDATE
so existing rows are updated, not duplicated.
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not installed.")
    print("Run: pip install psycopg2-binary --break-system-packages")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config -- update if your connection details differ
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host": "100.123.16.57",    # NAS Tailscale or local IP
    "port": 5433,                # ai-pgvector container
    "dbname": "nasai",
    "user": "nasai",
    "password": "nasai2024",
}

COLLECTIONS_JSON = PROJECT_ROOT / "config" / "collections.json"

# ---------------------------------------------------------------------------

def connect():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"ERROR: Could not connect to PostgreSQL: {e}")
        print(f"       Check NAS is reachable and ai-pgvector container is running.")
        sys.exit(1)


def migrate():
    if not COLLECTIONS_JSON.exists():
        print(f"ERROR: {COLLECTIONS_JSON} not found.")
        sys.exit(1)

    with open(COLLECTIONS_JSON, "r", encoding="utf-8") as f:
        collections = json.load(f)

    print(f"Found {len(collections)} collections in {COLLECTIONS_JSON}")
    print(f"Connecting to PostgreSQL at {DB_CONFIG['host']}:{DB_CONFIG['port']}...")

    conn = connect()
    cur = conn.cursor()

    inserted = 0
    updated = 0

    for name, cfg in collections.items():
        filters = cfg.get("filters") or {}

        # Normalise -- some collections use different key names
        allowed_filetypes  = cfg.get("allowed_filetypes") or []
        allowed_extensions = cfg.get("allowed_extensions") or []
        exclude_dirs       = cfg.get("exclude_dirs") or cfg.get("exclude_folders") or []
        exclude_extensions = cfg.get("exclude_extensions") or []
        asset_search_roots = cfg.get("asset_search_roots") or []

        cur.execute("""
            INSERT INTO collections (
                name,
                path,
                source_label,
                notes,
                allowed_filetypes,
                allowed_extensions,
                exclude_dirs,
                exclude_extensions,
                asset_search_roots,
                filters,
                created_at,
                updated_at
            ) VALUES (
                %s, %s, %s, %s,
                %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
                %s::jsonb,
                NOW(), NOW()
            )
            ON CONFLICT (name) DO UPDATE SET
                path                = EXCLUDED.path,
                source_label        = EXCLUDED.source_label,
                notes               = EXCLUDED.notes,
                allowed_filetypes   = EXCLUDED.allowed_filetypes,
                allowed_extensions  = EXCLUDED.allowed_extensions,
                exclude_dirs        = EXCLUDED.exclude_dirs,
                exclude_extensions  = EXCLUDED.exclude_extensions,
                asset_search_roots  = EXCLUDED.asset_search_roots,
                filters             = EXCLUDED.filters,
                updated_at          = NOW()
        """, (
            name,
            cfg.get("path", ""),
            cfg.get("source_label", ""),
            cfg.get("notes", ""),
            json.dumps(allowed_filetypes),
            json.dumps(allowed_extensions),
            json.dumps(exclude_dirs),
            json.dumps(exclude_extensions),
            json.dumps(asset_search_roots),
            json.dumps(filters),
        ))

        if cur.rowcount == 1:
            inserted += 1
            print(f"  ✅ inserted: {name}")
        else:
            updated += 1
            print(f"  🔄 updated:  {name}")

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nDone. {inserted} inserted, {updated} updated.")
    print("Verify in pgAdmin: SELECT name, source_label, path FROM collections ORDER BY name;")


if __name__ == "__main__":
    migrate()
