"""
clear_kb_schema.py — deletes the cached kb_docs schema so the next (force) re-ingest
RE-INFERS it with the new `tags` role instead of reloading the stale one.
Run:  python3 clear_kb_schema.py     then force-reingest kb_docs from the UI.
"""
import sys
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.db import fetchall, execute

COLLECTION = "kb_docs"

rows = fetchall("SELECT source_file_stem FROM schemas WHERE collection_name=%s", (COLLECTION,))
print(f"cached schemas for {COLLECTION}: {[r['source_file_stem'] for r in rows]}")

execute("DELETE FROM schemas WHERE collection_name=%s", (COLLECTION,))
print(f"deleted {len(rows)} cached schema row(s).")
print("Now force-reingest kb_docs from the UI — it will re-infer the schema "
      "(entity_row -> LLM, tags role detected) and populate tags.")
