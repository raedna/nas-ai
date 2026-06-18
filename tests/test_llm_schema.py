"""
tests/test_llm_schema.py
------------------------
Diagnostic script: runs LLM schema inference against live PostgreSQL data
for any collection, without modifying anything.

Usage:
    python tests/test_llm_schema.py                    # test all collections
    python tests/test_llm_schema.py bbg_fields         # test one collection
    python tests/test_llm_schema.py bbg_fields kb_docs # test specific ones

Output:
    For each collection, shows:
    - Current saved schema (from PostgreSQL)
    - LLM inferred schema (what LLM would pick fresh)
    - Diff: fields where LLM disagrees with current schema
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.db import fetchall
from core.schema_inference import llm_infer_schema, load_schema_from_db, list_schemas_from_db
from core.paths import CONFIG_DIR


def load_roles_config():
    from core.schema_inference import load_roles_config as _load
    return _load(CONFIG_DIR / "structured_roles.json")


def get_sample_rows(collection_name, limit=50):
    """Fetch sample rows from PostgreSQL chunks for a collection."""
    rows = fetchall("""
        SELECT payload
        FROM chunks
        WHERE collection_name = %s
        AND doc_type = 'structured'
        LIMIT %s
    """, (collection_name, limit))

    result = []
    for r in rows:
        payload = r.get("payload") or {}
        # Reconstruct a flat row from payload fields
        row = {}
        for k, v in payload.items():
            if k not in ("embedding", "nlp_text", "nlp_text_tsv", "link_keys",
                         "related_link_keys", "enum_values", "description_fields"):
                row[k] = v
        if row:
            result.append(row)
    return result


def get_collection_source_stems(collection_name):
    """Get source file stems for a collection from schemas table."""
    rows = fetchall("""
        SELECT source_file_stem FROM schemas
        WHERE collection_name = %s
    """, (collection_name,))
    return [r["source_file_stem"] for r in rows]


def diff_schemas(current, inferred):
    """Return dict of role -> (current_value, inferred_value) where they differ."""
    all_roles = set(list(current.keys()) + list(inferred.keys()))
    diffs = {}
    for role in sorted(all_roles):
        c = sorted(current.get(role) or [])
        i = sorted(inferred.get(role) or [])
        if c != i:
            diffs[role] = {"current": c, "llm": i}
    return diffs


def test_collection(collection_name):
    print(f"\n{'='*60}")
    print(f"  Collection: {collection_name}")
    print(f"{'='*60}")

    # Get sample rows
    rows = get_sample_rows(collection_name)
    if not rows:
        print(f"  ⚠️  No structured chunks found — skipping")
        return

    print(f"  Sample rows: {len(rows)} fetched")

    # Load current schema
    stems = get_collection_source_stems(collection_name)
    if not stems:
        print(f"  ⚠️  No schema found in PostgreSQL")
        current_schema = {}
    else:
        stem = stems[0]
        current_schema = load_schema_from_db(collection_name, stem) or {}
        print(f"  Schema stem: {stem}")

    print(f"\n  Current schema:")
    for role, fields in sorted(current_schema.items()):
        if fields:
            print(f"    {role}: {fields}")

    # Run LLM inference
    print(f"\n  Running LLM inference...")
    roles_config = load_roles_config()
    llm_schema = llm_infer_schema(rows, roles_config)

    if llm_schema is None:
        print(f"  ❌ LLM inference failed (LLM unavailable or returned invalid result)")
        return

    print(f"\n  LLM inferred schema:")
    for role, fields in sorted(llm_schema.items()):
        if fields:
            print(f"    {role}: {fields}")

    # Show diff
    if current_schema:
        diffs = diff_schemas(current_schema, llm_schema)
        if diffs:
            print(f"\n  ⚠️  Differences (current vs LLM):")
            for role, d in diffs.items():
                print(f"    {role}:")
                print(f"      current: {d['current']}")
                print(f"      llm:     {d['llm']}")
        else:
            print(f"\n  ✅ LLM agrees with current schema — no differences")
    else:
        print(f"\n  ℹ️  No current schema to compare against")


def main():
    if len(sys.argv) > 1:
        collections = sys.argv[1:]
    else:
        # Test all collections that have schemas
        db_schemas = list_schemas_from_db()
        seen = set()
        collections = []
        for row in db_schemas:
            name = row["collection_name"]
            if name not in seen:
                seen.add(name)
                collections.append(name)

    print(f"NAS-AI LLM Schema Inference Diagnostic")
    print(f"Testing {len(collections)} collection(s): {collections}")

    for collection_name in collections:
        try:
            test_collection(collection_name)
        except Exception as e:
            print(f"\n  ❌ Error testing {collection_name}: {e}")

    print(f"\n{'='*60}")
    print(f"Done.")


if __name__ == "__main__":
    main()
