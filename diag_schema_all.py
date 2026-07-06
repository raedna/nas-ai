"""
Schema inference dry-run — ALL collections with a stored schema
================================================================
For every schema in the DB: rebuilds the original source rows from the
ingested chunks, runs the CURRENT llm_infer_schema on them, and diffs the
inferred roles against the stored schema. Dry-run — nothing is written.

Detects silent role drift caused by inference changes (SCHEMA-01 class bugs)
across every collection at once, not just recon.

Usage:
    python diag_schema_all.py            # all stored schemas
    python diag_schema_all.py recon_assist_file   # one collection
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import fetchall
from core.schema_inference import llm_infer_schema, load_roles_config
from core.paths import CONFIG_DIR

ROLES = ("identifier", "primary_name", "aliases", "reference_identifier",
         "type", "description", "tags", "enum_value", "enum_name")
COMPARE_ROLES = ("identifier", "primary_name", "aliases",
                 "reference_identifier", "type")


def rebuild_rows(collection: str, schema: dict, source_file_stem: str) -> list:
    """Reconstruct original source rows from chunk payloads, using the stored
    schema to map system columns back to their original column names.
    Chunks are filtered to THIS schema's source file (a collection can hold
    many source files, each with its own schema). Column insertion order is
    kept deterministic: identifier, primary_name, aliases, then the labeled
    description_fields in their stored order, then references/tags."""
    chunks = fetchall(
        "SELECT identifier, primary_name, source_file, payload FROM chunks "
        "WHERE collection_name = %s AND source_file ILIKE %s",
        (collection, f"{source_file_stem}%"))
    rows, seen_keys = [], set()
    for c in chunks:
        p = c["payload"] if isinstance(c["payload"], dict) else json.loads(c["payload"])
        row = {}
        # role-mapped system values first, in key order (identifier leftmost —
        # mirrors the source convention the tie-break relies on)
        for role, sys_val in (
            ("identifier", c["identifier"]),
            ("primary_name", c["primary_name"]),
        ):
            cols = schema.get(role) or []
            if cols and sys_val:
                row[str(cols[0])] = str(sys_val)
        for role, key in (("aliases", "aliases"),
                          ("reference_identifier", "reference_identifiers")):
            cols = schema.get(role) or []
            vals = p.get(key) or []
            for i, col in enumerate(cols):
                if str(col) not in row and i < len(vals) and vals[i]:
                    row[str(col)] = str(vals[i])
        df = p.get("description_fields") or {}
        if isinstance(df, dict):
            for k, v in df.items():
                row.setdefault(str(k), str(v))
        desc_cols = schema.get("description") or []
        if desc_cols and p.get("description"):
            row.setdefault(str(desc_cols[0]), str(p["description"]))
        tags_cols = schema.get("tags") or []
        if tags_cols and p.get("tags"):
            t = p["tags"]
            row.setdefault(str(tags_cols[0]), ", ".join(t) if isinstance(t, list) else str(t))

        if not any(str(v).strip() for v in row.values()):
            continue
        # dedupe multi-chunk records (same identifier split into windows)
        key = (c["identifier"], c["primary_name"], tuple(sorted(row.items()))[:3])
        if key in seen_keys:
            continue
        seen_keys.add(key)
        rows.append(row)
    return rows


def run_one(collection: str, source_file_stem: str, roles_config) -> bool:
    from core.schema_inference import load_schema_from_db
    stored = load_schema_from_db(collection, source_file_stem) or {}
    if not stored:
        print(f"  no stored schema — skipped")
        return True

    rows = rebuild_rows(collection, stored, source_file_stem)
    if len(rows) < 3:
        print(f"  only {len(rows)} reconstructable rows — skipped (not enough signal)")
        return True
    rebuilt_cols = {k for r in rows for k in r}
    print(f"  rebuilt {len(rows)} rows, {len(rebuilt_cols)} columns: {sorted(rebuilt_cols)}")

    inferred = llm_infer_schema(rows, roles_config)
    if not inferred:
        print("  llm_infer_schema returned None — LLM unavailable or fell back; NOT comparable")
        return False

    # Compare ONLY columns that were reconstructable — a stored role column
    # missing from the rebuilt rows was never shown to the LLM, so its absence
    # from the inferred schema proves nothing.
    ok = True
    for role in COMPARE_ROLES:
        s = set(stored.get(role) or []) & rebuilt_cols
        i = set(inferred.get(role) or []) & rebuilt_cols
        if s != i:
            ok = False
            print(f"  [DIFF ] {role}: stored={sorted(s)} inferred={sorted(i)}")
        elif s:
            print(f"  [MATCH] {role}: {sorted(s)}")
    skipped = [c for role in COMPARE_ROLES
               for c in (stored.get(role) or []) if c not in rebuilt_cols]
    if skipped:
        print(f"  (not reconstructable, excluded from comparison: {sorted(set(skipped))})")
    print(f"  => {'PASS' if ok else 'FAIL'}")
    return ok


def main():
    only = sys.argv[1] if len(sys.argv) > 1 else None
    roles_config = load_roles_config(CONFIG_DIR / "structured_roles.json")

    schemas = fetchall("SELECT collection_name, source_file_stem FROM schemas ORDER BY collection_name", ())
    if only:
        schemas = [s for s in schemas if s["collection_name"] == only]
    if not schemas:
        print("no stored schemas found" + (f" for '{only}'" if only else ""))
        return

    results = {}
    for s in schemas:
        name = s["collection_name"]
        print("=" * 70)
        print(f"{name}  (schema: {s['source_file_stem']})")
        try:
            results[name] = run_one(name, s["source_file_stem"], roles_config)
        except Exception as e:
            import traceback
            traceback.print_exc()
            results[name] = False

    print("=" * 70)
    print("SUMMARY:")
    for name, ok in sorted(results.items()):
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"{sum(results.values())}/{len(results)} collections: inference reproduces stored schema")


if __name__ == "__main__":
    main()
