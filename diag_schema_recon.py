"""
Schema inference dry-run — recon_assist_file
=============================================
Rebuilds the original source rows from the ingested chunks, runs the CURRENT
llm_infer_schema on them, and compares the inferred roles against the pinned
schema in the DB (ground truth). Nothing is written anywhere.

Purpose: confirm whether automatic inference reproduces the pinned schema on
its own, or whether the SCHEMA-01 filename pre-pass still forces filename
columns out of identifier/aliases candidacy.

Usage:
    python diag_schema_recon.py
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import fetchall
from core.schema_inference import llm_infer_schema, load_schema_from_db, load_roles_config
from core.paths import CONFIG_DIR

COLLECTION = "recon_assist_file"

# ---------------------------------------------------------------- ground truth
pinned = load_schema_from_db(COLLECTION, COLLECTION) or {}
if not pinned:
    # schema may be stored under the source file stem instead of collection name
    rows_s = fetchall("SELECT collection_name, source_file_stem FROM schemas", ())
    print("schemas in DB:", [(r["collection_name"], r["source_file_stem"]) for r in rows_s])
    cands = [r for r in rows_s if r["collection_name"] == COLLECTION]
    if cands:
        pinned = load_schema_from_db(COLLECTION, cands[0]["source_file_stem"]) or {}

print("=" * 70)
print("PINNED schema (ground truth):")
for role in ("identifier", "primary_name", "aliases", "reference_identifier",
             "type", "description", "tags", "enum_value", "enum_name", "other"):
    if pinned.get(role):
        print(f"  {role}: {pinned[role]}")

# ------------------------------------------------- rebuild original source rows
chunks = fetchall(
    "SELECT identifier, primary_name, payload FROM chunks WHERE collection_name = %s",
    (COLLECTION,))

id_col = (pinned.get("identifier") or [None])[0]
name_col = (pinned.get("primary_name") or [None])[0]
alias_col = (pinned.get("aliases") or [None])[0]

rows = []
for c in chunks:
    p = c["payload"] if isinstance(c["payload"], dict) else json.loads(c["payload"])
    row = {}
    if id_col:
        row[id_col] = c["identifier"] or ""
    if name_col:
        row[name_col] = c["primary_name"] or ""
    al = p.get("aliases") or []
    if alias_col:
        row[alias_col] = al[0] if al else ""
    # description_fields carries original column names for everything else
    df = p.get("description_fields") or {}
    if isinstance(df, dict):
        for k, v in df.items():
            row.setdefault(k, v)
    if any(str(v).strip() for v in row.values()):
        rows.append(row)

all_cols = []
for r in rows:
    for k in r:
        if k not in all_cols:
            all_cols.append(k)
print(f"\nRebuilt {len(rows)} source rows, {len(all_cols)} columns: {all_cols}")

# ---------------------------------------------------------------- run inference
roles_config = load_roles_config(CONFIG_DIR / "structured_roles.json")
print("\nRunning llm_infer_schema (current code, dry-run — nothing saved)...\n")
inferred = llm_infer_schema(rows, roles_config)

print("=" * 70)
if not inferred:
    print("llm_infer_schema returned None (LLM unavailable or fell back).")
    sys.exit(1)

print("INFERRED schema:")
for role in ("identifier", "primary_name", "aliases", "reference_identifier",
             "type", "description", "tags", "enum_value", "enum_name", "other"):
    if inferred.get(role):
        print(f"  {role}: {inferred[role]}")

# ---------------------------------------------------------------- compare
print("\n" + "=" * 70)
print("Comparison (pinned vs inferred):")
ok = True
for role in ("identifier", "primary_name", "aliases", "reference_identifier", "type"):
    p_set = set(pinned.get(role) or [])
    i_set = set(inferred.get(role) or [])
    status = "MATCH" if p_set == i_set else "DIFF "
    if p_set != i_set:
        ok = False
    print(f"  [{status}] {role}:")
    print(f"        pinned:   {sorted(p_set)}")
    print(f"        inferred: {sorted(i_set)}")

print("\nRESULT:", "PASS — inference reproduces the pinned schema unaided"
      if ok else "FAIL — inference still diverges from the pinned schema")
