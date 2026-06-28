"""
diag_doctype_check.py — confirm why kb_docs didn't chunk. Read-only.
Run:  python3 diag_doctype_check.py
"""
import sys, json
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.db import fetchall

# 1. full saved schema (all roles)
print("=== saved kb_docs schema (all roles) ===")
rows = fetchall("SELECT schema_json FROM schemas WHERE collection_name='kb_docs'")
for r in rows:
    sch = r["schema_json"]
    if isinstance(sch, str):
        sch = json.loads(sch)
    for role, cols in sch.items():
        if cols:
            print(f"  {role:<22} {cols}")

# 2. doc_type actually stored on kb_docs chunks
print("\n=== kb_docs chunk doc_type distribution ===")
dt = fetchall("""SELECT payload->>'doc_type' AS dt, COUNT(*) AS n
                 FROM chunks WHERE collection_name='kb_docs' GROUP BY dt""")
for r in dt:
    print(f"  doc_type={r['dt']}  count={r['n']}")
