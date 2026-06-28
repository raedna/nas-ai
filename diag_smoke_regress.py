"""
diag_smoke_regress.py — diagnose the BBG/RECON smoke regressions. Read-only.
Run:  python3 diag_smoke_regress.py
"""
import sys
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.db import fetchall

print("=== doc_type per collection (recon should be 'structured') ===")
for c in ("recon_assist_file", "bbg_fields", "kb_docs"):
    rows = fetchall("""SELECT payload->>'doc_type' AS dt, COUNT(*) AS n
                       FROM chunks WHERE collection_name=%s GROUP BY dt""", (c,))
    print(f"  {c}: {[(r['dt'], r['n']) for r in rows]}")

print("\n=== recon description length (entity_row trips at median>=500 or max>=1500) ===")
r = fetchall("""SELECT AVG(LENGTH(payload->>'description'))::int AS avg,
                       MAX(LENGTH(payload->>'description')) AS max
                FROM chunks WHERE collection_name='recon_assist_file'""")
print(f"  avg={r[0]['avg']} max={r[0]['max']}")

print("\n=== gsact.txt record (RECON-01..05 target) ===")
g = fetchall("""SELECT payload->>'doc_type' AS dt, payload->>'identifier' AS ident,
                       payload->>'primary_name' AS name, payload->>'type' AS type,
                       LEFT(payload->>'description',150) AS desc,
                       payload->>'description_fields' AS dfields
                FROM chunks WHERE collection_name='recon_assist_file'
                  AND payload->>'identifier' ILIKE 'gsact.txt' LIMIT 1""")
if g:
    for k, v in g[0].items():
        print(f"  {k}: {str(v)[:200]}")
else:
    print("  gsact.txt NOT FOUND by identifier")

print("\n=== bbg nlp_text BM25 sanity: do 'ask'/'price' tokens exist? ===")
for term in ("ask", "price", "airlines", "goldman"):
    n = fetchall("""SELECT COUNT(*) AS n FROM chunks
                    WHERE collection_name IN ('bbg_fields','recon_assist_file')
                      AND nlp_text_tsv @@ plainto_tsquery('english', %s)""", (term,))
    print(f"  tsvector match '{term}': {n[0]['n']}")
