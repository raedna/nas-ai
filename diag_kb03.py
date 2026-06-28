"""
diag_kb03.py — why does 'moore PROD weekend checks' miss the 21R2 Weekend Restart
article? Shows the live ranking. Read-only.
Run:  python3 diag_kb03.py
"""
import sys
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.db import fetchall
from core.retrieval.semantic import semantic_search

Q = "steps for the moore PROD weekend checks"

print("=== Articles whose title contains '21R2' ===")
for r in fetchall("""SELECT DISTINCT payload->>'primary_name' AS n
                     FROM chunks WHERE collection_name='kb_docs'
                       AND payload->>'primary_name' ILIKE %s""", ('%21R2%',)):
    print(f"  {r['n']}")

print(f"\n=== Top-8 semantic results for {Q!r} ===")
pts = semantic_search("kb_docs", Q, limit=8)
for i, p in enumerate(pts, 1):
    pl = p.payload or {}
    name = pl.get("primary_name") or pl.get("identifier")
    ci = pl.get("chunk_index")
    print(f"  {i}. {str(name)[:70]}" + (f"  [chunk {ci}]" if ci else ""))

print("\n=== Where does the 21R2 Weekend Restart article rank? (top-25) ===")
pts25 = semantic_search("kb_docs", Q, limit=25)
found = [(i, (p.payload or {}).get("primary_name"))
         for i, p in enumerate(pts25, 1)
         if "21R2 Weekend" in str((p.payload or {}).get("primary_name") or "")
         or "Weekend Restart" in str((p.payload or {}).get("primary_name") or "")]
print("  ranks:", found or "NOT in top-25")
