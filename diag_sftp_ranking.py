"""
diag_sftp_ranking.py — why does the SFTP query return the wrong obsidian note?
Read-only. Run:  python3 diag_sftp_ranking.py
"""
import sys
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.db import fetchall
from core.retrieval.semantic import semantic_search

Q = "how can I check recon files on SFTP"

print("=== obsidian notes mentioning sftp (title) ===")
for r in fetchall("""SELECT DISTINCT payload->>'primary_name' AS n
                     FROM chunks WHERE collection_name='obsidian'
                       AND payload->>'primary_name' ILIKE %s""", ('%sftp%',)):
    print(f"  {r['n']}")

print(f"\n=== top-10 semantic results for {Q!r} (obsidian) ===")
pts = semantic_search("obsidian", Q, limit=10)
for i, p in enumerate(pts, 1):
    pl = p.payload or {}
    ci = pl.get("chunk_index")
    print(f"  {i}. {str(pl.get('primary_name') or pl.get('identifier'))[:60]}"
          + (f"  [chunk {ci}]" if ci else "")
          + f"   score={getattr(p, 'score', 0):.4f}")

print("\n=== where does the sFTP-for-Activity note rank? (top-25) ===")
pts25 = semantic_search("obsidian", Q, limit=25)
hits = [(i, (p.payload or {}).get('primary_name'))
        for i, p in enumerate(pts25, 1)
        if "sftp" in str((p.payload or {}).get('primary_name') or "").lower()
        or "4.3.1" in str((p.payload or {}).get('primary_name') or "")]
print("  ranks:", hits or "NOT in top-25")

print("\n=== what does the full query path return? ===")
try:
    from core.retrieval.router import run_query_with_method
    r = run_query_with_method("obsidian", Q)
    print("  method:", r.get("method"))
    print("  answer[:200]:", str(r.get("result"))[:200].replace("\n", " "))
except Exception as e:
    print("  FAILED:", repr(e))
