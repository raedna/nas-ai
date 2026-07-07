"""
PP-02 regression diagnosis — "whats teh fix tag for order quantity" returns
Tag 53 (Quantity) instead of Tag 38 (OrderQty). Was ✅ in the pre-overhaul eval.

Checks, in order:
1. What tag 38's chunk actually contains now (nlp_text — did re-ingest change it?)
2. BM25 ranking for 'order quantity' (suspect: camelCase 'OrderQty' is ONE
   tsvector token — matches neither 'order' nor 'quantity')
3. Vector ranking for the clean and typo'd question
4. Full router run for both phrasings

Usage: python diag_pp02.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import fetchall

print("=" * 70)
print("1. Tag 38 vs Tag 53 chunks (identifier, name, nlp_text head)")
for tag in ("38", "53"):
    rows = fetchall(
        """SELECT identifier, primary_name, source_file, LEFT(nlp_text, 300) AS head
           FROM chunks WHERE collection_name='xml_test' AND identifier=%s
           AND identifier_namespace='tag'""", (tag,))
    for r in rows:
        print(f"\n  tag {tag} [{r['source_file']}] {r['primary_name']}:")
        print(f"    {r['head']}")

print("\n" + "=" * 70)
print("2. BM25 (tsvector) ranking for 'order quantity' on xml_test")
rows = fetchall(
    """SELECT identifier, primary_name,
              ts_rank(to_tsvector('english', nlp_text),
                      plainto_tsquery('english', 'order quantity')) AS rank
       FROM chunks
       WHERE collection_name='xml_test'
         AND to_tsvector('english', nlp_text) @@ plainto_tsquery('english', 'order quantity')
       ORDER BY rank DESC LIMIT 10""", ())
if not rows:
    print("  NO BM25 matches at all for 'order quantity'")
for r in rows:
    print(f"  {r['rank']:.4f}  {r['identifier']}  {r['primary_name']}")
in_top = any(r["identifier"] == "38" for r in rows)
print(f"  -> tag 38 in BM25 top10: {in_top}")

print("\n" + "=" * 70)
print("3. Vector ranking (pgvector) for both phrasings")
from core.embedder import embed_text
for q in ("whats teh fix tag for order quantity", "what is the fix tag for order quantity"):
    v = embed_text(q)
    rows = fetchall(
        """SELECT identifier, primary_name, 1 - (embedding <=> %s::vector) AS sim
           FROM chunks WHERE collection_name='xml_test'
           ORDER BY embedding <=> %s::vector LIMIT 8""", (str(v), str(v)))
    print(f"\n  '{q}':")
    for r in rows:
        print(f"    {r['sim']:.4f}  {r['identifier']}  {r['primary_name']}")

print("\n" + "=" * 70)
print("4. Full router runs")
from core.retrieval.router import run_query_with_method
for q in ("whats teh fix tag for order quantity", "what is the fix tag for order quantity"):
    r = run_query_with_method("xml_test", q)
    print(f"\n  '{q}' -> method={r.get('method')}")
    print("  " + " ".join(str(r.get("result", ""))[:250].split()))
