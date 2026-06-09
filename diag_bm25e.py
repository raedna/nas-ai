"""
Test CTE approach for BM25 to avoid double tsquery issue.
Run from project root:
    python diag_bm25e.py
"""
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import fetchall

query = 'exec broker'
collection_name = 'xml_test'

# Approach 1: CTE with single tsquery
print("=== Approach 1: CTE ===")
rows = fetchall("""
    WITH q AS (SELECT plainto_tsquery('english', %s) AS tsq)
    SELECT id, primary_name, doc_type,
           ts_rank(nlp_text_tsv, q.tsq) AS bm25_score
    FROM chunks, q
    WHERE collection_name = %s
    AND nlp_text_tsv @@ q.tsq
    ORDER BY bm25_score DESC
    LIMIT 25
""", (query, collection_name))
print(f"Found: {len(rows)}")
for r in rows:
    print(f"  {r['primary_name']} score={r['bm25_score']:.4f}")

# Approach 2: lateral join
print("\n=== Approach 2: lateral ===")
rows = fetchall("""
    SELECT c.id, c.primary_name, c.doc_type,
           ts_rank(c.nlp_text_tsv, to_tsquery('english', %s)) AS bm25_score
    FROM chunks c
    WHERE c.collection_name = %s
    AND c.nlp_text_tsv @@ to_tsquery('english', %s)
    ORDER BY bm25_score DESC
    LIMIT 25
""", (query, collection_name, query))
print(f"Found: {len(rows)}")
for r in rows:
    print(f"  {r['primary_name']} score={r['bm25_score']:.4f}")
