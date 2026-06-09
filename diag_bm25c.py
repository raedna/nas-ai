"""
Isolate search_bm25 SQL issue.
Run from project root:
    python diag_bm25c.py
"""
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import fetchall, fetchone

# Test 1: raw SQL directly
print("=== Test 1: raw SQL ===")
rows = fetchall("""
    SELECT id, primary_name, doc_type,
           ts_rank(nlp_text_tsv, plainto_tsquery('english', %s)) AS bm25_score
    FROM chunks
    WHERE collection_name = %s
    AND nlp_text_tsv @@ plainto_tsquery('english', %s)
    ORDER BY bm25_score DESC
    LIMIT 5
""", ('exec broker', 'xml_test', 'exec broker'))
print(f"Found: {len(rows)}")
for r in rows:
    print(f"  {r['primary_name']} score={r['bm25_score']}")

# Test 2: without payload column
print("\n=== Test 2: with payload column ===")
rows = fetchall("""
    SELECT id, primary_name, doc_type, payload,
           ts_rank(nlp_text_tsv, plainto_tsquery('english', %s)) AS bm25_score
    FROM chunks
    WHERE collection_name = %s
    AND nlp_text_tsv @@ plainto_tsquery('english', %s)
    ORDER BY bm25_score DESC
    LIMIT 5
""", ('exec broker', 'xml_test', 'exec broker'))
print(f"Found: {len(rows)}")
for r in rows:
    print(f"  {r['primary_name']} score={r['bm25_score']}")

# Test 3: check param order in db_retrieval.search_bm25
print("\n=== Test 3: replicate db_retrieval param order ===")
collection_name = 'xml_test'
query = 'exec broker'
conditions = [
    "collection_name = %s",
    "nlp_text_tsv @@ plainto_tsquery('english', %s)"
]
params = [collection_name, query]
# no doc_type, no source_type
params.extend([query, 200])
print(f"params: {params}")
print(f"param count: {len(params)}")

sql = f"""
    SELECT id, primary_name, doc_type, payload,
           ts_rank(nlp_text_tsv, plainto_tsquery('english', %s)) AS bm25_score
    FROM chunks
    WHERE {" AND ".join(conditions)}
    ORDER BY bm25_score DESC
    LIMIT %s
"""
print(f"placeholder count: {sql.count('%s')}")
try:
    rows = fetchall(sql, tuple(params))
    print(f"Found: {len(rows)}")
    for r in rows:
        print(f"  {r['primary_name']}")
except Exception as e:
    print(f"ERROR: {e}")
