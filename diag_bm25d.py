"""
Isolate exact difference between working and failing.
Run from project root:
    python diag_bm25d.py
"""
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import fetchall, fetchone, _get_pool
import psycopg2.extras

# Test: call fetchall exactly as db_retrieval.search_bm25 does
print("=== Exact db_retrieval.search_bm25 call ===")
collection_name = 'xml_test'
query = 'exec broker'

conditions = [
    "collection_name = %s",
    "nlp_text_tsv @@ plainto_tsquery('english', %s)"
]
params = [collection_name, query]
# no optional filters
params.extend([query, 25])

sql = f"""
    SELECT id, collection_name, source_file, source_type, doc_type,
           identifier, identifier_field, identifier_namespace, identifier_kind,
           primary_name, description, nlp_text, payload,
           ts_rank(nlp_text_tsv, plainto_tsquery('english', %s)) AS bm25_score
    FROM chunks
    WHERE {" AND ".join(conditions)}
    ORDER BY bm25_score DESC
    LIMIT %s
"""

print(f"SQL placeholders: {sql.count('%s')}")
print(f"Params: {params}")
print(f"Param count: {len(params)}")

try:
    rows = fetchall(sql, tuple(params))
    print(f"Found: {len(rows)}")
    for r in rows:
        print(f"  {r.get('primary_name')} score={r.get('bm25_score')}")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

# Now test with nlp_text removed to see if that column causes issue
print("\n=== Without nlp_text column ===")
sql2 = f"""
    SELECT id, primary_name, doc_type, payload,
           ts_rank(nlp_text_tsv, plainto_tsquery('english', %s)) AS bm25_score
    FROM chunks
    WHERE {" AND ".join(conditions)}
    ORDER BY bm25_score DESC
    LIMIT %s
"""
try:
    rows = fetchall(sql2, tuple(params))
    print(f"Found: {len(rows)}")
    for r in rows:
        print(f"  {r.get('primary_name')} score={r.get('bm25_score')}")
except Exception as e:
    print(f"ERROR: {e}")
