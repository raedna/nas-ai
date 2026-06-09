"""
Diagnostic: check BM25 tsvector content in PostgreSQL.
Run from project root:
    python diag_bm25.py
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import fetchall, fetchone, fetchval

# 1. Check if tsvector column is populated
print("=== tsvector populated? ===")
result = fetchone("""
    SELECT id, primary_name,
           nlp_text_tsv IS NOT NULL as has_tsv,
           length(nlp_text::text) as nlp_len
    FROM chunks
    WHERE collection_name = 'xml_test'
    AND primary_name = 'ExecBroker'
    LIMIT 1
""")
print(result)

# 2. Try raw tsvector match
print("\n=== Raw tsvector match for 'exec broker' ===")
rows = fetchall("""
    SELECT id, primary_name,
           ts_rank(nlp_text_tsv, plainto_tsquery('english', 'exec broker')) as rank
    FROM chunks
    WHERE collection_name = 'xml_test'
    AND nlp_text_tsv @@ plainto_tsquery('english', 'exec broker')
    LIMIT 5
""")
print(f"Found: {len(rows)}")
for r in rows:
    print(f"  {r['primary_name']} rank={r['rank']}")

# 3. Check tsvector content for ExecBroker
print("\n=== tsvector content for ExecBroker ===")
result = fetchone("""
    SELECT primary_name,
           nlp_text_tsv::text as tsv_text,
           nlp_text
    FROM chunks
    WHERE collection_name = 'xml_test'
    AND primary_name = 'ExecBroker'
    LIMIT 1
""")
if result:
    print("primary_name:", result.get("primary_name"))
    print("nlp_text:", repr(str(result.get("nlp_text", ""))[:200]))
    print("tsv_text:", repr(str(result.get("tsv_text", ""))[:200]))

# 4. Try simple word search
print("\n=== Simple word search for 'broker' ===")
rows = fetchall("""
    SELECT primary_name
    FROM chunks
    WHERE collection_name = 'xml_test'
    AND nlp_text_tsv @@ to_tsquery('english', 'broker')
    LIMIT 5
""")
print(f"Found: {len(rows)}")
for r in rows:
    print(f"  {r['primary_name']}")
