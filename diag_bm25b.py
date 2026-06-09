"""
Quick diagnostic for search_bm25.
Run from project root:
    python diag_bm25b.py
"""
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.retrieval.db_retrieval import search_bm25

print("=== search_bm25 exec broker ===")
results = search_bm25('xml_test', 'exec broker', limit=5)
print(f"Found: {len(results)}")
for p in results:
    print(f"  score={p.score:.3f} name={p.payload.get('primary_name')} doc_type={p.payload.get('doc_type')}")

print("\n=== search_bm25 broker ===")
results = search_bm25('xml_test', 'broker', limit=5)
print(f"Found: {len(results)}")
for p in results:
    print(f"  score={p.score:.3f} name={p.payload.get('primary_name')}")
