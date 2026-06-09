"""
Quick discovery scoring diagnostic.
Run from project root:
    python diag_discovery.py
"""
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.retrieval.db_retrieval import search_bm25
from core.retrieval.discovery import score_discovery_payload

question = "what tags contain security"
candidates = search_bm25('xml_test', 'security', limit=10)
print(f"BM25 candidates: {len(candidates)}")
for p in candidates:
    payload = p.payload or {}
    score = score_discovery_payload(payload, question)
    print(f"  score={score:.1f} name={payload.get('primary_name')} bm25={p.score:.3f}")
