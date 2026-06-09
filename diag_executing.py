"""
Diagnostic: show top 10 scores for 'executing broker'.
Run from project root:
    python diag_executing.py
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.system_config import load_system_config
from qdrant_client import QdrantClient
from core.structured_plan_executor import (
    expand_search_concept_variants,
    _score_payload_against_search_concept,
    _is_structured_payload,
)

cfg = load_system_config()
client = QdrantClient(url=cfg["qdrant_url"])

points, _ = client.scroll(
    collection_name="xml_test",
    limit=10000,
    with_payload=True,
    with_vectors=False,
)

concept = "executing broker"
variants = expand_search_concept_variants(concept)
print(f"Variants: {variants}\n")

roles = ["primary_name", "description", "aliases"]

scored = []
for p in points:
    payload = p.payload or {}
    if not _is_structured_payload(payload):
        continue
    best = max(
        _score_payload_against_search_concept(payload, v, roles, "tag")
        for v in variants
    )
    if best > 0:
        scored.append((best, payload))

scored.sort(key=lambda x: x[0], reverse=True)

print("Top 10 results for 'executing broker':")
for score, payload in scored[:10]:
    print(f"  score={score:.1f}  name={repr(payload.get('primary_name'))}  id={payload.get('identifier')}")