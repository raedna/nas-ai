"""
Diagnostic: find ExecBroker in Qdrant and show exact payload fields.
Run from project root with nas-ai env active:
    python diag_payload.py
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

roles = ["primary_name", "description", "aliases"]

# Show top 10 scored results for each concept
for concept in ["exec broker", "execution broker"]:
    print(f"\n=== concept: {repr(concept)} ===")
    variants = expand_search_concept_variants(concept)

    scored = []
    for p in points:
        payload = p.payload or {}
        if not _is_structured_payload(payload):
            continue
        # score against each variant, take max
        scores = [
            _score_payload_against_search_concept(payload, v, roles, "tag")
            for v in variants
        ]
        best = max(scores) if scores else 0.0
        if best > 0:
            scored.append((best, payload))

    scored.sort(key=lambda x: x[0], reverse=True)

    print(f"Top 10 results:")
    for score, payload in scored[:10]:
        print(f"  score={score:.1f}  name={repr(payload.get('primary_name'))}  "
              f"id={payload.get('identifier')}  "
              f"ns={payload.get('identifier_namespace')}")

# Also show the raw ExecBroker payload
print("\n=== Raw ExecBroker payload ===")
for p in points:
    payload = p.payload or {}
    name = payload.get("primary_name", "")
    if "exec" in str(name).lower() or "broker" in str(name).lower():
        print(f"  primary_name: {repr(payload.get('primary_name'))}")
        print(f"  identifier:   {repr(payload.get('identifier'))}")
        print(f"  namespace:    {repr(payload.get('identifier_namespace'))}")
        print(f"  doc_type:     {repr(payload.get('doc_type'))}")
        print(f"  identifier_kind: {repr(payload.get('identifier_kind'))}")
        print(f"  description:  {repr(payload.get('description', '')[:80])}")
        print()