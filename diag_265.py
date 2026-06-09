"""
Diagnostic: why do OrderPercent and RoundingDirection score 265 for 'executing broker'?
Run from project root:
    python diag_265.py
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
    normalize_match_value,
    compact_match_value,
)

cfg = load_system_config()
client = QdrantClient(url=cfg["qdrant_url"])

points, _ = client.scroll(
    collection_name="xml_test",
    limit=10000,
    with_payload=True,
    with_vectors=False,
)

targets = ["OrderPercent", "RoundingDirection", "ExecBroker"]
concept = "executing broker"
variants = expand_search_concept_variants(concept)
roles = ["primary_name", "description", "aliases"]

for p in points:
    payload = p.payload or {}
    name = payload.get("primary_name", "")
    if name not in targets:
        continue

    print(f"\n=== {name} (id={payload.get('identifier')}) ===")
    print(f"description: {repr(payload.get('description', '')[:150])}")

    for role in roles:
        val = payload.get(role)
        if not val:
            continue
        role_text = normalize_match_value(str(val))
        role_compact = compact_match_value(str(val))
        for v in variants:
            v_norm = normalize_match_value(v)
            v_compact = compact_match_value(v)
            if v_compact in role_compact or v_norm in role_text:
                print(f"  MATCH role={role} variant={repr(v)}")
                print(f"    role_text:    {repr(role_text[:100])}")
                print(f"    role_compact: {repr(role_compact[:100])}")
