"""
Show full descriptions of OrderPercent and RoundingDirection.
Run from project root:
    python diag_fulldesc.py
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.system_config import load_system_config
from qdrant_client import QdrantClient

cfg = load_system_config()
client = QdrantClient(url=cfg["qdrant_url"])

points, _ = client.scroll(
    collection_name="xml_test",
    limit=10000,
    with_payload=True,
    with_vectors=False,
)

targets = ["OrderPercent", "RoundingDirection"]

for p in points:
    payload = p.payload or {}
    if payload.get("primary_name") in targets:
        print(f"\n=== {payload.get('primary_name')} ===")
        print(f"FULL description: {repr(payload.get('description', ''))}")
        print(f"nlp text in payload: {repr(str(payload.get('text', ''))[:300])}")
