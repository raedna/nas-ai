"""
Diagnostic: compare executor scores for ExecBroker vs RoundingDirection.
Run from project root with nas-ai env active:
    python diag_scoring.py
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.structured_plan_executor import (
    expand_search_concept_variants,
    _score_payload_against_search_concept,
)

exec_broker_payload = {
    "primary_name": "ExecBroker",
    "description": "Identifies executing / give-up broker. Standard NASD market-maker mnemonic is preferred.",
    "identifier_namespace": "tag",
    "identifier": "76",
}

rounding_payload = {
    "primary_name": "RoundingDirection",
    "description": (
        "Specifies which direction to round For CIV - indicates whether or not "
        "the quantity of shares/units is to be rounded and in which direction where "
        "CashOrdQty (52) or (for CIV only) OrderPercent (56) are specified on an order."
    ),
    "identifier_namespace": "tag",
    "identifier": "468",
}

roles = ["primary_name", "description", "aliases"]

for concept in ["exec broker", "execution broker"]:
    print(f"=== search_concept: {repr(concept)} ===")
    variants = expand_search_concept_variants(concept)
    print(f"variants: {variants}")
    s1 = _score_payload_against_search_concept(exec_broker_payload, concept, roles, "tag")
    s2 = _score_payload_against_search_concept(rounding_payload, concept, roles, "tag")
    print(f"ExecBroker score:        {s1}")
    print(f"RoundingDirection score: {s2}")
    print()
