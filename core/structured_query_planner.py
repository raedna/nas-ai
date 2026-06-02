import json
from typing import Any, Dict, List, Optional

from core.query_helpers import normalize_simple_text


def empty_plan(question: str, reason: str = "") -> Dict[str, Any]:
    return {
        "enabled": False,
        "confidence": 0.0,
        "reason": reason,
        "question": question,
        "intent": None,
        "target_type": None,
        "return_fields": [],
        "match": None,
        "filters": [],
        "limit": 10,
    }


def normalize_query_value(value: str) -> str:
    value = str(value or "").strip()
    value = value.replace("_", " ")
    value = normalize_simple_text(value)
    return value


def _strip_question_noise(question: str) -> str:
    q = normalize_query_value(question)

    noise_phrases = [
        "what is",
        "what are",
        "show me",
        "list",
        "find",
        "give me",
        "tell me",
        "the",
        "a",
        "an",
    ]

    for phrase in noise_phrases:
        if q.startswith(phrase + " "):
            q = q[len(phrase):].strip()

    return q


def _looks_structured_collection(collection_profile: Optional[Dict[str, Any]]) -> bool:
    if not collection_profile:
        return True

    doc_types = set(collection_profile.get("doc_types") or [])
    identifier_kinds = set(collection_profile.get("identifier_kinds") or [])

    if "structured" in doc_types:
        return True

    if "canonical" in identifier_kinds:
        return True

    return False


def plan_structured_query(
    question: str,
    collection_profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a normalized retrieval plan for structured/canonical payloads.

    V1 is deterministic and conservative.
    Later this function can call a local LLM to produce the same JSON contract.
    """

    if not _looks_structured_collection(collection_profile):
        return empty_plan(question, "collection does not look structured")

    q = normalize_query_value(question)
    stripped = _strip_question_noise(question)

    if not q:
        return empty_plan(question, "empty question")

    # -------------------------------------------------
    # LIST / CONTAINS style:
    # "what mnemonics contain price"
    # "what fields contain price"
    # "show fields with price"
    # -------------------------------------------------
    contains_markers = [
        " contain ",
        " contains ",
        " with ",
        " include ",
        " includes ",
    ]

    structured_targets = [
        "mnemonic",
        "mnemonics",
        "field",
        "fields",
        "tag",
        "tags",
    ]

    if any(t in q.split() for t in structured_targets):
        for marker in contains_markers:
            if marker in f" {q} ":
                value = q.split(marker.strip(), 1)[-1].strip()
                value = normalize_query_value(value)

                if value:
                    return {
                        "enabled": True,
                        "confidence": 0.85,
                        "reason": "structured list/contains query",
                        "question": question,
                        "intent": "list",
                        "target_type": "structured",
                        "return_fields": ["primary_name", "description", "identifier"],
                        "match": None,
                        "filters": [
                            {
                                "roles": ["primary_name", "description", "aliases"],
                                "operator": "contains",
                                "value": value,
                            }
                        ],
                        "limit": 20,
                    }

    # -------------------------------------------------
    # RETURN FIELD style:
    # "what is the mnemonic for ask price"
    # "what is the field name for ask price"
    # -------------------------------------------------
    return_field_markers = [
        ("mnemonic for", "primary_name"),
        ("field name for", "primary_name"),
        ("code for", "primary_name"),
        ("identifier for", "identifier"),
        ("id for", "identifier"),
    ]

    for marker, return_field in return_field_markers:
        if marker in q:
            value = q.split(marker, 1)[-1].strip()
            value = normalize_query_value(value)

            if value:
                return {
                    "enabled": True,
                    "confidence": 0.9,
                    "reason": f"structured return-field lookup: {return_field}",
                    "question": question,
                    "intent": "lookup",
                    "target_type": "structured",
                    "return_fields": [return_field, "description", "identifier"],
                    "match": {
                        "roles": ["description", "primary_name", "aliases"],
                        "operator": "semantic_or_contains",
                        "value": value,
                    },
                    "filters": [],
                    "limit": 1,
                }

    # -------------------------------------------------
    # Compact structured lookup:
    # "PX_ASK", "px ask", "tag 22"
    # This does not force answering; it only creates a plan.
    # -------------------------------------------------
    words = stripped.split()

    if 1 <= len(words) <= 4 and not any(w in {"how", "why", "when", "where"} for w in words):
        return {
            "enabled": True,
            "confidence": 0.7,
            "reason": "compact structured lookup",
            "question": question,
            "intent": "lookup",
            "target_type": "structured",
            "return_fields": ["primary_name", "description", "identifier"],
            "match": {
                "roles": ["primary_name", "description", "identifier", "aliases"],
                "operator": "semantic_or_contains",
                "value": stripped,
            },
            "filters": [],
            "limit": 5,
        }

    return empty_plan(question, "no structured plan matched")


def explain_plan(plan: Dict[str, Any]) -> str:
    return json.dumps(plan, indent=2, ensure_ascii=False)