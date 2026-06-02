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

def explain_plan(plan: Dict[str, Any]) -> str:
    return json.dumps(plan, indent=2, ensure_ascii=False)