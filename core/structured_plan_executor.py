from typing import Any, Dict, List, Tuple

from qdrant_client import QdrantClient

from core.query_helpers import infer_doc_type, normalize_simple_text
from core.system_config import load_system_config


cfg = load_system_config()
client = QdrantClient(url=cfg["qdrant_url"])


def normalize_match_value(value: Any) -> str:
    value = str(value or "").strip()
    value = value.replace("_", " ")
    value = normalize_simple_text(value)
    return value


def _payload_role_values(payload: Dict[str, Any], role: str) -> List[str]:
    value = payload.get(role)

    if value in [None, ""]:
        return []

    if isinstance(value, list):
        return [str(v) for v in value if v not in [None, ""]]

    return [str(value)]


def _normalized_role_text(payload: Dict[str, Any], roles: List[str]) -> str:
    values = []

    for role in roles or []:
        values.extend(_payload_role_values(payload, role))

    return normalize_match_value(" ".join(values))


def _score_payload_against_condition(
    payload: Dict[str, Any],
    condition: Dict[str, Any],
) -> float:
    roles = condition.get("roles") or []
    operator = condition.get("operator") or "contains"
    value = normalize_match_value(condition.get("value"))

    if not value:
        return 0.0

    value_terms = [w for w in value.split() if w]
    best_score = 0.0

    role_weights = {
        "description": 1.3,
        "primary_name": 1.0,
        "aliases": 1.0,
        "identifier": 0.8,
    }

    for role in roles:
        role_values = _payload_role_values(payload, role)

        for raw_role_value in role_values:
            role_text = normalize_match_value(raw_role_value)

            if not role_text:
                continue

            score = 0.0

            if operator in ["contains", "semantic_or_contains"]:
                if value == role_text:
                    score += 150.0
                elif role_text.startswith(value + " "):
                    score += 90.0
                elif role_text.endswith(" " + value):
                    score += 80.0
                elif value in role_text:
                    score += 45.0

                term_hits = sum(1 for w in value_terms if w in role_text)
                score += term_hits * 6.0

                if value_terms and all(w in role_text for w in value_terms):
                    score += 12.0

            elif operator == "equals":
                if value == role_text:
                    score += 150.0

            score *= role_weights.get(role, 1.0)

            if score > best_score:
                best_score = score

    return best_score


def _is_structured_payload(payload: Dict[str, Any]) -> bool:
    if infer_doc_type(payload) == "structured":
        return True

    if str(payload.get("identifier_kind") or "").lower() == "canonical":
        return True

    return False


def execute_structured_plan(
    collection: str,
    plan: Dict[str, Any],
) -> Dict[str, Any]:
    if not plan or not plan.get("enabled"):
        return {
            "matched": False,
            "reason": "plan disabled",
            "plan": plan,
            "items": [],
            "answer": "",
        }

    points, _ = client.scroll(
        collection_name=collection,
        limit=10000,
        with_payload=True,
        with_vectors=False,
    )

    scored: List[Tuple[float, Any]] = []

    intent = plan.get("intent")
    match = plan.get("match")
    filters = plan.get("filters") or []
    limit = int(plan.get("limit") or 10)

    for p in points:
        payload = p.payload or {}

        if not _is_structured_payload(payload):
            continue

        score = 0.0

        if match:
            score += _score_payload_against_condition(payload, match)

        for condition in filters:
            score += _score_payload_against_condition(payload, condition)

        if score > 0:
            scored.append((score, p))

    scored.sort(key=lambda x: x[0], reverse=True)

    selected = scored[:limit]
    items = []

    executor_debug_items = []

    for score, p in selected:
        payload = p.payload or {}

        item = {
            "score": score,
            "identifier": payload.get("identifier"),
            "identifier_field": payload.get("identifier_field"),
            "primary_name": payload.get("primary_name"),
            "description": payload.get("description"),
            "source_file": payload.get("source_file"),
            "payload": payload,
        }

        items.append(item)

        executor_debug_items.append(
            {
                "executor_score": score,
                "identifier": payload.get("identifier"),
                "identifier_field": payload.get("identifier_field"),
                "primary_name": payload.get("primary_name"),
                "description": payload.get("description"),
                "source_file": payload.get("source_file"),
                "doc_type": payload.get("doc_type"),
                "identifier_kind": payload.get("identifier_kind"),
            }
        )

    answer = synthesize_structured_plan_answer(plan, items)

    return {
        "matched": bool(items),
        "reason": "executed structured retrieval plan",
        "plan": plan,
        "items": items,
        "executor_debug_items": executor_debug_items,
        "answer": answer,
    }


def synthesize_structured_plan_answer(
    plan: Dict[str, Any],
    items: List[Dict[str, Any]],
) -> str:
    if not items:
        return "No matching structured records found."

    intent = plan.get("intent")
    return_fields = plan.get("return_fields") or [
        "primary_name",
        "description",
        "identifier",
    ]

    if intent == "lookup" and len(items) == 1:
        return _format_single_item(items[0], return_fields)

    lines = []

    if intent == "list":
        value = ""
        filters = plan.get("filters") or []
        if filters:
            value = filters[0].get("value") or ""

        if value:
            lines.append(f"Structured records matching '{value}':")
        else:
            lines.append("Structured records:")

    else:
        lines.append("Best structured matches:")

    for item in items:
        lines.append("- " + _format_inline_item(item, return_fields))

    return "\n".join(lines)


def _format_single_item(
    item: Dict[str, Any],
    return_fields: List[str],
) -> str:
    lines = []

    payload = item.get("payload") or {}

    for field in return_fields:
        value = payload.get(field)

        if value in [None, ""]:
            continue

        label = field.replace("_", " ").title()
        lines.append(f"{label}: {value}")

    if not lines:
        return _format_inline_item(item, return_fields)

    return "\n".join(lines)


def _format_inline_item(
    item: Dict[str, Any],
    return_fields: List[str],
) -> str:
    payload = item.get("payload") or {}

    primary_name = payload.get("primary_name")
    description = payload.get("description")
    identifier = payload.get("identifier")
    identifier_field = payload.get("identifier_field") or "identifier"

    parts = []

    if primary_name:
        parts.append(str(primary_name))

    if description:
        parts.append(str(description))

    text = " — ".join(parts) if parts else "Structured record"

    if identifier:
        text += f" ({identifier_field}: {identifier})"

    return text