from typing import Any, Dict, List, Tuple

from qdrant_client import QdrantClient

from core.query_helpers import infer_doc_type, normalize_simple_text
from core.system_config import load_system_config
from core.query_helpers import expand_terms_with_synonyms


cfg = load_system_config()
client = QdrantClient(url=cfg["qdrant_url"])


def normalize_match_value(value: Any) -> str:
    value = str(value or "").strip()
    value = value.replace("_", " ")
    value = normalize_simple_text(value)
    return value

def compact_match_value(value: Any) -> str:
    text = normalize_match_value(value)
    return "".join(ch for ch in text if ch.isalnum())

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

def _score_payload_against_search_concept(
    payload: Dict[str, Any],
    search_concept: str,
    search_roles: List[str],
    preferred_identifier_namespace: str = None,
) -> float:
    concept = normalize_match_value(search_concept)
    concept_compact = compact_match_value(search_concept)

    if not concept:
        return 0.0

    concept_terms = [w for w in concept.split() if w]
    best_score = 0.0

    role_weights = {
        "primary_name": 1.3,
        "aliases": 1.2,
        "description": 1.0,
        "identifier": 0.7,
    }

    for role in search_roles or []:
        role_values = _payload_role_values(payload, role)

        for raw_role_value in role_values:
            role_text = normalize_match_value(raw_role_value)
            role_compact = compact_match_value(raw_role_value)

            if not role_text:
                continue

            score = 0.0

            if concept_compact and concept_compact == role_compact:
                score += 220.0
            elif concept_compact and concept_compact in role_compact:
                score += 120.0

            if concept == role_text:
                score += 150.0
            elif concept in role_text:
                score += 70.0

            term_hits = sum(1 for w in concept_terms if w in role_text)
            score += term_hits * 10.0

            if concept_terms and all(w in role_text for w in concept_terms):
                score += 30.0

            score *= role_weights.get(role, 1.0)

            if score > best_score:
                best_score = score

    if preferred_identifier_namespace:
        payload_namespace = normalize_match_value(payload.get("identifier_namespace"))
        requested_namespace = normalize_match_value(preferred_identifier_namespace)

        if payload_namespace == requested_namespace:
            best_score += 25.0

    return best_score


def _is_structured_payload(payload: Dict[str, Any]) -> bool:
    if infer_doc_type(payload) == "structured":
        return True

    if str(payload.get("identifier_kind") or "").lower() == "canonical":
        return True

    return False

def expand_search_concept_variants(concept: str) -> List[str]:
    concept_norm = normalize_match_value(concept)
    if not concept_norm:
        return []

    words = [w for w in concept_norm.split() if w]
    variants = [concept_norm]

    # Build one-word-at-a-time phrase variants from synonyms.json
    for idx, word in enumerate(words):
        for syn in expand_terms_with_synonyms([word]) or []:
            syn_norm = normalize_match_value(syn)
            if not syn_norm:
                continue

            new_words = list(words)
            new_words[idx] = syn_norm
            variants.append(" ".join(new_words))

    # Add compact variant:
    # "exec broker" -> "execbroker"
    if len(words) >= 2:
        variants.append("".join(words))

    # Add compact variants for all generated phrases
    extra_compact = []
    for v in variants:
        v_words = [w for w in normalize_match_value(v).split() if w]
        if len(v_words) >= 2:
            extra_compact.append("".join(v_words))

    variants.extend(extra_compact)

    seen = set()
    clean = []
    for v in variants:
        v_norm = normalize_match_value(v)
        if v_norm and v_norm not in seen:
            seen.add(v_norm)
            clean.append(v_norm)

    return clean

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

    search_concept = plan.get("search_concept") or ""
    search_roles = plan.get("search_roles") or ["primary_name", "description", "aliases"]
    preferred_identifier_namespace = plan.get("preferred_identifier_namespace")
    direct_identifier = plan.get("direct_identifier")

    search_concepts = plan.get("search_concepts") or []

    if search_concept:
        search_concepts.append(search_concept)

    expanded_concepts = []

    for concept in search_concepts:
        expanded_concepts.extend(expand_search_concept_variants(concept))

    seen = set()
    search_concepts = []

    for concept in expanded_concepts:
        concept_norm = normalize_match_value(concept)
        if concept_norm and concept_norm not in seen:
            seen.add(concept_norm)
            search_concepts.append(concept_norm)

    for p in points:
        payload = p.payload or {}

        if not _is_structured_payload(payload):
            continue

        score = 0.0

        if direct_identifier:
            payload_identifier = normalize_match_value(payload.get("identifier"))
            requested_identifier = normalize_match_value(direct_identifier)

            if payload_identifier == requested_identifier:
                score += 200.0
            else:
                continue

            if preferred_identifier_namespace:
                requested_namespace = normalize_match_value(preferred_identifier_namespace)

                payload_namespace = normalize_match_value(
                    payload.get("identifier_namespace")
                    or payload.get("identifier_field")
                )

                if payload_namespace != requested_namespace:
                    continue

                score += 50.0

        elif search_concept:
            concept_scores = []

            for concept in search_concepts or [search_concept]:
                concept_scores.append(
                    _score_payload_against_search_concept(
                        payload,
                        concept,
                        search_roles,
                        preferred_identifier_namespace=preferred_identifier_namespace,
                    )
                )

            score += max(concept_scores) if concept_scores else 0.0

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

    if "enum_values" in return_fields and items:
        return _format_enum_values_answer(items[0])

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

def _format_enum_values_answer(item: Dict[str, Any]) -> str:
    payload = item.get("payload") or {}

    identifier = payload.get("identifier")
    identifier_field = payload.get("identifier_field") or "identifier"
    primary_name = payload.get("primary_name")
    enum_values = payload.get("enum_values") or []

    if identifier and primary_name:
        header = f"Allowed values for {identifier_field} {identifier} ({primary_name}):"
    elif primary_name:
        header = f"Allowed values for {primary_name}:"
    elif identifier:
        header = f"Allowed values for {identifier_field} {identifier}:"
    else:
        header = "Allowed values:"

    if not enum_values:
        return header + "\nNo enumerated values found."

    lines = [header]

    for e in enum_values:
        if isinstance(e, dict):
            val = e.get("enum_value") or e.get("Value") or e.get("value")
            name = e.get("enum_name") or e.get("SymbolicName") or e.get("name")
            desc = e.get("description")

            if val and name and desc and desc != name:
                lines.append(f"- {val}: {name} — {desc}")
            elif val and name:
                lines.append(f"- {val}: {name}")
            elif val:
                lines.append(f"- {val}")
            elif name:
                lines.append(f"- {name}")
        else:
            lines.append(f"- {e}")

    return "\n".join(lines)