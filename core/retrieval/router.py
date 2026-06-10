"""
core/retrieval/router.py
=========================
Query routing entry point for the retrieval package.

Replaces the public surface of query_router.py:
  run_query_with_method(collection, question, limit) -> dict
  route_query(collection, question, mode, limit) -> str

All sub-functions delegate to the specialised retrieval modules:
  structured.py   — namespace / identifier / name lookups
  lexical.py      — BM25 and lexical searches
  semantic.py     — vector similarity search
  crosslink.py    — relationships, enum lookups, payload merging
  discovery.py    — count / list queries
  reranker.py     — scoring and reranking
  answer.py       — answer synthesis

No hardcoding, no direct database access (all DB calls go through
db_retrieval.py via the sub-modules).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from core.query_helpers import (
    infer_doc_type,
    normalize_simple_text,
    load_doc_query_hints,
)
from core.field_map_loader import load_field_maps
from core.structured_query_planner import plan_structured_query
from core.structured_plan_executor import execute_structured_plan
from core.local_llm_client import load_nlp_config

# Retrieval sub-modules
from core.retrieval.structured import (
    extract_explicit_identifier_namespace,
    extract_explicit_identifier,
    namespace_lookup,
    entity_row_exact_title_match,
    structured_points_by_name_in_question,
    structured_points_by_primary_name,
    relationship_lookup,
)
from core.retrieval.lexical import (
    lexical_short_query_search,
    lexical_structured_search,
    lexical_chunk_search,
    lexical_entity_row_search,
)
from core.retrieval.semantic import semantic_search, filtered_semantic_search
from core.retrieval.crosslink import (
    fetch_points_by_identifier,
    fetch_points_by_identifier_namespace,
    fetch_structured_points_by_primary_name,
    fetch_structured_points_by_name_in_question,
    fetch_points_by_link_key,
    fetch_points_related_to_link_key,
    merge_payloads_for_identifier,
    expand_related_identifiers,
    reverse_lookup_by_enum_value,
    reverse_lookup_structured_by_requested_role,
    fetch_doc_chunks_by_source_file,
    build_fuller_doc_payload,
)
from core.retrieval.discovery import run_discovery_with_method, detect_ask_intent
from core.retrieval.reranker import rerank_points, dedupe_structured_results
from core.retrieval.answer import (
    synthesize_answer,
    build_answer,
    get_display_labels,
    get_source_label,
)

# ---------------------------------------------------------------------------
# Planner config helper
# ---------------------------------------------------------------------------

def _get_structured_planner_config() -> Dict:
    try:
        nlp_cfg = load_nlp_config()
        return nlp_cfg.get("structured_query_planner", {})
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Intent helpers (re-implemented without Qdrant / old client)
# ---------------------------------------------------------------------------

def detect_query_mode(question: str) -> Dict:
    """Choose between lexical_short and semantic based on question length/form."""
    q_norm = normalize_simple_text(question)
    words = [w for w in q_norm.split() if w]

    sentence_words = {
        "what", "how", "when", "where", "why",
        "can", "should", "do", "does", "is", "are",
        "could", "would", "will", "who",
    }

    is_sentence_like = any(w in sentence_words for w in words)

    if len(words) <= 2 and not is_sentence_like:
        return {
            "mode": "lexical_short",
            "reason": f"query is {len(words)} word(s) and not sentence-like",
        }

    return {
        "mode": "semantic",
        "reason": "query is sentence-like or longer than 2 words",
    }


def looks_like_relationship_query(question: str) -> bool:
    q = normalize_simple_text(question)
    hints = load_doc_query_hints()
    terms = hints.get("relationship_query_terms", [])
    for term in terms:
        term_norm = normalize_simple_text(term)
        if term_norm and re.search(rf"\b{re.escape(term_norm)}\b", q):
            return True
    return False


def looks_like_reverse_enum_query(question: str, collection: str = None) -> bool:
    # Guard 1: skip entirely if collection has no enums
    if collection:
        from core.retrieval.db_retrieval import collection_has_enums
        if not collection_has_enums(collection):
            return False

    q = normalize_simple_text(question)
    hints = load_doc_query_hints()
    terms = hints.get("enum_lookup_query_terms", [])

    # Guard 2: ambiguous terms only fire if a value indicator is also present
    ambiguous = {"contain", "contains", "has", "have"}
    value_indicators = {"value", "values", "allowed", "valid", "option", "options"}
    has_value_indicator = any(
        re.search(rf"\b{re.escape(v)}\b", q) for v in value_indicators
    )

    for term in terms:
        term_norm = normalize_simple_text(term)
        if not term_norm:
            continue
        if term_norm in ambiguous and not has_value_indicator:
            continue
        if re.search(rf"\b{re.escape(term_norm)}\b", q):
            return True
    return False


def _extract_reverse_lookup_candidate(question: str, field_maps: Dict) -> str:
    """Strip role keywords and noise to extract the value being looked up."""
    q_norm = normalize_simple_text(question)

    role_keywords = []
    for keywords in field_maps.values():
        role_keywords.extend(keywords)

    cleaned = q_norm
    for kw in sorted(role_keywords, key=len, reverse=True):
        kw_norm = normalize_simple_text(kw)
        if kw_norm:
            cleaned = cleaned.replace(kw_norm, " ")

    hints = load_doc_query_hints()
    noise = set(hints.get("discovery_noise_words", []))
    noise.update(hints.get("structured_namespace_terms", []))
    noise.update(hints.get("question_words", []))
    # Modal/auxiliary verbs that appear in reverse-enum questions
    noise.update({"can", "could", "would", "should", "may", "might", "do", "does", "did", "is", "are", "be"})

    words = [w for w in cleaned.split() if w and w not in noise]
    return " ".join(words).strip()


# ---------------------------------------------------------------------------
# Relationship answer builder
# ---------------------------------------------------------------------------

def _synthesize_relationship_answer(
    base_payload: Dict,
    related_points: List,
    collection_name: str,
) -> str:
    base_field = base_payload.get("identifier_field") or "identifier"
    base_id = base_payload.get("identifier")
    base_name = base_payload.get("primary_name")

    if base_id and base_name:
        header = f"{base_field} {base_id} ({base_name}) is related to:"
    elif base_id:
        header = f"{base_field} {base_id} is related to:"
    elif base_name:
        header = f"{base_name} is related to:"
    else:
        header = "Related items:"

    lines = [header]
    seen: set = set()

    for p in related_points or []:
        payload = p.payload or {}

        link_keys = payload.get("link_keys") or []
        key = "|".join(sorted(str(k) for k in link_keys)) or str(id(payload))
        if key in seen:
            continue
        seen.add(key)

        identifier_field = payload.get("identifier_field") or "identifier"
        identifier = payload.get("identifier")
        primary_name = payload.get("primary_name")
        description = payload.get("description")

        if identifier and primary_name:
            line = f"- {identifier_field} {identifier}: {primary_name}"
        elif identifier:
            line = f"- {identifier_field} {identifier}"
        elif primary_name:
            line = f"- {primary_name}"
        else:
            continue

        if description:
            line += f" — {description}"

        lines.append(line)

    if len(lines) == 1:
        lines.append("- No related items found.")

    return "\n".join(lines)


def _synthesize_reverse_enum_answer(matches: List[Dict], collection_name: str) -> str:
    lines = []

    for item in matches:
        payload = item.get("payload") or {}
        matched_enum = item.get("matched_enum") or {}

        identifier_field = payload.get("identifier_field") or "identifier"
        identifier = payload.get("identifier")
        primary_name = payload.get("primary_name")

        enum_value = matched_enum.get("enum_value")
        enum_name = matched_enum.get("enum_name")
        enum_desc = matched_enum.get("description")

        owner = ""
        if identifier and primary_name:
            owner = f"{identifier_field} {identifier} ({primary_name})"
        elif identifier:
            owner = f"{identifier_field} {identifier}"
        elif primary_name:
            owner = str(primary_name)

        enum_text = ""
        if enum_value and enum_name:
            enum_text = f"{enum_value}: {enum_name}"
        elif enum_value:
            enum_text = str(enum_value)
        elif enum_name:
            enum_text = str(enum_name)

        if enum_desc and enum_desc != enum_name:
            enum_text = f"{enum_text} — {enum_desc}" if enum_text else str(enum_desc)

        if owner and enum_text:
            lines.append(f"- {owner} has allowed value {enum_text}.")
        elif owner:
            lines.append(f"- {owner}")

    return "\n".join(lines).strip()


def _detect_requested_roles(question: str, field_maps: Dict) -> List[str]:
    """Return a list of role keys whose keywords appear in the question."""
    q = f" {normalize_simple_text(question)} "
    matched_roles = []
    for role, keywords in field_maps.items():
        for kw in keywords:
            kw_norm = normalize_simple_text(kw)
            if f" {kw_norm} " in q:
                matched_roles.append(role)
                break
    return matched_roles


# ---------------------------------------------------------------------------
# Candidate building — replaces build_candidate_points in query_router
# ---------------------------------------------------------------------------

class _DictPoint:
    """Lightweight wrapper so dict results from lexical_short_query_search
    behave like Point objects (have .payload and .score attributes)."""
    __slots__ = ("payload", "score", "id")

    def __init__(self, d: Dict):
        self.payload = d.get("payload") or {
            k: v for k, v in d.items()
            if k not in ("score",)
        }
        self.score = float(d.get("score") or 0.0)
        self.id = id(d)


def _normalise_to_points(items: List) -> List:
    """Ensure every item in a list has .payload and .score attributes."""
    if not items:
        return []
    if isinstance(items[0], dict):
        return [_DictPoint(d) for d in items]
    return items


def _build_candidate_points(collection: str, question: str, limit: int = 25) -> List:
    # Always try lexical first — BM25 is fast and exact matches score very high
    raw = lexical_short_query_search(collection, question, limit=limit)
    lex_points = _normalise_to_points(raw)

    # If top lexical result is a strong exact primary_name match, return immediately
    if lex_points:
        top = lex_points[0]
        top_score = float(getattr(top, "score", 0.0))
        if top_score >= 0.05:
            return lex_points

    # Otherwise merge lexical + semantic
    sem_points = semantic_search(collection, question, limit=limit)

    seen_ids: set = set()
    points = []
    for p in lex_points + _normalise_to_points(sem_points):
        pid = getattr(p, "id", None) or id(p)
        if pid not in seen_ids:
            seen_ids.add(pid)
            points.append(p)

    return points


# ---------------------------------------------------------------------------
# Main route_query — semantic/lexical fallback
# ---------------------------------------------------------------------------

def route_query(
    collection: str,
    question: str,
    mode: str = "best",
    limit: int = 25,
) -> str:
    """
    Low-level query runner: build candidates → rerank → synthesize answer.
    Called by run_query_with_method() for the semantic/lexical fallback path.
    Returns a string answer.
    """
    q = question.lower().strip()

    field_maps = load_field_maps()

    # Detect requested roles (field_maps role -> keyword mapping)
    roles = _detect_requested_roles(q, field_maps)

    # Build candidate points
    points = _build_candidate_points(collection, question, limit=limit)

    if not points:
        return "No answer found."

    # Rerank
    points = rerank_points(points, question)

    # Use the best point
    best = points[0]
    payload = best.payload or {}
    payload["_question"] = question

    # For chunked document payloads, enrich by merging nearby/same-section chunks
    doc_type = infer_doc_type(payload)
    if doc_type not in ("structured", "entity_row"):
        payload = build_fuller_doc_payload(collection, payload) or payload

    return synthesize_answer(payload, roles, collection)


# ---------------------------------------------------------------------------
# Main entry point — run_query_with_method
# ---------------------------------------------------------------------------

def run_query_with_method(
    collection: str,
    question: str,
    mode: str = "best",
    limit: int = 25,
) -> Dict:
    """
    Primary query entry point.  Returns a dict with keys:
      method   str    — routing method used
      reason   str    — why that method was chosen
      result   str    — human-readable answer

    Optional extra keys depending on method:
      namespace_debug, plan, executor_debug_items, structured_plan_dry_run
    """
    intent = detect_ask_intent(question)

    # ------------------------------------------------------------------
    # 1. Relationship queries (before plain namespace lookup)
    # ------------------------------------------------------------------
    if looks_like_relationship_query(question):
        namespace, identifier = extract_explicit_identifier_namespace(question)

        if namespace and identifier:
            base_points = fetch_points_by_identifier_namespace(
                collection,
                identifier=identifier,
                identifier_namespace=namespace,
                limit=5,
            )

            if base_points:
                base_payload = base_points[0].payload or {}

                related_points: List = []

                for link_key in base_payload.get("link_keys") or []:
                    for related_key in base_payload.get("related_link_keys") or []:
                        related_points.extend(
                            fetch_points_by_link_key(collection, related_key, limit=10)
                        )
                    related_points.extend(
                        fetch_points_related_to_link_key(collection, link_key, limit=50)
                    )

                return {
                    "method": "relationship_lookup",
                    "reason": f"matched link_keys / related_link_keys for {namespace}:{identifier}",
                    "result": _synthesize_relationship_answer(
                        base_payload, related_points, collection
                    ),
                }

    # ------------------------------------------------------------------
    # 2. Plain namespace lookup  (e.g. "what is tag 22")
    # ------------------------------------------------------------------
    namespace, identifier = extract_explicit_identifier_namespace(question)
    if namespace and identifier:
        points = fetch_points_by_identifier_namespace(
            collection,
            identifier=identifier,
            identifier_namespace=namespace,
            limit=5,
        )

        if points:
            payload = points[0].payload or {}
            payload["_question"] = question
            return {
                "method": "structured_namespace_lookup",
                "reason": f"explicit namespace+identifier detected: {namespace}:{identifier}",
                "namespace_debug": {
                    "namespace": namespace,
                    "identifier": identifier,
                    "matched_identifier": payload.get("identifier"),
                    "matched_identifier_field": payload.get("identifier_field"),
                    "matched_identifier_namespace": payload.get("identifier_namespace"),
                    "primary_name": payload.get("primary_name"),
                    "description": payload.get("description"),
                    "source_file": payload.get("source_file"),
                    "enum_values_count": len(payload.get("enum_values") or []),
                },
                "result": synthesize_answer(payload, [], collection),
            }

    # ------------------------------------------------------------------
    # 3. Reverse enum / value lookups  (e.g. "what tag can have value ISIN")
    # ------------------------------------------------------------------
    if looks_like_reverse_enum_query(question, collection):
        name_points = fetch_structured_points_by_name_in_question(
            collection, question, limit=5
        )

        if name_points:
            payload = name_points[0].payload or {}
            return {
                "method": "structured_primary_name_lookup",
                "reason": "matched structured primary_name/alias in question",
                "result": synthesize_answer(payload, [], collection),
            }

        field_maps = load_field_maps()
        candidate = _extract_reverse_lookup_candidate(question, field_maps)

        if candidate:
            enum_matches = reverse_lookup_by_enum_value(
                collection, candidate, limit=10
            )

            if enum_matches:
                return {
                    "method": "reverse_enum_lookup",
                    "reason": f"matched normalized enum value/name/description: {candidate}",
                    "result": _synthesize_reverse_enum_answer(enum_matches, collection),
                }

    # ------------------------------------------------------------------
    # 4. Structured query planner  (NLP-driven structured lookup)
    # ------------------------------------------------------------------
    structured_plan = None

    planner_cfg = _get_structured_planner_config()
    planner_enabled = bool(planner_cfg.get("enabled", True))
    planner_execute = bool(planner_cfg.get("execute", False))
    planner_dry_run = bool(planner_cfg.get("dry_run", True))
    planner_min_confidence = float(planner_cfg.get("min_confidence", 0.7))

    if planner_enabled:
        structured_plan = plan_structured_query(question, dry_run=planner_dry_run)

        if (
            structured_plan.get("enabled")
            and planner_execute
            and not structured_plan.get("dry_run", True)
            and structured_plan.get("confidence", 0) >= planner_min_confidence
        ):
            structured_result = execute_structured_plan(collection, structured_plan)

            if structured_result.get("matched"):
                return {
                    "method": "structured_query_plan",
                    "reason": structured_plan.get("reason"),
                    "plan": structured_plan,
                    "executor_debug_items": structured_result.get("executor_debug_items", []),
                    "result": structured_result.get("answer"),
                }

    # ------------------------------------------------------------------
    # 5. Discovery fallback  (count / list queries)
    # ------------------------------------------------------------------
    if intent["mode"] in ("discovery_count", "discovery_list"):
        return run_discovery_with_method(collection, question, limit=limit)

    # ------------------------------------------------------------------
    # 6. Normal semantic / lexical fallback
    # ------------------------------------------------------------------
    method_info = detect_query_mode(question)

    response: Dict = {
        "method": method_info["mode"],
        "reason": method_info["reason"],
        "result": route_query(collection, question, mode=mode, limit=limit),
    }

    if structured_plan:
        response["structured_plan_dry_run"] = structured_plan

    return response


# ---------------------------------------------------------------------------
# Debug entry point — mirrors debug_route_query in query_router.py
# ---------------------------------------------------------------------------

def debug_route_query(
    collection: str,
    question: str,
    limit: int = 25,
) -> Dict:
    """
    Like run_query_with_method but also returns the raw candidate points
    and per-point scores for debugging in the UI.
    """
    query_result = run_query_with_method(collection, question, limit=limit)

    # Build candidate set for debug display
    points = _build_candidate_points(collection, question, limit=limit)
    points = rerank_points(points, question)

    debug_points = []
    for p in points[:20]:
        payload = p.payload or {}
        debug_points.append({
            "id": getattr(p, "id", ""),
            "score": getattr(p, "score", 0.0),
            "primary_name": payload.get("primary_name"),
            "identifier": payload.get("identifier"),
            "doc_type": payload.get("doc_type"),
            "source_type": payload.get("source_type"),
            "description": str(payload.get("description") or "")[:200],
        })

    query_result["debug_points"] = debug_points
    return query_result


def explain_query_routing(collection: str, question: str) -> Dict:
    """Debug helper — show how a question would be routed without running it."""
    intent = detect_ask_intent(question)
    namespace, identifier = extract_explicit_identifier_namespace(question)

    enum_lookup_query = looks_like_reverse_enum_query(question, collection)
    enum_candidate = None
    if enum_lookup_query:
        field_maps = load_field_maps()
        enum_candidate = _extract_reverse_lookup_candidate(question, field_maps)

    return {
        "question": question,
        "intent_mode": intent.get("mode"),
        "intent_reason": intent.get("reason"),
        "relationship_query": looks_like_relationship_query(question),
        "enum_lookup_query": enum_lookup_query,
        "namespace": namespace,
        "identifier": identifier,
        "reverse_enum_candidate": enum_candidate,
    }


# ---------------------------------------------------------------------------
# Convenience re-exports for ui_app.py backward compatibility
# ---------------------------------------------------------------------------
# ui_app imports these names from query_router directly.
# After the cutover, it will import from here instead.

__all__ = [
    "run_query_with_method",
    "route_query",
    "debug_route_query",
    "explain_query_routing",
    "detect_query_mode",
    "get_display_labels",
    "get_source_label",
    "synthesize_answer",
    "build_answer",
    # score_point_shared re-exported from reranker
    "score_point_shared",
    # semantic_search re-exported from semantic
    "semantic_search",
    # entity row title fetch re-exported from structured
    "fetch_entity_row_by_title",
]

# Re-exports so ui_app can do: from core.retrieval.router import score_point_shared
from core.retrieval.reranker import score_point_shared  # noqa: E402
from core.retrieval.semantic import semantic_search  # noqa: E402
from core.retrieval.structured import entity_row_by_title as fetch_entity_row_by_title  # noqa: E402