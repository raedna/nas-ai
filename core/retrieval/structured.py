"""
core/retrieval/structured.py
=============================
Structured and namespace lookup retrieval.
Handles:
  - Direct identifier lookup (what is tag 22)
  - Structured name lookup (what tag is exec broker)
  - Entity row exact title match
  - Relationship lookup via link keys

All database access goes through db_retrieval.py.
No direct database imports here.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from core.query_helpers import (
    infer_doc_type,
    normalize_simple_text,
    expand_terms_with_synonyms,
    load_doc_query_hints,
)
from core.retrieval.db_retrieval import (
    get_by_identifier,
    get_by_identifier_namespace,
    get_by_primary_name,
    get_by_primary_name_contains,
    get_by_link_key,
    get_by_related_link_key,
    scroll_collection,
    search_enum_values,
    Point,
)


# ---------------------------------------------------------------------------
# Namespace + identifier extraction
# ---------------------------------------------------------------------------
def extract_explicit_identifier_namespace(question: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract namespace and identifier from a question.
    Examples:
        'what is tag 22'      -> ('tag', '22')
        'field PR004'         -> ('field', 'PR004')
        'component 21'        -> ('component', '21')
    Returns (namespace, identifier) or (None, None).
    """
    import re
    q = normalize_simple_text(question)

    namespace_patterns = [
        (r"\btag\s+(\d+)\b", "tag"),
        (r"\bfield\s+([A-Z0-9_]+)\b", "field"),
        (r"\bcomponent\s+(\d+)\b", "component"),
        (r"\bcomponentid\s+(\d+)\b", "componentid"),
    ]

    for pattern, namespace in namespace_patterns:
        match = re.search(pattern, q, re.IGNORECASE)
        if match:
            return namespace, match.group(1)

    return None, None


def extract_explicit_identifier(question: str) -> Optional[str]:
    """Extract just the identifier value from a question."""
    _, identifier = extract_explicit_identifier_namespace(question)
    return identifier


# ---------------------------------------------------------------------------
# Direct namespace lookup
# Handles: 'what is tag 22', 'tag 76', 'field PR004'
# ---------------------------------------------------------------------------
def namespace_lookup(
    collection_name: str,
    namespace: str,
    identifier: str,
    limit: int = 5,
) -> List[Point]:
    """
    Direct deterministic lookup by namespace + identifier.
    Most reliable path -- used when question contains explicit tag/field number.
    """
    return get_by_identifier_namespace(
        collection_name=collection_name,
        identifier=identifier,
        identifier_namespace=namespace,
        limit=limit,
    )


# ---------------------------------------------------------------------------
# Entity row exact title match
# Handles: short questions that exactly match a record name
# ---------------------------------------------------------------------------
def entity_row_exact_title_match(
    collection_name: str,
    question: str,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """
    Find entity_row records whose primary_name exactly matches the question.
    Used for short exact-match queries.
    Returns list of payload dicts (not Points).
    """
    q_norm = normalize_simple_text(question)

    points = scroll_collection(
        collection_name=collection_name,
        doc_type="entity_row",
        limit=5000,
    )

    matches = []
    for p in points:
        payload = p.payload or {}
        primary_name = str(payload.get("primary_name") or "")
        if normalize_simple_text(primary_name) == q_norm:
            matches.append(payload)

    return matches[:limit]


def entity_row_by_title(
    collection_name: str,
    title: str,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """
    Find entity_row records by title (primary_name match).
    Returns list of payload dicts.
    """
    title_norm = normalize_simple_text(title)

    points = scroll_collection(
        collection_name=collection_name,
        doc_type="entity_row",
        limit=5000,
    )

    matches = []
    for p in points:
        payload = p.payload or {}
        primary_name = str(payload.get("primary_name") or "")
        if normalize_simple_text(primary_name) == title_norm:
            matches.append(payload)

    return matches[:limit]


# ---------------------------------------------------------------------------
# Structured name lookup
# Handles: 'what tag is exec broker', 'what is PX ASK'
# ---------------------------------------------------------------------------
def structured_points_by_name_in_question(
    collection_name: str,
    question: str,
    limit: int = 10,
) -> List[Point]:
    """
    Find structured records whose primary_name appears in the question.
    Used for direct name lookups where the field name is in the question.
    """
    q_norm = normalize_simple_text(question)
    q_compact = "".join(q_norm.split())

    points = scroll_collection(
        collection_name=collection_name,
        doc_type="structured",
        limit=5000,
    )

    matches = []
    for p in points:
        payload = p.payload or {}
        name = str(payload.get("primary_name") or "")
        if not name:
            continue

        name_norm = normalize_simple_text(name)
        name_compact = "".join(name_norm.split())

        if name_norm in q_norm or name_compact in q_compact:
            matches.append(p)

    return matches[:limit]


def structured_points_by_primary_name(
    collection_name: str,
    search_text: str,
    limit: int = 10,
) -> List[Point]:
    """
    Find structured records by primary_name similarity.
    """
    return get_by_primary_name_contains(
        collection_name=collection_name,
        search_text=search_text,
        doc_type="structured",
        limit=limit,
    )


# ---------------------------------------------------------------------------
# Reverse enum lookup
# Handles: 'what tag can have a value ISIN'
# ---------------------------------------------------------------------------
def reverse_enum_lookup(
    collection_name: str,
    search_text: str,
    limit: int = 10,
) -> List[Point]:
    """
    Find structured records that have a given enum value or name.
    Uses normalized enum_values table for fast indexed lookup.
    """
    return search_enum_values(
        collection_name=collection_name,
        search_text=search_text,
        limit=limit,
    )


def extract_reverse_lookup_candidate(
    question: str,
    field_maps: Dict[str, Any],
) -> Optional[str]:
    """
    Extract the enum value candidate from a question.
    Example: 'what tag can have a value ISIN' -> 'ISIN'
    """
    import re
    q = normalize_simple_text(question)

    # Explicit value patterns
    patterns = [
        r"\bvalue\s+([A-Z0-9_]+)\b",
        r"\bvalues?\s+(?:of\s+|for\s+)?([A-Z0-9_]+)\b",
        r"\bhave\s+(?:a\s+)?value\s+([A-Z0-9_]+)\b",
        r"\bcan\s+be\s+([A-Z0-9_]+)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, question, re.IGNORECASE)
        if match:
            return match.group(1)

    # Fall back to last capitalized word if short question
    words = question.strip().split()
    if words:
        last = words[-1].strip("?.,")
        if last.isupper() or (len(last) >= 3 and last[0].isupper()):
            return last

    return None


# ---------------------------------------------------------------------------
# Relationship lookup
# Handles: 'what is related to tag 22', 'what values belong to tag 22'
# ---------------------------------------------------------------------------
def relationship_lookup(
    collection_name: str,
    identifier: str,
    identifier_namespace: str,
    limit: int = 50,
) -> Tuple[List[Point], List[Point]]:
    """
    Fetch base record and all related records via link keys.
    Returns (base_points, related_points).
    """
    base_points = get_by_identifier_namespace(
        collection_name=collection_name,
        identifier=identifier,
        identifier_namespace=identifier_namespace,
        limit=5,
    )

    if not base_points:
        return [], []

    base_payload = base_points[0].payload or {}
    base_link_keys = base_payload.get("link_keys") or []
    related_link_keys = base_payload.get("related_link_keys") or []

    related_points = []

    for link_key in base_link_keys:
        related_points.extend(
            get_by_related_link_key(collection_name, link_key, limit=50)
        )

    for related_key in related_link_keys:
        related_points.extend(
            get_by_link_key(collection_name, related_key, limit=10)
        )

    # Deduplicate by chunk id
    seen = set()
    deduped = []
    for p in related_points:
        if p.id not in seen:
            seen.add(p.id)
            deduped.append(p)

    return base_points, deduped[:limit]


def looks_like_relationship_query(question: str) -> bool:
    """
    Detect if a question is asking about relationships between records.
    """
    q = normalize_simple_text(question)
    hints = load_doc_query_hints()
    relationship_terms = hints.get("relationship_query_terms", [
        "related to", "belongs to", "part of", "contains",
        "what values", "which values", "components of",
        "messages containing", "fields in",
    ])

    for term in relationship_terms:
        if normalize_simple_text(term) in q:
            return True

    return False
