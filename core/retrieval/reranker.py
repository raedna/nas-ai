"""
core/retrieval/reranker.py
===========================
Scoring, deduplication, and reranking of candidate points.

Extracted verbatim from query_router.py — no logic changes, no hardcoding.
All helper functions that were inline in query_router are consolidated here.

Consumers:
  - answer.py  (calls rerank_points before synthesizing)
  - router.py  (calls rerank_points after building candidates)
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

from core.query_helpers import (
    infer_doc_type,
    normalize_simple_text,
    expand_terms_with_synonyms,
    load_doc_query_hints,
)
from core.retrieval_debug import (
    extract_negative_terms,
    remove_negative_terms_from_question,
    contains_negative_term,
)
from core.field_map_loader import load_field_maps
from pathlib import Path
import json

_QUERY_TERMS_PATH = Path(__file__).resolve().parents[2] / "config" / "query_terms.json"
_query_terms_cache: Dict | None = None


def load_query_terms() -> Dict:
    global _query_terms_cache
    if _query_terms_cache is None:
        try:
            with open(_QUERY_TERMS_PATH) as f:
                _query_terms_cache = json.load(f)
        except Exception:
            _query_terms_cache = {}
    return _query_terms_cache


# ---------------------------------------------------------------------------
# Payload classification helpers
# ---------------------------------------------------------------------------

def is_document_like_payload(payload: Dict) -> bool:
    """
    Returns True if the payload represents a chunked document rather than
    a structured record or entity row.  Used to cap reranking adjustments
    so semantic ordering is only nudged, not overridden.
    """
    payload = payload or {}

    doc_type = str(payload.get("doc_type") or "").lower()
    source_type = str(payload.get("source_type") or "").lower()
    point_type = str(payload.get("point_type") or payload.get("type") or "").lower()
    file_type = str(payload.get("file_type") or payload.get("filetype") or "").lower()

    if doc_type in {"procedural", "reference", "mixed", "narrative"}:
        return True
    if source_type in {"doc", "pdf"}:
        return True
    if point_type in {"doc_chunk", "pdf_chunk"}:
        return True
    if file_type in {"doc", "docx", "md", "txt", "rtf", "pdf"}:
        return True

    return False


# ---------------------------------------------------------------------------
# Deduplication helpers
# ---------------------------------------------------------------------------

def dedupe_entity_row_points(points: List) -> List:
    """Remove duplicate entity_row points by identifier or primary_name."""
    deduped = []
    seen: set = set()

    for p in points or []:
        payload = p.payload or {}

        identifier = str(payload.get("identifier") or "").strip()
        primary_name = normalize_simple_text(payload.get("primary_name") or "")

        if identifier:
            key = f"id:{identifier}"
        elif primary_name:
            key = f"name:{primary_name}"
        else:
            key = f"obj:{id(p)}"

        if key in seen:
            continue

        seen.add(key)
        deduped.append(p)

    return deduped


def dedupe_structured_results(items: List[Dict]) -> List[Dict]:
    """Remove duplicate structured result dicts by identifier or primary_name."""
    deduped = []
    seen: set = set()

    for item in items or []:
        identifier = str(item.get("identifier") or "").strip()
        primary_name = normalize_simple_text(item.get("primary_name") or "")
        description = normalize_simple_text(item.get("description") or "")

        if identifier:
            key = f"id:{identifier}"
        elif primary_name:
            key = f"name:{primary_name}"
        else:
            key = f"desc:{description[:120]}"

        if key in seen:
            continue

        seen.add(key)
        deduped.append(item)

    return deduped


# ---------------------------------------------------------------------------
# Core scoring function
# ---------------------------------------------------------------------------

def score_point_shared(p, question: str) -> float:
    """
    Score a single Point for relevance to question.
    Applied to structured, chunked, and image payloads.
    Entity_row has its own scoring path inside rerank_points().

    Returns a float score (higher = better match).
    """
    q = question.lower().strip()
    positive_q = remove_negative_terms_from_question(q)
    negative_terms = extract_negative_terms(q)

    payload = p.payload or {}
    base_score = float(getattr(p, "score", None) or 0.0)
    score = base_score

    name = str(payload.get("primary_name") or "").lower()
    desc = str(payload.get("description") or "").lower()
    doc_type = infer_doc_type(payload)

    words = [w for w in normalize_simple_text(positive_q).split() if w]

    # Generic lexical boosts (all doc types)
    for word in words:
        if word in name:
            score += 1.5

    for word in words:
        if word in desc:
            score += 0.5

    # -------------------------------------------------------------------
    # Structured-specific reranking
    # -------------------------------------------------------------------
    if doc_type == "structured":
        hints = load_doc_query_hints()
        stopwords = set(hints.get("stopwords", []))

        meaningful_words = [
            w for w in words
            if w not in stopwords and w not in {"tag", "field"}
        ]
        expanded_words = expand_terms_with_synonyms(meaningful_words)

        normalized_name = normalize_simple_text(name.replace("_", " "))
        normalized_desc = normalize_simple_text(desc)
        combined = f"{normalized_name} {normalized_desc}"

        exact_query = " ".join(meaningful_words).strip()

        if exact_query and normalized_name == exact_query:
            score += 100.0
        elif exact_query and normalized_name.startswith(exact_query + " "):
            score += 3.0
        elif exact_query and normalized_name.endswith(" " + exact_query):
            score += 3.0
        elif exact_query and exact_query in normalized_name:
            score += 1.5

        if exact_query and exact_query in normalized_desc:
            score += 2.0

        exact_name_hits = sum(1 for w in meaningful_words if w in normalized_name)
        exact_desc_hits = sum(1 for w in meaningful_words if w in normalized_desc)

        score += exact_name_hits * 2.5
        score += exact_desc_hits * 0.6

        if meaningful_words and all(w in normalized_name for w in meaningful_words):
            score += 6.0
        elif meaningful_words and all(w in combined for w in meaningful_words):
            score += 2.0

        expanded_name_hits = sum(1 for w in expanded_words if w in normalized_name)
        expanded_desc_hits = sum(1 for w in expanded_words if w in normalized_desc)

        score += expanded_name_hits * 0.8
        score += expanded_desc_hits * 0.2

    # -------------------------------------------------------------------
    # Entity_row: semantic-first with light lexical and negation support
    # -------------------------------------------------------------------
    if doc_type == "entity_row":
        hints = load_doc_query_hints()
        stopwords = set(hints.get("stopwords", []))

        meaningful_words = [w for w in words if w not in stopwords]
        normalized_name = normalize_simple_text(name)
        normalized_desc = normalize_simple_text(desc)

        exact_query = " ".join(meaningful_words).strip()

        if exact_query and normalized_name == exact_query:
            score += 6.0
        elif exact_query and exact_query in normalized_name:
            score += 2.0

        exact_name_hits = sum(1 for w in meaningful_words if w in normalized_name)
        score += exact_name_hits * 0.8

        if negative_terms:
            if contains_negative_term(name, negative_terms):
                score -= 50.0
            if contains_negative_term(desc, negative_terms):
                score -= 20.0

    # -------------------------------------------------------------------
    # Document-like payloads: cap reranking adjustment to ±nudge only
    # so semantic ordering is preserved
    # -------------------------------------------------------------------
    if is_document_like_payload(payload):
        adjustment = score - base_score
        if adjustment > 0.75:
            adjustment = 0.75
        elif adjustment < -2.0:
            adjustment = -2.0
        return base_score + adjustment

    return score


# ---------------------------------------------------------------------------
# Main reranking entry point
# ---------------------------------------------------------------------------

def rerank_points(points: List, question: str) -> List:
    """
    Rerank a list of Points by relevance to question.

    - entity_row: semantic-first with title boost + negation penalty
    - all other types: score_point_shared() sort
    """
    if not points:
        return []

    first_doc_type = infer_doc_type(points[0].payload or {})

    # -------------------------------------------------------------------
    # ENTITY_ROW path
    # -------------------------------------------------------------------
    if first_doc_type == "entity_row":
        points = dedupe_entity_row_points(points)

        q_norm = normalize_simple_text(question)
        positive_q = remove_negative_terms_from_question(q_norm)
        negative_terms = extract_negative_terms(q_norm)

        hints = load_doc_query_hints()
        stopwords = set(hints.get("stopwords", []))

        words = [w for w in positive_q.split() if w and w not in stopwords]
        exact_query = " ".join(words).strip()

        query_terms_cfg = load_query_terms()
        entity_row_ignore_terms = set(query_terms_cfg.get("entity_row_ignore_terms", []))

        positive_topic_terms = [
            w for w in words
            if w not in entity_row_ignore_terms
        ]

        scored = []

        for idx, p in enumerate(points):
            payload = p.payload or {}

            name = str(payload.get("primary_name") or "")
            desc = str(payload.get("description") or "")

            normalized_name = normalize_simple_text(name)
            normalized_desc = normalize_simple_text(desc)

            semantic_score = float(getattr(p, "score", None) or 0.0)
            title_boost = 0.0
            negative_penalty = 0.0

            # Hard exclude: title contains a negated term
            if negative_terms and contains_negative_term(name, negative_terms):
                negative_penalty += 80.0

            topic_hits = sum(
                1 for w in positive_topic_terms
                if w in normalized_name or w in normalized_desc
            )

            # Soft topic retention penalty if nothing relevant matches
            if positive_topic_terms and topic_hits == 0:
                negative_penalty += 120.0

            if exact_query and normalized_name == exact_query:
                title_boost += 100.0
            elif exact_query and exact_query in normalized_name:
                title_boost += 10.0

            # Softer penalty if negated term appears only in description
            if negative_terms and contains_negative_term(desc, negative_terms):
                negative_penalty += 80.0

            final_score = semantic_score + title_boost - negative_penalty + (topic_hits * 12.0)

            scored.append((final_score, idx, p))

        scored.sort(key=lambda x: (-x[0], x[1]))
        return [p for _, _, p in scored]

    # -------------------------------------------------------------------
    # Structured: trust BM25 ordering, apply only a small nudge
    # -------------------------------------------------------------------
    if first_doc_type == "structured":
        scored = []
        for p in points:
            bm25 = float(getattr(p, "score", 0.0))
            rerank = score_point_shared(p, question)
            adjustment = rerank - bm25
            # Allow large positive adjustment for exact matches (>50 rerank score)
            # but cap small adjustments to preserve BM25 ordering
            if rerank >= 50.0:
                final = rerank
            else:
                adjustment = max(min(adjustment, 2.0), -2.0)
                final = bm25 + adjustment
            scored.append((final, p))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [p for _, p in scored]

    # -------------------------------------------------------------------
    # All other types: shared scoring
    # -------------------------------------------------------------------
    return sorted(points, key=lambda p: score_point_shared(p, question), reverse=True)
