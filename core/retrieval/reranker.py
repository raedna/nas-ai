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


# ---------------------------------------------------------------------------
# BGE Cross-Encoder Reranker
# Uses BAAI/bge-reranker-large for precise (query, passage) scoring
# Much better than MiniLM for domain-specific content
# ---------------------------------------------------------------------------

_bge_reranker_cache = None

def get_bge_reranker():
    """Load and cache BGE reranker model."""
    global _bge_reranker_cache
    if _bge_reranker_cache is not None:
        return _bge_reranker_cache
    try:
        from sentence_transformers import CrossEncoder
        from core.system_config import load_system_config
        cfg = load_system_config()
        model_name = cfg.get("bge_reranker", {}).get(
            "model", "BAAI/bge-reranker-large"
        )
        _bge_reranker_cache = CrossEncoder(model_name)
        return _bge_reranker_cache
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"BGE reranker not available: {e}")
        return None


def bge_rerank(points: List, question: str) -> List:
    """
    Rerank candidates using BGE cross-encoder.
    Better than MiniLM for domain-specific content.
    Falls back to original RRF order if unavailable.
    """
    if not points or len(points) == 1:
        return points

    model = get_bge_reranker()
    if model is None:
        return points

    try:
        pairs = []
        for p in points:
            payload = p.payload or {}
            title = payload.get("primary_name") or payload.get("identifier") or ""
            text = str(payload.get("text") or payload.get("description") or "")[:512]
            passage = f"{title}\n{text}".strip()
            pairs.append((question, passage))

        scores = [float(s) for s in model.predict(pairs)]

        # Stage-awareness (same rule as rerank_points, scaled to the cross-encoder's
        # score spread): a query naming a stage (e.g. PROD) must not surface a
        # conflicting-stage doc (e.g. DEV). Conflict is pushed below all non-conflicting
        # candidates; a stage match is nudged up. No-stage queries are unaffected.
        spread = (max(scores) - min(scores)) or 1.0
        adjusted = []
        for s, p in zip(scores, points):
            sign = _stage_sign(question, p.payload or {})
            adj = (0.5 * spread) if sign > 0 else (-1.5 * spread) if sign < 0 else 0.0
            adjusted.append((s + adj, p))
        scored = sorted(adjusted, key=lambda x: x[0], reverse=True)
        return [p for _, p in scored]

    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"BGE rerank failed: {e}")
        return points

def llm_rerank(points: List, question: str) -> List:
    """
    Rerank candidates using LLaMA 8B prompt reasoning.
    Uses domain-aware prompt to handle implicit naming conventions.
    Falls back to original RRF order if LLM unavailable.
    """
    if not points or len(points) == 1:
        return points

    try:
        from core.local_llm_client import call_local_llm_json

        # Build candidate list with full text
        candidates = []
        for i, p in enumerate(points):
            payload = p.payload or {}
            name = payload.get("primary_name") or payload.get("identifier") or ""
            identifier = payload.get("identifier") or payload.get("Moore file name") or ""
            text = str(payload.get("text") or payload.get("description") or "")[:300]
            header = f"{identifier} ({name})" if identifier and name else (identifier or name)
            candidates.append(f"[Document {i+1}]\nTitle: {header}\nContent: {text}")

        candidates_text = "\n\n".join(candidates)

        system_prompt = (
            "You are an expert system engineer routing support files and knowledge base articles. "
            "Your task is to find the single most relevant document for the user's query. "
            "Key domain rules: "
            "- '21R2' and 'PROD' refer to the production environment. "
            "- 'DEV' and '23R3' refer to the development environment. "
            "- 'moore weekend checks' refers to PROD weekend restart procedures, NOT DEV health checks. "
            "- Weekend checks and weekend restarts are PROD activities unless explicitly stated as DEV. "
            "- File extensions (.txt, .csv) are system file identifiers. "
            "Output strictly as JSON: {\"ranking\": [1, 3, 2]} from most to least relevant. No explanation."
        )

        user_prompt = (
            f"User Query: \"{question}\"\n\n"
            f"List of Candidate Documents:\n{candidates_text}\n\n"
            f"Re-rank from most to least relevant. "
            f"Return only {{\"ranking\": [list of document numbers]}}"
        )

        result = call_local_llm_json(system_prompt, user_prompt, temperature=0.0)
        print(f"LLM RERANK query: {question}")
        print(f"LLM RERANK candidates: {[p.payload.get('primary_name') for p in points]}")
        print(f"LLM RERANK result: {result}")

        if isinstance(result, dict) and "ranking" in result:
            ranking = result["ranking"]
            reranked = []
            seen = set()
            for rank in ranking:
                idx = int(rank) - 1
                if 0 <= idx < len(points) and idx not in seen:
                    reranked.append(points[idx])
                    seen.add(idx)
            # Add any missed points at the end
            for i, p in enumerate(points):
                if i not in seen:
                    reranked.append(p)

            # RETR-05 subject guard (deterministic): distinctive question
            # tokens = non-noise words that appear in FEW candidates
            # (pool-IDF) — plus filename/code anchors, always. If the LLM's
            # top pick contains NONE of them while another candidate does,
            # the subject-bearing candidate takes the lead. 'FRA' appears in
            # one candidate of 25; a ranking that puts Bad-Dates runbooks
            # above it mistook topic-word density for relevance.
            import re as _re_sg
            try:
                from core.query_helpers import load_doc_query_hints as _ldqh_sg
                _noise_sg = set()
                for _k_sg in ("discovery_noise_words", "question_words",
                              "stopwords"):
                    _noise_sg.update(_ldqh_sg().get(_k_sg, []))
            except Exception:
                _noise_sg = set()
            # Subjects are USER-SIGNALED entities only: filename/code
            # anchors and tokens the user CAPITALIZED ('FRA', 'SP2').
            # Pool-rarity alone crowned 'acting' a subject and demoted the
            # right article (PP-03) — rare is not the same as subject.
            _q_toks_sg = [w.lower() for w in _re_sg.findall(
                              r"\b[A-Z][A-Z0-9]{1,}\b", question)
                          if w.lower() not in _noise_sg]
            _anchors_sg = set(
                m.lower() for m in _re_sg.findall(
                    r"\b[a-zA-Z0-9_\-]+\.[a-zA-Z0-9]{2,5}\b", question))
            _hay_sg = []
            for _p_sg in reranked:
                _pl_sg = _p_sg.payload or {}
                _hay_sg.append((str(_pl_sg.get("primary_name") or "") + " "
                                + str(_pl_sg.get("description") or "")
                                + " " + str(_pl_sg.get("text") or "")).lower())
            _n_sg = len(reranked)
            _subjects_sg = set(_anchors_sg)
            for _t_sg in set(_q_toks_sg):
                _df_sg = sum(1 for h in _hay_sg if _t_sg in h)
                if 0 < _df_sg <= max(2, _n_sg // 8):
                    _subjects_sg.add(_t_sg)
            if _subjects_sg:
                def _score_sg(i):
                    return sum(1 for t in _subjects_sg if t in _hay_sg[i])
                _top_score = _score_sg(0)
                _best_i = max(range(_n_sg), key=lambda i: (_score_sg(i), -i))
                if _top_score == 0 and _score_sg(_best_i) > 0:
                    print(f"RERANK subject guard: promoting "
                          f"{(reranked[_best_i].payload or {}).get('primary_name')!r}"
                          f" (subjects {sorted(_subjects_sg)})")
                    reranked.insert(0, reranked.pop(_best_i))
            return reranked

    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"LLM rerank failed: {e}")

    return points

# ---------------------------------------------------------------------------
# Cross-encoder reranker using sentence-transformers MiniLM
# ---------------------------------------------------------------------------

_cross_encoder_cache = None


def get_cross_encoder():
    """Load and cache the cross-encoder model (loads once, stays in memory)."""
    global _cross_encoder_cache
    if _cross_encoder_cache is not None:
        return _cross_encoder_cache
    try:
        from sentence_transformers import CrossEncoder
        from core.system_config import load_system_config
        cfg = load_system_config()
        model_name = cfg.get("cross_encoder", {}).get(
            "model", "cross-encoder/ms-marco-MiniLM-L-6-v2"
        )
        _cross_encoder_cache = CrossEncoder(model_name)
        return _cross_encoder_cache
    except Exception as e:
        return None


def cross_encoder_rerank(points: List, question: str) -> List:
    """
    Rerank points using MiniLM cross-encoder.
    Falls back to original order if cross-encoder unavailable.
    """
    if not points:
        return points

    model = get_cross_encoder()
    if model is None:
        return points

    try:
        pairs = []
        for p in points:
            payload = p.payload or {}
            text = (
                str(payload.get("primary_name") or "") + " " +
                str(payload.get("description") or payload.get("text") or "")
            ).strip()[:512]
            pairs.append((question, text))

        scores = model.predict(pairs)
        scored = sorted(zip(scores, points), key=lambda x: x[0], reverse=True)
        return [p for _, p in scored]

    except Exception as e:
        return points

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

# Generic software environment/stage vocabulary (not collection-specific). A query
# that names a stage (e.g. PROD) should prefer same-stage docs and avoid other-stage
# docs (e.g. a DEV runbook). Conservative on purpose — "test"/"stage" are excluded
# because they're ordinary English words.
_STAGE_TERMS = {
    "prod": ("prod", "production"),
    "dev": ("dev", "development"),
    "qa": ("qa",),
    "uat": ("uat",),
    "preprod": ("preprod", "preproduction"),
}
_STAGE_MATCH_BOOST = 20.0
_STAGE_CONFLICT_PENALTY = 40.0


def _stages_in(text: str) -> set:
    """Canonical stages present as WHOLE words in text (normalizer splits on
    punctuation/space, so 'product' tokenizes to 'product' and never matches 'prod')."""
    toks = set(normalize_simple_text(text).split())
    return {canon for canon, variants in _STAGE_TERMS.items()
            if any(v in toks for v in variants)}


def _stage_sign(question: str, payload: dict) -> int:
    """+1 if a candidate shares the query's stage, -1 if it names a conflicting stage,
    0 when the query names no stage or the candidate names none."""
    q_stages = _stages_in(question)
    if not q_stages:
        return 0
    text = f"{payload.get('primary_name') or ''} {payload.get('description') or ''} {payload.get('text') or ''}"
    d_stages = _stages_in(text)
    if not d_stages:
        return 0
    return 1 if (q_stages & d_stages) else -1


def _stage_adjustment(question: str, payload: dict) -> float:
    """Fixed-magnitude stage nudge for the large-score entity_row/shared paths."""
    sign = _stage_sign(question, payload)
    if sign > 0:
        return _STAGE_MATCH_BOOST
    if sign < 0:
        return -_STAGE_CONFLICT_PENALTY
    return 0.0


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

            final_score = (semantic_score + title_boost - negative_penalty
                           + (topic_hits * 12.0) + _stage_adjustment(question, payload))

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
    # All other types: shared scoring (+ stage-awareness)
    # -------------------------------------------------------------------
    return sorted(
        points,
        key=lambda p: score_point_shared(p, question) + _stage_adjustment(question, p.payload or {}),
        reverse=True,
    )
