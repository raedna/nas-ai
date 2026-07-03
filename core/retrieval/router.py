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

from core.retrieval.db_retrieval import collection_has_enums


# ---------------------------------------------------------------------------
# CL-04: wikilink one-hop traversal helper
# ---------------------------------------------------------------------------
def _build_section_for_link(link):
    """Build a related-section dict for a confirmed cross-link target. Used to follow
    one hop of confirmed wikilinks from an initial cross-link target (CL-04)."""
    from core.retrieval.db_retrieval import get_by_identifier
    from core.db import fetchall as _f
    import json as _j

    tc, ti = link["target_collection"], link["target_identifier"]
    lp, via = None, None
    pts = get_by_identifier(tc, ti)
    if pts:
        lp, via = (pts[0].payload or {}), "identifier"
    if lp is None:
        # Fallback: target stored as source_file, then primary_name (CL-03 links to
        # records that have no canonical identifier, e.g. some RECON rows).
        for _field, _tag in (("source_file", "source_file"), ("primary_name", "primary_name")):
            rows = _f(f"SELECT payload FROM chunks WHERE collection_name=%s "
                      f"AND payload->>'{_field}'=%s LIMIT 1", (tc, ti))
            if rows:
                lp = rows[0]["payload"] if isinstance(rows[0]["payload"], dict) else _j.loads(rows[0]["payload"])
                via = _tag
                break
    if lp is None:
        return None
    sf = lp.get("source_file") or ti
    desc = ""
    if via in ("identifier", "source_file"):  # doc-like: concat first chunks for context
        full = _f("SELECT payload->>'text' AS text FROM chunks WHERE collection_name=%s "
                  "AND payload->>'source_file'=%s ORDER BY id LIMIT 3", (tc, sf))
        desc = "\n\n".join(r["text"] for r in full if r["text"]) if full else ""
    if not desc:                              # structured / primary_name match: own description
        desc = str(lp.get("description") or "")
    return {"title": lp.get("primary_name") or ti, "collection": tc,
            "source_file": sf, "confidence": link.get("confidence", 1.0), "preview": desc}


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


def _get_bm25_queries(question: str) -> List[str]:
    """
    Build BM25 query variants from a question.
    Strips noise words, expands synonyms into separate query variants.
    """
    from core.query_helpers import load_doc_query_hints, expand_terms_with_synonyms
    
    q_norm = normalize_simple_text(question)
    hints = load_doc_query_hints()
    noise = set(hints.get("discovery_noise_words", []))
    noise.update(hints.get("question_words", []))
    noise.update(hints.get("structured_namespace_terms", []))
    noise.update(hints.get("stopwords", []))

    content_words = [w for w in q_norm.split() if w and w not in noise]
    if not content_words:
        return [q_norm]

    queries: set = set()
    queries.add(" ".join(content_words))

    for i, word in enumerate(content_words):
        synonyms = expand_terms_with_synonyms([word])
        for syn in synonyms:
            if syn != word:
                variant = content_words[:i] + [syn] + content_words[i+1:]
                queries.add(" ".join(variant))

    return list(queries)


def _build_candidate_points(collection: str, question: str, limit: int = 25) -> List:
    """
    Build candidate set using PostgreSQL RRF (BM25 + pgvector).
    Replaces separate BM25 + semantic merge with a single SQL query.
    """
    from core.retrieval.db_retrieval import search_rrf
    from core.retrieval.semantic import embed_question

    bm25_queries = _get_bm25_queries(question)
    embedding = embed_question(question)

    # Detect namespace filter from question
    q_norm = normalize_simple_text(question)
    namespace_filter = None
    if re.search(r'\btag\b', q_norm):
        namespace_filter = 'tag'
    elif re.search(r'\bcomponent\b|\bcomponentid\b', q_norm):
        namespace_filter = 'componentid'

    return search_rrf(
        collection_name=collection,
        bm25_queries=bm25_queries,
        embedding=embedding,
        limit=limit,
        identifier_namespace=namespace_filter,
    )


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

    # Use the best point — check doc_type before reranking
    best = points[0]
    payload = best.payload or {}
    payload["_chunk_db_id"] = str(getattr(best, "id", "") or "")
    doc_type = infer_doc_type(payload)

   # DISABLED RERANKER USING MINILM AFTER CHANGING RERANKING PROCESS TO NOT RELY ON RRF SCORE

    ## Confidence check — if top RRF score is below threshold return top 5
    from core.system_config import load_system_config
    CONFIDENCE_THRESHOLD = load_system_config().get("retrieval_confidence_threshold", 0.105)
    if doc_type == "structured" and getattr(best, "score", 0.0) < CONFIDENCE_THRESHOLD:
        top5 = points[:5]
        lines = [f"No exact match found for '{question}'. Closest results:"]
        for p in top5:
            pl = p.payload or {}
            name = pl.get("primary_name") or ""
            identifier = pl.get("identifier") or ""
            desc = str(pl.get("description") or "")[:80]
            lines.append(f"- {identifier}: {name} — {desc}" if desc else f"- {identifier}: {name}")
        return "\n".join(lines)


   # Stage 2: LLM reranker for structured and entity_row
    # Replaces MiniLM and confidence threshold gate
    from core.system_config import load_system_config
    llm_rerank_cfg = load_system_config().get("llm_reranker", {})
    llm_rerank_enabled = llm_rerank_cfg.get("enabled", False)
    llm_rerank_doc_types = llm_rerank_cfg.get("apply_to_doc_types", ["structured", "entity_row"])
    llm_rerank_top_k = llm_rerank_cfg.get("top_k", 10)

    # Stage 2: BGE reranker (primary) with LLM reranker as fallback
    bge_cfg = load_system_config().get("bge_reranker", {})
    bge_enabled = bge_cfg.get("enabled", False)
    bge_doc_types = bge_cfg.get("apply_to_doc_types", ["entity_row"])
    bge_top_k = bge_cfg.get("top_k", 25)

    if bge_enabled and doc_type in bge_doc_types:
        from core.retrieval.reranker import bge_rerank
        points = bge_rerank(points[:bge_top_k], question)
        best = points[0]
        payload = best.payload or {}
    elif llm_rerank_enabled and doc_type in llm_rerank_doc_types:
        from core.retrieval.reranker import llm_rerank
        points = llm_rerank(points[:llm_rerank_top_k], question)
        best = points[0]
        payload = best.payload or {}

    payload["_question"] = question
    _best_chunk_db_id = str(getattr(best, "id", "") or "")
    ## For chunked document payloads, enrich by merging nearby/same-section chunks
    if doc_type not in ("structured", "entity_row"):
        payload = build_fuller_doc_payload(collection, payload) or payload
    payload["_chunk_db_id"] = _best_chunk_db_id
    route_query._last_answer_payload = payload
    return synthesize_answer(payload, roles, collection)


# ---------------------------------------------------------------------------
# Main entry point — run_query_with_method
# ---------------------------------------------------------------------------

_HIJACK_STOP = {"what", "whats", "is", "are", "the", "a", "an", "for", "of", "to",
                "in", "on", "do", "does", "i", "me", "my", "tell", "show", "about",
                "can", "how", "please", "this", "that", "and", "with", "there",
                "then", "so", "its", "it"}


def _record_covers_question(question: str, fname: str, payload: Dict) -> bool:
    """True if the question's focus terms are actually present in this record. Used to
    stop a detected filename from hijacking a question whose real target (e.g. 'sFTP')
    isn't in the record — in that case we fall through to normal retrieval."""
    toks = re.findall(r"[a-z0-9]+", (question or "").lower())
    fbase = (fname or "").lower()
    focus = [t for t in toks if t not in _HIJACK_STOP and t not in fbase and len(t) > 2]
    if not focus:
        return True  # pure "what is <file>" — the record itself is the subject
    parts = []
    df = payload.get("description_fields") or {}
    if isinstance(df, dict):
        parts += list(df.keys()) + [str(v) for v in df.values()]
    for k in ("primary_name", "type", "identifier", "description"):
        if payload.get(k):
            parts.append(str(payload[k]))
    al = payload.get("aliases") or []
    if isinstance(al, list):
        parts += [str(a) for a in al]
    hay = " ".join(parts).lower()
    return any(t in hay for t in focus)


def run_query_with_method(
    collection: str,
    question: str,
    mode: str = "best",
    limit: int = 25,
    show_exact_links: bool = True,
    show_related_topics: bool = True,
    force_answer: bool = False,
) -> Dict:
    """
    Primary query entry point.  Returns a dict with keys:
      method   str    — routing method used
      reason   str    — why that method was chosen
      result   str    — human-readable answer

    Optional extra keys depending on method:
      namespace_debug, plan, executor_debug_items, structured_plan_dry_run
    """
    # Pre-normalization filename detection — before normalize_simple_text strips dots
    import re
    _filename_matches = re.findall(r'\b([a-zA-Z0-9_\-]+\.[a-zA-Z0-9]{2,5})\b', question)
    # Drop domain/URL-like matches (e.g. omnivista.com) — they're not file identifiers
    # and would otherwise trigger a dead-end identifier lookup.
    _DOMAIN_TLDS = {"com", "org", "net", "io", "gov", "edu", "co", "uk", "ai",
                    "biz", "info", "us", "ca", "dev", "app"}
    _filename_matches = [m for m in _filename_matches
                         if m.rsplit(".", 1)[-1].lower() not in _DOMAIN_TLDS]
    if _filename_matches:
        from core.retrieval.db_retrieval import get_by_identifier, fetchall
        for _fname in _filename_matches:
            _pts = get_by_identifier(collection, _fname)
            if _pts:
                _payload = _pts[0].payload or {}
                # Identifier-hijack guard: if the question's focus (e.g. "sFTP") isn't
                # in this record, don't answer from it — fall through to retrieval.
                if not _record_covers_question(question, _fname, _payload):
                    continue
                _payload["_question"] = question
                _roles = _detect_requested_roles(question, {})
                _answer = synthesize_answer(_payload, _roles, collection)

                # Enrich with cross-links — both confirmed exact links and concept similarity
                from core.cross_link_store import get_cross_links_for_identifier
                from core.concept_link_finder import find_concept_links
                from core.db import fetchall as _fetchall
                import json as _json

                _related_sections = []

                # Step 1: confirmed exact/name cross-links
                _links = get_cross_links_for_identifier(collection, _fname, status="confirmed") if show_exact_links else []
                for _link in _links:
                    _linked_pts = get_by_identifier(_link["target_collection"], _link["target_identifier"])
                    if not _linked_pts:
                        _linked_rows = _fetchall(
                            "SELECT payload FROM chunks WHERE collection_name = %s AND (payload->>'source_file' = %s OR payload->>'primary_name' = %s) LIMIT 1",
                            (_link["target_collection"], _link["target_identifier"], _link["target_identifier"])
                        )
                        if _linked_rows:
                            _linked_payload = _linked_rows[0]["payload"] if isinstance(_linked_rows[0]["payload"], dict) else _json.loads(_linked_rows[0]["payload"])
                            _linked_pts = [type("P", (), {"payload": _linked_payload})()]
                    if _linked_pts:
                        _lp = _linked_pts[0].payload or {}
                        _lname = _lp.get("primary_name") or _link["target_identifier"]
                        _source_file = _lp.get("source_file") or _link["target_identifier"]
                        _full = _fetchall(
                            """SELECT payload->>'text' AS text FROM chunks
                               WHERE collection_name = %s
                               AND payload->>'source_file' = %s
                               ORDER BY id LIMIT 3""",
                            (_link["target_collection"], _source_file)
                        )
                        if _full:
                            _ldesc = "\n\n".join(r["text"] for r in _full if r["text"])
                        else:
                            _ldesc = str(_lp.get("description") or "")
                        _related_sections.append({
                            "title": _lname,
                            "collection": _link["target_collection"],
                            "match_type": "confirmed",
                            "confidence": _link.get("confidence", 1.0),
                            "preview": _ldesc
                        })

                # CL-04: one hop along confirmed wikilinks from first-hop targets
                if show_exact_links and _links:
                    _seen_hop = {(s["collection"], s.get("source_file") or s["title"])
                                 for s in _related_sections}
                    for _l in _links:
                        for _hl in get_cross_links_for_identifier(
                                _l["target_collection"], _l["target_identifier"], status="confirmed"):
                            if _hl["target_collection"] == collection and _hl["target_identifier"] == _fname:
                                continue
                            _hk = (_hl["target_collection"], _hl["target_identifier"])
                            if _hk in _seen_hop:
                                continue
                            _seen_hop.add(_hk)
                            _sec = _build_section_for_link(_hl)
                            if _sec:
                                _sec["match_type"] = "wikilink_hop"
                                _sec["confidence"] = round(
                                    min(_sec["confidence"], _l.get("confidence", 1.0)) * 0.9, 3)
                                _related_sections.append(_sec)

                # Step 2: concept similarity links (semantic cross-collection)
                _chunk_id = _pts[0].id if hasattr(_pts[0], 'id') else None
                if _chunk_id and show_related_topics:
                    _concept_links = find_concept_links(collection, str(_chunk_id))
                    _seen_targets = {(_link["target_collection"], _link["target_identifier"]) for _link in _links}
                    for _cl in _concept_links:
                        _anchor_texts = _cl.get("anchor_texts") or []
                        _anchor_chunk_ids = _cl.get("anchor_chunk_ids") or []
                        if _anchor_chunk_ids:
                            from core.db import fetchall as _fetchall
                            _full_row = _fetchall(
                                "SELECT payload->>'text' AS text FROM chunks WHERE id = %s LIMIT 1",
                                (_anchor_chunk_ids[0],)
                            )
                            _preview = _full_row[0]["text"] if _full_row else (_anchor_texts[0] if _anchor_texts else "")
                        else:
                            _preview = _anchor_texts[0] if _anchor_texts else ""
                        _section_key = (_cl["target_collection"], _cl["group_value"])
                        if _section_key not in _seen_targets:
                            _seen_targets.add(_section_key)
                            _related_sections.append({
                                "title": _cl['group_value'],
                                "collection": _cl['target_collection'],
                                "match_type": "concept",
                                "confidence": round(_cl['similarity'], 2),
                                "preview": _preview,
                                "anchor_chunk_ids": _cl.get('anchor_chunk_ids', []),
                                })

                return {
                    "method": "identifier_lookup",
                    "reason": f"filename identifier detected: {_fname}",
                    "result": _answer,
                    "related_sections": _related_sections,
                }

            else:
                # Filename detected but not found. If the query is a sentence (the
                # filename was incidental), don't dead-end — fall through to semantic
                # search below. Only show the "did you mean" suggestion for short,
                # filename-style lookups.
                if len(question.split()) > 4:
                    break  # fall through to the semantic / lexical path
                try:
                    _similar = fetchall("""
                        SELECT DISTINCT
                            payload->>'identifier' AS identifier,
                            payload->>'primary_name' AS primary_name,
                            similarity(payload->>'identifier', %s) AS sim
                        FROM chunks
                        WHERE collection_name = %s
                        AND payload->>'identifier' IS NOT NULL
                        AND similarity(payload->>'identifier', %s) > 0.2
                        ORDER BY sim DESC
                        LIMIT 3
                    """, (_fname, collection, _fname))
                except Exception:
                    _similar = []

                if _similar:
                    _suggestions = "\n".join([
                        f"  - {r['identifier']} ({r['primary_name'] or ''})"
                        for r in _similar
                    ])
                    return {
                        "method": "identifier_lookup",
                        "reason": f"filename detected but not found: {_fname}",
                        "result": f"No exact match found for '{_fname}'.\n\nDid you mean one of these?\n{_suggestions}",
                    }
                else:
                    return {
                        "method": "identifier_lookup",
                        "reason": f"filename detected but not found: {_fname}",
                        "result": f"No record found for '{_fname}' in this collection. Please check the filename and try again.",
                    }

    from core.retrieval.discovery import llm_detect_intent
    intent = llm_detect_intent(question)
    # force_answer (used by Chat) ensures single-record questions still produce an
    # answer instead of bailing — but it must NOT override a genuine discovery /
    # analytics / comparison intent, or list/count questions ("what are the recon
    # files for Goldman", "how many files") get shoved through single-answer
    # synthesis and hedge with "no exact match found".
    if force_answer and intent["mode"] not in ("discovery_list", "discovery_count", "comparison"):
        intent["mode"] = "answer"

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
    # 5. Discovery  (count / list queries)
    # ------------------------------------------------------------------
    if intent["mode"] in ("discovery_count", "discovery_list"):
        return run_discovery_with_method(collection, question, limit=limit)

    # ------------------------------------------------------------------
    # 6. Normal semantic / lexical fallback
    # ------------------------------------------------------------------
    method_info = detect_query_mode(question)

    _route_result = route_query(collection, question, mode=mode, limit=limit)
    _last_payload = getattr(route_query, "_last_answer_payload", None)
    _related_sections = []

    if _last_payload and (show_exact_links or show_related_topics):
        from core.cross_link_store import get_cross_links_for_identifier
        from core.concept_link_finder import find_concept_links
        from core.db import fetchall as _fetchall
        import json as _json

        _identifier = _last_payload.get("identifier")
        _chunk_id = str(_last_payload.get("_chunk_db_id") or "")

        if show_exact_links and _identifier:
            _links = get_cross_links_for_identifier(collection, _identifier, status="confirmed")
            for _link in _links:
                from core.retrieval.db_retrieval import get_by_identifier
                _linked_pts = get_by_identifier(_link["target_collection"], _link["target_identifier"])
                if not _linked_pts:
                    _linked_rows = _fetchall(
                        "SELECT payload FROM chunks WHERE collection_name = %s AND (payload->>'source_file' = %s OR payload->>'primary_name' = %s) LIMIT 1",
                        (_link["target_collection"], _link["target_identifier"], _link["target_identifier"])
                    )
                    if _linked_rows:
                        _lp = _linked_rows[0]["payload"] if isinstance(_linked_rows[0]["payload"], dict) else _json.loads(_linked_rows[0]["payload"])
                        _linked_pts = [type("P", (), {"payload": _lp})()]
                if _linked_pts:
                    _lp = _linked_pts[0].payload or {}
                    _lname = _lp.get("primary_name") or _link["target_identifier"]
                    _source_file = _lp.get("source_file") or _link["target_identifier"]
                    _full = _fetchall(
                        """SELECT payload->>'text' AS text FROM chunks
                           WHERE collection_name = %s
                           AND payload->>'source_file' = %s
                           ORDER BY id LIMIT 3""",
                        (_link["target_collection"], _source_file)
                    )
                    if _full:
                        _ldesc = "\n\n".join(r["text"] for r in _full if r["text"])
                    else:
                        _ldesc = str(_lp.get("description") or "")
                    _related_sections.append({
                        "title": _lname,
                        "collection": _link["target_collection"],
                        "match_type": "confirmed",
                        "confidence": _link.get("confidence", 1.0),
                        "preview": _ldesc
                    })

        if show_related_topics and _chunk_id:
            _concept_links = find_concept_links(collection, _chunk_id)
            _seen = {(_s["collection"], _s["title"]) for _s in _related_sections}
            for _cl in _concept_links:
                _key = (_cl["target_collection"], _cl["group_value"])
                if _key not in _seen:
                    _seen.add(_key)
                    _anchor_chunk_ids = _cl.get("anchor_chunk_ids") or []
                    _anchor_texts = _cl.get("anchor_texts") or []
                    if _anchor_chunk_ids:
                        from core.db import fetchall as _fetchall
                        _full_row = _fetchall(
                            "SELECT payload->>'text' AS text FROM chunks WHERE id = %s LIMIT 1",
                            (_anchor_chunk_ids[0],)
                        )
                        _cl_preview = _full_row[0]["text"] if _full_row else (_anchor_texts[0] if _anchor_texts else "")
                    else:
                        _cl_preview = _anchor_texts[0] if _anchor_texts else ""
                    _related_sections.append({
                        "title": _cl["group_value"],
                        "collection": _cl["target_collection"],
                        "match_type": "concept",
                        "confidence": round(_cl["similarity"], 2),
                        "preview": _cl_preview,
                        "anchor_chunk_ids": _anchor_chunk_ids,
                    })

    response: Dict = {
        "method": method_info["mode"],
        "reason": method_info["reason"],
        "result": _route_result,
        "answer_payload": _last_payload,
        "related_sections": _related_sections,
    }

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
    Like run_query_with_method but also returns raw candidate points
    and per-point scores for debugging in the UI.
    """
    query_result = run_query_with_method(collection, question, limit=limit)

    # Lexical candidates
    from core.retrieval.lexical import lexical_short_query_search, lexical_chunk_search
    from core.retrieval.semantic import semantic_search

    lexical_short_raw = lexical_short_query_search(collection, question, limit=limit)
    lexical_short_items = lexical_short_raw  # already list of dicts

    sem_points = semantic_search(collection, question, limit=limit)
    lex_chunk_points = lexical_chunk_search(collection, question, limit=limit)

    # Merged + reranked
    merged = _build_candidate_points(collection, question, limit=limit)
    ranked = rerank_points(merged, question)

    query_result["lexical_short_items"] = lexical_short_items
    query_result["semantic_points"] = sem_points
    query_result["lexical_chunk_points"] = lex_chunk_points
    query_result["lexical_structured_points"] = []
    query_result["lexical_entity_points"] = []
    query_result["merged_points"] = merged
    query_result["ranked_points"] = ranked

    # Also keep flat debug_points for any new UI code
    debug_points = []
    for p in ranked[:20]:
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
    from core.retrieval.discovery import llm_detect_intent
    intent = llm_detect_intent(question)
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