from core.query_helpers import (
    infer_doc_type,
    load_doc_query_hints,
    normalize_simple_text,
    expand_terms_with_synonyms,
)

from core.embedder import embed_texts
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue
from core.schema_loader import load_collection_schemas, get_identifier_fields
from core.field_map_loader import load_field_maps
from core.collection_config import get_collection
from core.crosslink_engine import (
    fetch_points_by_identifier,
    merge_payloads_for_identifier,
    expand_related_identifiers,
    reverse_lookup_by_enum_value,
    reverse_lookup_structured_by_requested_role,
)
from core.discovery_engine import run_discovery_with_method, detect_ask_intent

DEBUG = True


# --- INIT CLIENT ---


def load_query_terms():
    path = Path("config/query_terms.json")
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

from core.system_config import load_system_config

cfg = load_system_config()
client = QdrantClient(url=cfg["qdrant_url"])

from pathlib import Path
import json

import re

def extract_reverse_lookup_candidate(question, field_maps):
    q_norm = normalize_simple_text(question)

    role_keywords = []
    for keywords in field_maps.values():
        role_keywords.extend(keywords)

    cleaned = q_norm
    for kw in sorted(role_keywords, key=len, reverse=True):
        kw_norm = normalize_simple_text(kw)
        if kw_norm:
            cleaned = cleaned.replace(kw_norm, " ")

    noise = {
        "what", "which", "tag", "field", "has", "have", "can", "a", "an", "the",
        "of", "for", "is", "are", "with"
    }

    words = [w for w in cleaned.split() if w and w not in noise]
    return " ".join(words).strip()

def extract_explicit_identifier(question: str):
    q = question.lower()

    # match "tag 22"
    m = re.search(r"\btag\s*(\d+)\b", q)
    if m:
        return m.group(1)

    # fallback: standalone number only if "tag" exists
    if "tag" in q:
        m = re.search(r"\b(\d{1,5})\b", q)
        if m:
            return m.group(1)

    return None

def extract_negative_terms(question: str):
    q = normalize_simple_text(question)

    patterns = [
        r"\bnot\s+([a-z0-9_]+(?:\s+[a-z0-9_]+){0,2})",
        r"\bexcluding\s+([a-z0-9_]+(?:\s+[a-z0-9_]+){0,2})",
        r"\bexclude\s+([a-z0-9_]+(?:\s+[a-z0-9_]+){0,2})",
        r"\bwithout\s+([a-z0-9_]+(?:\s+[a-z0-9_]+){0,2})"
    ]

    negatives = []
    for pattern in patterns:
        negatives.extend(re.findall(pattern, q))

    cleaned = []
    seen = set()

    for term in negatives:
        term = normalize_simple_text(term).strip()
        if not term:
            continue
        if term not in seen:
            seen.add(term)
            cleaned.append(term)

    return cleaned

def remove_negative_terms_from_question(question: str):
    q = normalize_simple_text(question)

    q = re.sub(r"\bnot\s+[a-z0-9_]+(?:\s+[a-z0-9_]+){0,2}", " ", q)
    q = re.sub(r"\bexcluding\s+[a-z0-9_]+(?:\s+[a-z0-9_]+){0,2}", " ", q)
    q = re.sub(r"\bexclude\s+[a-z0-9_]+(?:\s+[a-z0-9_]+){0,2}", " ", q)
    q = re.sub(r"\bwithout\s+[a-z0-9_]+(?:\s+[a-z0-9_]+){0,2}", " ", q)

    q = re.sub(r"\s+", " ", q).strip()
    return q

def fetch_entity_row_exact_title_match(collection, question, limit=10):
    q_norm = normalize_simple_text(question)

    points, _ = client.scroll(
        collection_name=collection,
        limit=5000,
        with_payload=True,
        with_vectors=False
    )

    matches = []

    for p in points:
        payload = p.payload or {}

        if infer_doc_type(payload) != "entity_row":
            continue

        primary_name = str(payload.get("primary_name") or "")
        if normalize_simple_text(primary_name) == q_norm:
            matches.append(payload)

    return matches[:limit]

def contains_negative_term(text: str, negative_terms):
    text_norm = normalize_simple_text(text)

    for term in negative_terms:
        if term == text_norm:
            return True
        if term in text_norm:
            return True

    return False

def detect_query_mode(question: str):
    q_norm = normalize_simple_text(question)
    words = [w for w in q_norm.split() if w]

    sentence_words = {
        "what", "how", "when", "where", "why",
        "can", "should", "do", "does", "is", "are",
        "could", "would", "will", "who"
    }

    is_sentence_like = any(w in sentence_words for w in words)

    if len(words) <= 2 and not is_sentence_like:
        return {
            "mode": "lexical_short",
            "reason": f"query is {len(words)} word(s) and not sentence-like"
        }

    return {
        "mode": "semantic",
        "reason": "query is sentence-like or longer than 2 words"
    }

def lexical_short_query_search(collection, question, limit=25):
    q_norm = normalize_simple_text(question)
    if not q_norm:
        return []

    words = [w for w in q_norm.split() if w]

    points, _ = client.scroll(
        collection_name=collection,
        limit=5000,
        with_payload=True,
        with_vectors=False
    )

    scored = []

    for p in points:
        payload = p.payload or {}

        primary_name = str(payload.get("primary_name") or "")
        description = str(payload.get("description") or "")

        name_norm = normalize_simple_text(primary_name)
        desc_norm = normalize_simple_text(description)
        combined = f"{name_norm} {desc_norm}"

        score = 0.0

        if q_norm == name_norm:
            score += 100.0
        elif q_norm in name_norm:
            score += 25.0
        elif q_norm in combined:
            score += 10.0

        word_hits_name = sum(1 for w in words if w in name_norm)
        word_hits_desc = sum(1 for w in words if w in desc_norm)

        score += word_hits_name * 8.0
        score += word_hits_desc * 2.0

        if words and all(w in combined for w in words):
            score += 8.0

        if score > 0:
            scored.append((score, p))

    scored.sort(key=lambda x: x[0], reverse=True)

    return [
        {
            "identifier": p.payload.get("identifier"),
            "primary_name": p.payload.get("primary_name"),
            "description": p.payload.get("description"),
            "score": score,
            "payload": p.payload
        }
        for score, p in scored[:limit]
    ]


def run_query_with_method(collection, question, mode="best", limit=25):
    intent = detect_ask_intent(question)

    if intent["mode"] in ["discovery_count", "discovery_list"]:
        return run_discovery_with_method(collection, question, limit=limit)

    method_info = detect_query_mode(question)

    return {
        "method": method_info["mode"],
        "reason": method_info["reason"],
        "result": route_query(collection, question, mode=mode, limit=limit)
    }
# OLD QUERY replaced with above
#def run_query_with_method(collection, question, mode="best", limit=25):
#    method_info = detect_query_mode(question)
#
#    # only do entity-row exact-title precheck for short lexical-style asks
#    if method_info["mode"] == "lexical_short":
#        exact_title_matches = fetch_entity_row_exact_title_match(collection, question, limit=1)
#        if exact_title_matches:
#            return {
#                "method": "exact_title_match",
#                "reason": "query exactly matches an article title",
#                "result": synthesize_answer(exact_title_matches[0], [], collection)
#            }
#
#    return {
#        "method": method_info["mode"],
#        "reason": method_info["reason"],
#        "result": route_query(collection, question, mode=mode, limit=limit)
#    }

def detect_requested_roles(question, field_maps):
    q = f" {normalize_simple_text(question)} "
    matched_roles = []

    for role, keywords in field_maps.items():
        for kw in keywords:
            kw_norm = normalize_simple_text(kw)
            if f" {kw_norm} " in q:
                matched_roles.append(role)
                break

    return matched_roles

def load_collection_schemas(collection_name):

    schema_dir = Path("schemas")

    schemas = {}

    for file in schema_dir.glob(f"{collection_name}_*_schema.json"):
        with open(file, "r", encoding="utf-8") as f:
            schemas[file.name] = json.load(f)

    return schemas

def detect_intent(question, intent_map):

    q = question.lower()

    scores = {k: 0 for k in intent_map}

    for intent, keywords in intent_map.items():
        for kw in keywords:
            if kw in q:
                scores[intent] += 1

    # pick best match
    best_intent = max(scores, key=scores.get)

    if scores[best_intent] == 0:
        return None

    return best_intent

def fetch_entity_row_by_title(collection, title, limit=10):
    title_norm = normalize_simple_text(title)

    points, _ = client.scroll(
        collection_name=collection,
        limit=5000,
        with_payload=True,
        with_vectors=False
    )

    matches = []

    for p in points:
        payload = p.payload or {}
        if infer_doc_type(payload) != "entity_row":
            continue

        primary_name = str(payload.get("primary_name") or "")
        if normalize_simple_text(primary_name) == title_norm:
            matches.append(payload)

    return matches[:limit]

def get_source_label(collection_name, payload):
    collection_cfg = get_collection(collection_name) or {}
    collection_source_label = (collection_cfg.get("source_label") or "").strip()
    if collection_source_label:
        return collection_source_label

    labels_cfg = load_source_labels()

    subtype = str(payload.get("subtype") or "").lower()
    source_type = str(payload.get("source_type") or "").lower()
    doc_type = str(payload.get("doc_type") or "").lower()
    source_file = payload.get("source_file") or "unknown source"

    subtype_map = labels_cfg.get("subtype", {})
    source_type_map = labels_cfg.get("source_type", {})
    doc_type_map = labels_cfg.get("doc_type", {})

    if subtype in subtype_map:
        return subtype_map[subtype]

    if source_type in source_type_map:
        return source_type_map[source_type]

    if doc_type in doc_type_map:
        return doc_type_map[doc_type]

    return f"Source file: {source_file}"

def load_source_labels():
    with open("config/source_labels.json", "r", encoding="utf-8") as f:
        return json.load(f)

# --- GENERIC IDENTIFIER DETECTION ---
import re

def detect_identifier(question: str):
    tokens = re.findall(r"[A-Za-z0-9_]+", question)

    scored = []

    for t in tokens:
        score = 0

        # numeric → strong candidate
        if t.isdigit():
            score += 3

        # mixed (e.g. PX_LAST)
        if any(c.isdigit() for c in t) and any(c.isalpha() for c in t):
            score += 2

        # uppercase tokens (common in symbols)
        if t.isupper() and len(t) > 1:
            score += 2

        # medium-length tokens (avoid "is", "what")
        if 3 <= len(t) <= 15:
            score += 1

        # penalize very short tokens
        if len(t) <= 2:
            score -= 1

        scored.append((t, score))

    # sort best first
    scored.sort(key=lambda x: x[1], reverse=True)

    return [t for t, _ in scored]


def get_display_labels(collection_name):
    schemas = load_collection_schemas(collection_name)

    identifier_label = "identifier"
    primary_name_label = "name"
    enum_value_label = "value"
    enum_name_label = "label"

    for _, schema in schemas.items():
        if schema.get("identifier"):
            identifier_label = schema["identifier"][0]
        if schema.get("primary_name"):
            primary_name_label = schema["primary_name"][0]
        if schema.get("enum_value"):
            enum_value_label = schema["enum_value"][0]
        if schema.get("enum_name"):
            enum_name_label = schema["enum_name"][0]
        break

    return {
        "identifier": identifier_label,
        "primary_name": primary_name_label,
        "enum_value": enum_value_label,
        "enum_name": enum_name_label,
    }


# --- FILTERED SEARCH ---
def filtered_search(collection, identifier, question):

    vector = embed_texts([question.lower().strip()])[0]

    results = client.query_points(
        collection_name=collection,
        query=vector,
        query_filter=Filter(
            must=[
                FieldCondition(
                    key="identifier",
                    match=MatchValue(value=str(identifier))
                )
            ]
        ),
        limit=3
    )

    return results.points

# --- SEMANTIC SEARCH ---
def semantic_search(collection, question, limit=5):
    vector = embed_texts([question.lower().strip()])[0]

    results = client.query_points(
        collection_name=collection,
        query=vector,
        limit=limit
    )

    return results.points


def _normalize_field_name(name):
    name = str(name or "").strip().lower()
    name = name.replace("_", " ")
    name = re.sub(r"[^a-z0-9\s\-]", " ", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def _metadata_field_match_score(query_norm, field_name):
    q = _normalize_field_name(query_norm)
    f = _normalize_field_name(field_name)

    if not q or not f:
        return 0

    if q == f:
        return 100

    if f in q:
        return 80

    q_terms = {t for t in q.split() if len(t) > 2}
    f_terms = {t for t in f.split() if len(t) > 2}

    if not q_terms or not f_terms:
        return 0

    overlap = len(q_terms & f_terms)
    if overlap == 0:
        return 0

    return overlap * 10


def _pick_best_metadata_field(payload, question):
    query_norm = normalize_simple_text(question)

    ignore_keys = {
        "text", "description", "primary_name", "identifier", "doc_type",
        "source_type", "source_file", "astro_format", "enum_values",
        "block_types", "section_heading"
    }

    best_key = None
    best_score = 0

    for key, value in (payload or {}).items():
        if key in ignore_keys:
            continue
        if value in [None, ""]:
            continue

        score = _metadata_field_match_score(query_norm, key)
        if score > best_score:
            best_score = score
            best_key = key

    return best_key, best_score

def _answer_structured_metadata_field(payload, question):
    field_name, score = _pick_best_metadata_field(payload, question)

    if not field_name or score <= 0:
        return None

    value = payload.get(field_name)
    primary_name = payload.get("primary_name")
    source_file = payload.get("source_file")

    pretty_field = field_name.replace("_", " ")

    if primary_name:
        return f"{pretty_field}: {value}"
    if source_file:
        return f"{pretty_field}: {value}"
    return f"{pretty_field}: {value}"

def pick_best_chunked_candidate(points, question):
    if not points:
        return None

    hints = load_doc_query_hints()
    stopwords = set(hints.get("stopwords", []))

    q_norm = normalize_simple_text(question)
    query_terms = [
        w for w in q_norm.split()
        if len(w) > 2 and w not in stopwords
    ]

    best_point = None
    best_score = -1

    for p in points:
        payload = p.payload or {}

        heading_raw = str(payload.get("section_heading") or "")
        text_raw = str(payload.get("text") or "")
        block_types = payload.get("block_types") or []

        heading = normalize_simple_text(heading_raw)
        text = normalize_simple_text(text_raw)

        score = getattr(p, "score", None)
        if score is None:
            score = 0.0

        # -------------------------
        # generic phrase + term overlap
        # -------------------------
        if q_norm and q_norm in text:
            score += 4.0
        elif q_norm and q_norm in heading:
            score += 3.0

        heading_hits = sum(1 for w in query_terms if w in heading)
        text_hits = sum(1 for w in query_terms if w in text)

        score += heading_hits * 1.5
        score += text_hits * 0.7

        if query_terms and all(w in text for w in query_terms):
            score += 2.5
        elif query_terms and all(w in heading for w in query_terms):
            score += 2.0

        text_len = len(text_raw.strip())
        heading_len = len(heading_raw.strip())
        has_meaningful_body = text_len > heading_len + 80

        if has_meaningful_body:
            score += 1.5
        elif query_terms:
            score -= 1.0

        if "heading" in block_types and len(block_types) > 1:
            score += 0.5

        heading_lower = heading_raw.strip().lower()
        if "@" in heading_lower:
            score -= 3.0
        if heading_lower.startswith("www.") or "http://" in heading_lower or "https://" in heading_lower:
            score -= 2.0

        if text_len < 40 and text_hits == 0 and heading_hits == 0:
            score -= 1.5

        # -------------------------
        # generic OCR-page scoring
        # -------------------------
        is_page_ocr = "page_ocr" in block_types

        if is_page_ocr and query_terms:
            # unique query-term coverage on the page
            covered_terms = [w for w in query_terms if w in text]
            unique_coverage = len(set(covered_terms))
            score += unique_coverage * 1.2

            # early-position bonus: matches near start of page matter more
            early_window = text[:500]
            early_hits = sum(1 for w in query_terms if w in early_window)
            score += early_hits * 0.8

            # compactness: reward pages where matched terms occur close together
            positions = []
            for w in set(covered_terms):
                pos = text.find(w)
                if pos >= 0:
                    positions.append(pos)

            if len(positions) >= 2:
                span = max(positions) - min(positions)
                if span < 200:
                    score += 2.0
                elif span < 500:
                    score += 1.0

            # OCR noise penalty: penalize pages with too much symbol/noise ratio
            chars = text_raw.strip()
            if chars:
                alpha_num = sum(ch.isalnum() for ch in chars)
                noise = len(chars) - alpha_num
                noise_ratio = noise / max(len(chars), 1)

                if noise_ratio > 0.45:
                    score -= 2.0
                elif noise_ratio > 0.35:
                    score -= 1.0

        if score > best_score:
            best_score = score
            best_point = p

    return best_point or points[0]

def lexical_chunk_search(collection, question, limit=25):
    hints = load_doc_query_hints()
    stopwords = set(hints.get("stopwords", []))

    q_norm = normalize_simple_text(question)
    query_terms = [
        w for w in q_norm.split()
        if len(w) > 2 and w not in stopwords
    ]

    if not query_terms:
        return []

    points, _ = client.scroll(
        collection_name=collection,
        limit=500,
        with_payload=True,
        with_vectors=False
    )

    scored = []

    for p in points:
        payload = p.payload or {}

        source_type = str(payload.get("source_type") or "").lower()
        file_type = str(payload.get("file_type") or "").lower()
        doc_type = str(payload.get("doc_type") or "").lower()

        # only chunked doc-like content
        is_chunked_doc_like = (
            bool(payload.get("section_heading") or payload.get("block_types"))
            and doc_type != "entity_row"
            and file_type != "image"
            and source_type not in ["image", "standalone_image"]
        )

        if not is_chunked_doc_like:
            continue

        text = normalize_simple_text(payload.get("text"))
        heading = normalize_simple_text(payload.get("section_heading"))

        score = 0.0

        if q_norm and q_norm in text:
            score += 8.0
        if q_norm and q_norm in heading:
            score += 6.0

        unique_hits = len({w for w in query_terms if w in text})
        heading_hits = len({w for w in query_terms if w in heading})

        score += unique_hits * 1.5
        score += heading_hits * 2.0

        if query_terms and all(w in text for w in query_terms):
            score += 4.0

        if score > 0:
            scored.append((score, p))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in scored[:limit]]

def lexical_entity_row_search(collection, question, limit=25):
    q_norm = normalize_simple_text(question)

    hints = load_doc_query_hints()
    stopwords = set(hints.get("stopwords", []))

    raw_words = [w for w in q_norm.split() if w]
    meaningful_words = [w for w in raw_words if w not in stopwords]
    expanded_words = expand_terms_with_synonyms(meaningful_words)

    if not meaningful_words and not expanded_words:
        return []

    points, _ = client.scroll(
        collection_name=collection,
        limit=5000,
        with_payload=True,
        with_vectors=False
    )

    scored = []

    for p in points:
        payload = p.payload or {}
        doc_type = infer_doc_type(payload)

        if doc_type != "entity_row":
            continue

        name = str(payload.get("primary_name") or "")
        desc = str(payload.get("description") or "")

        if not name and not desc:
            continue

        name_norm = normalize_simple_text(name)
        desc_norm = normalize_simple_text(desc)
        combined = f"{name_norm} {desc_norm}"

        score = 0.0

        exact_query = " ".join(meaningful_words).strip()

        if exact_query and name_norm == exact_query:
            score += 20.0
        elif exact_query and exact_query in name_norm:
            score += 8.0
        elif exact_query and exact_query in desc_norm:
            score += 2.0

        exact_name_hits = sum(1 for w in meaningful_words if w in name_norm)
        exact_desc_hits = sum(1 for w in meaningful_words if w in desc_norm)

        score += exact_name_hits * 3.0
        score += exact_desc_hits * 0.8

        if meaningful_words and all(w in name_norm for w in meaningful_words):
            score += 8.0
        elif meaningful_words and all(w in combined for w in meaningful_words):
            score += 3.0

        expanded_name_hits = sum(1 for w in expanded_words if w in name_norm)
        expanded_desc_hits = sum(1 for w in expanded_words if w in desc_norm)

        score += expanded_name_hits * 1.0
        score += expanded_desc_hits * 0.2

        if score > 0:
            scored.append((score, p))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in scored[:limit]]

def merge_points_by_id(*point_lists):
    merged = {}
    for point_list in point_lists:
        for p in point_list or []:
            pid = getattr(p, "id", None)
            key = str(pid) if pid is not None else id(p)
            if key not in merged:
                merged[key] = p
    return list(merged.values())

def build_candidate_points(collection, question, limit=25):
    semantic_points = semantic_search(collection, question, limit=limit)

    lexical_chunk_points = []
    lexical_structured_points = []

    if collection != "obsidian":
        lexical_chunk_points = lexical_chunk_search(collection, question, limit=limit)
        lexical_structured_points = lexical_structured_search(collection, question, limit=limit)

    points = merge_points_by_id(
        semantic_points,
        lexical_chunk_points,
        lexical_structured_points
    )

    return {
        "semantic_points": semantic_points,
        "lexical_chunk_points": lexical_chunk_points,
        "lexical_structured_points": lexical_structured_points,
        "merged_points": points
    }


def score_point_shared(p, question):
    q = question.lower().strip()
    positive_q = remove_negative_terms_from_question(q)
    negative_terms = extract_negative_terms(q)

    payload = p.payload or {}

    name = str(payload.get("primary_name") or "").lower()
    desc = str(payload.get("description") or "").lower()
    doc_type = infer_doc_type(payload)

    score = getattr(p, "score", None)
    if score is None:
        score = 0.0

    words = [w for w in normalize_simple_text(positive_q).split() if w]

    # generic lexical boosts
    for word in words:
        if word in name:
            score += 1.5

    for word in words:
        if word in desc:
            score += 0.5

    # structured-only reranking
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

        exact_query_terms = meaningful_words
        exact_query = " ".join(exact_query_terms).strip()

        if exact_query and normalized_name == exact_query:
            score += 20.0
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

    # entity_row stays mostly semantic
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

        # strong negation penalties
        if negative_terms:
            if contains_negative_term(name, negative_terms):
                score -= 50.0
            if contains_negative_term(desc, negative_terms):
                score -= 20.0

    return score

def dedupe_entity_row_points(points):
    deduped = []
    seen = set()

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

def dedupe_structured_results(items):
    deduped = []
    seen = set()

    for item in items or []:
        identifier = str(item.get("identifier") or "").strip()
        primary_name = normalize_simple_text(item.get("primary_name") or "")
        description = normalize_simple_text(item.get("description") or "")

        # best key = identifier if present
        if identifier:
            key = f"id:{identifier}"

        # otherwise collapse by normalized primary_name
        elif primary_name:
            key = f"name:{primary_name}"

        # last fallback
        else:
            key = f"desc:{description[:120]}"

        if key in seen:
            continue

        seen.add(key)
        deduped.append(item)

    return deduped

def rerank_points(points, question):
    if not points:
        return []

    first_payload = points[0].payload or {}
    first_doc_type = infer_doc_type(first_payload)

    # -------------------------------------------------
    # ENTITY_ROW:
    # semantic-first, deduped, with negation support
    # -------------------------------------------------
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

            semantic_score = getattr(p, "score", None)
            if semantic_score is None:
                semantic_score = 0.0

            title_boost = 0.0
            negative_penalty = 0.0

            # hard exclude title matches for negated terms
            if negative_terms and contains_negative_term(name, negative_terms):
                negative_penalty += 80.0

            topic_hits = sum(
                1 for w in positive_topic_terms
                if w in normalized_name or w in normalized_desc
            )

            # softer topic retention penalty
            if positive_topic_terms and topic_hits == 0:
                negative_penalty += 120.0

            if exact_query and normalized_name == exact_query:
                title_boost += 100.0
            elif exact_query and exact_query in normalized_name:
                title_boost += 10.0

            # softer penalty if negated term appears only in description
            if negative_terms and contains_negative_term(desc, negative_terms):
                negative_penalty += 80.0

            final_score = semantic_score + title_boost - negative_penalty + (topic_hits * 12.0)

            scored.append((final_score, idx, p))

        scored.sort(key=lambda x: (-x[0], x[1]))
        return [p for _, _, p in scored]

    # -------------------------------------------------
    # ALL OTHER TYPES:
    # keep existing shared reranking
    # -------------------------------------------------
    return sorted(points, key=lambda p: score_point_shared(p, question), reverse=True)

def debug_route_query(collection, question, limit=25):
    candidate_sets = build_candidate_points(collection, question, limit=limit)
    ranked_points = rerank_points(candidate_sets["merged_points"], question)

    return {
        "semantic_points": candidate_sets["semantic_points"],
        "lexical_chunk_points": candidate_sets["lexical_chunk_points"],
        "lexical_structured_points": candidate_sets["lexical_structured_points"],
        "merged_points": candidate_sets["merged_points"],
        "ranked_points": ranked_points,
        "final_result": None
    }

def lexical_structured_search(collection, question, limit=25):
    q_norm = normalize_simple_text(question)

    hints = load_doc_query_hints()
    stopwords = set(hints.get("stopwords", []))

    raw_words = [w for w in q_norm.split() if w]
    meaningful_words = [
        w for w in raw_words
        if w not in stopwords and w not in {"tag", "field"}
    ]
    expanded_words = expand_terms_with_synonyms(meaningful_words)

    if not expanded_words:
        return []

    points, _ = client.scroll(
        collection_name=collection,
        limit=5000,
        with_payload=True,
        with_vectors=False
    )

    scored = []

    for p in points:
        payload = p.payload or {}
        doc_type = infer_doc_type(payload)

        if doc_type != "structured":
            continue

        identifier = payload.get("identifier")
        primary_name = str(payload.get("primary_name") or "")
        description = str(payload.get("description") or "")

        if identifier in [None, ""] and not primary_name:
            continue

        name_norm = normalize_simple_text(primary_name.replace("_", " "))
        desc_norm = normalize_simple_text(description)
        combined = f"{name_norm} {desc_norm}"

        score = 0.0

        # exact phrase matches
        if q_norm and q_norm in name_norm:
            score += 8.0
        if q_norm and q_norm in desc_norm:
            score += 2.5

        # exact original words matter most
        exact_name_hits = sum(1 for w in meaningful_words if w in name_norm)
        exact_desc_hits = sum(1 for w in meaningful_words if w in desc_norm)

        score += exact_name_hits * 3.0
        score += exact_desc_hits * 0.8

        if meaningful_words and all(w in name_norm for w in meaningful_words):
            score += 8.0
        elif meaningful_words and all(w in combined for w in meaningful_words):
            score += 3.0

        # synonym-expanded support
        expanded_name_hits = sum(1 for w in expanded_words if w in name_norm)
        expanded_desc_hits = sum(1 for w in expanded_words if w in desc_norm)

        score += expanded_name_hits * 1.2
        score += expanded_desc_hits * 0.3

        # compact field names are valuable
        if primary_name and len(primary_name.strip()) <= 30:
            score += 0.5

        if score > 0:
            scored.append((score, p))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in scored[:limit]]


def has_structured_candidates(points):
    for p in points or []:
        payload = p.payload or {}
        if infer_doc_type(payload) == "structured":
            return True
    return False

def load_synonyms():
        path = Path("config/synonyms.json")
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)


def is_related_identifier_query(question: str) -> bool:
    q = normalize_simple_text(question)
    related_terms = {"related", "relation", "linked", "link", "associated", "association"}
    return any(term in q.split() for term in related_terms)


def fetch_points_by_identifier(collection, identifier, limit=20):
    results = client.query_points(
        collection_name=collection,
        query_filter=Filter(
            must=[
                FieldCondition(
                    key="identifier",
                    match=MatchValue(value=str(identifier))
                )
            ]
        ),
        limit=limit
    )
    return results.points


def merge_payloads_for_identifier(points, identifier):
    merged_payload = {
        "identifier": str(identifier),
        "enum_values": [],
        "source_files": [],
        "related_identifiers": []
    }

    seen_enums = set()
    seen_related = set()
    seen_sources = set()

    for p in points or []:
        payload = p.payload or {}

        if payload.get("primary_name") and not merged_payload.get("primary_name"):
            merged_payload["primary_name"] = payload.get("primary_name")

        if payload.get("description") and not merged_payload.get("description"):
            merged_payload["description"] = payload.get("description")

        for e in payload.get("enum_values", []) or []:
            key = str(e)
            if isinstance(e, dict):
                key = json.dumps(e, sort_keys=True)
            if key not in seen_enums:
                seen_enums.add(key)
                merged_payload["enum_values"].append(e)

        for rid in payload.get("related_identifiers", []) or []:
            rid_str = str(rid).strip()
            if rid_str and rid_str not in seen_related:
                seen_related.add(rid_str)
                merged_payload["related_identifiers"].append(rid_str)

        sf = payload.get("source_file")
        if sf and sf not in seen_sources:
            seen_sources.add(sf)
            merged_payload["source_files"].append(sf)

        if payload.get("source_type") and not merged_payload.get("source_type"):
            merged_payload["source_type"] = payload.get("source_type")

        if payload.get("doc_type") and not merged_payload.get("doc_type"):
            merged_payload["doc_type"] = payload.get("doc_type")

    return merged_payload

def fetch_doc_chunks_by_source_file(collection, source_file, limit=50):
    if not source_file:
        return []

    results = client.query_points(
        collection_name=collection,
        query_filter=Filter(
            must=[
                FieldCondition(
                    key="source_file",
                    match=MatchValue(value=str(source_file))
                )
            ]
        ),
        limit=limit
    )

    return results.points

def build_fuller_doc_payload(collection, best_payload):
    source_file = best_payload.get("source_file")
    if not source_file:
        return best_payload

    points = fetch_doc_chunks_by_source_file(collection, source_file, limit=50)
    if not points:
        return best_payload

    def chunk_sort_key(p):
        payload = p.payload or {}
        chunk_id = payload.get("chunk_id")
        try:
            return int(chunk_id)
        except Exception:
            return 999999

    ordered = sorted(points, key=chunk_sort_key)

    best_chunk_id = best_payload.get("chunk_id")
    try:
        best_chunk_id = int(best_chunk_id)
    except Exception:
        best_chunk_id = None

    best_heading = str(best_payload.get("section_heading") or "").strip()

    selected = []
    for p in ordered:
        payload = p.payload or {}
        cid = payload.get("chunk_id")
        try:
            cid = int(cid)
        except Exception:
            cid = None

        heading = str(payload.get("section_heading") or "").strip()

        if best_chunk_id is None:
            if best_heading:
                if heading == best_heading or not heading:
                    selected.append(payload)
            else:
                selected.append(payload)
        else:
            if cid is None:
                continue

            if best_heading:
                if heading == best_heading:
                    selected.append(payload)
                elif abs(cid - best_chunk_id) <= 1 and not heading:
                    selected.append(payload)
            else:
                if abs(cid - best_chunk_id) <= 1:
                    selected.append(payload)

    if not selected:
        selected = [best_payload]

    combined_parts = []
    seen_headings = set()
    seen_related = set()
    merged_related_titles = []

    note_title = str(best_payload.get("primary_name") or "").strip()

    for payload in selected:
        heading = str(payload.get("section_heading") or "").strip()
        text = str(payload.get("text") or payload.get("description") or "").strip()

        for title in payload.get("related_titles") or []:
            title = str(title).strip()
            if title and title not in seen_related:
                seen_related.add(title)
                merged_related_titles.append(title)

        if heading and heading not in seen_headings:
            seen_headings.add(heading)

            heading_norm = normalize_simple_text(heading)
            title_norm = normalize_simple_text(note_title)

            if heading_norm != title_norm and not text.startswith(heading):
                combined_parts.append(heading)

        if text:
            text_lines = [ln for ln in text.splitlines() if ln.strip()]
            if text_lines:
                first_line_norm = normalize_simple_text(text_lines[0])
                title_norm = normalize_simple_text(note_title)
                heading_norm = normalize_simple_text(heading)

                if first_line_norm == title_norm or (heading and first_line_norm == heading_norm):
                    text = "\n".join(text_lines[1:]).strip()

            if text:
                combined_parts.append(text)

    merged_payload = dict(best_payload)
    merged_payload["description"] = "\n\n".join(part for part in combined_parts if part).strip()
    merged_payload["related_titles"] = merged_related_titles

    return merged_payload

# --- MAIN ROUTER ---
def route_query(collection, question, mode="best", limit=25):

    q = question.lower().strip()

    vector = embed_texts([q])[0]

    schemas = load_collection_schemas(collection)

    # ✅ FIX 1: roles FIRST
    field_maps = load_field_maps()
    roles = detect_requested_roles(q, field_maps)

    def extract_reverse_lookup_candidate(question, field_maps):
        q_norm = normalize_simple_text(question)

        # remove known role cue phrases from field_maps
        role_keywords = []
        for keywords in field_maps.values():
            role_keywords.extend(keywords)

        cleaned = q_norm
        for kw in sorted(role_keywords, key=len, reverse=True):
            kw_norm = normalize_simple_text(kw)
            if kw_norm:
                cleaned = cleaned.replace(kw_norm, " ")

        noise = {
            "what", "which", "tag", "field", "has", "have", "can", "a", "an", "the",
            "of", "for", "is", "are", "with"
        }

        words = [w for w in cleaned.split() if w and w not in noise]
        return " ".join(words).strip()

    if DEBUG:
        print("ROLES:", roles)

    tokens = detect_identifier(question)

    explicit_id = extract_explicit_identifier(question)

    # =========================
    # RELATED IDENTIFIER QUERY
    # structured rows only
    # =========================
    if explicit_id and is_related_identifier_query(question):
        related_results = expand_related_identifiers(collection, explicit_id, limit_per_identifier=10)
        if related_results:
            return related_results

    # =========================
    # 🔥 FORCE IDENTIFIER MATCH
    # =========================
    if explicit_id:
        results = client.query_points(
            collection_name=collection,
            query_filter=Filter(
                must=[
                    FieldCondition(
                        key="identifier",
                        match=MatchValue(value=str(explicit_id))
                    )
                ]
            ),
            limit=50
        )

        if results.points:
            print(f"✅ DIRECT IDENTIFIER MATCH: {explicit_id}")

            print("RAW POINTS:")
            for p in results.points:
                print(p.payload)

            # 🔥 MERGE + DEDUPE ENUMS
            seen = set()
            deduped_enums = []

            merged_payload = {}

            # 🔥 ONLY use points that actually HAVE enums
            enum_points = [p for p in results.points if p.payload.get("enum_values")]

            for p in results.points:
                payload = p.payload

                # keep best name/description
                if payload.get("primary_name"):
                    merged_payload["primary_name"] = payload.get("primary_name")

                if payload.get("description"):
                    merged_payload["description"] = payload.get("description")

                for e in payload.get("enum_values", []):
                    if isinstance(e, dict):
                        key = (e.get("Value"), e.get("SymbolicName"))
                    else:
                        key = str(e)

                    if key not in seen:
                        seen.add(key)
                        deduped_enums.append(e)

            merged_payload["identifier"] = explicit_id
            merged_payload["enum_values"] = deduped_enums

            # preserve source metadata for source labeling
            source_types = []
            subtypes = []
            source_files = []

            for p in results.points:
                payload = p.payload

                if payload.get("source_type"):
                    source_types.append(payload.get("source_type"))

                if payload.get("subtype"):
                    subtypes.append(payload.get("subtype"))

                if payload.get("source_file"):
                    source_files.append(payload.get("source_file"))

            if source_types:
                merged_payload["source_type"] = source_types[0]

            if subtypes:
                merged_payload["subtype"] = subtypes[0]

            if source_files:
                merged_payload["source_file"] = source_files[0]
                merged_payload["source_files"] = list(dict.fromkeys(source_files))

            # 🔥 sort for stability
            payload["enum_values"] = sorted(
                deduped_enums,
                key=lambda x: str(x.get("Value"))
            )

            return synthesize_answer(merged_payload, roles, collection)

    # =========================
    # EXACT MATCH LOOP
    # =========================
    for token in tokens:

        results = client.query_points(
            collection_name=collection,
            query_filter=Filter(
                must=[
                    FieldCondition(
                        key="identifier",
                        match=MatchValue(value=str(token))
                    )
                ]
            ),
            limit=50
        )

        if results.points:
            print(f"✅ EXACT MATCH HIT: {token}")

            if mode == "top3":
                structured_results = [
                    {
                        "identifier": p.payload.get("identifier"),
                        "primary_name": p.payload.get("primary_name"),
                        "description": p.payload.get("description"),
                        "score": score_point_shared(p, question),
                        "payload": p.payload
                    }
                    for p in ranked[: max(top_n * 3, 15)]
                ]

                structured_results = dedupe_structured_results(structured_results)
                return structured_results[:top_n]

            return synthesize_answer(results.points[0].payload, roles, collection)

    # =========================
    # REVERSE ENUM LOOKUP
    # e.g. "what tag can have a value of CUSIP"
    # =========================
    enum_trigger_terms = ["value of", "allowed value", "can have a value", "has a value"]
    if any(term in q for term in enum_trigger_terms):
        reverse_matches = reverse_lookup_by_enum_value(collection, question, limit=5)
        if reverse_matches:
            return reverse_matches

    if "enum_value" in roles:
        enum_target = extract_reverse_lookup_candidate(question, field_maps)
        if enum_target:
            reverse_matches = reverse_lookup_by_enum_value(collection, enum_target, limit=5)
            if reverse_matches:
                return reverse_matches

    # =========================
    # STRUCTURED REVERSE LOOKUP
    # e.g. "what mnemonic is ask price"
    # =========================
    candidate_sets = build_candidate_points(collection, question, limit=limit)
    points = candidate_sets["merged_points"]

    if not points:
        return "No answer found"

    query_mode = detect_query_mode(question)
    first_payload = points[0].payload or {}
    first_doc_type = infer_doc_type(first_payload)

    if first_doc_type == "entity_row" and query_mode["mode"] == "lexical_short":
        exact_title_matches = fetch_entity_row_exact_title_match(collection, question, limit=1)
        if exact_title_matches:
            return synthesize_answer(exact_title_matches[0], roles, collection)

    top_payload = points[0].payload or {}
    top_doc_type = infer_doc_type(top_payload)

    if top_doc_type == "structured" and "primary_name" in roles:
        reverse_target = extract_reverse_lookup_candidate(question, field_maps)
        if reverse_target:
            reverse_matches = reverse_lookup_structured_by_requested_role(
                collection,
                reverse_target,
                requested_role="primary_name",
                limit=5
            )
            if reverse_matches:
                return reverse_matches

    # =========================
    # FALLBACK SEMANTIC
    # =========================
    print("⚠️ Falling back to semantic search")

    #candidate_sets = build_candidate_points(collection, question, limit=limit)
    #points = candidate_sets["merged_points"]
#
    #if not points:
    #    return "No answer found"

    query_mode = detect_query_mode(question)
    first_payload = points[0].payload or {}
    first_doc_type = infer_doc_type(first_payload)

    if query_mode["mode"] == "lexical_short" and first_doc_type == "entity_row":
        return lexical_short_query_search(collection, question, limit=limit)

    # chunked narrative/doc-style content (docs + readable pdf)
    top_payload = points[0].payload if points else {}
    top_source_type = str(top_payload.get("source_type") or "").lower()
    top_doc_type = str(top_payload.get("doc_type") or "").lower()
    top_file_type = str(top_payload.get("file_type") or "").lower()
    top_block_types = top_payload.get("block_types") or []

    is_chunked_doc_like = (
        bool(top_payload.get("section_heading") or top_block_types)
        and top_doc_type != "entity_row"
        and top_file_type != "image"
        and top_source_type not in ["image", "standalone_image"]
    )

    if is_chunked_doc_like:
        best_point = pick_best_chunked_candidate(points, question)
        fuller_payload = build_fuller_doc_payload(collection, best_point.payload)
        return synthesize_answer(fuller_payload, roles, collection)

    ranked = rerank_points(points, question)

    if not ranked:
        return "No answer found after applying filters."

    print("=== RERANKED DOCS ===")
    for p in ranked[:5]:
        print("PRIMARY_NAME:", p.payload.get("primary_name"))
        print("SECTION_HEADING:", p.payload.get("section_heading"))
        print("RERANKED SCORE:", score_point_shared(p, question))
        print("BLOCK_TYPES:", p.payload.get("block_types"))
        print("-----")

    top_n = 5

    top_payload = ranked[0].payload if ranked else {}
    top_doc_type = infer_doc_type(top_payload)

    # for structured semantic queries, return top matches instead of forcing one answer
    # but do not apply this to image results
    top_source_type = str(top_payload.get("source_type") or "").lower()
    top_file_type = str(top_payload.get("file_type") or "").lower()

    if (
        top_doc_type == "structured"
        and not explicit_id
        and top_source_type not in ["image", "standalone_image"]
        and top_file_type != "image"
    ):
        if top_payload.get("identifier") in [None, ""]:
            metadata_answer = _answer_structured_metadata_field(top_payload, question)
            if metadata_answer:
                return metadata_answer
            return "No matching metadata field found in this file."

        structured_results = []
        seen = set()

        for p in ranked:
            item = {
                "identifier": p.payload.get("identifier"),
                "primary_name": p.payload.get("primary_name"),
                "description": p.payload.get("description"),
                "score": score_point_shared(p, question),
                "payload": p.payload
            }

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
            structured_results.append(item)

            if len(structured_results) >= top_n:
                break

        return structured_results

    return synthesize_answer(ranked[0].payload, roles, collection)

def synthesize_answer(payload, roles, collection_name):
    if DEBUG:
        print("🔥 SYNTHESIZER V2 ACTIVE")

    labels = get_display_labels(collection_name)
    identifier_label = labels["identifier"]
    primary_name_label = labels["primary_name"]
    enum_value_label = labels["enum_value"]
    enum_name_label = labels["enum_name"]

    identifier = payload.get("identifier")
    name = payload.get("primary_name")
    description = payload.get("description")
    enums = payload.get("enum_values")
    doc_type = payload.get("doc_type")
    source_label = get_source_label(collection_name, payload)
    source_type = str(payload.get("source_type") or "").lower()

    if DEBUG:
        print("ENUMS:", enums)

    # =========================
    # 1. ENUM REQUEST
    # =========================
    if "enum_value" in roles:
        if enums:
            values = []

            for e in enums:
                if isinstance(e, dict):
                    val = e.get("Value") or e.get(enum_value_label) or e.get("value")
                    label = e.get("SymbolicName") or e.get(enum_name_label) or e.get("name")

                    if val and label:
                        values.append(f"{val}={label}")
                    elif val:
                        values.append(str(val))
                    elif label:
                        values.append(str(label))
                else:
                    values.append(str(e))

            preview = ", ".join(values[:30])
            if len(values) > 30:
                preview += ", ..."

            if identifier and name:
                return f"Allowed values for {identifier_label} {identifier} ({name}) are: {preview}"
            return f"Allowed values are: {preview}"

        return "No enumerated values found."

    # =========================
    # 2. DESCRIPTION REQUEST
    # =========================
    if "description" in roles:
        if identifier and name:
            return f"{identifier_label} {identifier} ({name}): {description}"
        if name:
            return f"{name}: {description}"
        return description or "No description available."

    # =========================
    # 3. ENTITY-ROW / ARTICLE STYLE
    # =========================
    if doc_type == "entity_row":
        parts = [f"Source: {source_label}"]

        if name:
            parts.append(name)

        if description:
            parts.append(description)

        related_titles = payload.get("related_titles") or []
        if related_titles:
            related_preview = "\n".join(f"- {t}" for t in related_titles[:5])
            parts.append(f"Related articles:\n{related_preview}")

        return "\n\n".join(parts) if parts else "No answer found."

    # =========================
    # 4. CHUNKED DOCUMENT STYLE
    # =========================
    is_chunked_doc_like = (
        bool(payload.get("section_heading") or payload.get("block_types"))
        and doc_type != "entity_row"
        and str(payload.get("file_type") or "").lower() != "image"
        and source_type not in ["image", "standalone_image"]
    )

    if is_chunked_doc_like:
        parts = [f"Source: {source_label}"]

        if name:
            parts.append(name)

        desc_text = str(description or payload.get("text") or "").strip()

        if desc_text:
            lines = [ln.rstrip() for ln in desc_text.splitlines()]

            cleaned = []
            title_norm = normalize_simple_text(name)
            last_heading_norm = None
            heading_like_norms = {"steps to resolve"}

            for ln in lines:
                stripped = ln.strip()
                if not stripped:
                    cleaned.append("")
                    continue

                stripped_norm = normalize_simple_text(stripped)

                if stripped_norm == title_norm:
                    continue

                if title_norm and stripped_norm == f"title {title_norm}":
                    continue

                if stripped.lower().startswith("title:"):
                    maybe_title = stripped.split(":", 1)[1].strip()
                    if normalize_simple_text(maybe_title) == title_norm:
                        continue

                is_heading_like = (
                    stripped_norm in heading_like_norms
                    or stripped.endswith(":")
                )

                if is_heading_like:
                    if stripped_norm == last_heading_norm:
                        continue
                    last_heading_norm = stripped_norm

                cleaned.append(stripped)

            desc_text = "\n".join(cleaned).strip()

            if desc_text:
                parts.append(desc_text)

        related_titles = payload.get("related_titles") or []
        if related_titles:
            related_preview = "\n".join(f"- {t}" for t in related_titles[:5])
            parts.append(f"Related notes:\n{related_preview}")

        return "\n\n".join(parts) if parts else "No answer found."

    # =========================
    # 5. IMAGE STYLE
    # =========================
    if source_type in ["image", "standalone_image"]:
        parts = [f"Source: {source_label}"]

        file_name = payload.get("file_name") or payload.get("source_file")
        doc_type = payload.get("doc_type")
        image_mode = payload.get("image_mode")
        ocr_text = payload.get("ocr_text") or payload.get("text") or ""
        caption = payload.get("caption") or ""

        if file_name:
            parts.append(f"Image: {file_name}")

        meta_bits = []
        if payload.get("format"):
            meta_bits.append(f"format={payload.get('format')}")
        if payload.get("width"):
            meta_bits.append(f"width={payload.get('width')}")
        if payload.get("height"):
            meta_bits.append(f"height={payload.get('height')}")

        summary_bits = []
        if doc_type:
            summary_bits.append(f"doc_type={doc_type}")
        if image_mode:
            summary_bits.append(f"image_mode={image_mode}")

        if summary_bits:
            parts.append("Image classification: " + ", ".join(summary_bits))

        if meta_bits:
            parts.append("Image metadata: " + ", ".join(meta_bits))

        if caption:
            parts.append(caption)

        if ocr_text:
            parts.append(ocr_text[:1500].strip())

        return "\n\n".join(parts) if parts else "No answer found."

    # =========================
    # 6. DEFAULT STRUCTURED STYLE
    # =========================
    parts = [f"Source: {source_label}"]

    if identifier and name:
        parts.append(f"The {primary_name_label} with {identifier_label} {identifier} is {name}.")
    elif name:
        parts.append(f"{primary_name_label}: {name}.")
    elif identifier:
        parts.append(f"{identifier_label}: {identifier}.")

    if description:
        parts.append(f"Its description is: {description.rstrip('.')}.")

    return " ".join(parts) if parts else "No answer found."

def build_answer(points, roles):

    if DEBUG:
        print("🔥 BUILD ANSWER ACTIVE")

    if not points:
        return "No answer found"

    p = points[0].payload

    if DEBUG:
        print("DEBUG PAYLOAD:", p)

    if roles:
        parts = []

        if "identifier" in p and "primary_name" in p:
            parts.append(f"Tag {p['identifier']} is {p['primary_name']}")

        if "description" in roles and "description" in p:
            parts.append(p["description"])

        if "enum_value" in roles and p.get("enum_values"):
            enum_texts = []

            for e in p["enum_values"]:
                if isinstance(e, dict):
                    key = list(e.keys())
                    val = list(e.values())

                    if len(val) >= 2:
                        enum_texts.append(f"{val[0]}={val[1]}")
                    else:
                        enum_texts.append(str(val[0]))
                else:
                    enum_texts.append(str(e))

            parts.append(" | ".join(enum_texts))

        return " | ".join(parts)

    parts = []

    if "identifier" in p and "primary_name" in p:
        parts.append(f"Tag {p['identifier']} is {p['primary_name']}")
    elif "identifier" in p:
        parts.append(f"{p['identifier']}")
    elif "primary_name" in p:
        parts.append(f"{p['primary_name']}")

    # description
    if "description" in p:
        parts.append(f"{p['description']}")

    # enum values
    if p.get("enum_values"):
        enum_texts = []

        for e in p["enum_values"]:
            if isinstance(e, dict):
                enum_texts.append(", ".join(str(v) for v in e.values()))
            else:
                enum_texts.append(str(e))

        parts.append(" | ".join(enum_texts))

    return " | ".join(parts)
