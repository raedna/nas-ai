from core.query_helpers import (
    infer_doc_type,
    load_doc_query_hints,
    normalize_simple_text,
    expand_terms_with_synonyms,
)


def extract_negative_terms(question: str):
    import re

    q = normalize_simple_text(question)

    patterns = [
        r"\bnot\s+([a-z0-9_]+(?:\s+[a-z0-9_]+){0,2})",
        r"\bexcluding\s+([a-z0-9_]+(?:\s+[a-z0-9_]+){0,2})",
        r"\bexclude\s+([a-z0-9_]+(?:\s+[a-z0-9_]+){0,2})",
        r"\bwithout\s+([a-z0-9_]+(?:\s+[a-z0-9_]+){0,2})",
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
    import re

    q = normalize_simple_text(question)

    q = re.sub(r"\bnot\s+[a-z0-9_]+(?:\s+[a-z0-9_]+){0,2}", " ", q)
    q = re.sub(r"\bexcluding\s+[a-z0-9_]+(?:\s+[a-z0-9_]+){0,2}", " ", q)
    q = re.sub(r"\bexclude\s+[a-z0-9_]+(?:\s+[a-z0-9_]+){0,2}", " ", q)
    q = re.sub(r"\bwithout\s+[a-z0-9_]+(?:\s+[a-z0-9_]+){0,2}", " ", q)

    q = re.sub(r"\s+", " ", q).strip()
    return q


def contains_negative_term(text: str, negative_terms):
    text_norm = normalize_simple_text(text)

    for term in negative_terms:
        if term == text_norm:
            return True
        if term in text_norm:
            return True

    return False


def score_point_shared_debug(p, question):
    q = str(question or "").lower().strip()
    positive_q = remove_negative_terms_from_question(q)
    negative_terms = extract_negative_terms(q)

    payload = p.payload or {}

    name = str(payload.get("primary_name") or "").lower()
    desc = str(payload.get("description") or "").lower()
    doc_type = infer_doc_type(payload)

    base_score = getattr(p, "score", None)
    if base_score is None:
        base_score = 0.0

    score = float(base_score)
    components = []

    words = [w for w in normalize_simple_text(positive_q).split() if w]

    for word in words:
        if word in name:
            score += 1.5
            components.append(f"+1.5 name contains '{word}'")

    for word in words:
        if word in desc:
            score += 0.5
            components.append(f"+0.5 description contains '{word}'")

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
            score += 20.0
            components.append("+20.0 structured exact name match")
        elif exact_query and normalized_name.startswith(exact_query + " "):
            score += 3.0
            components.append("+3.0 structured name starts with query")
        elif exact_query and normalized_name.endswith(" " + exact_query):
            score += 3.0
            components.append("+3.0 structured name ends with query")
        elif exact_query and exact_query in normalized_name:
            score += 1.5
            components.append("+1.5 structured query inside name")

        if exact_query and exact_query in normalized_desc:
            score += 2.0
            components.append("+2.0 structured query inside description")

        exact_name_hits = sum(1 for w in meaningful_words if w in normalized_name)
        exact_desc_hits = sum(1 for w in meaningful_words if w in normalized_desc)

        if exact_name_hits:
            boost = exact_name_hits * 2.5
            score += boost
            components.append(f"+{boost:.1f} structured name term hits ({exact_name_hits})")

        if exact_desc_hits:
            boost = exact_desc_hits * 0.6
            score += boost
            components.append(f"+{boost:.1f} structured description term hits ({exact_desc_hits})")

        if meaningful_words and all(w in normalized_name for w in meaningful_words):
            score += 6.0
            components.append("+6.0 structured all terms in name")
        elif meaningful_words and all(w in combined for w in meaningful_words):
            score += 2.0
            components.append("+2.0 structured all terms in name/description")

        expanded_name_hits = sum(1 for w in expanded_words if w in normalized_name)
        expanded_desc_hits = sum(1 for w in expanded_words if w in normalized_desc)

        if expanded_name_hits:
            boost = expanded_name_hits * 0.8
            score += boost
            components.append(f"+{boost:.1f} structured expanded name hits ({expanded_name_hits})")

        if expanded_desc_hits:
            boost = expanded_desc_hits * 0.2
            score += boost
            components.append(f"+{boost:.1f} structured expanded desc hits ({expanded_desc_hits})")

    if doc_type == "entity_row":
        hints = load_doc_query_hints()
        stopwords = set(hints.get("stopwords", []))

        meaningful_words = [w for w in words if w not in stopwords]
        normalized_name = normalize_simple_text(name)
        normalized_desc = normalize_simple_text(desc)

        exact_query = " ".join(meaningful_words).strip()

        if exact_query and normalized_name == exact_query:
            score += 6.0
            components.append("+6.0 entity exact name match")
        elif exact_query and exact_query in normalized_name:
            score += 2.0
            components.append("+2.0 entity query inside name")

        exact_name_hits = sum(1 for w in meaningful_words if w in normalized_name)
        if exact_name_hits:
            boost = exact_name_hits * 0.8
            score += boost
            components.append(f"+{boost:.1f} entity name term hits ({exact_name_hits})")

        if negative_terms:
            if contains_negative_term(name, negative_terms):
                score -= 50.0
                components.append("-50.0 negative term in name")
            if contains_negative_term(desc, negative_terms):
                score -= 20.0
                components.append("-20.0 negative term in description")

    return {
        "base_score": base_score,
        "final_score": score,
        "components": components,
        "reason": "; ".join(components),
    }