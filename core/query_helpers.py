import json
import re
from pathlib import Path


DOC_QUERY_HINTS_PATH = Path("config/doc_query_hints.json")


def infer_doc_type(payload):
    explicit = str(payload.get("doc_type") or "").strip().lower()
    if explicit:
        return explicit

    identifier = payload.get("identifier")
    primary_name = payload.get("primary_name")
    enum_values = payload.get("enum_values")

    # infer structured rows like FIX/XML/BBG entries
    if identifier not in [None, ""] and primary_name not in [None, ""]:
        return "structured"

    if enum_values:
        return "structured"

    return ""


def load_doc_query_hints():
    if DOC_QUERY_HINTS_PATH.exists():
        with open(DOC_QUERY_HINTS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def normalize_simple_text(text):
    text = str(text or "").lower().strip()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text


def expand_terms_with_synonyms(words):
    synonyms = load_synonyms()
    expanded = []

    for word in words:
        w = str(word).strip().lower()
        if not w:
            continue

        expanded.append(w)

        for s in synonyms.get(w, []):
            s_norm = str(s).strip().lower()
            if s_norm:
                expanded.append(s_norm)

    seen = set()
    deduped = []
    for w in expanded:
        if w not in seen:
            seen.add(w)
            deduped.append(w)

    return deduped


def load_synonyms():
        path = Path("config/synonyms.json")
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

