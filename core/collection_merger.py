# =========================================================
# COLLECTION MERGER
# =========================================================

DEBUG = True
DEBUG1 = False

from pathlib import Path
import re

from core.link_index import build_link_index


# =========================================================
# HELPERS
# =========================================================
def _group_docs_by_type(docs):
    grouped = {
        "structured": [],
        "entity_row": [],
        "procedural": [],
        "unknown": []
    }

    for doc in docs:
        doc_type = doc.get("doc_type")
        source_type = str(doc.get("source_type") or "").lower()
        file_type = str(doc.get("file_type") or "").lower()

        # keep image docs out of structured merge logic
        if source_type in ["image", "standalone_image"] or file_type == "image":
            grouped["unknown"].append(doc)
        elif doc_type == "structured":
            grouped["structured"].append(doc)
        elif doc_type == "entity_row":
            grouped["entity_row"].append(doc)
        elif doc_type == "procedural":
            grouped["procedural"].append(doc)
        else:
            grouped["unknown"].append(doc)

    return grouped


# =========================================================
# STRUCTURED DOC -> ROW ADAPTER
# =========================================================
def _structured_docs_to_rows(docs):
    all_rows = {}
    schema_map = {}

    for doc in docs:
        source_file = doc.get("source_file", "unknown_source")

        row = {
            "identifier": doc.get("identifier"),
            "primary_name": doc.get("primary_name"),
            "description": doc.get("description"),
            "type": doc.get("type")
        }

        if source_file not in all_rows:
            all_rows[source_file] = []

        all_rows[source_file].append(row)

        schema_map[source_file] = {
            "identifier": ["identifier"],
            "primary_name": ["primary_name"],
            "aliases": [],
            "description": ["description"],
            "type": ["type"],
            "enum_value": [],
            "enum_name": [],
            "other": []
        }

    return all_rows, schema_map


# =========================================================
# MERGE STRUCTURED DOCS
# =========================================================
def merge_structured_docs(docs):
    if not docs:
        return []

    docs_with_identifier = []
    docs_without_identifier = []

    for doc in docs:
        identifier = doc.get("identifier")
        if identifier not in [None, ""]:
            docs_with_identifier.append(doc)
        else:
            docs_without_identifier.append(doc)

    if len(docs_with_identifier) <= 1:
        return docs

    merged = []

    all_rows, schema_map = _structured_docs_to_rows(docs_with_identifier)
    link_index = build_link_index(all_rows, schema_map)

        for identifier, entry in link_index.get("identifier", {}).items():
            text_parts = []

            if entry.get("primary_name"):
                text_parts.append(entry["primary_name"])

            if entry.get("description"):
                text_parts.append(entry["description"])

            if entry.get("type"):
                text_parts.append(f"Type: {entry['type']}")

            text = "\n\n".join([p for p in text_parts if p]).strip()

            merged.append({
                "text": text,
                "identifier": identifier,
                "primary_name": entry.get("primary_name"),
                "description": entry.get("description"),
                "enum_values": entry.get("enum_values", []),
                "type": entry.get("type"),
                "doc_type": "structured",
                "source_files": entry.get("source_files", []),
                "related_identifiers": entry.get("related_identifiers", []),
                "source_file": source_file,
                "source_files": source_files,
                "source_type": "structured_merge",
                "file_type": "structured"
            })

    # preserve structured docs that do not have identifiers
    merged.extend(docs_without_identifier)

    return merged


# =========================================================
# ENTITY-ROW MERGE HELPERS
# =========================================================
def _normalize_text_for_match(text):
    if not text:
        return ""

    text = str(text).lower().strip()
    text = re.sub(r"[\r\n\t]+", " ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)

    return text


def _token_set(text):
    norm = _normalize_text_for_match(text)
    if not norm:
        return set()

    stop_tokens = {
        "runbook", "id", "article", "ticket", "checklist",
        "draft", "created", "from", "for", "the", "and",
        "or", "of", "to", "in", "on", "a", "an"
    }

    tokens = set()

    for tok in norm.split():
        if tok.isdigit():
            continue
        if len(tok) <= 2:
            continue
        if tok in stop_tokens:
            continue
        tokens.add(tok)

    return tokens


def _jaccard_similarity(a, b):
    if not a or not b:
        return 0.0

    inter = a & b
    union = a | b

    if not union:
        return 0.0

    return len(inter) / len(union)


def _entity_row_merge_key(doc):
    title = doc.get("primary_name") or ""
    return _normalize_text_for_match(title)


def _should_merge_entity_docs(doc1, doc2):
    key1 = _entity_row_merge_key(doc1)
    key2 = _entity_row_merge_key(doc2)

    if key1 and key2 and key1 == key2:
        return True

    title_tokens_1 = _token_set(doc1.get("primary_name", ""))
    title_tokens_2 = _token_set(doc2.get("primary_name", ""))
    desc_tokens_1 = _token_set(doc1.get("description", ""))
    desc_tokens_2 = _token_set(doc2.get("description", ""))

    title_sim = _jaccard_similarity(title_tokens_1, title_tokens_2)
    desc_sim = _jaccard_similarity(desc_tokens_1, desc_tokens_2)

    if title_sim >= 0.4:
        return True

    if title_sim >= 0.25 and desc_sim >= 0.15:
        return True

    return False


def _merge_text_blocks(text1, text2):
    t1 = (text1 or "").strip()
    t2 = (text2 or "").strip()

    if not t1:
        return t2
    if not t2:
        return t1

    n1 = _normalize_text_for_match(t1)
    n2 = _normalize_text_for_match(t2)

    if n1 == n2:
        return t1

    if n2 in n1:
        return t1

    if n1 in n2:
        return t2

    return f"{t1}\n\n---\n\n{t2}"


def _merge_list_unique(left, right):
    merged = []
    seen = set()

    for val in (left or []) + (right or []):
        key = str(val).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        merged.append(val)

    return merged


# =========================================================
# MERGE ENTITY-ROW DOCS
# =========================================================
def merge_entity_row_docs(docs):
    if not docs:
        return []

    linked_docs = []

    for i, doc in enumerate(docs):
        current = dict(doc)

        current_id = current.get("identifier")
        current_title = current.get("primary_name")

        related_docs = []
        related_titles = []

        for j, other in enumerate(docs):
            if i == j:
                continue

            if _should_merge_entity_docs(current, other):
                other_id = other.get("identifier")
                other_title = other.get("primary_name")

                if other_id and other_id != current_id:
                    related_docs.append(other_id)

                if other_title and other_title != current_title:
                    related_titles.append(other_title)

        current["related_docs"] = _merge_list_unique(
            current.get("related_docs", []),
            related_docs
        )

        current["related_titles"] = _merge_list_unique(
            current.get("related_titles", []),
            related_titles
        )

        if DEBUG1:
            print("LINK DEBUG:", current.get("identifier"), current.get("primary_name"))
            print("  related_docs:", current.get("related_docs"))
            print("  related_titles:", current.get("related_titles"))

        linked_docs.append(current)

    return linked_docs


# =========================================================
# MERGE PROCEDURAL DOCS
# =========================================================
def merge_procedural_docs(docs):
    return docs


# =========================================================
# IMAGE / EMBEDDED-IMAGE LINKING
# =========================================================
def _norm_path(value):
    if not value:
        return ""
    try:
        return str(Path(value)).strip().lower()
    except Exception:
        return str(value).strip().lower()


def _norm_name(value):
    if not value:
        return ""
    return Path(str(value).strip()).name.lower()


def _is_standalone_image_doc(doc):
    source_type = str(doc.get("source_type") or "").lower()
    file_type = str(doc.get("file_type") or "").lower()
    return source_type in ["image", "standalone_image"] or file_type == "image"


def _has_embedded_image_context(doc):
    return bool(doc.get("has_embedded_image_ocr"))


def _link_image_related_docs(docs):
    if not docs:
        return []

    linked = [dict(doc) for doc in docs]

    image_docs = [doc for doc in linked if _is_standalone_image_doc(doc)]
    embedded_docs = [doc for doc in linked if _has_embedded_image_context(doc)]

    for doc in linked:
        doc.setdefault("related_source_files", [])
        doc.setdefault("related_file_paths", [])
        doc.setdefault("related_image_targets", [])

    # link standalone image docs <-> doc chunks with embedded image OCR
    for img_doc in image_docs:
        img_name = _norm_name(img_doc.get("file_name") or img_doc.get("source_file"))
        img_path = _norm_path(img_doc.get("file_path"))

        for doc_chunk in embedded_docs:
            targets = [_norm_name(x) for x in (doc_chunk.get("embedded_image_targets") or [])]
            paths = [_norm_path(x) for x in (doc_chunk.get("embedded_image_paths") or [])]

            matched = False

            if img_name and img_name in targets:
                matched = True

            if not matched and img_path and img_path in paths:
                matched = True

            if not matched:
                continue

            # enrich image doc
            source_file = doc_chunk.get("source_file")
            if source_file:
                img_doc["related_source_files"] = _merge_list_unique(
                    img_doc.get("related_source_files", []),
                    [source_file]
                )

            doc_source_path = doc_chunk.get("source_path") or doc_chunk.get("file_path")
            if doc_source_path:
                img_doc["related_file_paths"] = _merge_list_unique(
                    img_doc.get("related_file_paths", []),
                    [doc_source_path]
                )

            img_doc["related_image_targets"] = _merge_list_unique(
                img_doc.get("related_image_targets", []),
                doc_chunk.get("embedded_image_targets", [])
            )

            # enrich doc chunk
            image_source_file = img_doc.get("source_file") or img_doc.get("file_name")
            if image_source_file:
                doc_chunk["related_source_files"] = _merge_list_unique(
                    doc_chunk.get("related_source_files", []),
                    [image_source_file]
                )

            image_file_path = img_doc.get("file_path")
            if image_file_path:
                doc_chunk["related_file_paths"] = _merge_list_unique(
                    doc_chunk.get("related_file_paths", []),
                    [image_file_path]
                )

            image_target = img_doc.get("file_name") or img_doc.get("source_file")
            if image_target:
                doc_chunk["related_image_targets"] = _merge_list_unique(
                    doc_chunk.get("related_image_targets", []),
                    [image_target]
                )

    return linked


# =========================================================
# MAIN ENTRYPOINT
# =========================================================
def merge_collection_docs(docs):
    grouped = _group_docs_by_type(docs)

    merged = []
    merged.extend(merge_structured_docs(grouped["structured"]))
    merged.extend(merge_entity_row_docs(grouped["entity_row"]))
    merged.extend(merge_procedural_docs(grouped["procedural"]))
    merged.extend(grouped["unknown"])

    # phase-1 post-processing links only
    merged = _link_image_related_docs(merged)

    return merged