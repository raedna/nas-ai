import re
import html as _html
from pathlib import Path

from core.nlp_generator import (
    build_structured_nlp_text,
    build_entity_row_nlp_text,
    build_procedural_nlp_text,
    clean_dedup_text,
)

DEBUG = True


def _parse_tag_list(values):
    """P4: split comma/semicolon/pipe tag strings into a clean, deduped list.
    Handles HTML entities (e.g. 'A&gt;B') and hierarchical 'A>B' tags.
    Input is the list of raw cell values mapped to the schema 'tags' role."""
    out, seen = [], set()
    for v in values or []:
        for part in re.split(r"[,;|]", _html.unescape(str(v))):
            t = part.strip()
            if t and t.lower() not in seen:
                seen.add(t.lower())
                out.append(t)
    return out


# =========================================================
# HELPERS
# =========================================================
def _row_norm(row):
    return {str(k).lower(): v for k, v in row.items()}


def _first_value(row_norm, fields):
    for f in fields:
        val = row_norm.get(str(f).lower())
        if val not in [None, ""]:
            return str(val).strip()
    return ""


def _all_values(row_norm, fields):
    values = []
    for f in fields:
        val = row_norm.get(str(f).lower())
        if val not in [None, ""]:
            values.append(str(val).strip())
    return values

def _labeled_values(row_norm, fields):
    """Return {original_field_name: value} for each non-empty field, preserving order."""
    labeled = {}
    for f in fields:
        val = row_norm.get(str(f).lower())
        if val not in [None, ""]:
            labeled[str(f)] = str(val).strip()
    return labeled


def _namespace_from_field(field):
    field = str(field or "").strip().lower()
    return "".join(ch for ch in field if ch.isalnum() or ch == "_")

def _apply_link_index_to_docs(docs, rows, schema, source_file):
    from core.link_index import build_link_index

    if not docs:
        return docs

    # Skip link index for large tables — O(n²) cross-referencing is too slow
    # Link index only makes sense for small structured reference datasets (FIX, RECON)
    if len(rows) > 2000:
        return docs

    # Only canonical structured docs should get identifier-based related links.
    # Source/generated identifiers are traceability IDs, not user-facing reference IDs.
    canonical_docs = [
        d for d in docs
        if d.get("doc_type") == "structured"
        and d.get("identifier_kind") == "canonical"
        and d.get("link_keys")
    ]

    if not canonical_docs:
        return docs

    link_index = build_link_index(
        {source_file: rows},
        {source_file: schema}
    )

    entries = link_index.get("identifier", {})

    for d in canonical_docs:
        related = set(d.get("related_link_keys") or [])

        for link_key in d.get("link_keys") or []:
            entry = entries.get(link_key) or {}
            for related_key in entry.get("related_link_keys") or []:
                if related_key not in d.get("link_keys", []):
                    related.add(related_key)

        d["related_link_keys"] = sorted(related)

    return docs


# =========================================================
# STRUCTURED TABLE DOC BUILDER
# =========================================================
def _build_structured_doc(row, schema, source_file):
    row_n = _row_norm(row)

    id_fields = schema.get("identifier", [])
    name_fields = schema.get("primary_name", [])
    desc_fields = schema.get("description", [])
    alias_fields = schema.get("aliases", [])
    type_fields = schema.get("type", [])

    identifier = _first_value(row_n, id_fields)
    identifier_field = id_fields[0] if id_fields else None
    identifier_namespace = _namespace_from_field(identifier_field)

    link_keys = []
    if identifier and identifier_namespace:
        link_keys.append(f"{identifier_namespace}:{identifier}")

    primary_name = _first_value(row_n, name_fields)
    description_values = _all_values(row_n, desc_fields)
    description = "\n\n".join(description_values) if description_values else None

    # Render ALL meaningful columns as labeled fields — not just the 'description' role.
    # A structured record lookup should surface every field of the record regardless of
    # how the (auto-inferred) schema classified it, so retrieval no longer depends on
    # perfect role assignment. Excludes the identifier and primary_name (shown elsewhere).
    _label_fields, _seen_lf = [], set()
    for _f in (list(desc_fields) + list(type_fields)
               + list(schema.get("reference_identifier", []))
               + list(schema.get("other", []))):
        if _f and _f not in _seen_lf and _f not in id_fields and _f not in name_fields:
            _seen_lf.add(_f)
            _label_fields.append(_f)
    description_fields = _labeled_values(row_n, _label_fields)
    aliases = _all_values(row_n, alias_fields)
    type_value = _first_value(row_n, type_fields)

    # Reference identifiers (schema role reference_identifier): keep them
    # searchable — payload list + a namespaced link_key per value, exactly like
    # the primary identifier. Without this they only exist inside the display
    # dict description_fields and no retrieval path can find them.
    ref_fields = schema.get("reference_identifier", [])
    reference_identifiers = []
    for _rf in ref_fields:
        _rv = _first_value(row_n, [_rf])
        if _rv:
            reference_identifiers.append(_rv)
            _rns = _namespace_from_field(_rf)
            if _rns:
                link_keys.append(f"{_rns}:{_rv}")

    text = build_structured_nlp_text(row, schema)

    if not text:
        return None

    return {
        "text": text,
        "identifier": identifier or None,
        "identifier_field": identifier_field,
        "identifier_namespace": identifier_namespace or None,
        "identifier_kind": "canonical",
        "primary_name": primary_name or None,
        "primary_name_field": (name_fields[0] if name_fields else None),
        "description": description or None,
        "description_fields": description_fields or None,
        "reference_identifiers": reference_identifiers,
        "enum_values": [],
        "link_keys": link_keys,
        "related_link_keys": [],
        "type": type_value or "structured",
        "type_field": (type_fields[0] if type_fields else None),
        "source_file": str(source_file),
        "doc_type": "structured",
        "aliases": aliases
    }


# =========================================================
# ENTITY-ROW TABLE DOC BUILDER
# =========================================================
def _build_entity_row_doc(row, schema, source_file):
    row_n = _row_norm(row)

    id_fields = schema.get("identifier", [])
    name_fields = schema.get("primary_name", [])
    desc_fields = schema.get("description", [])
    alias_fields = schema.get("aliases", [])
    type_fields = schema.get("type", [])
    tag_fields = schema.get("tags", [])

    identifier = _first_value(row_n, id_fields)
    identifier_field = id_fields[0] if id_fields else None
    identifier_namespace = _namespace_from_field(identifier_field)

    link_keys = []
    if identifier and identifier_namespace:
        link_keys.append(f"{identifier_namespace}:{identifier}")
    primary_name = _first_value(row_n, name_fields)
    description_values = _all_values(row_n, desc_fields)
    aliases = _all_values(row_n, alias_fields)
    type_value = _first_value(row_n, type_fields)
    kb_tags = _parse_tag_list(_all_values(row_n, tag_fields))  # P4: schema-driven tags

    text = build_entity_row_nlp_text(row, schema)

    if not text:
        return None

    return {
        "text": text,
        "identifier": identifier or None,
        "identifier_field": identifier_field,
        "identifier_namespace": identifier_namespace or None,
        "identifier_kind": "source",
        "primary_name": primary_name or None,
        "description": (clean_dedup_text(description_values) or None) if description_values else None,
        "enum_values": [],
        "link_keys": link_keys,
        "related_link_keys": [],
        "type": type_value or "entity_row",
        "source_file": str(source_file),
        "doc_type": "entity_row",
        "aliases": aliases,
        "tags": kb_tags,
    }


# =========================================================
# PROCEDURAL TABLE DOC BUILDER
# =========================================================
def _build_procedural_doc(row, schema, source_file, row_index):
    row_n = _row_norm(row)

    id_fields = schema.get("identifier", [])
    name_fields = schema.get("primary_name", [])
    desc_fields = schema.get("description", [])

    identifier = _first_value(row_n, id_fields) or f"row_{row_index}"
    identifier_field = id_fields[0] if id_fields else "row_index"
    identifier_namespace = _namespace_from_field(identifier_field)

    link_keys = []
    if identifier and identifier_namespace:
        link_keys.append(f"{identifier_namespace}:{identifier}")

    primary_name = _first_value(row_n, name_fields)
    description_values = _all_values(row_n, desc_fields)

    text = build_procedural_nlp_text(row, schema)

    if not text:
        return None

    return {
        "text": text,
        "identifier": identifier,
        "identifier_field": identifier_field,
        "identifier_namespace": identifier_namespace or None,
        "identifier_kind": "generated",
        "primary_name": primary_name or None,
        "description": "\n\n".join(description_values) if description_values else None,
        "enum_values": [],
        "link_keys": link_keys,
        "related_link_keys": [],
        "type": "procedural",
        "source_file": str(source_file),
        "doc_type": "procedural"
    }


# =========================================================
# MODE-SPECIFIC PROCESSORS
# =========================================================
def process_structured_table(rows, schema, source_file):
    docs = []

    for row in rows:
        doc = _build_structured_doc(row, schema, source_file)
        if doc:
            docs.append(doc)

    return docs


def process_entity_row_table(rows, schema, source_file):
    docs = []

    for row in rows:
        doc = _build_entity_row_doc(row, schema, source_file)
        if doc:
            docs.extend(_split_entity_row_doc(doc))

    return docs


def _split_entity_row_doc(doc):
    """P0: split a long entity-row doc into multiple chunks that fit under the embed
    window. All chunks keep the SAME identifier/primary_name/source_file/link_keys/
    kb_tags (storage id is seq-based, so they don't collide); each adds chunk_index/
    chunk_total and repeats the title so every chunk embeds self-contained.
    Short rows return a single unchanged doc."""
    from core.chunking import split_text

    text = doc.get("text") or ""
    chunks = split_text(text)
    if len(chunks) <= 1:
        return [doc]

    title = (doc.get("primary_name") or "").strip()
    out = []
    for i, body in enumerate(chunks):
        if title and not body.lstrip().startswith(title):
            body = f"{title}\n\n{body}"
        d = dict(doc)
        d["text"] = body
        d["chunk_index"] = i + 1
        d["chunk_total"] = len(chunks)
        out.append(d)
    return out


def process_procedural_table(rows, schema, source_file):
    docs = []

    for i, row in enumerate(rows, start=1):
        doc = _build_procedural_doc(row, schema, source_file, i)
        if doc:
            docs.append(doc)

    return docs


# =========================================================
# MAIN SERIALIZER ENTRYPOINT
# =========================================================
def table_serializer(parsed, file_path, template_config, file_tags, collection_name):
    from TABLES.table_detector import detect_table_type

    rows = parsed.get("rows", [])
    schema = parsed.get("schema")

    if not rows:
        return []

    if not schema:
        from TABLES.schema_inference_table import infer_table_schema

        schema = infer_table_schema(
            rows,
            collection_name=collection_name,
            source_file=Path(file_path).name
        )

    source_file = Path(file_path).name
    source_path = str(file_path)
    table_type = detect_table_type(rows, schema, template_config)
    
    if DEBUG:
        print(f"[TABLE SERIALIZER] {source_file} -> {table_type}")

    if table_type == "structured":
        docs = process_structured_table(rows, schema, source_file)
    elif table_type == "entity_row":
        docs = process_entity_row_table(rows, schema, source_file)
    elif table_type == "procedural":
        docs = process_procedural_table(rows, schema, source_file)
    else:
        raise ValueError(f"Unknown table type: {table_type}")

    docs = _apply_link_index_to_docs(docs, rows, schema, source_file)

    for d in docs:
        d["file_path"] = source_path
        d.update(file_tags)

    return docs