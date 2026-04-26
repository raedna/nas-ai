from pathlib import Path

from core.nlp_generator import (
    build_structured_nlp_text,
    build_entity_row_nlp_text,
    build_procedural_nlp_text,
)

DEBUG = True


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
    primary_name = _first_value(row_n, name_fields)
    description = _first_value(row_n, desc_fields)
    aliases = _all_values(row_n, alias_fields)
    type_value = _first_value(row_n, type_fields)

    text = build_structured_nlp_text(row, schema)

    if not text:
        return None

    return {
        "text": text,
        "identifier": identifier or None,
        "primary_name": primary_name or None,
        "description": description or None,
        "enum_values": [],
        "type": type_value or "structured",
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

    identifier = _first_value(row_n, id_fields)
    primary_name = _first_value(row_n, name_fields)
    description_values = _all_values(row_n, desc_fields)
    aliases = _all_values(row_n, alias_fields)
    type_value = _first_value(row_n, type_fields)

    text = build_entity_row_nlp_text(row, schema)

    if not text:
        return None

    return {
        "text": text,
        "identifier": identifier or None,
        "primary_name": primary_name or None,
        "description": "\n\n".join(description_values) if description_values else None,
        "enum_values": [],
        "type": type_value or "entity_row",
        "source_file": str(source_file),
        "doc_type": "entity_row",
        "aliases": aliases
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
    primary_name = _first_value(row_n, name_fields)
    description_values = _all_values(row_n, desc_fields)

    text = build_procedural_nlp_text(row, schema)

    if not text:
        return None

    return {
        "text": text,
        "identifier": identifier,
        "primary_name": primary_name or None,
        "description": "\n\n".join(description_values) if description_values else None,
        "enum_values": [],
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
            docs.append(doc)

    return docs


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
    table_type = detect_table_type(rows, schema)

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

    for d in docs:
        d.update(file_tags)

    return docs