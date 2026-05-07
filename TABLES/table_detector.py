def _clean_type(value):
    value = str(value or "").strip().lower()
    allowed = {"structured", "entity_row", "procedural"}
    return value if value in allowed else ""

def _description_stats(rows, desc_fields, sample_limit=50):
    lengths = []

    for row in rows[:sample_limit]:
        row_norm = {str(k).lower(): v for k, v in row.items()}

        text_parts = []
        for f in desc_fields:
            v = row_norm.get(str(f).lower())
            if v not in [None, ""]:
                text_parts.append(str(v).strip())

        text = " ".join(text_parts).strip()
        if text:
            lengths.append(len(text))

    if not lengths:
        return {"median": 0, "max": 0}

    lengths = sorted(lengths)
    mid = len(lengths) // 2

    if len(lengths) % 2:
        median = lengths[mid]
    else:
        median = (lengths[mid - 1] + lengths[mid]) / 2

    return {
        "median": median,
        "max": max(lengths),
    }


def detect_table_type(rows, schema, template_config=None):
    """
    Detect table type without source-specific column names.

    Priority:
    1. Explicit template override
    2. Explicit schema override
    3. Generic schema-role inference
    """

    if not rows:
        return "procedural"

    template_config = template_config or {}
    schema = schema or {}

    # 1. Template-level override
    explicit_type = _clean_type(
        template_config.get("table_type")
        or template_config.get("doc_type")
    )
    if explicit_type:
        return explicit_type

    # 2. Schema-level override
    explicit_schema_type = _clean_type(
        schema.get("table_type")
        or schema.get("doc_type")
        or schema.get("structured_subtype")
    )
    if explicit_schema_type:
        return explicit_schema_type

    id_fields = schema.get("identifier", []) or []
    name_fields = schema.get("primary_name", []) or []
    desc_fields = schema.get("description", []) or []
    type_fields = schema.get("type", []) or []
    enum_value_fields = schema.get("enum_value", []) or []
    enum_name_fields = schema.get("enum_name", []) or []
    reference_fields = schema.get("reference_identifier", []) or []

    has_identifier = bool(id_fields)
    has_name = bool(name_fields)
    has_description = bool(desc_fields)
    has_type = bool(type_fields)
    has_enum = bool(enum_value_fields or enum_name_fields)
    has_reference = bool(reference_fields)

    # No meaningful roles found
    if not (has_identifier or has_name or has_description):
        return "procedural"

    # Dictionary/reference/code-list style
    if has_identifier and has_name and (has_description or has_type) and (has_enum or has_reference):
        return "structured"

    if has_identifier and has_name and has_description:
        stats = _description_stats(rows, desc_fields)

        if stats["median"] >= 500 or stats["max"] >= 1500:
            return "entity_row"

        return "structured"

    # One row represents a document/article/entity
    if has_name and has_description and not has_enum:
        return "entity_row"

    # Identifier + description without a strong name often behaves like procedural/reference notes
    if has_identifier and has_description and not has_name:
        return "entity_row"

    # Fallback: meaningful row-level structure
    return "entity_row"