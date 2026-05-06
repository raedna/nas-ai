def _clean_type(value):
    value = str(value or "").strip().lower()
    allowed = {"structured", "entity_row", "procedural"}
    return value if value in allowed else ""


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

    # Strong reference/catalog style, but no enum/reference links
    if has_identifier and has_name and has_description and has_type:
        return "structured"

    # One row represents a document/article/entity
    if has_name and has_description and not has_enum:
        return "entity_row"

    # Identifier + description without a strong name often behaves like procedural/reference notes
    if has_identifier and has_description and not has_name:
        return "entity_row"

    # Fallback: meaningful row-level structure
    return "entity_row"