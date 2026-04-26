# =========================================================
# GENERIC HELPERS
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
# STRUCTURED NLP TEXT
# =========================================================
def build_structured_nlp_text(row, schema):
    row_n = _row_norm(row)

    id_fields = schema.get("identifier", [])
    name_fields = schema.get("primary_name", [])
    desc_fields = schema.get("description", [])
    alias_fields = schema.get("aliases", [])
    type_fields = schema.get("type", [])
    other_fields = schema.get("other", [])

    identifier = _first_value(row_n, id_fields)
    primary_name = _first_value(row_n, name_fields)
    description = _first_value(row_n, desc_fields)
    aliases = _all_values(row_n, alias_fields)
    type_value = _first_value(row_n, type_fields)

    parts = []

    if primary_name:
        parts.append(primary_name)
    elif identifier:
        parts.append(identifier)

    if description:
        parts.append(description)

    if type_value:
        parts.append(f"Type: {type_value}")

    # optional useful "other" fields such as category
    other_lines = []
    for f in other_fields:
        val = row_n.get(str(f).lower())
        if val not in [None, ""]:
            other_lines.append(f"{f}: {str(val).strip()}")

    if other_lines:
        parts.append("\n".join(other_lines))

    if aliases:
        parts.append(f"Also known as: {', '.join(aliases)}")

    return "\n\n".join([p for p in parts if p]).strip()


# =========================================================
# ENTITY-ROW NLP TEXT
# =========================================================
def build_entity_row_nlp_text(row, schema):
    row_n = _row_norm(row)

    name_fields = schema.get("primary_name", [])
    desc_fields = schema.get("description", [])

    primary_name = _first_value(row_n, name_fields)
    description_values = _all_values(row_n, desc_fields)

    parts = []

    if primary_name:
        parts.append(primary_name)

    if description_values:
        parts.append("\n\n".join(description_values))

    return "\n\n".join([p for p in parts if p]).strip()


# =========================================================
# PROCEDURAL NLP TEXT
# =========================================================
def build_procedural_nlp_text(row, schema):
    row_n = _row_norm(row)

    name_fields = schema.get("primary_name", [])
    desc_fields = schema.get("description", [])

    primary_name = _first_value(row_n, name_fields)
    description_values = _all_values(row_n, desc_fields)

    parts = []

    if primary_name:
        parts.append(primary_name)

    if description_values:
        parts.append("\n\n".join(description_values))

    if not parts:
        fallback = []
        for k, v in row.items():
            if v not in [None, ""]:
                fallback.append(f"{k}: {str(v).strip()}")
        if fallback:
            parts.append("\n".join(fallback))

    return "\n\n".join([p for p in parts if p]).strip()