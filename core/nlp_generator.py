DEBUG = False

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

if DEBUG:
    print("[NLP_GENERATOR LOADED FROM]", __file__)

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

    other_fields = schema.get("other", [])
    alias_fields = schema.get("aliases", [])

    other_values = _all_values(row_n, other_fields)
    alias_values = _all_values(row_n, alias_fields)

    parts = []

    if primary_name:
        parts.append(primary_name)

    if description_values:
        parts.append("\n\n".join(description_values))

    if other_values:
        parts.append("\n".join(str(v) for v in other_values if v))

    if alias_values:
        parts.append("Also known as: " + ", ".join(alias_values))

    return "\n\n".join([p for p in parts if p]).strip()


# =========================================================
# ENTITY-ROW NLP TEXT
# =========================================================
def build_entity_row_nlp_text(row, schema):
    row_n = _row_norm(row)
    name_fields = schema.get("primary_name", [])
    desc_fields = schema.get("description", [])
    other_fields = schema.get("other", [])
    alias_fields = schema.get("aliases", [])

    primary_name = _first_value(row_n, name_fields)
    description_values = _all_values(row_n, desc_fields)
    other_values = _all_values(row_n, other_fields)
    alias_values = _all_values(row_n, alias_fields)

    
    if DEBUG:
        if '21R2 Weekend' in str(row_n.get('abstract', '')):
            print(f"[NLP DEBUG] row keys={list(row_n.keys())[:5]} other_values={_all_values(row_n, schema.get('other', []))}")

    parts = []

    if primary_name:
        parts.append(primary_name)

    if description_values:
        parts.append("\n\n".join(description_values))

    if other_values:
        # Only include short non-HTML values (tags, categories) — skip HTML blobs
        clean_others = [
            str(v) for v in other_values
            if v and len(str(v)) < 200 and '<' not in str(v)
        ]
        if clean_others:
            parts.append("\n".join(clean_others))

    if alias_values:
        parts.append("Also known as: " + ", ".join(alias_values))

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