def normalize_link_index(link_index, schema=None):
    """
    Converts link_index into normalized documents
    """

    normalized = []

    for identifier, entry in link_index.get("identifier", {}).items():

        description = entry.get("description")
        primary_name = entry.get("primary_name")

        doc = {
            "identifier": str(entry.get("identifier") or identifier).split(":", 1)[-1],
            "identifier_field": entry.get("identifier_field"),
            "identifier_namespace": entry.get("identifier_namespace"),
            "primary_name": str(primary_name).strip() if primary_name else None,
            "description": str(description).strip() if description else None,
            "aliases": entry.get("aliases", []),
            "enum_values": entry.get("enum_values", []),
            "type": entry.get("type"),
            "raw": entry
        }

        normalized.append(doc)

    return normalized

def filter_rows(rows, schema, rules=None):
    """
    Generic row filtering (schema-aware)
    """

    if not rules:
        return rows

    filtered = []

    exclude_rules = rules.get("exclude_if", {})
    required_roles = rules.get("require_non_empty", [])

    for row in rows:

        row_norm = {k.lower(): v for k, v in row.items()}

        # --- EXCLUDE ---
        skip = False
        for field, bad_values in exclude_rules.items():
            val = row_norm.get(field.lower())

            if val is not None and str(val).strip() in bad_values:
                skip = True
                break

        if skip:
            continue

        # --- REQUIRED ---
        if required_roles:

            missing = False

            for role in required_roles:
                fields = schema.get(role, [])

                if not any(row_norm.get(f.lower()) for f in fields):
                    missing = True
                    break

            if missing:
                continue

        filtered.append(row)

    return filtered

def deduplicate_rows(rows, schema):
    """
    Remove duplicates using identifier if available,
    otherwise fallback to full row hash
    """

    seen = set()
    deduped = []

    id_fields = schema.get("identifier", [])

    for row in rows:

        row_norm = {k.lower(): v for k, v in row.items()}

        key = None

        # try identifier first
        for f in id_fields:
            val = row_norm.get(f.lower())
            if val:
                key = f"id::{val}"
                break

        # fallback → full row
        if not key:
            key = tuple(sorted(row_norm.items()))

        if key in seen:
            continue

        seen.add(key)
        deduped.append(row)

    return deduped

def normalize_rows(rows, schema):
    """
    Normalize table rows (KB, BBG, etc.)
    Currently supports entity-row (KB style)
    """

    normalized = []

    id_fields = schema.get("identifier", [])
    name_fields = schema.get("primary_name", [])
    desc_fields = schema.get("description", [])
    type_fields = schema.get("type", [])
    alias_fields = schema.get("aliases", [])

    for row in rows:

        row_norm = {k.lower(): v for k, v in row.items()}

        # --- IDENTIFIER ---
        identifier = None
        for f in id_fields:
            val = row_norm.get(f.lower())
            if val:
                identifier = str(val).strip()
                break

        # --- PRIMARY NAME ---
        primary_name = None
        for f in name_fields:
            val = row_norm.get(f.lower())
            if val:
                primary_name = str(val).strip()
                break

        # --- DESCRIPTION (merge all) ---
        descriptions = []
        for f in desc_fields:
            val = row_norm.get(f.lower())
            if val:
                descriptions.append(str(val).strip())

        description = "\n\n".join(descriptions) if descriptions else None

        # --- TYPE ---
        doc_type = None
        for f in type_fields:
            val = row_norm.get(f.lower())
            if val:
                doc_type = str(val).strip()
                break

        # --- ALIASES ---
        aliases = []
        for f in alias_fields:
            val = row_norm.get(f.lower())
            if val:
                val = str(val).strip()
                if val and val != primary_name:
                    aliases.append(val)

        # --- BUILD DOC ---
        doc = {
            "identifier": identifier,
            "primary_name": primary_name,
            "description": description,
            "aliases": aliases,
            "enum_values": [],   # not applicable for KB
            "type": doc_type,
            "raw": row
        }

        normalized.append(doc)

    return normalized

