def detect_table_type(rows, schema):
    """
    Detect table type:
    - structured: dictionary/catalog style rows (e.g. BBG fields)
    - entity_row: one row = one record/document (e.g. KB CSV)
    - procedural: no meaningful structural roles found
    """

    if not rows:
        return "procedural"

    id_fields = schema.get("identifier", [])
    name_fields = schema.get("primary_name", [])
    desc_fields = schema.get("description", [])
    type_fields = schema.get("type", [])
    other_fields = [str(f).lower() for f in schema.get("other", [])]

    # no meaningful structure
    if not id_fields and not name_fields and not desc_fields:
        return "procedural"

    # KB-style signals
    kb_signals = {
        "resolution",
        "kbtags",
        "kbinactive",
        "kbinternalmemo",
        "descriptionmarkdown",
        "resolutionmarkdown",
        "kbdocprocessed",
        "kbdocneedsprocessing",
    }

    if kb_signals.intersection(other_fields) or "resolution" in [f.lower() for f in desc_fields]:
        return "entity_row"

    # structured/catalog signal like BBG
    if id_fields and name_fields and desc_fields and type_fields:
        return "structured"

    # fallback
    if id_fields or name_fields or desc_fields:
        return "entity_row"

    return "procedural"