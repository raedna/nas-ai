import re


def _normalize_namespace(field_name):
    return re.sub(r"[^a-z0-9]+", "", str(field_name or "").lower())


def _get_first_identifier(row_norm, id_fields):
    for f in id_fields:
        val = row_norm.get(f.lower())
        if val not in [None, ""]:
            identifier = str(val).strip()
            identifier_field = f
            identifier_namespace = _normalize_namespace(f)
            link_key = f"{identifier_namespace}:{identifier}"
            return identifier, identifier_field, identifier_namespace, link_key

    return None, None, None, None

def _extract_related_identifiers_from_text(text, known_identifiers, self_identifier=None):
    text = str(text or "").strip()
    if not text:
        return set()

    token_matches = set(re.findall(r"[A-Za-z0-9_]+", text))
    related = set()

    for ident in known_identifiers:
        ident_str = str(ident).strip()
        if not ident_str:
            continue

        if self_identifier is not None and ident_str == str(self_identifier).strip():
            continue

        if ident_str in token_matches:
            related.add(ident_str)

    return related


def build_link_index(all_rows, schema_map):
    link_index = {
        "identifier": {}
    }

    # =========================
    # COLLECT KNOWN IDENTIFIERS
    # =========================
    known_identifiers = set()

    for source_file, rows in all_rows.items():
        schema = schema_map.get(source_file, {})
        id_fields = schema.get("identifier", [])

        for row in rows:
            if not isinstance(row, dict):
                continue

            row_norm = {k.lower(): v for k, v in row.items()}

            for f in id_fields:
                val = row_norm.get(f.lower())
                if val:
                    known_identifiers.add(str(val).strip())

    # =========================
    # BASE PASS (FIELDS)
    # =========================
    for source_file, rows in all_rows.items():

        schema = schema_map.get(source_file, {})

        # skip enum datasets in base pass
        if schema.get("enum_value"):
            continue

        id_fields = schema.get("identifier", [])
        name_fields = schema.get("primary_name", [])
        desc_fields = schema.get("description", [])

        for row in rows:

            if not isinstance(row, dict):
                continue

            row_norm = {k.lower(): v for k, v in row.items()}

            # --- IDENTIFIER ---
            identifier, identifier_field, identifier_namespace, link_key = _get_first_identifier(
                row_norm,
                id_fields
            )

            if not link_key:
                continue

            # --- NAMES ---
            names = []
            for f in name_fields:
                val = row_norm.get(f.lower())
                if val:
                    names.append((f, str(val)))

            # --- DESCRIPTIONS ---
            descs = []
            for f in desc_fields:
                val = row_norm.get(f.lower())
                if val:
                    descs.append(val)

            for key in [link_key]:

                if key not in link_index["identifier"]:
                    link_index["identifier"][key] = {
                        "identifier": identifier,
                        "identifier_field": identifier_field,
                        "identifier_namespace": identifier_namespace,
                        "primary_name": None,
                        "aliases": [],
                        "description": None,
                        "enum_values": [],
                        "source_files": set(),
                        "related_identifiers": set()
                    }

                entry = link_index["identifier"][key]

                # =========================
                # PRIMARY NAME (schema-driven)
                # =========================
                best_name = None

                for f in name_fields:
                    for field, val in names:
                        if field == f:
                            best_name = val
                            break
                    if best_name:
                        break

                if not best_name and names:
                    best_name = names[0][1]

                if best_name:
                    entry["primary_name"] = best_name

                # aliases
                for field, val in names:
                    if val != entry["primary_name"] and val not in entry["aliases"]:
                        entry["aliases"].append(val)

                # =========================
                # DESCRIPTION
                # =========================
                if descs and not entry["description"]:
                    entry["description"] = str(descs[0])

                # =========================
                # RELATED IDENTIFIERS
                # explicit identifier refs in description text
                # =========================
                for desc in descs:
                    related = _extract_related_identifiers_from_text(
                        desc,
                        known_identifiers=known_identifiers,
                        self_identifier=key
                    )
                    entry["related_identifiers"].update(related)

                entry["source_files"].add(source_file)

    # =========================
    # ENUM LINKING (schema-driven)
    # =========================
    for source_file, rows in all_rows.items():

        schema = schema_map.get(source_file, {})

        id_fields = schema.get("identifier", [])
        value_fields = schema.get("enum_value", [])
        name_fields = schema.get("enum_name", []) or schema.get("primary_name", [])

        # skip non-enum datasets
        if not value_fields:
            continue

        for row in rows:

            row_norm = {k.lower(): v for k, v in row.items()}

            # =========================
            # IDENTIFIER (schema-driven)
            # =========================
            identifier, identifier_field, identifier_namespace, link_key = _get_first_identifier(
                row_norm,
                id_fields
            )

            if not link_key:
                continue

            key = link_key

            if key not in link_index["identifier"]:
                link_index["identifier"][key] = {
                    "identifier": identifier,
                    "identifier_field": identifier_field,
                    "identifier_namespace": identifier_namespace,
                    "primary_name": None,
                    "aliases": [],
                    "description": None,
                    "enum_values": [],
                    "source_files": set(),
                    "related_identifiers": set()
                }

            entry = link_index["identifier"][key]

            # =========================
            # ENUM VALUE
            # =========================
            enum_val = None
            for f in value_fields:
                val = row_norm.get(f.lower())
                if val:
                    enum_val = str(val)
                    break

            # =========================
            # ENUM NAME (schema-driven)
            # =========================
            enum_name = None
            for f in name_fields:
                val = row_norm.get(f.lower())
                if val:
                    enum_name = str(val)
                    break

            if enum_val:
                enum_entry = {}

                # assign using schema column names
                for f in value_fields:
                    v = row_norm.get(f.lower())
                    if v:
                        enum_entry[f] = str(v)

                for f in name_fields:
                    v = row_norm.get(f.lower())
                    if v:
                        enum_entry[f] = str(v)

                def is_duplicate(e):
                    for f in value_fields:
                        if e.get(f) == enum_val:
                            return True
                    return False

                if not any(is_duplicate(e) for e in entry["enum_values"]):
                    entry["enum_values"].append(enum_entry)

    # =========================
    # FINALIZE
    # =========================
    for k in link_index["identifier"]:
        link_index["identifier"][k]["source_files"] = sorted(
            list(link_index["identifier"][k]["source_files"])
        )
        link_index["identifier"][k]["related_identifiers"] = sorted(
            list(link_index["identifier"][k]["related_identifiers"])
        )

    return link_index