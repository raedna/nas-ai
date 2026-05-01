import re


def _normalize_namespace(field_name):
    return re.sub(r"[^a-z0-9]+", "", str(field_name or "").lower())

def _extract_related_link_keys_from_text(text, known_identifiers, namespace, self_identifier=None):
    text = str(text or "").strip()
    if not text or not namespace:
        return set()

    related = set()

    for ident in known_identifiers:
        ident_str = str(ident).strip()
        if not ident_str:
            continue

        if self_identifier is not None and ident_str == str(self_identifier).strip():
            continue

        escaped = re.escape(ident_str)

        explicit_patterns = [
            rf"\btag\s*\(?\s*{escaped}\s*\)?\b",
            rf"\bfield\s*\(?\s*{escaped}\s*\)?\b",
            rf"\({escaped}\)",
        ]

        if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in explicit_patterns):
            related.add(f"{namespace}:{ident_str}")

    return related


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
    known_identifiers_by_namespace = {}

    for source_file, rows in all_rows.items():
        schema = schema_map.get(source_file, {})
        id_fields = schema.get("identifier", [])

        for row in rows:
            if not isinstance(row, dict):
                continue

            row_norm = {k.lower(): v for k, v in row.items()}

            identifier, identifier_field, identifier_namespace, link_key = _get_first_identifier(
                row_norm,
                id_fields
            )

            if identifier and identifier_namespace:
                known_identifiers.add(identifier)
                known_identifiers_by_namespace.setdefault(identifier_namespace, set()).add(identifier)

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
                        "related_identifiers": set(),
                        "link_keys": set([link_key]),
                        "related_link_keys": set(),
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
                    same_namespace_ids = known_identifiers_by_namespace.get(identifier_namespace, set())

                    related_link_keys = _extract_related_link_keys_from_text(
                        desc,
                        known_identifiers=same_namespace_ids,
                        namespace=identifier_namespace,
                        self_identifier=identifier
                    )

                    entry["related_link_keys"].update(related_link_keys)

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
                enum_entry = {
                    "enum_value": str(enum_val),
                    "enum_name": enum_name,
                }

                # optional enum description, schema-driven
                enum_description = None
                for f in desc_fields:
                    v = row_norm.get(f.lower())
                    if v:
                        enum_description = str(v)
                        break

                if enum_description:
                    enum_entry["description"] = enum_description

                def is_duplicate(e):
                    return str(e.get("enum_value") or "") == str(enum_val)

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
        link_index["identifier"][k]["link_keys"] = sorted(
            list(link_index["identifier"][k].get("link_keys", []))
        )

        link_index["identifier"][k]["related_link_keys"] = sorted(
            list(link_index["identifier"][k].get("related_link_keys", []))
        )

    return link_index