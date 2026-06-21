from core.link_index import build_link_index
from core.normalizer import normalize_link_index

DEBUG = True


def xml_serializer(parsed, file_path, template_config, file_tags, collection_name):

    xml_serializer._expected_files = template_config.get("expected_files", 1)

    # buffer state across files
    if not hasattr(xml_serializer, "_all_rows"):
        xml_serializer._all_rows = {}

    if not hasattr(xml_serializer, "_schemas"):
        xml_serializer._schemas = {}

    if not hasattr(xml_serializer, "_files_seen"):
        xml_serializer._files_seen = set()

    xml_serializer._expected_files = template_config.get("expected_files", 1)

    if not hasattr(xml_serializer, "_finalized"):
        xml_serializer._finalized = False

    rows = parsed.get("rows", [])
    schema = parsed.get("schema")

    # 🔥 ensure schema exists — check PostgreSQL first, then LLM, then heuristic
    if not schema:
        from TABLES.schema_inference_table import infer_table_schema
        schema = infer_table_schema(
            rows,
            collection_name=collection_name,
            source_file=str(file_path)
        )

    if isinstance(rows, dict):
        rows = [rows]

    if not isinstance(rows, list):
        print("❌ BAD ROW TYPE:", type(rows), file_path.name)
        rows = []

    xml_serializer._all_rows[file_path.name] = rows
    xml_serializer._schemas[file_path.name] = schema
    xml_serializer._files_seen.add(file_path.name)

    # never emit here; orchestrator will call finalize()
    return []

def merge_rows_by_version(all_rows, file_kind):
    """
    Merge rows across FIX version files for the same logical record.
    file_kind: 'fields' or 'enums' — determines merge key.
    Returns: dict {merged_key: merged_row}
    """
    import re

    def extract_version(filename):
        m = re.search(r'FIX(\d)(\d)', filename)
        if m:
            return int(m.group(1)) * 10 + int(m.group(2))  # FIX44 -> 44
        return 0  # unknown version, lowest priority

    grouped = {}  # key -> list of (version, row)

    for filename, rows in all_rows.items():
        version = extract_version(filename)
        if version == 0:
            continue  # not a versioned FIX file, skip merge for these rows

        for row in rows:
            if file_kind == "enums":
                key = (row.get("Tag"), row.get("Value"))
            else:
                key = row.get("Tag")

            if key is None:
                continue

            grouped.setdefault(key, []).append((version, row, filename))

    merged = {}
    for key, version_rows in grouped.items():
        version_rows.sort(key=lambda x: x[0])  # ascending version
        latest_version, latest_row, latest_file = version_rows[-1]

        merged_row = dict(latest_row)
        merged_row["_version_history"] = [
            {"version": v, "file": f, "data": r}
            for v, r, f in version_rows
        ]
        merged_row["_latest_version"] = latest_version
        merged[key] = merged_row

    return merged


def xml_finalize(file_path, collection_name, file_tags):

    if DEBUG:
        print("🔥 FINALIZE RUNNING")

    all_rows = getattr(xml_serializer, "_all_rows", {})
    schemas = getattr(xml_serializer, "_schemas", {})
    files_seen = getattr(xml_serializer, "_files_seen", set())

    # DEBUG
    print("FILES_SEEN:", files_seen)
    print("ALL_ROWS:", list(all_rows.keys()))

    # only finalize when ALL files parsed
    files_seen = getattr(xml_serializer, "_files_seen", set())
    expected = getattr(xml_serializer, "_expected_files", 1)

    # ONLY run on last file
    if len(files_seen) < expected:
        return []

    all_rows = getattr(xml_serializer, "_all_rows", {})
    schemas = getattr(xml_serializer, "_schemas", {})

    # Merge FIX version files by Tag before building link index
    fields_files = {k: v for k, v in all_rows.items() if k.lower().startswith("fields_")}
    enums_files = {k: v for k, v in all_rows.items() if k.lower().startswith("enums_")}
    other_files = {k: v for k, v in all_rows.items() if k not in fields_files and k not in enums_files}

    merged_fields = merge_rows_by_version(fields_files, "fields") if fields_files else {}
    merged_enums = merge_rows_by_version(enums_files, "enums") if enums_files else {}

    # Rebuild all_rows: merged fields/enums as single synthetic file, others untouched
    all_rows_merged = dict(other_files)
    if merged_fields:
        all_rows_merged["_merged_fields"] = list(merged_fields.values())
    if merged_enums:
        all_rows_merged["_merged_enums"] = list(merged_enums.values())

    # only run merge if we actually found versioned FIX files
    if merged_fields or merged_enums:
        all_rows = all_rows_merged
        schemas = {**schemas, "_merged_fields": schemas.get(next(iter(fields_files), ""), {}),
                   "_merged_enums": schemas.get(next(iter(enums_files), ""), {})}

    # build merged index across ALL files
    link_index = build_link_index(all_rows, schemas)

    from core.schema_inference import save_schema_to_db
    from pathlib import Path as _Path

    # 🔥 save all schemas to PostgreSQL
    for src_file, schema in schemas.items():
        if schema:
            source_stem = _Path(src_file).stem
            save_schema_to_db(schema, collection_name, source_stem)

    
    if DEBUG:
        print("=== DEBUG LINK INDEX ===")

        print("IDENTIFIER COUNT:", len(link_index.get("identifier", {})))

        print("\nCHECK TAG 22:")
        print(link_index.get("identifier", {}).get("22"))

        print("\nENUM COUNT FOR 22:")
        if link_index.get("identifier", {}).get("22"):
            print(len(link_index["identifier"]["22"].get("enum_values", [])))

        print("=== END DEBUG ===")
    normalized_docs = normalize_link_index(link_index)

    items = []

    for doc in normalized_docs:
        text_parts = []

        if doc.get("primary_name"):
            text_parts.append(doc["primary_name"])

        if doc.get("description"):
            text_parts.append(doc["description"])

        if doc.get("enum_values"):
            enum_texts = []
            for e in doc["enum_values"]:
                if isinstance(e, dict):
                    val = e.get("Value")
                    name = e.get("SymbolicName")

                    if val and name:
                        enum_texts.append(f"{val}={name}")
                    elif name:
                        enum_texts.append(name)
                    elif val:
                        enum_texts.append(val)
                else:
                    enum_texts.append(str(e))

            if enum_texts:
                text_parts.append(", ".join(enum_texts))

        text = "\n\n".join(text_parts).strip()
        if not text:
            continue

        source_files = doc.get("raw", {}).get("source_files", [])
        primary_source_file = source_files[0] if source_files else str(file_path)

        payload = {
            "identifier_field": doc.get("identifier_field"),
            "identifier_namespace": doc.get("identifier_namespace"),
            "identifier": doc.get("identifier"),
            "primary_name": doc.get("primary_name"),
            "primary_name_field": doc.get("primary_name_field"),
            "description": doc.get("description"),
            "enum_values": doc.get("enum_values"),
            "type": doc.get("type"),
            "type_field": doc.get("type_field"),
            "doc_type": doc.get("doc_type"),
            "source_file": primary_source_file,
            "source_files": source_files,
            "ingest_source": primary_source_file,
            "link_keys": doc.get("link_keys", []),
            "related_link_keys": doc.get("related_link_keys", []),
            **file_tags
        }

        items.append({
            "text": text,
            **payload
        })

    # TEMP DEBUG — BEFORE CLEANUP
    print("ALL_ROWS KEYS:", list(all_rows.keys()))
    for fname, rows in all_rows.items():
        print(
            fname,
            "sample keys:",
            rows[0].keys() if isinstance(rows, list) and rows else "EMPTY"
        )

   # ONLY cleanup when we actually finalize full batch
    if len(all_rows) == len(files_seen):
        xml_serializer._all_rows = {}
        xml_serializer._schemas = {}
        xml_serializer._files_seen = set()

    return items


xml_serializer.finalize = xml_finalize