from core.schema_inference import infer_schema, load_roles_config
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

    # 🔥 ensure schema exists
    if not schema:
        roles = load_roles_config("config/structured_roles.json")
        schema = infer_schema(rows, roles)

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

    # build merged index across ALL files
    link_index = build_link_index(all_rows, schemas)

    from core.schema_inference import save_schema

    # 🔥 save all schemas once (correct place)
    for src_file, schema in schemas.items():
        if schema:
            save_schema(
                schema,
                src_file,
                "schemas",
                collection_name
            )

    
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
            "description": doc.get("description"),
            "enum_values": doc.get("enum_values"),
            "type": doc.get("type"),
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