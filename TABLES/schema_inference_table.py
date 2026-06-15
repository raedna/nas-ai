from core.schema_inference import infer_schema, load_roles_config, save_schema
from core.paths import CONFIG_DIR, SCHEMAS_DIR

DEBUG = True


def infer_table_schema(rows, collection_name=None, source_file=None):
    roles = load_roles_config(CONFIG_DIR / "structured_roles.json")
    schema = infer_schema(rows, roles)

    # make sure expected keys always exist
    for key in [
        "identifier",
        "primary_name",
        "aliases",
        "description",
        "type",
        "enum_value",
        "enum_name",
        "other",
    ]:
        schema.setdefault(key, [])

    if DEBUG:
        print("[TABLE SCHEMA] Inferred schema:")
        print(schema)

    if collection_name and source_file:
        from pathlib import Path as _Path
        source_stem = _Path(source_file).stem
        schema_path = SCHEMAS_DIR / f"{collection_name}_{source_stem}_schema.json"
        print(f"[SCHEMA] checking path: {schema_path} exists: {schema_path.exists()}")
        if not schema_path.exists():
            save_schema(schema, source_file, SCHEMAS_DIR, collection_name)
        else:
            import json
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)
            print(f"[SCHEMA] loaded existing schema, identifier={schema.get('identifier')}")

    return schema