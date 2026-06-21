from core.schema_inference import (
    infer_schema, llm_infer_schema, load_roles_config,
    save_schema, save_schema_to_db, load_schema_from_db
)
from core.paths import CONFIG_DIR, SCHEMAS_DIR

DEBUG = True


def infer_table_schema(rows, collection_name=None, source_file=None):
    roles = load_roles_config(CONFIG_DIR / "structured_roles.json")

    source_file_stem = None
    if source_file:
        from pathlib import Path
        source_file_stem = Path(source_file).stem

    # ── 1. Check PostgreSQL first (manual overrides or previously inferred)
    if collection_name and source_file_stem:
        schema = load_schema_from_db(collection_name, source_file_stem)
        if schema:
            print(f"[SCHEMA] Loaded from PostgreSQL: {collection_name}/{source_file_stem}")
            return schema

    # ── 2. Fall back to disk JSON file (legacy)
    if collection_name and source_file_stem:
        schema_path = SCHEMAS_DIR / f"{collection_name}_{source_file_stem}_schema.json"
        if schema_path.exists():
            import json
            with open(schema_path, "r", encoding="utf-8") as f:
                schema = json.load(f)
            print(f"[SCHEMA] Loaded from disk: {schema_path.name} — migrating to PostgreSQL")
            # Migrate to PostgreSQL and keep disk file for now
            save_schema_to_db(schema, collection_name, source_file_stem)
            return schema

    # ── 3. Heuristic first (fast), LLM only if key roles missing
    print(f"[SCHEMA] No existing schema — running heuristic for {source_file_stem}")
    schema = infer_schema(rows, roles)

    for key in ["identifier", "primary_name", "aliases", "description",
                "type", "enum_value", "enum_name", "reference_identifier", "other"]:
        schema.setdefault(key, [])

    if not schema.get("identifier") or not schema.get("primary_name"):
        print(f"[SCHEMA] Heuristic missed key roles — escalating to LLM")
        llm_result = llm_infer_schema(rows, roles)
        if llm_result:
            schema = llm_result
            for key in ["identifier", "primary_name", "aliases", "description",
                        "type", "enum_value", "enum_name", "reference_identifier", "other"]:
                schema.setdefault(key, [])
        else:
            print("[SCHEMA] LLM escalation failed — keeping heuristic result")

    if DEBUG:
        print("[TABLE SCHEMA] Inferred schema:")
        print(schema)

    # Save to PostgreSQL (primary) and disk (legacy backup)
    if collection_name and source_file_stem:
        save_schema_to_db(schema, collection_name, source_file_stem)
        #save_schema(schema, source_file, SCHEMAS_DIR, collection_name) # saving schema to disk

    return schema

