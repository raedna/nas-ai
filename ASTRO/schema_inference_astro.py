from core.schema_inference import (
    llm_infer_schema, load_roles_config,
    save_schema_to_db, load_schema_from_db,
)

from core.paths import CONFIG_DIR
    
def infer_astro_schema(metadata_rows, collection_name, source_file_stem):
    """Existing schema -> use; else LLM over metadata keys/values -> save; else None."""
    schema = load_schema_from_db(collection_name, source_file_stem)
    if schema:
        return schema
    metadata_rows = [{k: v for k, v in r.items() if not k.startswith("file_")}
                     for r in metadata_rows]
    roles = load_roles_config(CONFIG_DIR / "structured_roles.json")
    schema = llm_infer_schema(metadata_rows, roles)
    if schema:
        save_schema_to_db(schema, collection_name, source_file_stem)
    return schema