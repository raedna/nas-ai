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

    # (Legacy disk-JSON resurrection removed — PostgreSQL is the only schema
    # store (Phase 2). A stale disk file silently overwriting a deliberately
    # deleted DB schema is exactly the failure mode we're closing.)

    # ── 2. LLM inference is PRIMARY for tables; the heuristic is only a
    # fallback when the LLM is unreachable. The LLM path carries the
    # deterministic guards (filename-role constraint, cardinality checks,
    # leftmost-key tie-break); the raw heuristic has none of them.
    print(f"[SCHEMA] No existing schema — running LLM inference for {source_file_stem}")
    schema = llm_infer_schema(rows, roles)
    _source = "llm"
    if not schema:
        print("[SCHEMA] LLM inference unavailable — trying heuristic fallback")
        schema = infer_schema(rows, roles)
        _source = "heuristic"

    for key in ["identifier", "primary_name", "aliases", "description",
                "type", "tags", "enum_value", "enum_name",
                "reference_identifier", "other"]:
        schema.setdefault(key, [])

    # ── 3. Never persist a junk schema. A saved wrong schema short-circuits
    # every future ingest and silently poisons retrieval; a missing schema is
    # recoverable by re-running when the LLM is back.
    if _source == "heuristic" and not schema.get("identifier"):
        raise RuntimeError(
            f"Schema inference failed for {source_file_stem}: LLM unavailable and "
            "heuristic found no identifier. Refusing to save a junk schema — "
            "retry when the LLM is reachable, or define the schema manually.")

    if DEBUG:
        print(f"[TABLE SCHEMA] Inferred schema (source: {_source}):")
        print(schema)

    if collection_name and source_file_stem:
        save_schema_to_db(schema, collection_name, source_file_stem)

    return schema

