from pathlib import Path
import json
from core.paths import SCHEMAS_DIR
schema_dir = SCHEMAS_DIR


def load_collection_schemas(collection_name):
    """
    Load all schemas for a collection.
    Priority: PostgreSQL → disk JSON files.
    Returns dict of {source_file_stem: schema_dict}
    """
    schemas = {}

    # ── 1. Try PostgreSQL first
    try:
        from core.schema_inference import ensure_schemas_table
        from core.db import fetchall
        ensure_schemas_table()
        rows = fetchall("""
            SELECT source_file_stem, schema_json FROM schemas
            WHERE collection_name = %s
        """, (collection_name,))
        for row in rows:
            schema = row["schema_json"]
            if isinstance(schema, str):
                schema = json.loads(schema)
            schemas[row["source_file_stem"]] = schema
        if schemas:
            return schemas
    except Exception as e:
        pass

    # ── 2. Fall back to disk JSON files
    for file in schema_dir.iterdir():
        if file.name.startswith(f"{collection_name}_") and file.name.endswith("_schema.json"):
            stem = file.stem
            if stem.endswith("_schema"):
                stem = stem[:-7]
            # Remove collection_name prefix to get source_file_stem
            if stem.startswith(f"{collection_name}_"):
                stem = stem[len(f"{collection_name}_"):]
            with open(file, "r", encoding="utf-8") as f:
                schemas[stem] = json.load(f)

    return schemas


def get_identifier_fields(schemas):
    identifier_fields = set()
    for schema in schemas.values():
        for field in schema.get("identifier", []):
            identifier_fields.add(field.lower())
    return list(identifier_fields)

