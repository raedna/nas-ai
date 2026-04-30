from pathlib import Path
import json
from core.paths import SCHEMAS_DIR
schema_dir = SCHEMAS_DIR

def load_collection_schemas(collection_name):

    schemas = {}

    for file in schema_dir.iterdir():   # 🔥 CHANGE HERE
        if file.name.startswith(f"{collection_name}_") and file.name.endswith("_schema.json"):
            with open(file, "r", encoding="utf-8") as f:
                schemas[file.name] = json.load(f)

    return schemas

def get_identifier_fields(schemas):

    identifier_fields = set()

    for schema in schemas.values():
        for field in schema.get("identifier", []):
            identifier_fields.add(field.lower())

    return list(identifier_fields)

