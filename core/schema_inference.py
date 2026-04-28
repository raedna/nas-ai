import json
from pathlib import Path

DEBUG = True

def load_roles_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def refine_schema_roles(schema):
    reference_ids = set(schema.get("reference_identifier", []))

    if reference_ids and "identifier" in schema:
        schema["identifier"] = [
            col for col in schema.get("identifier", [])
            if col not in reference_ids
        ]

    return schema


def infer_schema(rows, roles_config):

    columns = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        for k in row.keys():
            if k != "source_file" and k not in columns:
                columns.append(k)

    schema = {role: [] for role in roles_config.keys()}
    schema["other"] = []

    for role, patterns in roles_config.items():

        for p in patterns:
            norm_p = p.replace("_", "").replace(" ", "")

            for col in columns:

                col_lower = col.lower()
                norm_col = col_lower.replace("_", "").replace(" ", "")

                if norm_col == norm_p or norm_col.endswith(norm_p):

                    if col not in schema[role]:
                        schema[role].append(col)

    # assign unmatched columns to "other"
    for col in columns:
        if not any(col in schema[r] for r in roles_config):
            schema["other"].append(col)

    return refine_schema_roles(schema)

def save_schema(schema, source_file, output_dir, collection_name):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{collection_name}_{Path(source_file).stem}_schema.json"
    path = output_dir / filename

    with open(path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)

    return path