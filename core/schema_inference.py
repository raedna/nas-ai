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


# ---------------------------------------------------------------------------
# PostgreSQL schema storage
# ---------------------------------------------------------------------------

def ensure_schemas_table():
    """Create the schemas table if it doesn't exist."""
    try:
        from core.db import execute
        execute("""
            CREATE TABLE IF NOT EXISTS schemas (
                id SERIAL PRIMARY KEY,
                collection_name TEXT NOT NULL,
                source_file_stem TEXT NOT NULL,
                schema_json JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE (collection_name, source_file_stem)
            )
        """)
    except Exception as e:
        print(f"[SCHEMA DB] Could not create schemas table: {e}")


def save_schema_to_db(schema, collection_name, source_file_stem):
    """Save schema to PostgreSQL schemas table."""
    try:
        ensure_schemas_table()
        from core.db import execute
        execute("""
            INSERT INTO schemas (collection_name, source_file_stem, schema_json, updated_at)
            VALUES (%s, %s, %s::jsonb, NOW())
            ON CONFLICT (collection_name, source_file_stem)
            DO UPDATE SET schema_json = EXCLUDED.schema_json, updated_at = NOW()
        """, (collection_name, source_file_stem, json.dumps(schema)))
        print(f"[SCHEMA DB] Saved schema for {collection_name}/{source_file_stem}")
        return True
    except Exception as e:
        print(f"[SCHEMA DB] Could not save schema: {e}")
        return False


def load_schema_from_db(collection_name, source_file_stem):
    """Load schema from PostgreSQL schemas table. Returns None if not found."""
    try:
        ensure_schemas_table()
        from core.db import fetchall
        rows = fetchall("""
            SELECT schema_json FROM schemas
            WHERE collection_name = %s AND source_file_stem = %s
        """, (collection_name, source_file_stem))
        if rows:
            schema = rows[0]["schema_json"]
            if isinstance(schema, str):
                schema = json.loads(schema)
            print(f"[SCHEMA DB] Loaded schema for {collection_name}/{source_file_stem}")
            return schema
        return None
    except Exception as e:
        print(f"[SCHEMA DB] Could not load schema: {e}")
        return None


def list_schemas_from_db():
    """List all schemas stored in PostgreSQL."""
    try:
        ensure_schemas_table()
        from core.db import fetchall
        rows = fetchall("""
            SELECT collection_name, source_file_stem, updated_at
            FROM schemas ORDER BY collection_name, source_file_stem
        """, ())
        return rows
    except Exception as e:
        print(f"[SCHEMA DB] Could not list schemas: {e}")
        return []


def delete_schema_from_db(collection_name, source_file_stem):
    """Delete a schema from PostgreSQL."""
    try:
        from core.db import execute
        execute("""
            DELETE FROM schemas
            WHERE collection_name = %s AND source_file_stem = %s
        """, (collection_name, source_file_stem))
        return True
    except Exception as e:
        print(f"[SCHEMA DB] Could not delete schema: {e}")
        return False


def migrate_schemas_from_disk(schemas_dir, dry_run=False):
    """
    One-time migration: load all JSON schema files from disk into PostgreSQL.
    File naming convention: {collection_name}_{source_file_stem}_schema.json
    """
    schemas_dir = Path(schemas_dir)
    migrated = []
    skipped = []

    for f in sorted(schemas_dir.glob("*_schema.json")):
        # Parse collection_name and source_file_stem from filename
        stem = f.stem  # e.g. "xml_test_Fields_FIX42_schema" → need to strip "_schema"
        if stem.endswith("_schema"):
            stem = stem[:-7]  # remove "_schema" suffix

        # Try to find collection_name by matching against known collections
        # Convention: collection_name is the part before the first underscore group
        # that matches a known collection. Fall back to first token.
        parts = stem.split("_")
        # Try progressively longer prefixes as collection_name
        collection_name = parts[0]
        source_file_stem = "_".join(parts[1:]) if len(parts) > 1 else stem

        # Better: check if first two tokens form a known collection name
        if len(parts) >= 2:
            candidate = f"{parts[0]}_{parts[1]}"
            # heuristic: if second token is short (like "test", "docs"), use two tokens
            if parts[1] in ("test", "docs", "assist", "file", "fields", "catalog"):
                collection_name = candidate
                source_file_stem = "_".join(parts[2:]) if len(parts) > 2 else parts[1]

        try:
            with open(f, "r", encoding="utf-8") as fp:
                schema = json.load(fp)
        except Exception as e:
            print(f"[MIGRATE] Could not read {f.name}: {e}")
            skipped.append(f.name)
            continue

        if dry_run:
            print(f"[MIGRATE DRY RUN] {f.name} → collection={collection_name}, stem={source_file_stem}")
            migrated.append(f.name)
            continue

        ok = save_schema_to_db(schema, collection_name, source_file_stem)
        if ok:
            migrated.append(f.name)
            print(f"[MIGRATE] ✅ {f.name} → {collection_name}/{source_file_stem}")
        else:
            skipped.append(f.name)

    print(f"[MIGRATE] Done: {len(migrated)} migrated, {len(skipped)} skipped")
    return migrated, skipped


# ---------------------------------------------------------------------------
# LLM schema inference
# ---------------------------------------------------------------------------

def llm_infer_schema(rows, roles_config):
    """
    Use LLaMA 8B to infer column-to-role mapping from column names + sample values.
    Falls back to heuristic infer_schema() if LLM unavailable or returns invalid result.
    """
    try:
        from core.local_llm_client import call_local_llm_json

        columns = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            for k in row.keys():
                if k != "source_file" and k not in columns:
                    columns.append(k)

        if not columns:
            return None

        # Pick rows with most non-empty values for better LLM context
        all_rows = [r for r in list(rows) if isinstance(r, dict)]
        scored_rows = sorted(
            all_rows,
            key=lambda r: sum(1 for v in r.values() if str(v or "").strip() not in ("", "None", "nan")),
            reverse=True
        )
        samples = []
        for row in scored_rows[:5]:
            sample = {k: str(v)[:80] for k, v in row.items() if k in columns}
            samples.append(sample)

        available_roles = list(roles_config.keys()) + ["other"]

        system_prompt = (
            "You are a data schema classifier. Given column names and sample values from a CSV file, "
            "map each column to exactly one of these roles:\n\n"
            "- identifier: the primary unique key (e.g. ID, code, tag number, catalog number, ngc_id)\n"
            "- primary_name: the human-readable name or title (e.g. name, title, label, common_name, "
            "or a comments column containing names like Great Orion Nebula)\n"
            "- aliases: alternative names or secondary IDs (e.g. also_known_as, alt_id, messier_id)\n"
            "- description: longer descriptive text, notes, or definitions\n"
            "- type: category, classification, or data type (e.g. type, category, class, object_type)\n"
            "- enum_value: allowed values or codes for a field\n"
            "- enum_name: labels for enum values\n"
            "- reference_identifier: foreign key referencing another table (e.g. ref, source, fk_)\n"
            "- other: coordinates (ra, dec), magnitudes, sizes, dates, boolean flags, or anything else\n\n"
            "Rules:\n"
            "- Only ONE column should be identifier - pick the most unique primary ID\n"
            "- Only ONE column should be primary_name - pick the column with human-readable names\n"
            "- A column named comments or name or label with short descriptive strings -> primary_name\n"
            "- If two ID columns exist (e.g. ngc_id and messier_id), more comprehensive = identifier, other = aliases\n"
            "- ra, dec, lat, lon, magnitude, size, date, boolean flags -> other\n"
            "- Every column must appear exactly once\n"
            "- Return only JSON: {\"role_name\": [\"col1\", \"col2\"], ...}"
        )

        user_prompt = (
            f"Columns: {columns}\n\n"
            f"Sample values (5 most populated rows):\n{json.dumps(samples, indent=2)}\n\n"
            f"Map each column to one of: {available_roles}\n\n"
            "Think step by step: which column is the primary unique ID? Which has human-readable names?"
        )

        result = call_local_llm_json(system_prompt, user_prompt, temperature=0.0)

        if not isinstance(result, dict):
            return None

        all_mapped = []
        for role_cols in result.values():
            if isinstance(role_cols, list):
                all_mapped.extend(role_cols)

        missing = [c for c in columns if c not in all_mapped]
        if missing:
            result.setdefault("other", []).extend(missing)

        for key in ["identifier", "primary_name", "aliases", "description",
                    "type", "enum_value", "enum_name", "reference_identifier", "other"]:
            result.setdefault(key, [])

        print(f"[SCHEMA LLM] Inferred schema: {result}")
        return refine_schema_roles(result)

    except Exception as e:
        print(f"[SCHEMA LLM] Failed ({e}), falling back to heuristic")
        return None


# ---------------------------------------------------------------------------
# Heuristic inference (fallback)
# ---------------------------------------------------------------------------

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

    for col in columns:
        if not any(col in schema[r] for r in roles_config):
            schema["other"].append(col)

    return refine_schema_roles(schema)


# ---------------------------------------------------------------------------
# Legacy disk-based save/load (kept for migration only)
# ---------------------------------------------------------------------------

def save_schema(schema, source_file, output_dir, collection_name):
    """Legacy: save schema to disk JSON file. Use save_schema_to_db() instead."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{collection_name}_{Path(source_file).stem}_schema.json"
    path = output_dir / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)
    return path