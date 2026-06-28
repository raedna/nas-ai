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


def promote_prose_other(schema, rows, min_median_chars=200, sample=200):
    """Signal-based, file-agnostic: move 'other' columns whose values are long prose
    into 'description'. Entity-row answers are synthesized from the description role,
    so substantive free text (e.g. a 'resolution' column) must land there regardless
    of whether the heuristic or the LLM classified the schema. No column-name rules."""
    other = list(schema.get("other") or [])
    sample_rows = [r for r in (rows or []) if isinstance(r, dict)][:sample]
    if not other or not sample_rows:
        return schema

    promote = []
    for col in other:
        lengths = [len(str(r.get(col) or "").strip()) for r in sample_rows
                   if str(r.get(col) or "").strip() not in ("", "None", "nan")]
        if not lengths:
            continue
        lengths.sort()
        median = lengths[len(lengths) // 2]
        if median >= min_median_chars:
            promote.append(col)

    if promote:
        schema["other"] = [c for c in other if c not in promote]
        schema.setdefault("description", []).extend(promote)
        print(f"[SCHEMA] promoted long-prose columns to description: {promote}")
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

        all_columns = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            for k in row.keys():
                if k != "source_file" and k not in all_columns:
                    all_columns.append(k)

        if not all_columns:
            return None

        all_rows = [r for r in list(rows) if isinstance(r, dict)]

        # P1 guardrail: prioritize by SIGNAL, not by name. Prune near-empty columns
        # (e.g. blank date-matrix columns) BEFORE the width check so wide-but-real
        # tables still get LLM inference on their meaningful columns. Pruned columns
        # are empty, so they contribute nothing to embedded text; they're routed to
        # 'other' at the end for completeness.
        _sample_rows = all_rows[:200]

        def _fill_rate(col):
            if not _sample_rows:
                return 1.0
            n = sum(1 for r in _sample_rows
                    if str(r.get(col) or "").strip() not in ("", "None", "nan"))
            return n / len(_sample_rows)

        columns = [c for c in all_columns if _fill_rate(c) >= 0.05]
        if not columns:                      # everything sparse — keep original
            columns = list(all_columns)
        pruned = [c for c in all_columns if c not in columns]
        if pruned:
            print(f"[SCHEMA LLM] pruned {len(pruned)} near-empty columns "
                  f"({len(all_columns)} -> {len(columns)})")

        # Final safety net: only bail if STILL very wide after pruning real columns.
        if len(columns) > 40:
            print(f"[SCHEMA LLM] {len(columns)} populated columns — too wide, using heuristic")
            return None

        # Pick rows with most non-empty values for better LLM context
        scored_rows = sorted(
            all_rows,
            key=lambda r: sum(1 for v in r.values() if str(v or "").strip() not in ("", "None", "nan")),
            reverse=True
        )
        samples = []
        for row in scored_rows[:5]:
            sample = {k: str(v)[:80] for k, v in row.items() if k in columns}
            samples.append(sample)

        # Cardinality signal — the strongest cue for identifier vs type/category.
        # An identifier is near-unique (distinct ≈ row count); a low-cardinality
        # column (few repeated values, e.g. a broker or status) is type/category.
        _card_rows = all_rows[:1000]
        _card = {}
        for c in columns:
            vals = [v for v in (str(r.get(c) or "").strip() for r in _card_rows)
                    if v not in ("", "None", "nan")]
            _card[c] = (len(set(vals)), len(vals))
        card_lines = "\n".join(
            f"  {c}: {d} distinct / {t} non-empty"
            + ("   <- near-unique: identifier candidate"
               if t >= 5 and d / t >= 0.9 else
               "   <- low-cardinality: type/category, NOT identifier"
               if t and d <= max(1, 0.10 * t) else "")
            for c, (d, t) in _card.items()
        )

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
            "- tags: a column whose cells hold MULTIPLE comma/semicolon-separated keywords "
            "or category labels per row (e.g. 'email,Office365,VPN'). Not boolean flags, "
            "not single values.\n"
            "- enum_value: allowed values or codes for a field\n"
            "- enum_name: labels for enum values\n"
            "- reference_identifier: foreign key referencing another table (e.g. ref, source, fk_)\n"
            "- other: coordinates (ra, dec), magnitudes, sizes, dates, boolean flags, or anything else\n\n"
            "Rules:\n"
            "- identifier MUST be a near-unique column (distinct count close to the number "
            "of rows). NEVER choose a low-cardinality column (few repeated values, like a "
            "broker, status, or category) as identifier — those are 'type'. Use the supplied "
            "cardinality stats to decide.\n"
            "- Only ONE column should be identifier - the most unique key by cardinality\n"
            "- Only ONE column should be primary_name - a human-readable label (often higher "
            "cardinality than 'type', but it need not be unique)\n"
            "- A column named comments or name or label with short descriptive strings -> primary_name\n"
            "- If two ID columns exist (e.g. ngc_id and messier_id), more comprehensive = identifier, other = aliases\n"
            "- ra, dec, lat, lon, magnitude, size, date, boolean flags -> other\n"
            "- Every column must appear exactly once\n"
            "- Return only JSON mapping each role to its list of columns"
        )

        user_prompt = (
            f"Columns: {columns}\n\n"
            f"Cardinality (distinct values per column — use this for identifier vs type):\n{card_lines}\n\n"
            f"Sample values (5 most populated rows):\n{json.dumps(samples, indent=2)}\n\n"
            f"Map each column to one of: {available_roles}\n\n"
            "Think step by step: which column is near-unique (the identifier)? "
            "Which is a human-readable name? Which are low-cardinality categories (type)?"
        )

        # P1 guardrail: structured output -> guaranteed valid JSON in the exact
        # {role: [columns]} shape (no more best-effort parsing / null returns).
        response_format = {
            "type": "json_schema",
            "json_schema": {
                "name": "column_roles",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        role: {"type": "array", "items": {"type": "string"}}
                        for role in available_roles
                    },
                    "required": available_roles,
                    "additionalProperties": False,
                },
            },
        }

        result = call_local_llm_json(
            system_prompt, user_prompt, temperature=0.0,
            response_format=response_format,
        )

        if not isinstance(result, dict):
            return None

        # P1 guardrail: structured output guarantees JSON shape but NOT one-column-one-
        # role. De-duplicate across roles by priority so each column lands exactly once.
        _priority = ["identifier", "primary_name", "aliases", "reference_identifier",
                     "type", "tags", "enum_value", "enum_name", "description", "other"]
        ordered_roles = _priority + [r for r in result if r not in _priority]
        deduped = {}
        seen = set()
        for role in ordered_roles:
            for col in result.get(role, []) or []:
                if col in columns and col not in seen:
                    seen.add(col)
                    deduped.setdefault(role, []).append(col)
        result = deduped

        # Any column the model omitted, plus the pruned near-empty columns, -> 'other'
        # (full accounting of every original column; no silent loss).
        missing = [c for c in all_columns if c not in seen]
        if missing:
            result.setdefault("other", []).extend(missing)

        for key in ["identifier", "primary_name", "aliases", "description",
                    "type", "enum_value", "enum_name", "reference_identifier", "other"]:
            result.setdefault(key, [])

        # Enforce a single canonical identifier: if the model picked several near-unique
        # columns (e.g. two filename columns), keep the most-unique as identifier and
        # demote the rest to aliases so they stay searchable but there's one stable key.
        if len(result.get("identifier", [])) > 1:
            _ids = sorted(result["identifier"],
                          key=lambda c: _card.get(c, (0, 0))[0], reverse=True)
            result["identifier"] = [_ids[0]]
            result["aliases"] = list(result.get("aliases", [])) + _ids[1:]
            print(f"[SCHEMA LLM] collapsed identifiers -> {_ids[0]}; aliases += {_ids[1:]}")

        # Same for primary_name — one canonical display name; extras become aliases.
        if len(result.get("primary_name", [])) > 1:
            _pn = sorted(result["primary_name"],
                         key=lambda c: _card.get(c, (0, 0))[0], reverse=True)
            result["primary_name"] = [_pn[0]]
            result["aliases"] = list(result.get("aliases", [])) + _pn[1:]

        result = promote_prose_other(result, all_rows)
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

    schema = promote_prose_other(schema, rows)
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