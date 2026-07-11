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


# --- SPEED-01: in-process schema cache -------------------------------------
# The schemas table is read on EVERY routing call (Tier 1.2b) and every
# metadata query — round-trip waste for data that only changes at ingest or
# a manual save. Short TTL + explicit invalidation on save/delete.
_SCHEMA_CACHE = {"rows": None, "ts": 0.0}
_SCHEMA_CACHE_TTL = 60.0


def get_all_schemas_cached():
    """All schema rows [{collection_name, source_file_stem, schema_json}] — cached."""
    import time as _t
    if (_SCHEMA_CACHE["rows"] is not None
            and _t.time() - _SCHEMA_CACHE["ts"] < _SCHEMA_CACHE_TTL):
        return _SCHEMA_CACHE["rows"]
    from core.db import fetchall as _fa
    rows = _fa(
        "SELECT collection_name, source_file_stem, schema_json FROM schemas", ())
    _SCHEMA_CACHE["rows"] = rows
    _SCHEMA_CACHE["ts"] = _t.time()
    return rows


def invalidate_schema_cache():
    _SCHEMA_CACHE["rows"] = None
    _SCHEMA_CACHE["ts"] = 0.0


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
        invalidate_schema_cache()
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
        invalidate_schema_cache()
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

def _schema_model():
    """Optional dedicated model for schema inference (config:
    local_llm.schema_model). Schema inference runs once per file, offline —
    it can afford a bigger/slower model than the chat path. Falls back to the
    default model when unset."""
    try:
        from core.local_llm_client import load_nlp_config
        return load_nlp_config().get("local_llm", {}).get("schema_model") or None
    except Exception:
        return None


def llm_infer_schema(rows, roles_config):
    """
    LLM column-to-role mapping from column names + sample values + cardinality.
    Uses local_llm.schema_model when configured (bigger model for judgment),
    else the default model. Falls back to heuristic infer_schema() if LLM
    unavailable or returns invalid result.
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

        import re
        # Filename-valued columns: core signal is "ends in a dot-extension".
        # The body allows word chars, hyphens, spaces, $, &, commas and inner
        # dots — real-world names like 'boa cash.csv', 'confirmed
        # trades.MOORE.txt', 'OTC_DCM_1_$y4$m4$d4.csv' (verified on live data:
        # filename columns score 87-100%, all others 0%).
        _fname_pat = re.compile(r'^[\w\-\$&., ]*\.[A-Za-z][A-Za-z0-9]{1,3}$')
        def _is_filename_col(col):
            vals = [str(r.get(col) or "").strip() for r in _sample_rows]
            vals = [v for v in vals if v not in ("", "None", "nan")]
            if not vals:
                return False
            hits = sum(1 for v in vals if _fname_pat.match(v))
            return hits / len(vals) >= 0.7

        # Constrain, don't hide: filename columns STAY in the LLM prompt — the
        # LLM must choose which is the primary identifier vs alias vs reference,
        # and it can't do that for columns it never sees. Their allowed roles
        # are restricted to identifier/aliases/reference_identifier via a prompt
        # note and enforced after the LLM responds.
        _filename_cols = [c for c in columns if _is_filename_col(c)]
        if _filename_cols:
            print(f"[SCHEMA LLM] filename-valued columns (roles constrained to "
                  f"identifier/aliases/reference_identifier): {_filename_cols}")

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
            "- type: category, classification, or data type (e.g. type, category, class, object_type). "
            "Also columns whose values are entity/organization names repeating across rows "
            "(brokers, clients, vendors). Do NOT include numeric measurements (ra, dec, exposure, gain, temperature) or "
            "dates/timestamps — those are 'other', even if low-cardinality. A column whose values look like filenames "
            "(word characters followed by a dot and a short alphabetic extension) is never type. "
            "Also check the column header — a header containing a drive letter+colon or slash "
            "(e.g. 'K:/path') signals filename/script, not type even if low-cardinality\n"
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
            "- primary_name is the column a user would call the record by. A comments/notes column "
            "is description, not primary_name, unless no better name column exists\n"
            "- If two ID columns exist (e.g. ngc_id and messier_id), more comprehensive = identifier, other = aliases\n"
            "- ra, dec, lat, lon, magnitude, size, date, boolean flags -> other\n"
            "- Every column must appear exactly once\n"
            "- Return only JSON mapping each role to its list of columns"
        )

        _fname_note = ""
        if _filename_cols:
            _fname_note = (
                f"\nFilename-valued columns: {_filename_cols}. These may ONLY be "
                "classified as identifier, aliases, or reference_identifier — never "
                "type, description, tags, or other. Among them: the table's OWN "
                "file/record key (the name users refer to the record by) is the "
                "identifier; an equivalent external/source system's filename for the "
                "same record is aliases; a script or unrelated file reference is "
                "reference_identifier.\n"
            )

        user_prompt = (
            f"Columns: {columns}\n\n"
            f"Cardinality (distinct values per column — use this for identifier vs type):\n{card_lines}\n\n"
            f"Sample values (5 most populated rows):\n{json.dumps(samples, indent=2)}\n"
            f"{_fname_note}\n"
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

        _model = _schema_model()
        if _model:
            print(f"[SCHEMA LLM] using schema model: {_model}")
        result = call_local_llm_json(
            system_prompt, user_prompt, temperature=0.0,
            model=_model,
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

        # Enforce the filename-role constraint: a filename column belongs in a
        # NAME-class role (identifier/primary_name/aliases/reference_identifier).
        # The original SCHEMA-01 bug (filename -> type/description) stays
        # impossible, but the LLM chooses WHICH filename column plays which
        # name role. primary_name is allowed — tables whose records ARE files
        # legitimately display them by filename (e.g. FIXMLFileName).
        _fn_allowed = ("identifier", "primary_name", "aliases", "reference_identifier")

        def _near_unique(col):
            d, t = _card.get(col, (0, 0))
            return t >= 5 and d / t >= 0.9

        for col in _filename_cols:
            if col in seen:
                _placed = next((r for r, cols_ in result.items() if col in (cols_ or [])), None)
                if _placed and _placed not in _fn_allowed:
                    result[_placed].remove(col)
                    result.setdefault("reference_identifier", []).append(col)
                    print(f"[SCHEMA LLM] moved filename column '{col}' from "
                          f"'{_placed}' to reference_identifier (constraint)")
                elif _placed in ("identifier", "primary_name", "aliases") and not _near_unique(col):
                    # 1:1 name roles — a filename shared across many rows (low
                    # cardinality, e.g. a script) cannot be one; it is a reference.
                    result[_placed].remove(col)
                    result.setdefault("reference_identifier", []).append(col)
                    print(f"[SCHEMA LLM] moved filename column '{col}' from "
                          f"'{_placed}' to reference_identifier (not near-unique)")
            else:
                result.setdefault("reference_identifier", []).append(col)
                seen.add(col)

        # Deterministic tie-break: when 2+ NEAR-UNIQUE filename columns compete
        # for the record key, the LEFTMOST in the source file wins identifier
        # (table convention — authors lead with the key the table is organized
        # around); the rest become aliases. Ends LLM coin-flips between
        # equivalent name columns across re-ingests. Only fires when the LLM
        # itself put a filename column in identifier.
        _fn_keys = [c for c in all_columns if c in _filename_cols and _near_unique(c)]
        if len(_fn_keys) >= 2 and any(c in (result.get("identifier") or []) for c in _fn_keys):
            _leader = _fn_keys[0]  # all_columns preserves source column order
            if (result.get("identifier") or []) != [_leader]:
                _former = list(result.get("identifier") or [])
                # Strip the competing filename keys from EVERY role before
                # reassigning — a leftover copy (e.g. in reference_identifier)
                # makes the downstream dedupe empty the identifier again.
                for role in list(result.keys()):
                    result[role] = [c for c in (result.get(role) or []) if c not in _fn_keys]
                result["identifier"] = [_leader] + [c for c in _former if c not in _fn_keys]
                result.setdefault("aliases", []).extend(c for c in _fn_keys if c != _leader)
                print(f"[SCHEMA LLM] filename-key tie-break: identifier -> '{_leader}' "
                      f"(leftmost); aliases += {[c for c in _fn_keys if c != _leader]}")

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