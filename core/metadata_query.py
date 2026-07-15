"""
core/metadata_query.py
======================
Phase 4.6 — Metadata/SQL query path for aggregation intents.

When llm_detect_intent returns discovery_count / discovery_list / aggregation,
this module answers via direct parameterized SQL against the chunks table
instead of retrieval-based counting (which is top-k capped and wrong).

Safety:
- LLM only extracts a structured spec (operation, filters, fields).
- Fields are validated against the collection's ACTUAL payload keys + columns.
- SQL is built from fixed parameterized templates — the LLM never writes SQL.
- SELECT-only. Falls back to None so the caller can use the discovery engine.
"""

from typing import Dict, List, Optional

from core.db import fetchall

# Real table columns usable in filters/aggregation (beyond payload keys)
_TABLE_COLUMNS = {"identifier", "primary_name", "description", "doc_type",
                  "source_file", "source_type", "identifier_namespace", "nlp_text"}

_OPERATIONS = {"count", "count_distinct", "list_distinct", "group_by"}
_FILTER_OPS = {"equals", "contains"}


# Pipeline bookkeeping keys — present in every payload but NOT data. Letting
# the LLM filter/target on these produced junk specs (doc_type=structured,
# type_field=Category match every row).
_SYSTEM_KEYS = {
    "doc_type", "type_field", "identifier_field", "identifier_kind",
    "source_type", "ingest_source", "link_keys", "related_link_keys",
    "related_file_paths", "related_source_files", "related_image_targets",
    "file_path", "text", "embedded_image_paths", "embedded_image_ocr_map",
    "description_fields", "enum_values", "_question", "_version_history",
    "_latest_version",
}


def _collection_fields(collection: str):
    """Returns (queryable field names, description_fields keys). Queryable =
    payload keys + table columns + the labeled description_fields columns
    (original source column names — e.g. 'DataType', 'Recon Tool File
    Format'), minus pipeline bookkeeping keys."""
    rows = fetchall("""
        SELECT DISTINCT jsonb_object_keys(payload) AS k
        FROM (SELECT payload FROM chunks
              WHERE collection_name = %s LIMIT 200) s
    """, (collection,))
    keys = {r["k"] for r in rows} - _SYSTEM_KEYS

    df_rows = fetchall("""
        SELECT DISTINCT jsonb_object_keys(payload->'description_fields') AS k
        FROM (SELECT payload FROM chunks
              WHERE collection_name = %s
                AND jsonb_typeof(payload->'description_fields') = 'object'
              LIMIT 200) s
    """, (collection,))
    df_keys = {r["k"] for r in df_rows} - keys - _TABLE_COLUMNS - _SYSTEM_KEYS

    return (keys | _TABLE_COLUMNS | df_keys) - _SYSTEM_KEYS, df_keys

def _field_values(collection: str, fields: set, df_keys=frozenset()) -> Dict[str, list]:
    """Distinct values for low-cardinality fields — grounds the LLM in real
    values. Side effect: CONSTANT fields (exactly one distinct value in the
    whole collection, e.g. source_file in a single-file collection) are
    removed from `fields` — they carry zero information as filter or target
    and only mislead the LLM."""
    values = {}
    for f in sorted(fields):
        expr = _field_expr(f, df_keys)
        # jsonb-array fields (e.g. versions: ["4.2","4.4"]): list the ELEMENTS
        # as the field's values, not the raw JSON text — the LLM must ground
        # filters in '4.4', never in '["4.2", "4.4"]'.
        _key = f.replace("'", "")
        _arr = fetchall(
            f"""SELECT 1 FROM chunks WHERE collection_name = %s
                AND jsonb_typeof(payload->'{_key}') = 'array' LIMIT 1""",
            (collection,)) if expr.startswith("payload->>") else []
        if _arr:
            rows = fetchall(
                f"""SELECT DISTINCT _v AS v FROM chunks,
                    jsonb_array_elements_text(payload->'{_key}') _v
                    WHERE collection_name = %s
                    AND jsonb_typeof(payload->'{_key}') = 'array' LIMIT 25""",
                (collection,))
        else:
            rows = fetchall(
                f"SELECT DISTINCT {expr} AS v FROM chunks WHERE collection_name = %s AND {expr} IS NOT NULL LIMIT 25",
                (collection,))
        vals = [str(r["v"]) for r in rows if r["v"]]
        if len(vals) == 1:
            fields.discard(f)
            continue
        # Enum-ish fields have SHORT values. A "value" longer than ~80 chars
        # is prose (description bodies) — in a small collection every field
        # clears the <=20-distinct bar, and listing ticket bodies as values
        # exploded one spec prompt to 34k chars (> model context, HTTP 400).
        # The field stays queryable; its values just aren't enumerated.
        if any(len(v) > 80 for v in vals):
            continue
        if 0 < len(vals) <= 20:
            values[f] = vals
    return values

def _collection_schema(collection: str) -> Dict:
    """Union of ALL stored schemas for a collection (role -> source columns).
    Schemas are keyed by (collection, source_file_stem) — looking up
    (collection, collection) silently misses single-file collections whose
    stem is the file name, and multi-file collections entirely."""
    merged: Dict[str, list] = {}
    try:
        from core.schema_inference import get_all_schemas_cached
        rows = [r for r in get_all_schemas_cached()
                if r["collection_name"] == collection]
        import json as _json
        merged["_schema_count"] = len(rows)
        for r in rows:
            s = r["schema_json"]
            s = s if isinstance(s, dict) else _json.loads(s)
            for role, cols in s.items():
                if isinstance(cols, list):
                    for c in cols:
                        if c and c not in merged.setdefault(role, []):
                            merged[role].append(c)
    except Exception:
        pass
    return merged


def _schema_role_lines(collection: str) -> str:
    """Ground the extraction LLM in the collection's schema: which system field
    holds which source column ('identifier holds: Moore file name'). Without
    this, the LLM cannot know that a question about 'files' maps to the
    identifier field. Read from the stored schema at runtime — nothing named."""
    try:
        schema = _collection_schema(collection)
        lines = []
        for role in ("identifier", "primary_name", "aliases", "type",
                     "description", "reference_identifier", "tags"):
            cols = schema.get(role) or []
            if cols:
                lines.append(f"- The field '{role}' holds: {', '.join(str(c) for c in cols)}\n")
        if lines:
            return ("Field meanings for this collection (source column names):\n"
                    + "".join(lines))
    except Exception:
        pass
    return ""


def _extract_spec(question: str, collection: str, fields: set, field_values: Dict) -> Optional[Dict]:
    """LLM extracts a structured aggregation spec. Returns None if unusable."""
    from core.local_llm_client import call_local_llm_json

    field_list = ", ".join(sorted(fields))
    schema_lines = _schema_role_lines(collection)
    system_prompt = (
        "You translate a user question into a JSON aggregation spec for a database "
        "of document chunks. Return ONLY JSON with fields:\n"
        "- operation: one of 'count', 'count_distinct', 'list_distinct', 'group_by'\n"
        "- target_field: the field to count/list/group (must be from the allowed list), "
        "or null for plain row counts\n"
        "- filters: list of {field, op, value} where op is 'equals' or 'contains'; "
        "field must be from the allowed list; empty list if no filter\n"
        "- reason: brief, MAX 8 words\n\n"
        "Rules:\n"
        "- 'how many X' counting records -> operation=count_distinct, target_field=identifier "
        "(the unique key). Only use another field when the question asks to count "
        "that field's distinct values specifically.\n"
        "that identifies one X (e.g. primary_name for articles/records).\n"
        "- 'how many rows/chunks' -> operation=count, target_field=null.\n"
        "- 'how many X mention/contain Y' -> count_distinct + filter "
        "{field: nlp_text, op: contains, value: Y}.\n"
        "- 'list all X' -> list_distinct on the field holding X values.\n"
        "- Choose target_field by matching what the question asks about to the field "
        "whose LISTED VALUES contain those things (e.g. if the question asks about "
        "brokers and a field's values are broker names, use that field). "
        "Do the same when choosing filter fields.\n"
        f"- Allowed fields: {field_list}\n"
        + (schema_lines and schema_lines + "- Use these meanings to pick target/filter "
           "fields: if the question asks for the things a field HOLDS (per the meanings "
           "above), target THAT field.\n" or "")
        + "".join(f"- Values of '{k}': {', '.join(v)}\n" for k, v in field_values.items())
        + "- Filter values MUST be copied exactly from the listed values above. "
          "If the question does not mention one of these values, do NOT add that filter.\n"
        "- If the question cannot be answered by counting/listing/grouping these fields, "
        "return {\"operation\": null}.\n"
        "Return only JSON."
    )

    spec = call_local_llm_json(system_prompt, question, temperature=0.0)
    if not isinstance(spec, dict) or spec.get("operation") not in _OPERATIONS:
        return None

    print(f"[METADATA DEBUG] raw spec: {spec}")


    tf = spec.get("target_field")
    if tf is not None and tf not in fields:
        return None
    filters = spec.get("filters") or []
    clean = []
    for f in filters:
        if (isinstance(f, dict) and f.get("field") in fields
                and f.get("op") in _FILTER_OPS and f.get("value") not in (None, "")):
            clean.append({"field": f["field"], "op": f["op"], "value": str(f["value"])})
        else:
            return None  # any invalid filter -> refuse, fall back
    spec["filters"] = clean
    return spec


def _field_expr(field: str, df_keys=frozenset()) -> str:
    """SQL expression for a field — real column, labeled description_fields
    column, or payload key. Field is pre-validated against the collection's
    actual keys, never user-raw."""
    if field in _TABLE_COLUMNS:
        return field
    if field in df_keys:
        return "payload->'description_fields'->>'{}'".format(field.replace("'", ""))
    return "payload->>'{}'".format(field.replace("'", ""))


def _where(collection: str, filters: List[Dict], df_keys=frozenset()):
    clauses = ["collection_name = %s"]
    params: list = [collection]
    for f in filters:
        expr = _field_expr(f["field"], df_keys)
        if f["op"] == "equals":
            # Array-aware equality: a jsonb-array payload field (e.g.
            # versions: ["4.2","4.4"]) matches when it CONTAINS the value;
            # scalar fields compare as before. Decided per-row by type, so
            # mixed collections stay correct.
            if expr.startswith("payload->>"):
                _key = f["field"].replace("'", "")
                clauses.append(
                    "(CASE WHEN jsonb_typeof(payload->'{k}') = 'array' "
                    "THEN EXISTS (SELECT 1 FROM jsonb_array_elements_text("
                    "payload->'{k}') _v WHERE LOWER(_v) = LOWER(%s)) "
                    "ELSE LOWER({e}) = LOWER(%s) END)".format(k=_key, e=expr))
                params.extend([f["value"], f["value"]])
            else:
                clauses.append(f"LOWER({expr}) = LOWER(%s)")
                params.append(f["value"])
        else:
            clauses.append(f"{expr} ILIKE %s")
            params.append(f"%{f['value']}%")
    return " AND ".join(clauses), params

def _best_value_field(question: str, collection: str) -> Optional[str]:
    """Match question against concept vector group labels; return their group_field."""
    try:
        from core.embedder import embed_text
        import numpy as np
        rows = fetchall(
            "SELECT DISTINCT group_field, group_value FROM concept_vectors WHERE collection = %s",
            (collection,))
        if not rows:
            return None
        by_field: Dict[str, list] = {}
        for r in rows:
            by_field.setdefault(r["group_field"], []).append(r["group_value"])
        q = np.array(embed_text(question), dtype=np.float32)
        best, best_sim = None, -1.0
        for f, labels in by_field.items():
            v = np.array(embed_text(", ".join(labels)), dtype=np.float32)
            sim = float(np.dot(q, v) / (np.linalg.norm(q) * np.linalg.norm(v)))
            if sim > best_sim:
                best, best_sim = f, sim
        return best
    except Exception:
        return None

def _concept_label_filter(question: str, collection: str) -> Optional[Dict]:
    """If one concept-vector label clearly matches the question, add it as a filter."""
    try:
        from core.embedder import embed_text
        import numpy as np
        rows = fetchall(
            "SELECT DISTINCT group_field, group_value FROM concept_vectors WHERE collection = %s",
            (collection,))
        if not rows:
            return None
        q = np.array(embed_text(question), dtype=np.float32)
        scored = []
        for r in rows:
            v = np.array(embed_text(str(r["group_value"])), dtype=np.float32)
            sim = float(np.dot(q, v) / (np.linalg.norm(q) * np.linalg.norm(v)))
            scored.append((sim, r["group_field"], r["group_value"]))
        scored.sort(reverse=True)
        top = scored[0]
        # Clear winner only: high absolute sim + margin over runner-up
        if top[0] >= 0.6 and (len(scored) == 1 or top[0] - scored[1][0] >= 0.05):
            return {"field": top[1], "op": "equals", "value": top[2]}
        return None
    except Exception:
        return None

def run_metadata_query(collection: str, question: str, intent_mode: str = None) -> Optional[Dict]:
    """Entry point. Returns {'result': str, 'spec': dict} or None (caller falls back)."""
    try:
        fields, df_keys = _collection_fields(collection)
        if not fields:
            return None
        field_values = _field_values(collection, fields, df_keys)
        spec = _extract_spec(question, collection, fields, field_values)
        if not spec:
            return None

        # The upstream intent classifier already decided list vs count — that
        # is HOW the question reached this path. The spec extraction must not
        # silently overrule it (LLM variance flips 'what tags contain X'
        # between list_distinct and count_distinct run to run).
        if intent_mode == "discovery_list" and spec["operation"] in ("count", "count_distinct"):
            print(f"[METADATA] operation coerced {spec['operation']} -> list_distinct "
                  f"(upstream intent: {intent_mode})")
            spec["operation"] = "list_distinct"
            spec["target_field"] = spec.get("target_field") or "identifier"
        elif intent_mode == "discovery_count" and spec["operation"] in ("list_distinct",):
            print(f"[METADATA] operation coerced {spec['operation']} -> count_distinct "
                  f"(upstream intent: {intent_mode})")
            spec["operation"] = "count_distinct"

        # Chunks are storage units, not records: COUNT(*) answers "how many
        # chunks", which is never what a user asking "how many X" means. A
        # plain count is only honored when the question literally asks for
        # rows/chunks; otherwise count DISTINCT records by the identifier key.
        if spec["operation"] == "count" and not any(
                w in question.lower() for w in ("chunk", "row")):
            print("[METADATA] operation coerced count -> count_distinct(identifier) "
                  "(record count, not chunk count)")
            spec["operation"] = "count_distinct"
            spec["target_field"] = spec.get("target_field") or "identifier"

        # Preemptive concept-vector override applies to group_by ONLY. For
        # list_distinct it repeatedly replaced correct validated LLM picks
        # (unguarded embedding argmax — no threshold/margin); the tautology and
        # degenerate-result guards below are the list safety net instead.
        if spec["operation"] == "group_by":
            from core.schema_inference import load_schema_from_db
            _schema = load_schema_from_db(collection, collection) or {}
            _generic = set((_schema.get("primary_name") or []) + (_schema.get("identifier") or [])
                           + ["primary_name", "identifier"])
            better = _best_value_field(question, collection)
            if better and spec.get("target_field") in _generic and better != spec.get("target_field"):
                print(f"[METADATA] target_field override: {spec.get('target_field')} -> {better}")
                spec["target_field"] = better

        if not spec["filters"]:
            _f = _concept_label_filter(question, collection)
            if _f:
                print(f"[METADATA] filter added from concept labels: {_f}")
                spec["filters"] = [_f]

        # A filter whose VALUE is actually a FIELD NAME is the LLM confusing
        # schema with data (type = 'Prime Broker' where 'Prime Broker' is the
        # column) — drop it outright.
        import re as _re0
        _field_compacts = {_re0.sub(r"[^a-z0-9]", "", str(f0).lower())
                           for f0 in fields}
        _kept_f = []
        for f in spec["filters"]:
            _vc = _re0.sub(r"[^a-z0-9]", "", str(f["value"]).lower())
            if _vc in _field_compacts:
                print(f"[METADATA] dropped field-name-valued filter: {f}")
                continue
            _kept_f.append(f)
        spec["filters"] = _kept_f

        # Value-anchor injection (deterministic): a question token that EXACTLY
        # equals a listed value of a low-cardinality field is an explicit
        # constraint — 'tags' -> identifier_namespace = tag, 'goldman' ->
        # Prime Broker = Goldman. The embedding-based concept filter misses
        # this on variance runs; exact token=value equality never does.
        _qt = {t for t in _re0.findall(r"[a-z0-9]{3,}", question.lower())}
        _qt |= {t[:-1] for t in list(_qt) if t.endswith("s") and len(t) > 3}
        # Declared value aliases (config value_aliases.<collection>): site
        # vocabulary mapped to the data's own terms ('resolved' -> 'Closed').
        # DECLARED, not guessed — the LLM never invents these mappings.
        try:
            from core.system_config import load_system_config
            _aliases = {str(k).lower(): str(v) for k, v in
                        (load_system_config().get("value_aliases", {})
                         .get(collection, {}) or {}).items()}
        except Exception:
            _aliases = {}
        _alias_targets = {v for t, v in _aliases.items() if t in _qt}
        _filtered_fields = {f["field"] for f in spec["filters"]}
        for _fld, _vals in sorted(field_values.items()):
            if _fld in _filtered_fields:
                continue
            for _v in _vals:
                if str(_v).lower() in _qt:
                    spec["filters"].append(
                        {"field": _fld, "op": "equals", "value": str(_v),
                         "_injected": True})
                    _filtered_fields.add(_fld)
                    print(f"[METADATA] value-anchor filter injected: {_fld} = {_v}")
                    break
                if str(_v) in _alias_targets:
                    spec["filters"].append(
                        {"field": _fld, "op": "equals", "value": str(_v),
                         "_injected": True})
                    _filtered_fields.add(_fld)
                    print(f"[METADATA] alias filter injected: {_fld} = {_v} "
                          f"(declared alias)")
                    break

        for f in spec["filters"]:
            if any(f["value"].lower() == v.lower() for v in field_values.get(f["field"], [])):
                continue
            for fld, vals in sorted(field_values.items()):
                if any(f["value"].lower() == v.lower() for v in vals):
                    if f["field"] != fld or f["op"] != "equals":
                        print(f"[METADATA] filter regrounded: {f} -> {fld} equals {f['value']}")
                        f["field"], f["op"] = fld, "equals"
                    break

        # Value-groundedness guard (deterministic): an equals filter is a CLAIM
        # that the question mentions that value. Shown the listed values, the
        # LLM force-maps unknown terms to the nearest one ('Barclays' -> BOA)
        # despite the prompt forbidding it. Grounded means: the raw value is a
        # substring of the question (covers short/multi-word values like 'GS',
        # 'Goldman Sachs'), OR value tokens overlap question tokens, OR a
        # question token appears inside the value's compact form ('goldman' in
        # 'goldmansachs'). Zero overlap = invented — drop. If the drop leaves
        # no filters, the metadata path cannot honestly answer a constrained
        # question: abort to retrieval (low-coverage banner) rather than list
        # everything (the NA-04 dump lesson, from the other direction).
        _g_kept, _g_dropped = [], False
        _q_low = question.lower()
        for f in spec["filters"]:
            if f.get("op") != "equals":
                _g_kept.append(f)
                continue
            if f.get("_injected"):
                # deterministically injected (token==value or declared
                # alias) — grounded by construction, not an LLM claim
                _g_kept.append(f)
                continue
            _v_raw = str(f.get("value", "")).lower()
            _vtoks = {t for t in _re0.findall(r"[a-z0-9]{3,}", _v_raw)}
            _vcompact = _re0.sub(r"[^a-z0-9]", "", _v_raw)
            _grounded = (
                (_v_raw and _v_raw in _q_low)
                or bool(_vtoks & _qt)
                or any(t in _vcompact for t in _qt)
            )
            if _grounded:
                _g_kept.append(f)
            else:
                _g_dropped = True
                print(f"[METADATA] dropped ungrounded filter (value not in question): {f}")
        if _g_dropped and not _g_kept:
            print("[METADATA] all filters ungrounded — aborting metadata path")
            return None
        spec["filters"] = _g_kept

        # Role-name matcher (deterministic, schema-driven): if a question token
        # compact-matches a schema role's SOURCE COLUMN NAME ('files' matches
        # 'Moore file name'), and the LLM's target matches NO question token,
        # the question names its target explicitly and the LLM missed it —
        # retarget. Self-match protection: an LLM pick whose own name matches
        # a question token ('dates' -> date-obs) is never overridden.
        if spec["operation"] == "list_distinct" and spec.get("target_field"):
            try:
                import re as _re2
                _sch = _collection_schema(collection)
                # Role-name matching is only meaningful when the collection has
                # ONE schema — a multi-schema collection (xml_test: 16 files)
                # unions its roles into ambiguity and the matcher fires on noise.
                if _sch.get("_schema_count", 0) != 1:
                    raise StopIteration
                _toks = {t for t in _re2.findall(r"[a-z0-9]+", question.lower()) if len(t) > 2}
                _toks |= {t[:-1] for t in list(_toks) if t.endswith("s") and len(t) > 3}

                def _match_toks(names):
                    out = set()
                    for name in names:
                        compact = _re2.sub(r"[^a-z0-9]", "", str(name).lower())
                        out |= {t for t in _toks if t in compact}
                    return out

                _tf0 = spec["target_field"]
                _tf_names = [_tf0]
                for _r in ("identifier", "primary_name", "aliases", "type", "description"):
                    if _tf0 == _r or _tf0 in (_sch.get(_r) or []):
                        _tf_names += (_sch.get(_r) or [])
                _tf_toks = _match_toks(_tf_names)
                _id_toks = _match_toks(["identifier"] + (_sch.get("identifier") or []))

                if _tf0 != "identifier" and _id_toks and _tf_toks <= _id_toks:
                    # Everything justifying the LLM's pick also justifies the
                    # record key ('files' matches both filename columns) — the
                    # identifier wins ties; a pick justified by EXTRA tokens
                    # ('prime broker files' -> aliases) is kept.
                    print(f"[METADATA] role-name match: target '{_tf0}' -> "
                          f"'identifier' (tie or miss; question tokens "
                          f"{sorted(_id_toks)} name {_sch.get('identifier')})")
                    spec["target_field"] = "identifier"
                elif not _tf_toks:
                    for _role in ("primary_name", "aliases", "type"):
                        if _match_toks(_sch.get(_role) or []):
                            print(f"[METADATA] role-name match: target "
                                  f"'{_tf0}' -> '{_role}' (question names "
                                  f"{_sch.get(_role)})")
                            spec["target_field"] = _role
                            break
            except Exception:
                pass

        # Tautology guard (deterministic, field-agnostic): SELECT DISTINCT x
        # WHERE x = v always returns {v} for ANY field/value — zero information.
        # If the target field collides with an equals-filter field, retarget to
        # the canonical record key (identifier column): a "list the Xs" question
        # wants the matching records, not the filter value echoed back.
        if spec["operation"] in ("list_distinct", "group_by") and spec.get("target_field"):
            _eq_fields = {f["field"] for f in spec["filters"] if f["op"] == "equals"}
            if spec["target_field"] in _eq_fields:
                print(f"[METADATA] tautology guard: target '{spec['target_field']}' "
                      f"is an equals-filter field -> identifier")
                spec["target_field"] = "identifier"

        def _count_with(filters):
            w, p = _where(collection, filters, df_keys)
            if spec["operation"] == "count" or not spec.get("target_field"):
                return fetchall(f"SELECT COUNT(*) AS n FROM chunks WHERE {w}", tuple(p))[0]["n"]
            e = _field_expr(spec["target_field"], df_keys)
            return fetchall(f"SELECT COUNT(DISTINCT {e}) AS n FROM chunks WHERE {w} AND {e} IS NOT NULL", tuple(p))[0]["n"]

        # Zero-result repair for ALL operations (was counts only — a junk
        # filter on a LIST question sent the whole answer to the degenerate
        # guard and lost the collection its seat in arbitration).
        if spec["filters"] and _count_with(spec["filters"]) == 0:
            # Injected-anchor retreat FIRST: anchors are heuristic guesses,
            # LLM filters that survived the groundedness guard are claims.
            # When a guess contradicts a claim across chunk levels
            # (action_type=Resolved lives on ticket_action rows; the injected
            # identifier_namespace=ticket matched only headers -> 0), the
            # guess retreats. Claims are NEVER dropped this way (NA-04 law).
            _claims = [f for f in spec["filters"] if not f.get("_injected")]
            if _claims and len(_claims) < len(spec["filters"])                     and _count_with(_claims) > 0:
                print(f"[METADATA] injected anchors retreated (contradicted "
                      f"grounded claims): kept {_claims}")
                spec["filters"] = _claims
        if spec["filters"] and _count_with(spec["filters"]) == 0:
            _keep = [f for f in spec["filters"] if _count_with([f]) > 0]
            if _keep and _count_with(_keep) > 0:
                print(f"[METADATA] dropped zero-result filters, kept: {_keep}")
                spec["filters"] = _keep
            # NOTE: never drop ALL filters. When every filter matches nothing,
            # that IS the answer ("FIX 5.0 SP2" doesn't exist in the data) —
            # dropping them turned a no-answer trap into a 200-row dump
            # (NA-04 regression). Junk filters are handled upstream by the
            # field-name-valued drop; honest-but-unmatched filters must fail.

        # Dedupe filters — value-anchor injection + regrounding can mirror the
        # LLM's own filter ("Prime Broker = Goldman" twice + "type = Goldman").
        # Same VALUE on multiple fields is also redundant: keep the first.
        _seen_fv, _dedup = set(), []
        for f in spec["filters"]:
            _k = str(f["value"]).lower()
            if (f["field"], f["op"], _k) in _seen_fv or _k in {v for _, _, v in _seen_fv}:
                continue
            _seen_fv.add((f["field"], f["op"], _k))
            _dedup.append(f)
        spec["filters"] = _dedup

        where, params = _where(collection, spec["filters"], df_keys)
        op = spec["operation"]
        tf = spec.get("target_field")

        # Human-readable filter summary — every answer states WHAT was matched
        # (e.g. "type = String"), so the answer is self-explanatory.
        _fdesc = ", ".join(
            f"{f['field']} {'=' if f['op'] == 'equals' else 'contains'} {f['value']}"
            for f in spec["filters"])
        _suffix = f" matching {_fdesc}" if _fdesc else ""

        _for = f" for {_fdesc}" if _fdesc else ""
        if op == "count" or (op == "count_distinct" and not tf):
            rows = fetchall(f"SELECT COUNT(*) AS n FROM chunks WHERE {where}", tuple(params))
            answer = f"{rows[0]['n']} record(s) match{_for}."
        elif op == "count_distinct":
            expr = _field_expr(tf, df_keys)
            rows = fetchall(
                f"SELECT COUNT(DISTINCT {expr}) AS n FROM chunks WHERE {where} AND {expr} IS NOT NULL",
                tuple(params))
            answer = f"There are {rows[0]['n']} matching {tf or 'record'}(s){_for}."
        elif op == "list_distinct":
            if not tf:
                return None
            expr = _field_expr(tf, df_keys)
            # Record-style listing for the name-ish system columns: a bare value
            # carries little meaning alone, so append a companion column —
            # identifier gets its primary_name, primary_name gets its description
            # (truncated). Other targets (dates, types, paths) stay bare values.
            _companion = {"identifier": "primary_name", "primary_name": "description"}.get(tf)
            if _companion:
                # DISPLAY key: when the collection's schema maps the identifier
                # role to a real payload field (halo: ticket_id), listings show
                # THAT value — '44539', not the internal chunk id '44539-a17'.
                # Query semantics unchanged; label only. Collections whose
                # schema column mirrors the chunk identifier (recon) render
                # identically.
                _disp = tf
                if tf == "identifier":
                    try:
                        _id_cols = _collection_schema(collection).get("identifier") or []
                        _all_fields, _dfk = fields, df_keys
                        for _c in _id_cols:
                            if _c in _all_fields and _c != "identifier":
                                _disp = _field_expr(_c, _dfk)
                                break
                    except Exception:
                        _disp = tf
                rows = fetchall(
                    f"SELECT DISTINCT {tf} AS v0, COALESCE({_disp}, {tf}) AS v, "
                    f"{_companion} AS c FROM chunks "
                    f"WHERE {where} AND {tf} IS NOT NULL ORDER BY v LIMIT 200",
                    tuple(params))
                # Markdown-safe: blank line before the list (CommonMark) and
                # code spans around values so underscores aren't italicized.
                def _trunc(t):
                    t = " ".join(str(t or "").split())
                    return t[:100] + ("…" if len(t) > 100 else "")
                items = []
                _seen_v = set()
                for r in rows:
                    if not r["v"] or r["v"] in _seen_v:
                        continue  # one line per distinct value even if companions differ
                    _seen_v.add(r["v"])
                    _seen_v.add(r.get("v0"))
                    c = _trunc(r.get("c"))
                    # Code-like companions (no spaces, e.g. job names) need code
                    # spans so markdown doesn't italicize their underscores.
                    if c and " " not in c:
                        c = f"`{c}`"
                    items.append(f"`{r['v']}`" + (f" — {c}" if c else ""))
                if not items:
                    print("[METADATA] degenerate list result -> fallback to discovery")
                    return None
                answer = (f"{len(items)} record(s){_suffix}:\n\n"
                          + "\n".join(f"- {i}" for i in items))
                return {"result": answer, "spec": spec}
            # Array-valued payload fields (aliases, reference_identifiers):
            # list DISTINCT ELEMENTS, not raw JSON strings.
            _is_array = bool(fetchall(
                f"SELECT 1 FROM chunks WHERE collection_name = %s "
                f"AND jsonb_typeof(payload->'{tf.replace(chr(39), '')}') = 'array' LIMIT 1",
                (collection,))) if tf not in _TABLE_COLUMNS and tf not in df_keys else False
            if _is_array:
                rows = fetchall(
                    f"SELECT DISTINCT jsonb_array_elements_text(payload->'{tf.replace(chr(39), '')}') AS v "
                    f"FROM chunks WHERE {where} ORDER BY v LIMIT 200",
                    tuple(params))
            else:
                rows = fetchall(
                    f"SELECT DISTINCT {expr} AS v FROM chunks WHERE {where} AND {expr} IS NOT NULL ORDER BY v LIMIT 200",
                    tuple(params))
            vals = [r["v"] for r in rows if r["v"]]

            # Degenerate-result guard: a list that is empty or merely echoes the
            # filter value(s) back carries no information — return None so the
            # caller falls back to the discovery engine.
            _fvals = {str(f["value"]).lower() for f in spec["filters"]}
            if not vals or all(str(v).lower() in _fvals for v in vals):
                print("[METADATA] degenerate list result -> fallback to discovery")
                return None

            import re as _re
            _ts = [_v for _v in vals if _re.match(r'^\d{4}-\d{2}-\d{2}T', str(_v))]
            if len(_ts) == len(vals) and vals:
                vals = sorted({str(_v)[:10] for _v in vals})

            answer = (f"{len(vals)} value(s){_suffix}:\n\n"
                      + "\n".join(f"- `{v}`" for v in vals))
        else:  # group_by
            if not tf:
                return None
            expr = _field_expr(tf, df_keys)
            rows = fetchall(
                f"SELECT {expr} AS v, COUNT(*) AS n FROM chunks WHERE {where} AND {expr} IS NOT NULL "
                f"GROUP BY v ORDER BY n DESC LIMIT 50",
                tuple(params))
            if not rows:
                print("[METADATA] empty group_by result -> fallback to discovery")
                return None
            answer = "\n".join(f"- `{r['v']}`: {r['n']}" for r in rows)

        return {"result": answer, "spec": spec}
    except Exception as e:
        print(f"[METADATA] query failed, falling back to discovery: {e}")
        return None