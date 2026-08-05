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
_FILTER_OPS = {"equals", "not_equals", "contains"}
_OP_SYM = {"equals": "=", "not_equals": "!="}


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
    # ingestion-time 'about' scan annotations — audit/rerank metadata,
    # never filterable fields
    "about", "keywords", "related",
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
                f"SELECT DISTINCT {expr} AS v FROM chunks WHERE collection_name = %s AND {expr} IS NOT NULL ORDER BY v LIMIT 25",
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


# Spec cache: the spec depends only on (question, collection, schema) —
# none change within a turn, yet the answer path, the discovery path and
# every widening retry each re-extracted it (6 x 10-27s of LLM time on one
# question). Deep-copied on store AND fetch: downstream mutates filters in
# place (coercions, splits), and a shared dict would poison the next
# consumer with the previous one's mutations. TTL declared in config
# (metadata_spec_cache_ttl) so schema edits age out.
_SPEC_CACHE: Dict = {}


def _spec_cache_ttl() -> float:
    try:
        import json as _json
        from core.paths import SYSTEM_CONFIG_PATH
        with open(SYSTEM_CONFIG_PATH, "r", encoding="utf-8") as f:
            return float(_json.load(f).get("metadata_spec_cache_ttl", 300))
    except Exception:
        return 300.0


def _extract_spec(question: str, collection: str, fields: set, field_values: Dict) -> Optional[Dict]:
    """LLM extracts a structured aggregation spec. Returns None if unusable.
    Memoized per (collection, normalized question) — see _SPEC_CACHE."""
    import copy as _copy
    import re as _re_sc
    import time as _time_sc
    _key = (collection,
            _re_sc.sub(r"[^a-z0-9]+", " ", str(question).lower()).strip())
    _hit = _SPEC_CACHE.get(_key)
    if _hit and _time_sc.time() - _hit[0] < _spec_cache_ttl():
        print(f"[METADATA] spec cache hit for {collection}")
        return _copy.deepcopy(_hit[1])
    _spec = _extract_spec_llm(question, collection, fields, field_values)
    _SPEC_CACHE[_key] = (_time_sc.time(), _copy.deepcopy(_spec))
    if len(_SPEC_CACHE) > 200:
        _SPEC_CACHE.pop(next(iter(_SPEC_CACHE)))
    return _spec


def _extract_spec_llm(question: str, collection: str, fields: set, field_values: Dict) -> Optional[Dict]:
    """The actual LLM call — only reached on cache miss."""
    from core.local_llm_client import call_local_llm_json

    field_list = ", ".join(sorted(fields))
    schema_lines = _schema_role_lines(collection)
    system_prompt = (
        "You translate a user question into a JSON aggregation spec for a database "
        "of document chunks. Return ONLY JSON with fields:\n"
        "- operation: one of 'count', 'count_distinct', 'list_distinct', 'group_by'\n"
        "- target_field: the field to count/list/group (must be from the allowed list), "
        "or null for plain row counts\n"
        "- filters: list of {field, op, value} where op is 'equals', 'not_equals' or 'contains'; use not_equals for NEGATED questions ('not resolved', 'except closed', 'other than X'); "
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
        if f["op"] == "not_equals":
            # NULL-safe scalar inequality; array fields exclude rows whose
            # array CONTAINS the value.
            if expr.startswith("payload->>"):
                _key = f["field"].replace("'", "")
                clauses.append(
                    "(CASE WHEN jsonb_typeof(payload->'{k}') = 'array' "
                    "THEN NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text("
                    "payload->'{k}') _v WHERE LOWER(_v) = LOWER(%s)) "
                    "ELSE LOWER(COALESCE({e}, '')) != LOWER(%s) END)".format(
                        k=_key, e=expr))
                params.extend([f["value"], f["value"]])
            else:
                clauses.append(f"LOWER(COALESCE({expr}, '')) != LOWER(%s)")
                params.append(f["value"])
        elif f["op"] == "equals":
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
            # (Tried and reverted 2026-07-29: OR-ing the 'about' line into
            # nlp_text contains-filters — abouts generically say issue/fix/
            # order, listing noise jumped 7->13. Synonym gaps ('bad dates'
            # vs 'incorrect dates') are declared-glossary territory, not a
            # substring problem.)
            _syns = [s for s in (f.get("_synonyms") or []) if str(s).strip()]
            if _syns:
                # Declared-equivalent phrases OR together: the site says
                # 'bad dates' == 'incorrect dates'; matching either
                # satisfies this filter.
                _alts = [f["value"]] + _syns
                clauses.append("(" + " OR ".join(
                    [f"{expr} ILIKE %s"] * len(_alts)) + ")")
                params.extend([f"%{a}%" for a in _alts])
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

def _about_closest(collection, filters, question, df_keys, count_with,
                   limit=5):
    """Anchored semantic fallback for zeroed descriptor claims: entries
    matching the STILL-GROUNDED claim filters (+injected anchors), ranked
    by about-vector similarity to the question. Returns an honest
    closest-match answer string, or None when no claim grounds (a question
    about nothing real must stay a zero — the Bloomberg lesson) or no
    about vectors exist."""
    try:
        _alive = [f for f in filters
                  if f.get("_injected") or count_with([f]) > 0]
        if not any(not f.get("_injected") for f in _alive):
            return None
        w, p = _where(collection, _alive, df_keys)
        rows = fetchall(
            f"SELECT primary_name, MIN(identifier) AS ident FROM chunks "
            f"WHERE {w} AND COALESCE(primary_name,'') != '' "
            f"GROUP BY primary_name LIMIT 50", tuple(p))
        if not rows:
            return None
        import json as _json
        from core.embedder import embed_text
        _qv = _json.dumps(embed_text(question))
        _names = [r["primary_name"] for r in rows]
        _ids = {r["primary_name"]: r["ident"] for r in rows}
        av = fetchall(
            "SELECT primary_name, about, 1 - (embedding <=> %s::vector) AS sim "
            "FROM about_vectors WHERE collection_name = %s "
            "AND primary_name = ANY(%s) ORDER BY sim DESC LIMIT %s",
            (_qv, collection, _names, int(limit)))
        if not av:
            return None
        _grounded_desc = ", ".join(
            f"{f['field']} {_OP_SYM.get(f['op'], 'contains')} "
            f"{f['value']}" for f in _alive if not f.get("_injected"))
        lines = [f"No exact match found for the full wording — closest "
                 f"entries among those matching {_grounded_desc}, by what "
                 f"they are about:"]
        for r in av:
            _about_s = " ".join(str(r["about"] or "")[:180].split())
            lines.append(f"- `{_ids.get(r['primary_name'], '')}` — "
                         f"{r['primary_name']}: {_about_s}")
        print(f"[METADATA] about-closest fallback: {len(av)} entries "
              f"(anchor: {_grounded_desc})")
        return "\n\n".join(lines)
    except Exception as e:
        print(f"[METADATA] about-closest fallback failed: {e}")
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
        _alias_pairs = [(t, v) for t, v in _aliases.items() if t in _qt]
        _alias_targets = {v for _, v in _alias_pairs}

        def _negated(site_word):
            """Deterministic negation scent: a negator within two words
            before the site word ('not resolved', "aren't closed",
            'not yet resolved')."""
            return bool(_re0.search(
                r"\b(?:not|no|non|never|isn'?t|aren'?t|wasn'?t|weren'?t|"
                r"without|except|excluding|un)\W+(?:\w+\W+){0,2}?"
                + _re0.escape(site_word), question.lower()))

        _filtered_fields = {f["field"] for f in spec["filters"]}
        for _fld, _vals in sorted(field_values.items()):
            if _fld in _filtered_fields:
                continue
            for _v in _vals:
                if str(_v).lower() in _qt:
                    _op = ("not_equals" if _negated(str(_v).lower())
                           else "equals")
                    spec["filters"].append(
                        {"field": _fld, "op": _op, "value": str(_v),
                         "_injected": True})
                    _filtered_fields.add(_fld)
                    print(f"[METADATA] value-anchor filter injected: "
                          f"{_fld} {_op} {_v}")
                    break
                _t_hit = next((t for t, v in _alias_pairs if v == str(_v)),
                              None)
                if _t_hit is not None:
                    # negation is checked against the USER'S word ('not
                    # resolved'), not the data value ('Closed')
                    _op = ("not_equals" if _negated(_t_hit) else "equals")
                    spec["filters"].append(
                        {"field": _fld, "op": _op, "value": str(_v),
                         "_injected": True})
                    _filtered_fields.add(_fld)
                    print(f"[METADATA] alias filter injected: {_fld} {_op} "
                          f"{_v} (declared alias)")
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

        # ALIAS POLARITY GUARD (deterministic): a value grounded by a
        # declared alias must carry the operator the QUESTION's polarity
        # dictates — 'are resolved' (alias resolved->Closed, not negated)
        # means equals Closed; the LLM emitted not_equals Closed and
        # inverted AR-02 (2026-08-03). The declaration outranks the LLM's
        # choice of operator, both directions.
        for f in spec["filters"]:
            if f.get("op") not in ("equals", "not_equals"):
                continue
            _tok_ap = next((t for t, v in _alias_pairs
                            if v == str(f.get("value"))), None)
            if _tok_ap is None:
                continue
            _want_ap = "not_equals" if _negated(_tok_ap) else "equals"
            if f["op"] != _want_ap:
                print(f"[METADATA] alias polarity corrected: {f['field']} "
                      f"{f['op']} -> {_want_ap} {f['value']} "
                      f"(alias '{_tok_ap}')")
                f["op"] = _want_ap

        # Multi-word CONTAINS phrases match nothing unless the words are
        # adjacent in the text ('FRA dates' never occurs; 'FRA' and 'dates'
        # both do). Degrade to one contains-filter per content word — AND
        # semantics preserved, adjacency requirement dropped.
        _split_f = []
        for f in spec["filters"]:
            _v = str(f.get("value", ""))
            if (f.get("op") == "contains" and " " in _v.strip()
                    and not f.get("_injected")):
                # Declared glossary phrases stay WHOLE with their synonyms as
                # OR-alternatives ('bad dates' also matches 'incorrect
                # dates') — the site declared them equivalent; splitting
                # would lose the phrase, skipping would lose the synonyms.
                _v_low_cf = _v.lower()
                _gloss_hit = None
                try:
                    from core.glossary import load_glossary, synonyms_for
                    for _term_cf in sorted(load_glossary(), key=len,
                                           reverse=True):
                        if " " in _term_cf and _re0.search(
                                r"\b" + _re0.escape(_term_cf) + r"\b",
                                _v_low_cf):
                            _gloss_hit = (_term_cf, synonyms_for(_term_cf))
                            break
                except Exception:
                    _gloss_hit = None
                if _gloss_hit:
                    _term_cf, _syns_cf = _gloss_hit
                    print(f"[METADATA] glossary phrase filter: {_term_cf!r} "
                          f"(+ synonyms {_syns_cf})")
                    _split_f.append({"field": f["field"], "op": "contains",
                                     "value": _term_cf,
                                     "_synonyms": _syns_cf,
                                     "_split_group": _v})
                    _v_rest_cf = _re0.sub(
                        r"\b" + _re0.escape(_term_cf) + r"\b", " ", _v_low_cf)
                else:
                    _v_rest_cf = _v_low_cf
                _words_cf = [w for w in _re0.findall(r"[a-z0-9]{3,}",
                                                     _v_rest_cf)]
                # De-pluralize: contains is substring, so 'date' already
                # matches 'dates' — but 'issues' can never match 'issue'.
                # Filtering on the singular form matches both.
                _words_cf = [(w[:-1] if w.endswith("s") and len(w) > 3
                              and not w.endswith("ss") else w)
                             for w in _words_cf]
                # After a glossary phrase was extracted, the ORIGINAL filter
                # must never fall through (its unsplit phrase would AND-kill
                # the group) — remainder words join the split regardless of
                # count.
                if _gloss_hit or len(_words_cf) > 1:
                    if _words_cf:
                        print(f"[METADATA] split phrase contains filter: "
                              f"{_v!r} -> {_words_cf}")
                    for _w_cf in _words_cf:
                        _split_f.append({"field": f["field"], "op": "contains",
                                         "value": _w_cf,
                                         "_split_group": _v})
                    continue
            _split_f.append(f)
        spec["filters"] = _split_f

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
            if f.get("op") not in ("equals", "not_equals"):
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
            # Numeric values ground NUMERICALLY: JSON floats render '30'
            # as '30.0', which fails every string test against a question
            # that says 'exposure 30'. If the value parses as a number,
            # equality against any number token in the question grounds it
            # ('30' == 30.0); short numbers (<3 chars) are invisible to the
            # token regex, so scan raw number tokens here.
            _num_grounded = False
            try:
                _v_num = float(_v_raw)
                _num_grounded = any(
                    float(t) == _v_num
                    for t in _re0.findall(r"\d+(?:\.\d+)?", _q_low))
            except ValueError:
                pass
            # Compact containment needs COVERAGE: 'goldman' grounding
            # 'goldmansachs' (58%) is a name-prefix; 'halo' inside
            # 'halodemoorganization' (20%) blessed an invented filter
            # (eval 2026-08-02). Ratio declared in config.
            try:
                import json as _json_gr
                from core.paths import SYSTEM_CONFIG_PATH as _SCP_GR
                with open(_SCP_GR, "r", encoding="utf-8") as _fgr:
                    _min_ratio = float(_json_gr.load(_fgr).get(
                        "grounding_min_token_ratio", 0.4))
            except Exception:
                _min_ratio = 0.4
            _grounded = (
                _num_grounded
                or (_v_raw and _v_raw in _q_low)
                or bool(_vtoks & _qt)
                or any(t in _vcompact
                       and len(t) >= _min_ratio * len(_vcompact)
                       for t in _qt)
                # declared alias target ('resolved' in question grounds
                # 'Closed') — trusted by declaration, both polarities
                or str(f.get("value")) in _alias_targets
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
            # Split-phrase parts are ONE claim: if any part of a group died,
            # the whole group dies with it — keeping 'fix' after 'sp2'
            # zeroed turned the FIX 5.0 SP2 no-answer trap into a 24-row
            # dump (the phrase was the claim, not its words).
            _dead_groups = {f.get("_split_group") for f in spec["filters"]
                            if f.get("_split_group") and f not in _keep}
            _keep = [f for f in _keep
                     if f.get("_split_group") not in _dead_groups]
            _had_claims = any(not f.get("_injected") for f in spec["filters"])
            _kept_claims = any(not f.get("_injected") for f in _keep)
            if _had_claims and not _kept_claims:
                # every CONTENT claim matches nothing — that IS the answer.
                # Keeping only injected anchors would answer "about FRA"
                # with "all tickets" (honest-looking breadth dump).
                print("[METADATA] all content claims zero-result — honest "
                      "zero kept (injected anchors may not stand alone)")
                # ANCHORED about-similarity fallback: descriptor words
                # zeroed ('bad dates' — the data says 'dates are being
                # modified'), but some claim words still ground ('fra').
                # Rank ONLY the grounded entries by about-vector similarity
                # to the full question and answer with an honest
                # closest-match label (existing weakness marker, so
                # arbitration/widening treat it correctly). Vocabulary
                # chasing via synonyms was rejected as hardcoding; semantic
                # aboutness is what the ingestion scan is FOR.
                _sem = _about_closest(collection, spec["filters"], question,
                                      df_keys, _count_with)
                if _sem:
                    return {"result": _sem, "spec": spec}
            elif _keep and _count_with(_keep) > 0:
                print(f"[METADATA] dropped zero-result filters, kept: {_keep}")
                spec["filters"] = _keep
            elif _keep:
                # The kept combination itself zeroes — usually a surviving
                # claim on one chunk level AND an injected anchor on
                # another (action_type=Resolved lives on actions; the
                # namespace=ticket anchor matches headers). Anchors are
                # guesses: retreat them WITHIN the kept set too.
                _keep_claims = [f for f in _keep if not f.get("_injected")]
                if _keep_claims and _count_with(_keep_claims) > 0:
                    print(f"[METADATA] injected anchors retreated within "
                          f"kept set: {_keep_claims}")
                    spec["filters"] = _keep_claims
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
            f"{f['field']} {_OP_SYM.get(f['op'], 'contains')} {f['value']}"
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
                    # identifier listings show BOTH the raw id and the
                    # display value when they differ ('959 — AuctionType —
                    # desc', user request 2026-08-05) — a bare name loses
                    # the tag number, a bare number loses the name.
                    _v0 = str(r.get("v0") or "")
                    _vd = str(r["v"])
                    # prefix-related = the display DERIVES from the raw id
                    # (halo '44539-a17' -> '44539'): keep hiding the
                    # internal id. Unrelated (tag '959' vs 'AuctionType'):
                    # show both.
                    if (_v0 and _v0 != _vd
                            and not _v0.startswith(_vd)
                            and not _vd.startswith(_v0)):
                        _lead = f"`{_v0}` — `{_vd}`"
                    else:
                        _lead = f"`{_vd}`"
                    # drop the companion when it just repeats the display
                    if c and c.strip("`") == _vd:
                        c = ""
                    items.append(_lead + (f" — {c}" if c else ""))
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