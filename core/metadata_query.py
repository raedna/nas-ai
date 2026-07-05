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


def _collection_fields(collection: str) -> set:
    """All payload keys present in this collection (sampled) + table columns."""
    rows = fetchall("""
        SELECT DISTINCT jsonb_object_keys(payload) AS k
        FROM (SELECT payload FROM chunks
              WHERE collection_name = %s LIMIT 200) s
    """, (collection,))
    keys = {r["k"] for r in rows}
    return keys | _TABLE_COLUMNS

def _field_values(collection: str, fields: set) -> Dict[str, list]:
    """Distinct values for low-cardinality fields — grounds the LLM in real values."""
    values = {}
    for f in fields:
        expr = _field_expr(f)
        rows = fetchall(
            f"SELECT DISTINCT {expr} AS v FROM chunks WHERE collection_name = %s AND {expr} IS NOT NULL LIMIT 25",
            (collection,))
        vals = [str(r["v"]) for r in rows if r["v"]]
        if 0 < len(vals) <= 20:
            values[f] = vals
    return values

def _extract_spec(question: str, collection: str, fields: set, field_values: Dict) -> Optional[Dict]:
    """LLM extracts a structured aggregation spec. Returns None if unusable."""
    from core.local_llm_client import call_local_llm_json

    field_list = ", ".join(sorted(fields))
    system_prompt = (
        "You translate a user question into a JSON aggregation spec for a database "
        "of document chunks. Return ONLY JSON with fields:\n"
        "- operation: one of 'count', 'count_distinct', 'list_distinct', 'group_by'\n"
        "- target_field: the field to count/list/group (must be from the allowed list), "
        "or null for plain row counts\n"
        "- filters: list of {field, op, value} where op is 'equals' or 'contains'; "
        "field must be from the allowed list; empty list if no filter\n"
        "- reason: brief\n\n"
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


def _field_expr(field: str) -> str:
    """SQL expression for a field — real column or payload key. Field is
    pre-validated against the collection's actual keys, never user-raw."""
    if field in _TABLE_COLUMNS:
        return field
    return "payload->>'{}'".format(field.replace("'", ""))


def _where(collection: str, filters: List[Dict]):
    clauses = ["collection_name = %s"]
    params: list = [collection]
    for f in filters:
        expr = _field_expr(f["field"])
        if f["op"] == "equals":
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

def run_metadata_query(collection: str, question: str) -> Optional[Dict]:
    """Entry point. Returns {'result': str, 'spec': dict} or None (caller falls back)."""
    try:
        fields = _collection_fields(collection)
        if not fields:
            return None
        field_values = _field_values(collection, fields)
        spec = _extract_spec(question, collection, fields, field_values)
        if not spec:
            return None

        if spec["operation"] in ("group_by", "list_distinct"):
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

        for f in spec["filters"]:
            if any(f["value"].lower() == v.lower() for v in field_values.get(f["field"], [])):
                continue
            for fld, vals in sorted(field_values.items()):
                if any(f["value"].lower() == v.lower() for v in vals):
                    if f["field"] != fld or f["op"] != "equals":
                        print(f"[METADATA] filter regrounded: {f} -> {fld} equals {f['value']}")
                        f["field"], f["op"] = fld, "equals"
                    break

        def _count_with(filters):
            w, p = _where(collection, filters)
            if spec["operation"] == "count" or not spec.get("target_field"):
                return fetchall(f"SELECT COUNT(*) AS n FROM chunks WHERE {w}", tuple(p))[0]["n"]
            e = _field_expr(spec["target_field"])
            return fetchall(f"SELECT COUNT(DISTINCT {e}) AS n FROM chunks WHERE {w} AND {e} IS NOT NULL", tuple(p))[0]["n"]

        if spec["operation"] in ("count", "count_distinct") and spec["filters"]:
            if _count_with(spec["filters"]) == 0:
                _keep = [f for f in spec["filters"] if _count_with([f]) > 0]
                if _keep and _count_with(_keep) > 0:
                    print(f"[METADATA] dropped zero-result filters, kept: {_keep}")
                    spec["filters"] = _keep

        where, params = _where(collection, spec["filters"])
        op = spec["operation"]
        tf = spec.get("target_field")

        if op == "count" or (op == "count_distinct" and not tf):
            rows = fetchall(f"SELECT COUNT(*) AS n FROM chunks WHERE {where}", tuple(params))
            answer = f"{rows[0]['n']} records match."
        elif op == "count_distinct":
            expr = _field_expr(tf)
            rows = fetchall(
                f"SELECT COUNT(DISTINCT {expr}) AS n FROM chunks WHERE {where} AND {expr} IS NOT NULL",
                tuple(params))
            answer = f"There are {rows[0]['n']} matching {tf or 'record'}(s)."
        elif op == "list_distinct":
            if not tf:
                return None
            expr = _field_expr(tf)
            rows = fetchall(
                f"SELECT DISTINCT {expr} AS v FROM chunks WHERE {where} AND {expr} IS NOT NULL ORDER BY v LIMIT 200",
                tuple(params))
            vals = [r["v"] for r in rows if r["v"]]

            import re as _re
            _ts = [_v for _v in vals if _re.match(r'^\d{4}-\d{2}-\d{2}T', str(_v))]
            if len(_ts) == len(vals) and vals:
                vals = sorted({str(_v)[:10] for _v in vals})
                
            answer = f"{len(vals)} value(s): " + ", ".join(vals)
        else:  # group_by
            if not tf:
                return None
            expr = _field_expr(tf)
            rows = fetchall(
                f"SELECT {expr} AS v, COUNT(*) AS n FROM chunks WHERE {where} AND {expr} IS NOT NULL "
                f"GROUP BY v ORDER BY n DESC LIMIT 50",
                tuple(params))
            answer = "\n".join(f"- {r['v']}: {r['n']}" for r in rows)

        return {"result": answer, "spec": spec}
    except Exception as e:
        print(f"[METADATA] query failed, falling back to discovery: {e}")
        return None