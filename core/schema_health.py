"""core/schema_health.py — self-service audit of stored schemas vs actual
rows (user request 2026-08-06, after three fossil-schema hunts: the
user-defined CSV's null primary_names, astro_catalog's label-free rows,
and v_mag riding the type role).

Every check is a data pattern, no domain knowledge:
  * null-name rate        — primary_name empty on entity rows
  * label-free rate       — description AND description_fields both empty
                            (render has nothing but bare text)
  * numeric type role     — type values parse as continuous numbers
                            (a measure column scrambled into the role)
  * identifier health     — null / duplicate-heavy identifiers
  * stale schema columns  — schema names a column no payload carries
"""
from typing import Dict, List

from core.db import fetchall


def _f(v):
    try:
        float(str(v))
        return True
    except Exception:
        return False


def schema_health(collection: str) -> List[Dict]:
    out = []

    def add(check, severity, detail):
        out.append({"collection": collection, "check": check,
                    "severity": severity, "detail": detail})

    base = fetchall("""
        SELECT COUNT(*) AS n,
               COUNT(*) FILTER (WHERE COALESCE(primary_name,'') = '')
                   AS null_names,
               COUNT(*) FILTER (WHERE COALESCE(description,'') = ''
                   AND COALESCE(payload->>'description_fields','') IN
                       ('', '{}', 'null')) AS label_free,
               COUNT(*) FILTER (WHERE COALESCE(identifier,'') = '')
                   AS null_ids,
               COUNT(DISTINCT identifier) AS distinct_ids
        FROM chunks WHERE collection_name = %s
    """, (collection,))
    if not base or not base[0]["n"]:
        return out
    b = base[0]
    n = b["n"]

    if b["null_names"] / n > 0.5:
        add("null primary_name", "HIGH",
            f"{b['null_names']}/{n} rows have no name — the schema's "
            f"primary_name role probably misses the real name column "
            f"(user-defined CSV disease)")
    if b["label_free"] / n > 0.5:
        add("label-free rows", "HIGH",
            f"{b['label_free']}/{n} rows have neither description nor "
            f"labeled fields — renders show bare text (astro_catalog "
            f"disease); re-check role assignments")
    if b["null_ids"] / n > 0.2:
        add("null identifiers", "MEDIUM",
            f"{b['null_ids']}/{n} rows lack an identifier")

    # type role sanity: continuous numbers = probable scramble
    tvals = fetchall("""
        SELECT DISTINCT payload->>'type' AS v FROM chunks
        WHERE collection_name = %s AND payload->>'type' IS NOT NULL
        LIMIT 50
    """, (collection,))
    vals = [r["v"] for r in tvals if r["v"]]
    if len(vals) >= 10 and sum(1 for v in vals if _f(v)) / len(vals) > 0.8:
        add("numeric type role", "HIGH",
            f"type role holds numbers ({', '.join(vals[:5])}…) — a "
            f"measure column (v_mag disease) is probably mapped into it")

    # stale schema columns: schema names columns no payload carries
    try:
        from core.schema_inference import get_all_schemas_cached
        import json as _j
        keys_rows = fetchall("""
            SELECT DISTINCT jsonb_object_keys(payload) AS k
            FROM (SELECT payload FROM chunks
                  WHERE collection_name = %s LIMIT 200) s
        """, (collection,))
        df_rows = fetchall("""
            SELECT DISTINCT jsonb_object_keys(payload->'description_fields') AS k
            FROM (SELECT payload FROM chunks
                  WHERE collection_name = %s
                  AND jsonb_typeof(payload->'description_fields') = 'object'
                  LIMIT 200) s
        """, (collection,))
        known = ({r["k"] for r in keys_rows} | {r["k"] for r in df_rows})
        for srow in get_all_schemas_cached():
            if srow["collection_name"] != collection:
                continue
            sch = srow["schema_json"]
            sch = sch if isinstance(sch, dict) else _j.loads(sch)
            missing = []
            for role, cols in sch.items():
                if not isinstance(cols, list):
                    continue
                for c in cols:
                    if c and c not in known:
                        missing.append(f"{role}:{c}")
            if missing:
                add("stale schema columns", "MEDIUM",
                    f"stem '{srow['source_file_stem']}' maps columns not "
                    f"present in any payload: {', '.join(missing[:8])}")
    except Exception as e:
        add("schema read", "LOW", f"could not audit stored schemas: {e}")

    if not out:
        add("ok", "OK", f"{n} rows — no schema-health flags")
    return out


def schema_health_all() -> List[Dict]:
    rows = fetchall(
        "SELECT DISTINCT collection_name AS c FROM chunks ORDER BY 1", ())
    out = []
    for r in rows:
        out.extend(schema_health(r["c"]))
    return out
