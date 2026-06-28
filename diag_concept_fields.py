"""
diag_concept_fields.py
Diagnostic for concept_vector_builder redesign (CV-01..04).
Run:  python3 diag_concept_fields.py
Gathers everything needed to design grouping logic without guessing payload shapes.
"""
import sys, json, collections
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.db import fetchall


def jget(r, k):
    """fetchall rows may be dicts or tuples — normalize to dict access by alias."""
    return r[k]


def main():
    cols = fetchall("SELECT DISTINCT collection_name AS c FROM chunks ORDER BY c")
    col_names = [jget(r, "c") for r in cols]
    print("=" * 70)
    print("COLLECTIONS:", col_names)
    print("=" * 70)

    for c in col_names:
        print("\n" + "#" * 70)
        print(f"# COLLECTION: {c}")
        print("#" * 70)

        # total
        n = fetchall("SELECT COUNT(*) AS n FROM chunks WHERE collection_name=%s", (c,))
        total = jget(n[0], "n")
        print(f"total chunks: {total}")

        # payload keys present
        keys = fetchall("""
            SELECT DISTINCT jsonb_object_keys(payload) AS k
            FROM chunks WHERE collection_name=%s
        """, (c,))
        keylist = sorted(jget(r, "k") for r in keys)
        print(f"payload keys ({len(keylist)}): {keylist}")

        # doc_type distribution
        dt = fetchall("""
            SELECT payload->>'doc_type' AS dt, COUNT(*) AS n
            FROM chunks WHERE collection_name=%s
            GROUP BY dt ORDER BY n DESC
        """, (c,))
        print("doc_type:", [(jget(r, "dt"), jget(r, "n")) for r in dt])

        # presence + samples of the fields we care about
        for fld in ("section_heading", "kb_tags", "folder_path", "category",
                    "source_file", "identifier", "primary_name", "type",
                    "identifier_namespace"):
            present = fetchall("""
                SELECT COUNT(*) AS n
                FROM chunks
                WHERE collection_name=%s AND payload->>%s IS NOT NULL
                  AND payload->>%s <> ''
            """, (c, fld, fld))
            cnt = jget(present[0], "n")
            if cnt == 0:
                continue
            distinct = fetchall("""
                SELECT COUNT(DISTINCT payload->>%s) AS n
                FROM chunks WHERE collection_name=%s
            """, (fld, c))
            ndist = jget(distinct[0], "n")
            samples = fetchall("""
                SELECT DISTINCT payload->>%s AS v
                FROM chunks WHERE collection_name=%s AND payload->>%s IS NOT NULL
                LIMIT 8
            """, (fld, c, fld))
            svals = [jget(r, "v") for r in samples]
            print(f"  {fld}: present={cnt}/{total} distinct={ndist} samples={svals}")

        # which text field has content
        for fld in ("description", "text"):
            present = fetchall("""
                SELECT COUNT(*) AS n FROM chunks
                WHERE collection_name=%s
                  AND COALESCE(payload->>%s,'') <> ''
            """, (c, fld))
            print(f"  text-field '{fld}': non-empty {jget(present[0],'n')}/{total}")

        # raw payload sample (first row) so we see exact kb_tags / section_heading shape
        raw = fetchall("""
            SELECT payload FROM chunks WHERE collection_name=%s LIMIT 1
        """, (c,))
        if raw:
            p = jget(raw[0], "payload")
            if isinstance(p, str):
                try: p = json.loads(p)
                except Exception: pass
            if isinstance(p, dict):
                # trim long values for readability
                trimmed = {k: (str(v)[:120] + ("..." if len(str(v)) > 120 else ""))
                           for k, v in p.items()}
                print("  RAW payload sample:")
                print("   ", json.dumps(trimmed, indent=2, default=str)[:2000])


if __name__ == "__main__":
    main()
