"""
diag_verify_ingestion.py — consolidated post-ingest verification (read-only).
Run AFTER: clear_kb_schema.py + force-reingest kb_docs (and ideally obsidian/pdf_test).

Checks:
  1. Truncation — per collection: chunk count, max chars, # over the ~2500 cap.
  2. kb_docs schema — `tags` role present and holds the tags column.
  3. kb_tags — % of kb_docs chunks carrying a non-empty kb_tags list (+ samples).
  4. Non-lossy — resolution-style long content is embedded (kb max chars > old 2k,
     but no chunk exceeds the cap).
Run:  python3 diag_verify_ingestion.py
"""
import sys, json
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.db import fetchall

CAP = 2500


def section(t):
    print("\n" + "=" * 60 + f"\n{t}\n" + "=" * 60)


def main():
    # 1. truncation across collections
    section("1. Truncation (chars) per collection")
    cols = [r["c"] for r in fetchall("SELECT DISTINCT collection_name AS c FROM chunks ORDER BY c")]
    print(f"{'collection':<22}{'chunks':>8}{'max':>8}{'>cap':>7}")
    print("-" * 45)
    for c in cols:
        rows = fetchall("""SELECT LENGTH(payload->>'text') AS n FROM chunks
                           WHERE collection_name=%s AND payload->>'text' IS NOT NULL""", (c,))
        lens = [r["n"] for r in rows if r["n"]]
        if not lens:
            continue
        over = sum(1 for n in lens if n > CAP)
        flag = "  <-- still truncating" if over else ""
        print(f"{c:<22}{len(lens):>8}{max(lens):>8}{over:>7}{flag}")

    # 2. kb_docs schema
    section("2. kb_docs schema (tags role?)")
    srows = fetchall("SELECT source_file_stem, schema_json FROM schemas WHERE collection_name=%s",
                     ("kb_docs",))
    if not srows:
        print("  no cached schema (will infer on next ingest)")
    for s in srows:
        sch = s["schema_json"]
        if isinstance(sch, str):
            sch = json.loads(sch)
        print(f"  {s['source_file_stem']}:")
        print(f"    tags        -> {sch.get('tags')}")
        print(f"    description -> {sch.get('description')}")
        print(f"    other       -> {sch.get('other')}")

    # 3. kb_tags coverage
    section("3. kb_tags coverage in kb_docs payloads")
    total = fetchall("SELECT COUNT(*) AS n FROM chunks WHERE collection_name='kb_docs'")[0]["n"]
    withtags = fetchall("""SELECT COUNT(*) AS n FROM chunks
        WHERE collection_name='kb_docs'
          AND payload->>'tags' IS NOT NULL
          AND payload->>'tags' NOT IN ('', '[]')""")[0]["n"]
    print(f"  chunks with non-empty kb_tags: {withtags}/{total}")
    samples = fetchall("""SELECT DISTINCT payload->>'tags' AS t FROM chunks
        WHERE collection_name='kb_docs' AND payload->>'tags' NOT IN ('', '[]')
        LIMIT 5""")
    for s in samples:
        print(f"    {s['t']}")

    # 4. non-lossy sanity
    section("4. Non-lossy check (kb_docs)")
    mx = fetchall("""SELECT MAX(LENGTH(payload->>'text')) AS m FROM chunks
                     WHERE collection_name='kb_docs'""")[0]["m"]
    print(f"  kb_docs max chunk chars: {mx}  (should be <= {CAP})")
    print("  (content beyond the old ~2k is now spread across chunks, not dropped)")


if __name__ == "__main__":
    main()
