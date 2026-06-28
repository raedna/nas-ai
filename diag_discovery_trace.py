"""
diag_discovery_trace.py — trace discovery term extraction vs the BM25 index. Read-only.
Run:  python3 diag_discovery_trace.py
"""
import sys
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.retrieval.discovery import llm_extract_search_terms
from core.db import fetchall

print("=== 1. What does qwen extract, and does THAT term hit the index? ===")
for q, col in [
    ("what fields contain ask price", "bbg_fields"),
    ("what fields are in category airlines", "bbg_fields"),
    ("show all files for Goldman", "recon_assist_file"),
]:
    term = llm_extract_search_terms(q)
    n = fetchall("""SELECT COUNT(*) AS n FROM chunks WHERE collection_name=%s
                    AND nlp_text_tsv @@ websearch_to_tsquery('english', %s)""",
                 (col, term or ""))[0]["n"]
    print(f"  {q!r}\n    extracted -> {term!r}   bm25 on {col} = {n}")

print("\n=== 2. Isolate the SQL: manual terms via websearch_to_tsquery ===")
for t in ["ask price", "airlines", "Goldman", "ask", "price"]:
    n = fetchall("""SELECT COUNT(*) AS n FROM chunks
                    WHERE collection_name IN ('bbg_fields','recon_assist_file')
                    AND nlp_text_tsv @@ websearch_to_tsquery('english', %s)""", (t,))[0]["n"]
    print(f"  websearch {t!r} -> {n}")

print("\n=== 3. recon: does gsact exist anywhere? ===")
print("  identifiers like 'gs%':",
      [r["i"] for r in fetchall("""SELECT DISTINCT payload->>'identifier' AS i
            FROM chunks WHERE collection_name='recon_assist_file'
            AND payload->>'identifier' ILIKE 'gs%' ORDER BY i""")])
print("  any 'gsact' (id/name/alias):",
      fetchall("""SELECT payload->>'identifier' AS i, payload->>'primary_name' AS n
            FROM chunks WHERE collection_name='recon_assist_file'
            AND (payload->>'identifier' ILIKE '%gsact%'
                 OR payload->>'primary_name' ILIKE '%gsact%'
                 OR payload->>'aliases' ILIKE '%gsact%')"""))
print("  total recon identifiers:",
      fetchall("""SELECT COUNT(DISTINCT payload->>'identifier') AS n
            FROM chunks WHERE collection_name='recon_assist_file'""")[0]["n"])
