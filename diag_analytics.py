"""
diag_analytics.py — validate the text-to-SQL analytics engine end-to-end.
Read-only. Run on the Mac (needs Postgres + LM Studio):  python3 diag_analytics.py

Edit COLLECTION / QUESTIONS below to match your data.
"""
import sys
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")

from core.retrieval.analytics import run_analytics, schema_context

COLLECTION = "xml_test"   # change to a real collection
QUESTIONS = [
    "how many files are in this collection",
    "how many records are there by type",
    "how many fits files are there",            # try on an astro/fits collection
    "how many documents mention goldman",
    "what is tag 22",                            # NOT analytics -> should fall back (is_analytics False)
]

print("=== schema_context (truncated) ===")
print(schema_context(COLLECTION)[:1200])
print("\n=== questions ===\n")

for q in QUESTIONS:
    print(f"Q: {q}")
    res = run_analytics(COLLECTION, q)
    print(f"   is_analytics: {res.get('is_analytics')}")
    if not res.get("is_analytics"):
        print(f"   (fell back) reason: {res.get('reason')}\n")
        continue
    if res.get("error"):
        print(f"   ERROR: {res['error']}")
        print(f"   SQL:   {res.get('sql')}\n")
        continue
    print(f"   SQL:    {res.get('sql')}")
    print(f"   result: {res.get('result')}")
    print(f"   reason: {res.get('reason')}\n")
