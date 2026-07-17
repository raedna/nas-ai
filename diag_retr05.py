"""
diag_retr05.py — RETR-05 term-collision batch: reproduce the three exhibits
and dump what each stage decided (BM25 variants, candidate pool, rerank
choice, final method/answer).

Run on the Mac:  python3 diag_retr05.py
"""
import sys
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")

from core.retrieval.router import run_query_with_method

EXHIBITS = [
    # (label, collection, question, what SHOULD win)
    ("A: bad-dates hijack", "kb_docs",
     "have there been issues with FRA bad date",
     "FRA Waiting Period issue on Unwind Order"),
    ("B: wrong entity", "kb_docs",
     "what time does the gsact.txt file arrive?",
     "nothing confident — gsact arrival time is NOT in kb"),
    ("C: exact-match miss", "recon_assist_file",
     "what time does the gsact.txt arrive?",
     "identifier_lookup on gsact.txt (the record)"),
]

for label, coll, q, expect in EXHIBITS:
    print(f"\n{'='*70}\n{label}\n  Q: {q}\n  collection: {coll}\n  expect: {expect}\n{'-'*70}")
    try:
        r = run_query_with_method(coll, q, limit=25, show_exact_links=False,
                                  show_related_topics=False, force_answer=True)
        print(f"  method: {r.get('method')} | reason: {str(r.get('reason'))[:80]}")
        txt = r.get("result")
        txt = txt if isinstance(txt, str) else str(txt)
        print("  answer head:", " ".join(txt[:220].split()))
    except Exception as e:
        print(f"  ERROR {type(e).__name__}: {e}")
