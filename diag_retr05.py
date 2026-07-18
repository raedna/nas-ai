"""
diag_retr05.py — RETR-05 term-collision batch: reproduce the three exhibits
and dump what each stage decided (BM25 variants, candidate pool, rerank
choice, final method/answer).

Run on the Mac:  python3 diag_retr05.py
Custom question:  python3 diag_retr05.py --q "what to do if CTM is down" --col kb_docs
"""
import argparse
import sys
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")

from core.retrieval.router import run_query_with_method

_ap = argparse.ArgumentParser()
_ap.add_argument("--q", help="custom question (runs INSTEAD of the fixed exhibits)")
_ap.add_argument("--col", default="kb_docs", help="collection for --q (default kb_docs)")
_args = _ap.parse_args()

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

if _args.q:
    EXHIBITS = [("CUSTOM", _args.col, _args.q, "(no expectation — inspect stages)")]


def _subject_report(question, coll):
    """Read-only mirror of the reranker's subject guard: show which question
    tokens qualify as subjects and the pool document-frequency that admits or
    rejects each — so a silent guard is explainable."""
    import re
    from core.db import fetchall
    try:
        from core.query_helpers import load_doc_query_hints
        noise = set()
        for k in ("discovery_noise_words", "question_words", "stopwords"):
            noise.update(load_doc_query_hints().get(k, []))
    except Exception:
        noise = set()
    caps = [w.lower() for w in re.findall(r"\b[A-Z][A-Z0-9]{1,}\b", question)
            if w.lower() not in noise]
    anchors = [m.lower() for m in re.findall(
        r"\b[a-zA-Z0-9_\-]+\.[a-zA-Z0-9]{2,5}\b", question)]
    if not caps and not anchors:
        print("  subject report: no CAPITALIZED tokens or filename anchors in question")
        return
    rows = fetchall(
        "SELECT lower(coalesce(primary_name,'') || ' ' || coalesce(nlp_text,'')) AS h "
        "FROM chunks WHERE collection_name = %s", (coll,))
    n = len(rows)
    print(f"  subject report ({n} chunks in {coll}; caps tokens and anchors are "
          f"subjects whenever pool-df > 0; title hits dominate body hits):")
    for t in dict.fromkeys(anchors):
        df = sum(1 for r in rows if t in r["h"])
        print(f"    anchor  '{t}': collection-df {df}")
    for t in dict.fromkeys(caps):
        df = sum(1 for r in rows if t in r["h"])
        print(f"    caps    '{t}': collection-df {df}")


for label, coll, q, expect in EXHIBITS:
    print(f"\n{'='*70}\n{label}\n  Q: {q}\n  collection: {coll}\n  expect: {expect}\n{'-'*70}")
    if _args.q:
        try:
            _subject_report(q, coll)
        except Exception as e:
            print(f"  subject report ERROR {type(e).__name__}: {e}")
    try:
        r = run_query_with_method(coll, q, limit=25, show_exact_links=False,
                                  show_related_topics=False, force_answer=True)
        print(f"  method: {r.get('method')} | reason: {str(r.get('reason'))[:80]}")
        txt = r.get("result")
        txt = txt if isinstance(txt, str) else str(txt)
        print("  answer head:", " ".join(txt[:220].split()))
    except Exception as e:
        print(f"  ERROR {type(e).__name__}: {e}")
