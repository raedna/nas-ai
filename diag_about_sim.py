"""
diag_about_sim.py — hypothesis test BEFORE building the about-vector guard:
does cosine similarity between question and stored `about` lines separate
'is about' from 'mentions'? No writes, no infra — embeds ad hoc.

Run on the Mac:  python3 diag_about_sim.py
Custom:          python3 diag_about_sim.py --q "..." --col kb_docs
"""
import argparse
import math
import sys
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")

from core.db import fetchall
from core.embedder import embed_text, embed_texts

_ap = argparse.ArgumentParser()
_ap.add_argument("--q", action="append", help="question (repeatable)")
_ap.add_argument("--col", default="kb_docs")
_ap.add_argument("--top", type=int, default=8)
_args = _ap.parse_args()

QUESTIONS = _args.q or [
    "what to do if CTM is down",
    "what to do if ctm is down",
    "have there been issues with FRA bad date",
    "who is handling the FRA issue internally at Moore?",
]

rows = fetchall(
    "SELECT DISTINCT primary_name, payload->>'about' AS about "
    "FROM chunks WHERE collection_name = %s "
    "AND payload ? 'about' AND COALESCE(payload->>'about','') != ''",
    (_args.col,))
print(f"{len(rows)} annotated entries in {_args.col}")
names = [r["primary_name"] for r in rows]
vecs = embed_texts([r["about"] for r in rows])


def _cos(a, b):
    num = sum(x * y for x, y in zip(a, b))
    da = math.sqrt(sum(x * x for x in a))
    db = math.sqrt(sum(x * x for x in b))
    return num / (da * db) if da and db else 0.0


for q in QUESTIONS:
    qv = embed_text(q)
    ranked = sorted(((_cos(qv, v), n) for v, n in zip(vecs, names)),
                    reverse=True)
    print(f"\n=== {q}")
    for s, n in ranked[:_args.top]:
        print(f"  {s:.4f}  {n}")
