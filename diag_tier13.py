"""
diag_tier13.py — who seats whom for the three regressed questions.
For each question: per-word Tier 1.3 vocabulary ownership (the suspect),
then the full select_collections outcome with DEBUG on.

Run on the Mac:  python3 diag_tier13.py
"""
import sys
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")
import re

import core.chat_engine as ce
ce.DEBUG = True
from core.db import fetchall
from core.ui_data import collection_stats

cols = [r["name"] for r in collection_stats() if r["chunks"]]

QUESTIONS = [
    ("AG-07", "list all prime brokers in the recon file"),
    ("AG-09", "how many images with gain 100"),
    ("MI-03", "compare FIX tag 38 and tag 152"),
]

for qid, q in QUESTIONS:
    print(f"\n===== {qid}: {q}")
    words = [w for w in re.findall(r"[a-z0-9]+", q.lower()) if len(w) >= 4]
    for w in dict.fromkeys(words):
        owners = fetchall(
            "SELECT collection, ndoc FROM collection_vocab WHERE word = %s "
            "ORDER BY ndoc DESC LIMIT 6", (w,))
        if owners:
            print(f"  vocab '{w}': " + ", ".join(
                f"{r['collection']}({r['ndoc']})" for r in owners))
        else:
            print(f"  vocab '{w}': (no owner)")
    print("  --- select_collections ---")
    try:
        sel = ce.select_collections(q, [], cols)
        print("  SELECTED:", sel)
    except Exception as e:
        print("  select_collections failed:", type(e).__name__, e)
