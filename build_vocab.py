"""
build_vocab.py — initial/refresh build of per-collection vocabularies
(VOCAB-01). Run on the Mac:  python3 build_vocab.py
"""
import sys
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")

from core.db import fetchall
from core.vocab import build_collection_vocab

cols = [r["collection_name"] for r in
        fetchall("SELECT DISTINCT collection_name FROM chunks", ())]
total = 0
for c in cols:
    try:
        total += build_collection_vocab(c)
    except Exception as e:
        print(f"  {c}: FAILED {type(e).__name__}: {e}")
print(f"\ntotal lexemes: {total}")

# quick correction sanity checks
from core.vocab import correct_word
for w, coll in (("brodcaster", "kb_docs"), ("agian", "kb_docs"),
                ("goldmann", "recon_assist_file"), ("broadcaster", "kb_docs")):
    print(f"  correct_word({w!r}, {coll}) -> {correct_word(w, coll)}")
