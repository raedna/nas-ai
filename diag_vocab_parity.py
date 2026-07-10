"""
diag_vocab_parity.py — SPEED-01 step 2 acceptance: batched correct_words must
produce IDENTICAL output to the per-word path for every eval question, per
collection and globally. Also times both paths.

Run on the Mac:  python3 diag_vocab_parity.py
"""
import sys, time, re
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")

from core.vocab import correct_word, correct_words
from core.db import fetchall

QUESTIONS = [
    "what is FIX tag 22", "what is gsact.txt", "jpm_activity.xlsx details",
    "brodcaster acting up agian", "whats teh fix tag for order quantity",
    "list all prime brokers in the recon file", "how many images with gain 100",
    "compare FIX tag 38 and tag 152", "how many fields are in FIX 4.4",
    "which recon files come from Barclays", "goldman prio pull job",
    "how to rerun a tidal recon job", "message broadcaster down",
    "how many galaxies are in the catalog", "NGC 2064",
]
cols = [r["c"] for r in fetchall(
    "SELECT DISTINCT collection AS c FROM collection_vocab", ())] + [None]

mismatch = 0
t_new = 0.0
for q in QUESTIONS:
    words = [w for w in re.findall(r"[a-z0-9]{3,}", q.lower())]
    for col in cols:
        t = time.perf_counter()
        got, got_ch = correct_words(words, col)
        t_new += time.perf_counter() - t
        exp = [correct_word(w, col)[0] for w in words]
        if got != exp:
            mismatch += 1
            print(f"MISMATCH [{col}] {q!r}: batched={got} per-word={exp}")

print(f"\n{len(QUESTIONS) * len(cols)} combinations, {mismatch} mismatches")
print(f"batched total: {t_new:.2f}s")
print("PASS" if mismatch == 0 else "FAIL")
