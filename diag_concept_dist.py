"""
diag_concept_dist.py — distribution of centroid-vs-centroid similarities
(CL diagnosis: is 0.75 above or below the ambient floor?).

For every source cluster, computes its similarity to ALL clusters in every
other collection, then prints per (source_collection -> target_collection):
min / median / max / spread, and flags pairs where max barely beats median
(ambient noise) vs pairs with a real standout.

Run on the Mac:  python3 diag_concept_dist.py
"""
import sys
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")

from statistics import median
from core.db import fetchall

pairs = fetchall("""
    SELECT s.collection AS src_coll, s.group_value AS src_group,
           t.collection AS tgt_coll, t.group_value AS tgt_group,
           1 - (s.centroid::vector <=> t.centroid::vector) AS sim
    FROM concept_vectors s
    JOIN concept_vectors t ON t.collection != s.collection
""", ())

by_src = {}
for r in pairs:
    key = (r["src_coll"], r["src_group"], r["tgt_coll"])
    by_src.setdefault(key, []).append((float(r["sim"]), r["tgt_group"]))

print(f"{'source':<40} {'target':<18} {'min':>5} {'med':>5} {'max':>5} {'marg':>5}  best_cluster")
noise, signal = 0, 0
for (sc, sg, tc), sims in sorted(by_src.items()):
    vals = sorted(s for s, _ in sims)
    mx_sim, mx_grp = max(sims)
    med = median(vals)
    margin = mx_sim - med
    tag = "SIGNAL" if margin >= 0.05 else "noise "
    if margin >= 0.05:
        signal += 1
    else:
        noise += 1
    src = f"{sc}/{str(sg)[:25]}"
    print(f"{src:<40} {tc:<18} {vals[0]:.3f} {med:.3f} {mx_sim:.3f} {margin:+.3f}  [{tag}] {str(mx_grp)[:30]}")

print(f"\n{signal} pairs with standout (margin>=0.05), {noise} ambient-only.")
print("If most maxes sit above 0.75 with tiny margins, the absolute threshold is the bug.")
