"""
diag_embed_bakeoff.py — EMBED-01: score candidate embedding models WITHOUT
migrating anything. Two measurements per model:

  1. SEPARATION (the acceptance test): junk concept pairs vs genuine ones.
     bge-large scored genuine 0.506 inside the junk band 0.495-0.532 — a
     candidate must put daylight between them.
  2. ROUTING: for hand-labeled questions, embed the question + a sample
     centroid per collection; report how often the right collection ranks
     top and the mean top1-top2 margin (bigger margin = sturdier routing).

Run on the Mac (model must be available in LM Studio):
    python3 diag_embed_bakeoff.py text-embedding-nomic-embed-text-v1.5
    python3 diag_embed_bakeoff.py text-embedding-bge-large-en-v1.5   # baseline
"""
import sys
import numpy as np
import requests

sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")
from core.db import fetchall
from core.system_config import load_system_config

MODEL = sys.argv[1] if len(sys.argv) > 1 else "text-embedding-bge-large-en-v1.5"
URL = load_system_config().get("embeddings_url",
                               "http://localhost:1234/v1/embeddings")

def embed(texts):
    out = []
    for i in range(0, len(texts), 16):
        r = requests.post(URL, json={"model": MODEL, "input": texts[i:i+16]},
                          timeout=120)
        r.raise_for_status()
        out += [d["embedding"] for d in r.json()["data"]]
    return [np.array(v) / (np.linalg.norm(v) or 1.0) for v in out]

def cos(a, b):
    return float(np.dot(a, b))

# ---- 1. separation: question vs anchor-chunk TEXT (re-embedded fresh) ----
PAIRS = [
    # (question, chunk primary_name to fetch, is_genuine)
    ("message broadcaster is down", "PB_PULL", False),
    ("message broadcaster is down", "Completed Abnormally", False),
    ("message broadcaster is down", "MARS_TSS_LOAD", False),
    ("bad dates alert for citi, which file and what steps",
     "2.0 - TIDAL Recon - Bad Dates", True),
    ("how do I fix an FRA date issue?",
     "FRA Waiting Period issue on Unwind Order", True),
]
print(f"\n=== {MODEL} ===\n-- separation (junk vs genuine concept links)")
junk, genuine = [], []
for q, pname, is_gen in PAIRS:
    rows = fetchall(
        "SELECT payload->>'text' AS t FROM chunks "
        "WHERE primary_name ILIKE %s LIMIT 1", (f"%{pname}%",))
    if not rows:  # cluster labels aren't always chunk names — search text
        rows = fetchall(
            "SELECT payload->>'text' AS t FROM chunks "
            "WHERE collection_name = 'kb_docs' AND payload->>'text' ILIKE %s "
            "ORDER BY length(payload->>'text') DESC LIMIT 1", (f"%{pname}%",))
    if not rows or not rows[0]["t"]:
        print(f"  SKIP {pname!r} (chunk not found)")
        continue
    qv, cv = embed([q, rows[0]["t"][:2000]])
    sim = cos(qv, cv)
    (genuine if is_gen else junk).append(sim)
    print(f"  {'GENUINE' if is_gen else 'junk   '} {sim:.3f}  {pname[:40]}")
if junk and genuine:
    gap = min(genuine) - max(junk)
    print(f"  --> junk max {max(junk):.3f} | genuine min {min(genuine):.3f} "
          f"| GAP {gap:+.3f}  ({'SEPARATED' if gap > 0.03 else 'no separation'})")

# ---- 2. routing: labeled questions vs per-collection sample centroids ----
LABELED = [
    ("what is FIX tag 22", "xml_test"),
    ("what is gsact.txt", "recon_assist_file"),
    ("message broadcaster down", "kb_docs"),
    ("how to rerun a tidal recon job", "obsidian"),
    ("how many galaxies are in the catalog", "astro_catalog"),
    ("ARD_OPERATING_EXP_PER_ASM_ASK", "bbg_fields"),
    ("are there tickets about FRA dates", "halo_tickets"),
]
cols = sorted({c for _, c in LABELED})
cents = {}
for c in cols:
    rows = fetchall(
        "SELECT payload->>'text' AS t FROM chunks "
        "WHERE collection_name = %s AND payload->>'text' IS NOT NULL "
        "ORDER BY random() LIMIT 20", (c,))
    texts = [r["t"][:1500] for r in rows if r["t"]]
    if texts:
        vs = embed(texts)
        cents[c] = np.mean(vs, axis=0)
        cents[c] /= (np.linalg.norm(cents[c]) or 1.0)

print("\n-- routing (labeled questions vs sample centroids)")
hits, margins = 0, []
for q, want in LABELED:
    qv = embed([q])[0]
    scored = sorted(((cos(qv, v), c) for c, v in cents.items()), reverse=True)
    top, second = scored[0], scored[1]
    ok = top[1] == want
    hits += ok
    margins.append(top[0] - second[0])
    print(f"  {'OK ' if ok else 'MISS'} {q[:45]:45s} -> {top[1]} "
          f"({top[0]:.3f}, margin {top[0]-second[0]:+.3f}, want {want})")
print(f"  --> {hits}/{len(LABELED)} correct | mean margin "
      f"{float(np.mean(margins)):+.3f}")
