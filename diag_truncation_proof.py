"""
diag_truncation_proof.py — definitively measures the embed truncation point WITHOUT
relying on token-usage reporting. Read-only.

Idea: embed the full long text, then embed growing prefixes of it. Past the model's
real cap, adding more text changes nothing -> cosine similarity to the full-text vector
hits ~1.0. The smallest prefix where sim ~= 1.0 IS the effective cap (in chars).
If the longest article's tail genuinely mattered, sim would keep rising with N.

Run:  python3 diag_truncation_proof.py
"""
import sys, math, requests
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.db import fetchall
from core.system_config import load_system_config

cfg = load_system_config()
EMBED_URL, MODEL = cfg["embeddings_url"], cfg["embeddings_model"]


def embed(text):
    r = requests.post(EMBED_URL, json={"model": MODEL, "input": text}, timeout=90)
    return r.json()["data"][0]["embedding"]


def cos(a, b):
    dot = sum(x*y for x, y in zip(a, b))
    na = math.sqrt(sum(x*x for x in a)); nb = math.sqrt(sum(y*y for y in b))
    return dot / (na*nb) if na and nb else 0.0


def main():
    print(f"embed model: {MODEL}\n")
    # grab the single longest stored chunk (kb_docs is the worst offender)
    row = fetchall("""SELECT collection_name AS c, payload->>'text' AS t,
                             LENGTH(payload->>'text') AS n
                      FROM chunks WHERE payload->>'text' IS NOT NULL
                      ORDER BY LENGTH(payload->>'text') DESC LIMIT 1""")[0]
    text, total = row["t"], row["n"]
    print(f"longest chunk: {row['c']}  ({total} chars)\n")

    full = embed(text)
    print(f"{'prefix chars':>13}{'cos vs full':>13}")
    print("-" * 26)
    prev = None
    cap = None
    for n in [500, 1000, 1500, 2000, 2500, 3000, 4000, 6000, 8000, total]:
        if n > total:
            continue
        s = cos(embed(text[:n]), full)
        print(f"{n:>13}{s:>13.4f}")
        # first prefix that is already essentially identical to the full vector
        if cap is None and s >= 0.999:
            cap = n
        prev = s
    print()
    if cap:
        print(f"=> Vector stops changing at ~{cap} chars: text beyond that is IGNORED.")
        print(f"   This chunk loses ~{total-cap} chars ({100*(total-cap)/total:.0f}%) of content.")
    else:
        print("=> Similarity kept rising — no hard truncation detected at these sizes.")


if __name__ == "__main__":
    main()
