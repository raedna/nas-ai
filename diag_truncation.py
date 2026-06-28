"""
diag_truncation.py — quantifies embedding truncation. Read-only.
Measures, per collection, how many stored chunks have embedded text longer than
the embed model's token window, and estimates how much text was silently dropped.

Method:
  1. Probe the embeddings endpoint with a very long input -> the reported
     prompt_tokens plateaus at the model's hard cap (proves the cap).
  2. Sample real texts to measure a chars-per-token ratio for THIS model.
  3. Per collection: char-length stats of payload->>'text' (the embedded text),
     and % of chunks estimated to exceed the cap. Confirms with a live token
     count on each collection's longest chunk.

Run:  python3 diag_truncation.py
"""
import sys, requests, statistics
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.db import fetchall
from core.system_config import load_system_config

cfg = load_system_config()
EMBED_URL = cfg["embeddings_url"]
MODEL = cfg["embeddings_model"]


def prompt_tokens(text):
    try:
        r = requests.post(EMBED_URL, json={"model": MODEL, "input": text}, timeout=90)
        return r.json().get("usage", {}).get("prompt_tokens")
    except Exception as e:
        print("  embed call failed:", repr(e)); return None


def main():
    print(f"embed model: {MODEL}\n{'='*60}")

    # 1. find the hard token cap
    cap = prompt_tokens("word " * 6000)
    print(f"reported prompt_tokens for a 6000-word input: {cap}")
    print("(if this is a round number like 512 it's the truncation cap)\n")

    # 2. chars-per-token ratio from short, un-truncated samples
    sample = fetchall("""
        SELECT payload->>'text' AS t FROM chunks
        WHERE payload->>'text' IS NOT NULL AND LENGTH(payload->>'text') BETWEEN 200 AND 800
        LIMIT 12
    """)
    ratios = []
    for s in sample:
        n = prompt_tokens(s["t"])
        if n: ratios.append(len(s["t"]) / n)
    cpt = statistics.median(ratios) if ratios else 4.0
    cap = cap or 512
    cap_chars = int(cap * cpt)
    print(f"chars/token (median): {cpt:.2f}  ->  cap ≈ {cap_chars} chars per chunk\n")

    # 3. per-collection truncation rate
    cols = [r["c"] for r in fetchall(
        "SELECT DISTINCT collection_name AS c FROM chunks ORDER BY c")]
    print(f"{'collection':<22}{'chunks':>7}{'median':>8}{'max':>8}{'>cap':>7}{'% trunc':>9}")
    print("-" * 61)
    worst = {}
    for c in cols:
        rows = fetchall("""SELECT LENGTH(payload->>'text') AS n FROM chunks
                           WHERE collection_name=%s AND payload->>'text' IS NOT NULL""", (c,))
        lens = sorted(r["n"] for r in rows if r["n"])
        if not lens: continue
        over = sum(1 for n in lens if n > cap_chars)
        pct = 100.0 * over / len(lens)
        print(f"{c:<22}{len(lens):>7}{int(statistics.median(lens)):>8}{max(lens):>8}{over:>7}{pct:>8.1f}%")
        if over:
            worst[c] = max(lens)

    # 4. ground-truth: longest chunk per affected collection actually truncated?
    print("\nLive token check on the longest chunk of each affected collection:")
    for c, _ in sorted(worst.items(), key=lambda x: -x[1]):
        row = fetchall("""SELECT payload->>'text' AS t, LENGTH(payload->>'text') AS n
                          FROM chunks WHERE collection_name=%s
                          ORDER BY LENGTH(payload->>'text') DESC LIMIT 1""", (c,))
        if not row: continue
        chars = row[0]["n"]; toks = prompt_tokens(row[0]["t"])
        lost = max(0, chars - cap_chars)
        flag = "  <-- TRUNCATED" if toks and toks >= cap else ""
        print(f"  {c:<22} {chars:>6} chars -> model saw {toks} tokens"
              f"  (~{lost} chars / {100*lost/chars:.0f}% unembedded){flag}")


if __name__ == "__main__":
    main()
