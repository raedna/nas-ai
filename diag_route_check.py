"""
diag_route_check.py — trace why a query routed to the wrong collection. Read-only.
Run:  python3 diag_route_check.py "what is ask price"
"""
import sys, json
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.chat_engine import detect_chat_intent, select_collections
from core.retrieval.router import run_query_with_method

Q = sys.argv[1] if len(sys.argv) > 1 else "what is ask price"
cols = list(json.load(open('config/collections.json')).keys())

print(f"QUESTION: {Q!r}\n")
print("available collections:", cols)
print("\n=== Stage 0: intent ===")
print(detect_chat_intent(Q, []))

print("\n=== Tier 2: collection routing (LLM) ===")
sel = select_collections(Q, [], cols)
print("ROUTED TO (in order):", sel)

print("\n=== Per-collection answers (bbg_fields vs kb_docs) ===")
for c in ["bbg_fields", "kb_docs"]:
    try:
        r = run_query_with_method(c, Q)
        print(f"\n--- {c} ---")
        print("method:", r.get("method"), "| reason:", r.get("reason"))
        print("result:", str(r.get("result"))[:400])
    except Exception as e:
        print(f"\n--- {c} --- FAILED: {e!r}")
