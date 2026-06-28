"""
diag_chat_path.py — isolate why Chat returns the wrong obsidian note vs Ask. Read-only.
Run:  python3 diag_chat_path.py
"""
import sys, json
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.retrieval.router import run_query_with_method
from core.chat_engine import augment_query_with_focus, chat_turn, select_collections

Q = "how do I check recon files in sFTP"


def first_note(resp):
    res = resp.get("result") if isinstance(resp, dict) else resp
    return str(res)[:140].replace("\n", " ")


print(f"Q = {Q!r}\n")
print("1. default params (limit=25, force_answer=False):")
print("   ", first_note(run_query_with_method("obsidian", Q)))

print("\n2. chat params (mode=best, limit=10, links, topics, skip_planner, force_answer):")
print("   ", first_note(run_query_with_method("obsidian", Q, "best", 10, True, True, True, True)))

aug = augment_query_with_focus(Q, [])
print(f"\n3. augmented query (no history) = {aug!r}")
print("   chat params + augmented:")
print("   ", first_note(run_query_with_method("obsidian", aug, "best", 10, True, True, True, True)))

print("\n4. limit=25 + chat flags (isolate the limit):")
print("   ", first_note(run_query_with_method("obsidian", Q, "best", 25, True, True, True, True)))

cols = list(json.load(open("config/collections.json")).keys())
print("\n5. select_collections:", select_collections(Q, [], cols))
ct = chat_turn(Q, [], cols)
print("6. full chat_turn -> collection:", ct.get("collection"))
print("   ", first_note({"result": ct.get("content")}))
