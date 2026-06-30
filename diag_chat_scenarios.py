"""
diag_chat_scenarios.py — expose follow-up vs new-question handling in chat.
Read-only. Run on the Mac:  python3 diag_chat_scenarios.py

For each turn it prints what the chat engine actually does internally:
  - augment_query_with_focus()  -> the (possibly polluted) query sent to retrieval
  - select_collections()        -> routed collections (history-biased?)
  - chat_turn().content         -> the final answer (truncated)

Scenarios mix genuine follow-ups with hard topic switches so we can see where
prior context leaks into a brand-new question.
"""
import json
import sys
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")

from core.chat_engine import (
    augment_query_with_focus, select_collections, chat_turn,
)

COLLECTIONS = list(json.load(open("config/collections.json")).keys())

SCENARIOS = {
    "A. genuine follow-up": [
        "what files does Goldman send",
        "what about Citi",                       # should become a Citi query
    ],
    "B. hard topic switch (the bug)": [
        "what is tag 22",
        "steps for manual file loading in recon",  # must NOT carry tag 22 / FIX context
    ],
    "C. switch to a different collection": [
        "sftp folder for gsact.txt",
        "what is the ask price field",            # new, bbg_fields — not recon
    ],
    "D. discovery / list + count in chat": [
        "what are the recon files for Goldman",   # discovery_list — should LIST files, not hedge
        "how many recon files does Goldman have",  # discovery_count / analytics
    ],
}


def run():
    for title, turns in SCENARIOS.items():
        print("=" * 70)
        print(title)
        print("=" * 70)
        history = []
        for q in turns:
            aug = augment_query_with_focus(q, history)
            cols = select_collections(q, history, COLLECTIONS)
            turn = chat_turn(q, history, COLLECTIONS)
            content = (turn.get("content") or "").replace("\n", " ")
            print(f"\nQ: {q!r}")
            print(f"   augmented -> {aug!r}")
            print(f"   collections -> {cols}")
            print(f"   answered from -> {turn.get('collection')}")
            print(f"   answer -> {content[:200]}")
            history.append({"role": "user", "content": q})
            history.append({"role": "assistant", "content": turn.get("content", "")})
        print()


if __name__ == "__main__":
    run()
