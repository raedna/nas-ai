"""
diag_chat_memory.py — replay the exact tag-22 chat sequence and expose the internals
per turn, to separate a memory/contextualizer problem from intent-classification
variance. Read-only. Run on the Mac:  python3 diag_chat_memory.py
"""
import json
import sys
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")

from core.chat_engine import contextualize_query, chat_turn
from core.retrieval.discovery import llm_detect_intent

COLLECTIONS = list(json.load(open("config/collections.json")).keys())

SEQUENCE = [
    "what is tag 22",
    "what values can tag 22 have",      # <-- the one that failed to list values
    "what values can it have",          # follow-up; worked
    "and what tags is it related to",   # missed tag 48
    "so what is tag 48 then",
]


def _has_values(text):
    t = str(text).lower()
    return "allowed values" in t or "cusip" in t or "sedol" in t


def _snip(text):
    return str(text).replace("\n", " ")[:150]


print("=== Per-turn internals (memory vs intent) ===\n")
history = []
for q in SEQUENCE:
    ctx = contextualize_query(q, history)
    standalone = ctx["standalone_query"]
    # Intent on the STANDALONE query, 3x to catch non-determinism.
    intents = []
    for _ in range(3):
        it = llm_detect_intent(standalone)
        intents.append((it.get("mode"), it.get("role"), it.get("target")))
    turn = chat_turn(q, history, COLLECTIONS)
    content = turn.get("content", "")
    print(f"Q: {q!r}")
    print(f"   is_followup : {ctx['is_followup']}  ({ctx.get('reason')})")
    print(f"   standalone  : {standalone!r}")
    print(f"   intent x3   : {intents}")
    print(f"   method      : {turn.get('method')} / collection={turn.get('collection')}")
    print(f"   has_values  : {_has_values(content)}")
    print(f"   answer      : {_snip(content)}")
    print()
    history.append({"role": "user", "content": q})
    history.append({"role": "assistant", "content": turn.get("content", "")})
