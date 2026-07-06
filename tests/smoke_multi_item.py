"""
Multi-Item (CODE-023) Smoke Test Runner — Chat path
===================================================
Stage A: deterministic gate (no LLM, no DB)
Stage B: LLM splitter (LM Studio required)
Stage C: end-to-end chat_turn for the MI eval questions + single-item regression

Usage:
    python tests/smoke_multi_item.py
"""

import sys
import traceback
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.chat_engine import (
    _is_multi_item_candidate,
    _identifier_tokens,
    split_multi_item_question,
    chat_turn,
)

PASS, FAIL = "PASS", "FAIL"
results = {"A": [], "B": [], "C": []}


def record(stage, tid, ok, detail=""):
    results[stage].append((tid, ok))
    print(f"[{PASS if ok else FAIL}] {tid} {detail}")


# ---------------------------------------------------------------- Stage A
print("=" * 70)
print("Stage A — deterministic gate (offline)")
print("=" * 70)

GATE_CASES = [
    ("A-01", "what are tags 22, 35 and 54", True),
    ("A-02", "give me the tidal jobs for gsact.txt and gspos.txt", True),
    ("A-03", "compare FIX tag 38 and tag 152", True),
    ("A-04", "what is FIX tag 22", False),
    ("A-05", "how many images with gain 100", False),
    ("A-06", "recon file missing, what do I do", False),
    ("A-07", "what is gsact.txt and how do I check it", False),
    ("A-08", "how many fields are in FIX 4.4", False),
    ("A-09", "how many images with gain 100 and exposure 300", True),  # gates in; splitter must refuse
]
for tid, q, exp in GATE_CASES:
    got = _is_multi_item_candidate(q)
    record("A", tid, got == exp, f"gate={got} tokens={_identifier_tokens(q)} :: {q}")

# ---------------------------------------------------------------- Stage B
print()
print("=" * 70)
print("Stage B — LLM splitter (live)")
print("=" * 70)

SPLIT_CASES = [
    # (id, question, expect_split, min_subs)
    ("B-01", "what are tags 22, 35 and 54", True, 3),
    ("B-02", "give me the tidal jobs for gsact.txt and gspos.txt", True, 2),
    ("B-03", "compare FIX tag 38 and tag 152", True, 2),
    ("B-04", "how many images with gain 100 and exposure 300", False, 0),  # combined filters — must NOT split
]
for tid, q, exp_split, min_subs in SPLIT_CASES:
    try:
        subs = split_multi_item_question(q)
    except Exception as e:
        record("B", tid, False, f"EXCEPTION {e} :: {q}")
        traceback.print_exc()
        continue
    ok = (len(subs) >= min_subs) if exp_split else (subs == [])
    record("B", tid, ok, f":: {q}")
    for s in subs:
        print(f"        sub: {s}")

# ---------------------------------------------------------------- Stage C
print()
print("=" * 70)
print("Stage C — end-to-end chat_turn")
print("=" * 70)

try:
    from core.ui_data import collection_stats
    AVAILABLE = [r["name"] for r in collection_stats() if r["chunks"]]
except Exception as e:
    print(f"could not load collections: {e}")
    AVAILABLE = []
print(f"available collections: {AVAILABLE}")

E2E_CASES = [
    # (id, question, must_contain, must_not_contain, expect_answer_kind, require_collection)
    ("MI-01", "what are tags 22, 35 and 54",
     ["SecurityIDSource", "MsgType", "Side"], [], "multi_item", None),
    ("MI-02", "give me the tidal jobs for gsact.txt and gspos.txt",
     ["gsact.txt", "gspos.txt"], [], "multi_item", None),
    ("MI-03", "compare FIX tag 38 and tag 152",
     ["OrderQty", "CashOrderQty"], [], "multi_item", None),
    # single-item regression — must NOT take the multi-item path
    ("REG-01", "what is FIX tag 22",
     ["SecurityIDSource"], [], None, None),
    # DL-02: must actually resolve the record in recon_assist_file, not
    # false-pass on a "No record found for 'gsact.txt'" message.
    ("REG-02", "what is gsact.txt",
     ["gsact.txt", "GOLDMAN"], ["No record found", "couldn't find"], None, "recon_assist_file"),
]

for tid, q, must, must_not, exp_kind, req_col in E2E_CASES:
    print("-" * 70)
    print(f"{tid}: {q}")
    try:
        resp = chat_turn(q, [], AVAILABLE)
    except Exception as e:
        record("C", tid, False, f"EXCEPTION {e}")
        traceback.print_exc()
        continue
    content = resp.get("content") or ""
    kind = resp.get("answer_kind")
    missing = [m for m in must if m not in content]
    forbidden = [m for m in must_not if m in content]
    kind_ok = (kind == exp_kind) if exp_kind else (kind != "multi_item")
    col_ok = (req_col in (resp.get("collections_queried") or [])) if req_col else True
    ok = not missing and not forbidden and kind_ok and col_ok
    record("C", tid, ok,
           f"kind={kind} collections={resp.get('collections_queried')} "
           f"missing={missing} forbidden={forbidden} col_ok={col_ok}")
    print("  --- content ---")
    for line in content.splitlines():
        print("  " + line)

# ---------------------------------------------------------------- Summary
print()
print("=" * 70)
for stage in ("A", "B", "C"):
    total = len(results[stage])
    passed = sum(1 for _, ok in results[stage] if ok)
    print(f"Stage {stage}: {passed}/{total}")
print("=" * 70)
