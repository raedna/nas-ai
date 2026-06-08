"""
FIX Smoke Test Runner
=====================
Runs the standard FIX question set against the live system and reports
pass/fail for each question.

Usage (from project root, with nas-ai conda env active):
    python tests/smoke_fix.py

    # Save results to file:
    python tests/smoke_fix.py --out results/smoke_fix_baseline.json

    # Verbose (show full answer for each question):
    python tests/smoke_fix.py --verbose

No hardcoding: collection name and expected patterns are defined in
SMOKE_TESTS below as plain data — edit there if the collection is renamed
or expectations change.
"""

import argparse
import json
import sys
import re
import traceback
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure project root is on the path when run from any working directory.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Test definitions
# Each entry is a dict:
#   question     : str   — the question to ask
#   must_contain : list  — every item must appear somewhere in the answer
#                          (case-insensitive substring or regex)
#   must_not_contain : list  — none of these may appear in the answer
#   description  : str   — human-readable expectation for the report
# ---------------------------------------------------------------------------
COLLECTION = "xml_test"

SMOKE_TESTS = [
    {
        "id": "FIX-01",
        "question": "what is tag 22",
        "description": "Returns tag 22 name + description, NOT a flood of enum values",
        "must_contain": ["22", "SecurityIDSource"],
        "must_not_contain": [],
        "enum_count_max": 3,   # answer should not list more than 3 enum lines unprompted
    },
    {
        "id": "FIX-02",
        "question": "what values can tag 22 have",
        "description": "Returns enum values for tag 22 including ISIN",
        "must_contain": ["22", "ISIN"],
        "must_not_contain": [],
        "enum_count_max": None,
    },
    {
        "id": "FIX-03",
        "question": "what tag is exec broker",
        "description": "Returns tag 76 ExecBroker — both tag number AND name must appear",
        "must_contain": ["76", "ExecBroker"],
        "must_not_contain": [],
        "enum_count_max": None,
    },
    {
        "id": "FIX-04",
        "question": "what tag is execution broker",
        "description": "Returns tag 76 ExecBroker (synonym path) — both tag number AND name must appear",
        "must_contain": ["76", "ExecBroker"],
        "must_not_contain": [],
        "enum_count_max": None,
    },
    {
        "id": "FIX-05",
        "question": "what tag can have a value ISIN",
        "description": "Reverse enum lookup — returns tag 22 SecurityIDSource",
        "must_contain": ["22", "ISIN"],
        "must_not_contain": [],
        "enum_count_max": None,
    },
    {
        "id": "FIX-06",
        "question": "what tags contain security",
        "description": "Discovery — returns list of tags with security in name/desc",
        "must_contain": ["security"],
        "must_not_contain": [],
        "enum_count_max": None,
    },
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def answer_to_text(answer) -> str:
    """Flatten whatever the router returns into a single string."""
    if answer is None:
        return ""
    if isinstance(answer, str):
        return answer
    if isinstance(answer, list):
        parts = []
        for item in answer:
            if isinstance(item, dict):
                parts.append(json.dumps(item))
            else:
                parts.append(str(item))
        return "\n".join(parts)
    if isinstance(answer, dict):
        return json.dumps(answer)
    return str(answer)


def count_enum_lines(text: str) -> int:
    """Count lines that look like enum value lines (start with '- ')."""
    return sum(1 for line in text.splitlines() if line.strip().startswith("- "))


def evaluate(test: dict, answer_text: str) -> dict:
    result = {
        "id": test["id"],
        "question": test["question"],
        "description": test["description"],
        "pass": True,
        "failures": [],
        "answer_preview": answer_text[:300].replace("\n", " "),
    }

    lower = answer_text.lower()

    for term in test.get("must_contain", []):
        if not re.search(re.escape(term.lower()), lower):
            result["pass"] = False
            result["failures"].append(f"MISSING expected term: '{term}'")

    for term in test.get("must_not_contain", []):
        if re.search(re.escape(term.lower()), lower):
            result["pass"] = False
            result["failures"].append(f"FOUND forbidden term: '{term}'")

    max_enums = test.get("enum_count_max")
    if max_enums is not None:
        enum_count = count_enum_lines(answer_text)
        if enum_count > max_enums:
            result["pass"] = False
            result["failures"].append(
                f"TOO MANY enum lines: got {enum_count}, max allowed {max_enums} "
                f"(RET-011 — tag answer should not dump all enums unprompted)"
            )

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(verbose: bool = False) -> list:
    # Import here so path insertion above takes effect first.
    try:
        from core.query_router import run_query_with_method
    except Exception as exc:
        print(f"\n❌  Could not import query_router: {exc}")
        print("     Is Qdrant reachable? Is the nas-ai conda env active?")
        traceback.print_exc()
        sys.exit(1)

    results = []

    print(f"\n{'='*60}")
    print(f"  NAS-AI FIX Smoke Test  —  collection: {COLLECTION}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    for test in SMOKE_TESTS:
        print(f"[{test['id']}] {test['question']}")
        print(f"      Expect: {test['description']}")

        try:
            response = run_query_with_method(COLLECTION, test["question"])
            # run_query_with_method returns a dict with 'result' key
            if isinstance(response, dict):
                raw_answer = response.get("result", response)
                method = response.get("method", "unknown")
            else:
                raw_answer = response
                method = "unknown"

            answer_text = answer_to_text(raw_answer)
            result = evaluate(test, answer_text)
            result["method"] = method

        except Exception as exc:
            answer_text = f"EXCEPTION: {exc}"
            result = {
                "id": test["id"],
                "question": test["question"],
                "description": test["description"],
                "pass": False,
                "failures": [f"EXCEPTION: {exc}"],
                "answer_preview": answer_text[:300],
                "method": "error",
            }

        status = "✅ PASS" if result["pass"] else "❌ FAIL"
        print(f"      Method:  {result.get('method', '?')}")
        print(f"      Status:  {status}")

        if not result["pass"]:
            for f in result["failures"]:
                print(f"      ⚠  {f}")

        if verbose:
            print(f"      Answer:\n{answer_text[:600]}\n")

        print()
        results.append(result)

    # Summary
    passed = sum(1 for r in results if r["pass"])
    total = len(results)
    print(f"{'='*60}")
    print(f"  Result: {passed}/{total} passed")
    print(f"{'='*60}\n")

    return results


def main():
    parser = argparse.ArgumentParser(description="FIX smoke test runner")
    parser.add_argument("--out", type=str, help="Save JSON results to this path")
    parser.add_argument("--verbose", action="store_true", help="Print full answers")
    args = parser.parse_args()

    results = run(verbose=args.verbose)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "run_at": datetime.now().isoformat(),
            "collection": COLLECTION,
            "results": results,
            "summary": {
                "passed": sum(1 for r in results if r["pass"]),
                "total": len(results),
            },
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"Results saved to: {out_path}")


if __name__ == "__main__":
    main()
