"""
FIX Smoke Test Runner
=====================
Runs the standard FIX question set against the live system and reports
pass/fail for each question.

Usage (from project root, with nas-ai conda env active):
    python tests/smoke_fix.py
    python tests/smoke_fix.py --verbose
    python tests/smoke_fix.py --out results/smoke_fix.json

No hardcoding: collection name and expected patterns are defined in
SMOKE_TESTS below as plain data.
"""

import argparse
import json
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

COLLECTION = "xml_test"

SMOKE_TESTS = [
    {
        "id": "FIX-01",
        "question": "what is tag 22",
        "description": "Single field lookup — returns tag 22 name + description, no enum flood",
        "must_contain": ["22", "SecurityIDSource"],
        "must_not_contain": [],
        "expected_methods": ["structured_namespace_lookup"],
        "min_results": 1,
        "enum_count_max": 3,
    },
    {
        "id": "FIX-02",
        "question": "what values can tag 22 have",
        "description": "Enum lookup — returns enum values for tag 22 including ISIN",
        "must_contain": ["22", "ISIN"],
        "must_not_contain": [],
        "expected_methods": ["structured_namespace_lookup", "reverse_enum_lookup"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "FIX-03",
        "question": "what tag is exec broker",
        "description": "Name lookup — returns tag 76 ExecBroker",
        "must_contain": ["76", "ExecBroker"],
        "must_not_contain": [],
        "expected_methods": ["structured_query_plan", "lexical_short", "semantic"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "FIX-04",
        "question": "what tag is execution broker",
        "description": "Synonym lookup — returns tag 76 ExecBroker via synonym",
        "must_contain": ["76", "ExecBroker"],
        "must_not_contain": [],
        "expected_methods": ["structured_query_plan", "lexical_short", "semantic"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "FIX-05",
        "question": "what tag can have a value ISIN",
        "description": "Reverse enum lookup — returns tag 22 SecurityIDSource",
        "must_contain": ["22", "ISIN"],
        "must_not_contain": [],
        "expected_methods": ["reverse_enum_lookup", "structured_namespace_lookup"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "FIX-06",
        "question": "what tags contain security",
        "description": "Discovery list — returns multiple tags with security in name/desc",
        "must_contain": ["security"],
        "must_not_contain": [],
        "expected_methods": ["discovery_list", "structured_query_plan"],
        "min_results": 3,
        "enum_count_max": None,
    },
    {
        "id": "FIX-07",
        "question": "what tags contain broker",
        "description": "Discovery list — returns multiple tags with broker in name/desc",
        "must_contain": ["broker"],
        "must_not_contain": [],
        "expected_methods": ["discovery_list", "structured_query_plan"],
        "min_results": 3,
        "enum_count_max": None,
    },
    {
        "id": "FIX-08",
        "question": "which tag is order quantity",
        "description": "Name lookup — returns tag 38 OrderQty",
        "must_contain": ["38", "OrderQty"],
        "must_not_contain": [],
        "expected_methods": ["structured_query_plan", "lexical_short", "semantic"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "FIX-09",
        "question": "which tag is cum qty",
        "description": "Name lookup — returns tag 14 CumQty",
        "must_contain": ["14", "CumQty"],
        "must_not_contain": [],
        "expected_methods": ["structured_query_plan", "lexical_short", "semantic"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "FIX-10",
        "question": "what is last quantity",
        "description": "Name lookup — returns tag 32 LastQty",
        "must_contain": ["32", "LastQty"],
        "must_not_contain": [],
        "expected_methods": ["structured_query_plan", "lexical_short", "semantic"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "FIX-11",
        "question": "which tag is exec qty",
        "description": "Name lookup — should return a quantity tag, NOT ExecBroker",
        "must_contain": [],
        "must_not_contain": ["ExecBroker", "76"],
        "expected_methods": ["structured_query_plan", "lexical_short", "semantic"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "FIX-12",
        "question": "which tag is exec quantity",
        "description": "Name lookup — should return a quantity tag, NOT ExecBroker",
        "must_contain": [],
        "must_not_contain": ["ExecBroker", "76"],
        "expected_methods": ["structured_query_plan", "lexical_short", "semantic"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "FIX-13",
        "question": "which tag is targetcompid",
        "description": "Name lookup — returns tag 56 TargetCompID",
        "must_contain": ["56", "TargetCompID"],
        "must_not_contain": [],
        "expected_methods": ["structured_query_plan", "lexical_short", "semantic"],
        "min_results": 1,
        "enum_count_max": None,
    },
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def answer_to_text(answer) -> str:
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
        # Discovery result dict — format as readable list
        if "results" in answer and "total_matches" in answer:
            lines = [f"{answer['total_matches']} match(es) found."]
            for item in answer.get("results", []):
                name = item.get("primary_name") or item.get("identifier") or ""
                preview = item.get("preview") or ""
                lines.append(f"- {name}: {preview}" if preview else f"- {name}")
            return "\n".join(lines)
        return json.dumps(answer)
    return str(answer)


def count_enum_lines(text: str) -> int:
    return sum(1 for line in text.splitlines() if line.strip().startswith("- "))


def count_result_lines(text: str) -> int:
    """Count non-empty lines as a proxy for number of results returned."""
    return sum(1 for line in text.splitlines() if line.strip())


def evaluate(test: dict, answer_text: str, method: str) -> dict:
    result = {
        "id": test["id"],
        "question": test["question"],
        "description": test["description"],
        "pass": True,
        "failures": [],
        "answer_preview": answer_text[:300].replace("\n", " "),
        "method": method,
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
                f"TOO MANY enum lines: got {enum_count}, max {max_enums}"
            )

    min_results = test.get("min_results")
    if min_results and min_results > 1:
        line_count = count_result_lines(answer_text)
        if line_count < min_results:
            result["pass"] = False
            result["failures"].append(
                f"TOO FEW results: got {line_count} lines, expected at least {min_results}"
            )

    expected_methods = test.get("expected_methods", [])
    if expected_methods and method not in expected_methods:
        result["pass"] = False
        result["failures"].append(
            f"WRONG METHOD: got '{method}', expected one of {expected_methods}"
        )

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(verbose: bool = False) -> list:
    try:
        from core.retrieval.router import run_query_with_method
    except Exception as exc:
        print(f"\n❌  Could not import retrieval router: {exc}")
        print("     Is PostgreSQL reachable? Is the nas-ai conda env active?")
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
            if isinstance(response, dict):
                raw_answer = response.get("result", response)
                method = response.get("method", "unknown")
            else:
                raw_answer = response
                method = "unknown"

            answer_text = answer_to_text(raw_answer)
            result = evaluate(test, answer_text, method)

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