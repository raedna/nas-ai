"""
BBG Smoke Test Runner
=====================
Runs the standard BBG question set against the live system and reports
pass/fail for each question.

BBG fields have no enum values — tests focus on:
  - Mnemonic (primary_name) lookup
  - Description lookup
  - DataType lookup
  - Discovery list (multiple results by keyword or category)

Usage (from project root, with nas-ai conda env active):
    python tests/smoke_bbg.py
    python tests/smoke_bbg.py --verbose
    python tests/smoke_bbg.py --out results/smoke_bbg.json
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

COLLECTION = "bbg_fields"

SMOKE_TESTS = [
    {
        "id": "BBG-01",
        "question": "what is PX_ASK",
        "description": "Mnemonic lookup — returns PX_ASK with description 'Ask Price'",
        "must_contain": ["PX_ASK", "Ask Price"],
        "must_not_contain": [],
        "expected_methods": ["structured_namespace_lookup", "lexical_short", "semantic"],
        "min_results": 1,
        "enum_count_max": 0,
    },
    {
        "id": "BBG-02",
        "question": "what is the description of ARD_OPERATING_EXP_PER_ASM_ASK",
        "description": "Description lookup — returns ARD Operating Expense Per ASM",
        "must_contain": ["ARD", "ASM"],
        "must_not_contain": [],
        "expected_methods": ["structured_namespace_lookup", "lexical_short", "semantic"],
        "min_results": 1,
        "enum_count_max": 0,
    },
    {
        "id": "BBG-03",
        "question": "what type is PX_ASK",
        "description": "Type lookup — returns DataType Double for PX_ASK",
        "must_contain": ["Double"],
        "must_not_contain": [],
        "expected_methods": ["structured_namespace_lookup", "lexical_short", "semantic"],
        "min_results": 1,
        "enum_count_max": 0,
    },
    {
        "id": "BBG-04",
        "question": "what fields contain ask price",
        "description": "Discovery list — returns multiple fields with ask price in name/desc",
        "must_contain": ["ASK"],
        "must_not_contain": [],
        "expected_methods": ["discovery_list", "metadata_sql"],
        "min_results": 3,
        "enum_count_max": None,
    },
    {
        "id": "BBG-05",
        "question": "what fields are in category airlines",
        "description": "Category discovery — returns multiple ARD airline fields",
        "must_contain": ["ARD"],
        "must_not_contain": [],
        "expected_methods": ["discovery_list", "metadata_sql"],
        "min_results": 3,
        "enum_count_max": None,
    },
    {
        "id": "BBG-06",
        "question": "what string fields are available",
        "description": "Type discovery — returns multiple String type fields",
        "must_contain": ["String"],
        "must_not_contain": [],
        "expected_methods": ["discovery_list", "metadata_sql"],
        "min_results": 3,
        "enum_count_max": None,
    },
    {
        "id": "BBG-07",
        "question": "how many fields contain ask price",
        "description": "Count query — returns number of matching fields",
        # Ground truth computed at runtime via SQL (see GROUND_TRUTH_SQL below) —
        # never a hardcoded count.
        "must_contain": ["__SQL_COUNT__:ask price"],
        "must_not_contain": [],
        "expected_methods": ["discovery_count", "metadata_sql"],
        "min_results": 1,
        "enum_count_max": None,
    },
]

# ---------------------------------------------------------------------------
# Helpers (shared pattern with smoke_fix.py)
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
                type_val = item.get("payload", {}).get("type", "") if item.get("payload") else ""
                suffix = f" [{type_val}]" if type_val and type_val != "structured" else ""
                lines.append(f"- {name}: {preview}{suffix}" if preview else f"- {name}{suffix}")
            return "\n".join(lines)
        return json.dumps(answer)
    return str(answer)


def count_enum_lines(text: str) -> int:
    return sum(1 for line in text.splitlines() if line.strip().startswith("- "))


def count_result_lines(text: str) -> int:
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
        # "__SQL_COUNT__:<substring>" — ground truth computed live from the DB:
        # the answer must contain the exact distinct-identifier count of chunks
        # whose nlp_text contains <substring>. Never a hardcoded number.
        if term.startswith("__SQL_COUNT__:"):
            from core.db import fetchall as _fa
            _sub = term.split(":", 1)[1]
            _n = _fa(
                "SELECT COUNT(DISTINCT identifier) AS n FROM chunks "
                "WHERE collection_name = %s AND nlp_text ILIKE %s",
                (COLLECTION, f"%{_sub}%"))[0]["n"]
            if str(_n) not in answer_text:
                result["pass"] = False
                result["failures"].append(
                    f"MISSING ground-truth count {_n} (SQL: nlp_text contains '{_sub}')")
            continue
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
                f"TOO MANY enum lines: got {enum_count}, max {max_enums} "
                f"(BBG fields have no enums — something is wrong)"
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
    print(f"  NAS-AI BBG Smoke Test  —  collection: {COLLECTION}")
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
    parser = argparse.ArgumentParser(description="BBG smoke test runner")
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