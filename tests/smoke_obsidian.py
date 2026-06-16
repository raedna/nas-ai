"""
Obsidian Smoke Test Runner
==========================
Tests retrieval against the obsidian collection.
Covers: single-note lookup, category/folder queries, image-bearing notes,
discovery list/count, and procedural note retrieval.

Usage:
    python tests/smoke_obsidian.py
    python tests/smoke_obsidian.py --verbose
    python tests/smoke_obsidian.py --out results/smoke_obsidian_$(date +%Y%m%d_%H%M%S).json
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

COLLECTION = "obsidian"

SMOKE_TESTS = [
    # --- Single note lookup (answer) ---
    {
        "id": "OBS-01",
        "question": "what is the sftp folder for gsact.txt",
        "description": "Single note lookup — EOD CRD note, sftp path",
        "must_contain": ["outgoing", "sftp"],
        "must_not_contain": [],
        "expected_methods": ["semantic", "answer", "lexical_short"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "OBS-02",
        "question": "what is the EOD process for Moore",
        "description": "Single note lookup — End Of Day Processing CRD note",
        "must_contain": ["EOD"],
        "must_not_contain": [],
        "expected_methods": ["semantic", "answer", "lexical_short"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "OBS-03",
        "question": "how does the mosaic workflow work",
        "description": "Single note lookup — Mosaic Workflow note",
        "must_contain": ["mosaic", "Mosaic"],
        "must_not_contain": [],
        "expected_methods": ["semantic", "answer", "lexical_short"],
        "min_results": 1,
        "enum_count_max": None,
    },
    # --- Category/folder discovery (discovery_list) ---
    {
        "id": "OBS-04",
        "question": "what are the Moore notes",
        "description": "Category list — discovery_list for Moore folder notes",
        "must_contain": ["Moore"],
        "must_not_contain": [],
        "expected_methods": ["discovery_list"],
        "min_results": 2,
        "enum_count_max": None,
    },
    {
        "id": "OBS-05",
        "question": "show me all notes about Moore",
        "description": "Category list — show me all phrasing for Moore folder",
        "must_contain": ["Moore"],
        "must_not_contain": [],
        "expected_methods": ["discovery_list"],
        "min_results": 2,
        "enum_count_max": None,
    },
    {
        "id": "OBS-06",
        "question": "what are the Astro Workflows notes",
        "description": "Category list — Astro Workflows folder notes",
        "must_contain": ["Astro", "Workflow"],
        "must_not_contain": [],
        "expected_methods": ["discovery_list"],
        "min_results": 1,
        "enum_count_max": None,
    },
    # --- Discovery count ---
    {
        "id": "OBS-07",
        "question": "how many Moore notes are there",
        "description": "Count query — should return a number, not 0",
        "must_contain": ["match"],
        "must_not_contain": ["0 match"],
        "expected_methods": ["discovery_count"],
        "min_results": 1,
        "enum_count_max": None,
    },
    # --- Procedural / how-to (answer, not list) ---
    {
        "id": "OBS-08",
        "question": "how to check the log directory for sequence of events",
        "description": "Procedural lookup — Check Log Directory note",
        "must_contain": ["log"],
        "must_not_contain": [],
        "expected_methods": ["semantic", "answer", "lexical_short"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "OBS-09",
        "question": "how to check sftp for activity",
        "description": "Procedural lookup — Checking sFTP for Activity note",
        "must_contain": ["sftp", "sFTP"],
        "must_not_contain": [],
        "expected_methods": ["semantic", "answer", "lexical_short"],
        "min_results": 1,
        "enum_count_max": None,
    },
    # --- Personal Notes folder ---
    {
        "id": "OBS-10",
        "question": "how do I run rsync on the NAS",
        "description": "Single note lookup — NAS rsync commands note",
        "must_contain": ["rsync"],
        "must_not_contain": [],
        "expected_methods": ["semantic", "answer", "lexical_short"],
        "min_results": 1,
        "enum_count_max": None,
    },
]


# ---------------------------------------------------------------------------
# Helpers (shared pattern with other smoke tests)
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
        if "results" in answer and "total_matches" in answer:
            lines = [f"{answer['total_matches']} match(es) found."]
            for item in answer.get("results", []):
                name = item.get("primary_name") or item.get("identifier") or ""
                preview = item.get("preview") or ""
                lines.append(f"- {name}: {preview}" if preview else f"- {name}")
            return "\n".join(lines)
        return json.dumps(answer)
    return str(answer)


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
        if not re.search(re.escape(term.lower()), lower):
            result["pass"] = False
            result["failures"].append(f"MISSING expected term: '{term}'")

    for term in test.get("must_not_contain", []):
        if re.search(re.escape(term.lower()), lower):
            result["pass"] = False
            result["failures"].append(f"FOUND forbidden term: '{term}'")

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
        traceback.print_exc()
        sys.exit(1)

    results = []

    print(f"\n{'='*60}")
    print(f"  NAS-AI Obsidian Smoke Test  —  collection: {COLLECTION}")
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
    parser = argparse.ArgumentParser(description="Obsidian smoke test runner")
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
