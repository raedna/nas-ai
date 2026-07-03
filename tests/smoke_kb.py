"""
KB Docs Smoke Test Runner
=========================
Tests retrieval against the kb_docs collection (HaloITSM KB articles).
Focuses on: entity_row retrieval, article title matching, keyword search,
abbreviation handling, and multi-article topics.

Usage:
    python tests/smoke_kb.py --out results/smoke_kb_$(date +%Y%m%d_%H%M%S).json
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

COLLECTION = "kb_docs"

SMOKE_TESTS = [
    {
        "id": "KB-01",
        "question": "what is tidal",
        "description": "Topic lookup — returns introduction to Tidal article",
        "must_contain": ["tidal", "Tidal"],
        "must_not_contain": [],
        "expected_methods": ["semantic", "lexical_short"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "KB-02",
        "question": "how to troubleshoot tidal",
        "description": "Troubleshooting lookup — returns Tidal job alert troubleshooting article",
        "must_contain": ["tidal", "troubleshoot"],
        "must_not_contain": [],
        "expected_methods": ["semantic", "lexical_short"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "KB-03",
        "question": "steps for the moore prod weekend restart",
        "description": "Stage-aware: PROD weekend restart procedure, NOT the 23R3 DEV health check",
        "must_contain": ["Weekend Restart"],
        "must_not_contain": ["23R3"],
        "expected_methods": ["semantic", "lexical_short"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "KB-04",
        "question": "steps for the moore 21r2 prod archive",
        "description": "Process lookup — returns automated 21r2 PROD Archive article",
        "must_contain": ["21R2", "PROD Archive"],
        "must_not_contain": [],
        "expected_methods": ["semantic", "lexical_short"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "KB-05",
        "question": "one madison failed file loading",
        "description": "Incident lookup — returns one madison daily file load failure article",
        "must_contain": ["madison", "file"],
        "must_not_contain": [],
        "expected_methods": ["semantic", "lexical_short"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "KB-06",
        "question": "how to check recon files on sftp",
        "description": "Process lookup — returns logging in to sftp & checking log files article",
        "must_contain": ["sftp"],
        "must_not_contain": [],
        "expected_methods": ["semantic", "lexical_short"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "KB-07",
        "question": "how to check recon files in pb folders on server",
        "description": "Process lookup — returns Checking files on us1-proc server article",
        "must_contain": ["Prime Broker", "US1-Proc02"],
        "must_not_contain": [],
        "must_not_contain": [],
        "expected_methods": ["semantic", "lexical_short"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "KB-08",
        "question": "error for 1mad s3 buckets",
        "description": "Abbreviation lookup — 1mad S3 buckets → One Madison S3 article",
        "must_contain": ["S3", "Madison"],
        "must_not_contain": [],
        "must_not_contain": [],
        "expected_methods": ["semantic", "lexical_short"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "KB-09",
        "question": "where is the operator release in tidal",
        "description": "Navigation lookup — returns Tidal Operator release article",
        "must_contain": ["Tidal", "operator"],
        "must_not_contain": [],
        "expected_methods": ["semantic", "lexical_short"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "KB-10",
        "question": "FX files not sent issue",
        "description": "Incident lookup — returns Tidal Traiana outbound files article",
        "must_contain": ["Traiana", "FX"],
        "must_not_contain": [],
        "expected_methods": ["semantic", "lexical_short"],
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
    print(f"  NAS-AI KB Smoke Test  —  collection: {COLLECTION}")
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
    parser = argparse.ArgumentParser(description="KB smoke test runner")
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