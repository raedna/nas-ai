"""
Recon Assist File Smoke Test Runner
=====================================
Tests retrieval against the recon_assist_file collection (RECON Moore-PB mapping).
Covers: structured record lookup, labeled key-value answer rendering,
Prime Broker field display, discovery list by broker, and count queries.

Usage:
    python tests/smoke_recon.py
    python tests/smoke_recon.py --verbose
    python tests/smoke_recon.py --out results/smoke_recon_$(date +%Y%m%d_%H%M%S).json
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

COLLECTION = "recon_assist_file"

SMOKE_TESTS = [
    # --- Single structured record lookup ---
    {
        "id": "RECON-01",
        "question": "what is the move script for gsact.txt",
        "description": "Structured lookup — gsact.txt move script (K:/Recon/FTP)",
        "must_contain": ["GSCopy_srpb.bat"],
        "must_not_contain": [],
        "expected_methods": ["identifier_lookup", "semantic", "answer"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "RECON-02",
        "question": "what is the file format for gsact.txt",
        "description": "Structured lookup — gsact.txt recon tool file format",
        "must_contain": ["GS Cash Activity"],
        "must_not_contain": [],
        "expected_methods": ["identifier_lookup", "semantic", "answer"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "RECON-03",
        "question": "what is the recon job for gsact.txt",
        "description": "Structured lookup — gsact.txt primary name (Tidal job)",
        "must_contain": ["019_W_RECON_GOLDMAN"],
        "must_not_contain": [],
        "expected_methods": ["identifier_lookup", "semantic", "answer"],
        "min_results": 1,
        "enum_count_max": None,
    },
    # --- Labeled key-value rendering (Phase 3.18 feature) ---
    {
        "id": "RECON-04",
        "question": "what is the recon tool data source for gsact.txt",
        "description": "Labeled field rendering — recon tool data source",
        "must_contain": ["Transaction"],
        "must_not_contain": [],
        "expected_methods": ["identifier_lookup", "semantic", "answer"],
        "min_results": 1,
        "enum_count_max": None,
    },
    {
        "id": "RECON-05",
        "question": "what is the prime broker for gsact.txt",
        "description": "Type field label — should show 'Prime Broker: Goldman'",
        "must_contain": ["Goldman"],
        "must_not_contain": [],
        "expected_methods": ["identifier_lookup", "semantic", "answer"],
        "min_results": 1,
        "enum_count_max": None,
    },
    # --- Discovery list by broker ---
    {
        "id": "RECON-06",
        "question": "what are the goldman files",
        "description": "Discovery list — Goldman recon files (record listing must include gsact.txt)",
        "must_contain": ["gsact.txt"],
        "must_not_contain": ["0 match", "0 record"],
        "expected_methods": ["discovery_list", "metadata_sql"],
        "min_results": 2,
        "enum_count_max": None,
    },
    {
        "id": "RECON-07",
        "question": "show all files for Goldman",
        "description": "Discovery list — all Goldman recon files (record listing must include gsact.txt)",
        "must_contain": ["gsact.txt"],
        "must_not_contain": ["0 match", "0 record"],
        "expected_methods": ["discovery_list", "metadata_sql"],
        "min_results": 2,
        "enum_count_max": None,
    },
    # --- Discovery count ---
    {
        "id": "RECON-08",
        "question": "how many goldman files are there",
        "description": "Count query — Goldman file count, must be non-zero",
        "must_contain": ["match"],
        "must_not_contain": ["0 match"],
        "expected_methods": ["discovery_count", "metadata_sql"],
        "min_results": 1,
        "enum_count_max": None,
    },
    # --- Different identifier lookup ---
    {
        "id": "RECON-09",
        "question": "what is the recon job for gsact_fx.txt",
        "description": "Structured lookup — gsact_fx.txt recon job (Tidal job name)",
        "must_contain": ["020_W_RECON_GOLDMAN_PB_PULL"],
        "must_not_contain": [],
        "expected_methods": ["identifier_lookup", "semantic", "answer"],
        "min_results": 1,
        "enum_count_max": None,
    },
    # --- Data source field ---
    {
        "id": "RECON-10",
        "question": "what is the recon tool data source for gsact_fx.txt",
        "description": "Labeled field lookup — Recon Tool Data Source for gsact_fx",
        "must_contain": ["Transaction", "FX"],
        "must_not_contain": [],
        "expected_methods": ["identifier_lookup", "semantic", "answer"],
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
    print(f"  NAS-AI Recon Smoke Test  —  collection: {COLLECTION}")
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
    parser = argparse.ArgumentParser(description="Recon smoke test runner")
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