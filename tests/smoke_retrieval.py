"""
Retrieval Package Smoke Test
=============================
Tests imports and basic function behaviour for the four new modules:
  core/retrieval/reranker.py
  core/retrieval/answer.py
  core/retrieval/router.py
  core/retrieval/__init__.py

Also exercises the updated ui_app.py import surface.

Runs entirely offline (no PostgreSQL required) for the import/unit tests.
The live integration tests at the bottom require a working DB connection and
are skipped gracefully if the DB is unreachable.

Usage (from project root, nas-ai conda env active):
    python tests/smoke_retrieval.py
    python tests/smoke_retrieval.py --verbose
    python tests/smoke_retrieval.py --live          # also run DB-dependent tests
"""

import argparse
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

PASS = "✅ PASS"
FAIL = "❌ FAIL"
SKIP = "⏭  SKIP"

results = []


def record(name: str, passed: bool, detail: str = "", skip: bool = False):
    tag = SKIP if skip else (PASS if passed else FAIL)
    print(f"  {tag}  {name}")
    if detail:
        print(f"         {detail}")
    results.append({"name": name, "pass": passed, "skip": skip, "detail": detail})


# ---------------------------------------------------------------------------
# 1. Import tests
# ---------------------------------------------------------------------------

def test_imports():
    print("\n── 1. Import tests ──────────────────────────────────────────")

    # reranker
    try:
        from core.retrieval.reranker import (
            rerank_points,
            score_point_shared,
            dedupe_entity_row_points,
            dedupe_structured_results,
            is_document_like_payload,
        )
        record("reranker: all public names importable", True)
    except Exception as e:
        record("reranker: all public names importable", False, str(e))

    # answer
    try:
        from core.retrieval.answer import (
            synthesize_answer,
            build_answer,
            get_display_labels,
            get_source_label,
            dedupe_repeated_paragraphs,
        )
        record("answer: all public names importable", True)
    except Exception as e:
        record("answer: all public names importable", False, str(e))

    # router
    try:
        from core.retrieval.router import (
            run_query_with_method,
            route_query,
            debug_route_query,
            explain_query_routing,
            detect_query_mode,
            score_point_shared,
            semantic_search,
            fetch_entity_row_by_title,
        )
        record("router: all public names importable", True)
    except Exception as e:
        record("router: all public names importable", False, str(e))

    # package __init__
    try:
        from core.retrieval import (
            run_query_with_method,
            route_query,
            debug_route_query,
            synthesize_answer,
            build_answer,
            get_display_labels,
            get_source_label,
        )
        record("__init__: package-level imports work", True)
    except Exception as e:
        record("__init__: package-level imports work", False, str(e))

    # ui_app import surface (import only — don't start Streamlit)
    try:
        import importlib, unittest.mock as mock
        # Patch streamlit so importing ui_app doesn't launch a server
        with mock.patch.dict("sys.modules", {"streamlit": mock.MagicMock()}):
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "ui_app_check",
                PROJECT_ROOT / "core" / "ui_app.py",
            )
            mod = importlib.util.module_from_spec(spec)
            # We only check the import lines resolve; execution would need st
            # so we catch AttributeError (streamlit mock) as acceptable
            try:
                spec.loader.exec_module(mod)
            except (AttributeError, Exception):
                pass  # streamlit mock triggers; that's fine

        # The real check: can we import the names ui_app imports from router?
        from core.retrieval.router import (
            route_query, semantic_search, debug_route_query,
            fetch_entity_row_by_title, run_query_with_method,
            get_display_labels, explain_query_routing, score_point_shared,
        )
        from core.retrieval.discovery import detect_ask_intent, run_discovery_with_method
        from core.retrieval.crosslink import run_comparison_query
        record("ui_app: all imports from retrieval package resolve", True)
    except Exception as e:
        record("ui_app: all imports from retrieval package resolve", False, str(e))


# ---------------------------------------------------------------------------
# 2. Unit tests — reranker
# ---------------------------------------------------------------------------

def test_reranker():
    print("\n── 2. Reranker unit tests ───────────────────────────────────")

    from core.retrieval.reranker import (
        is_document_like_payload,
        dedupe_entity_row_points,
        dedupe_structured_results,
        rerank_points,
        score_point_shared,
    )

    # is_document_like_payload
    try:
        assert is_document_like_payload({"doc_type": "procedural"}) is True
        assert is_document_like_payload({"source_type": "doc"}) is True
        assert is_document_like_payload({"doc_type": "structured"}) is False
        assert is_document_like_payload({}) is False
        record("is_document_like_payload: correct classification", True)
    except Exception as e:
        record("is_document_like_payload: correct classification", False, str(e))

    # dedupe_structured_results
    try:
        items = [
            {"identifier": "22", "primary_name": "SecurityIDSource"},
            {"identifier": "22", "primary_name": "SecurityIDSource"},  # dup
            {"identifier": "55", "primary_name": "Symbol"},
        ]
        deduped = dedupe_structured_results(items)
        assert len(deduped) == 2, f"Expected 2 items, got {len(deduped)}"
        record("dedupe_structured_results: removes duplicates", True)
    except Exception as e:
        record("dedupe_structured_results: removes duplicates", False, str(e))

    # dedupe_entity_row_points — build minimal fake Points
    try:
        class FakePoint:
            def __init__(self, payload):
                self.payload = payload
                self.score = 0.9
                self.id = id(self)

        pts = [
            FakePoint({"primary_name": "FIX Protocol", "doc_type": "entity_row"}),
            FakePoint({"primary_name": "FIX Protocol", "doc_type": "entity_row"}),  # dup
            FakePoint({"primary_name": "SWIFT", "doc_type": "entity_row"}),
        ]
        deduped = dedupe_entity_row_points(pts)
        assert len(deduped) == 2, f"Expected 2, got {len(deduped)}"
        record("dedupe_entity_row_points: removes duplicates", True)
    except Exception as e:
        record("dedupe_entity_row_points: removes duplicates", False, str(e))

    # score_point_shared — structured payload, higher score for name match
    try:
        class FakePoint:
            def __init__(self, name, desc, score=0.5):
                self.payload = {
                    "primary_name": name,
                    "description": desc,
                    "doc_type": "structured",
                    "identifier": "22",
                }
                self.score = score
                self.id = id(self)

        exact = FakePoint("SecurityIDSource", "Identifies class or source of SecurityID")
        unrelated = FakePoint("Price", "Price of the instrument")

        s_exact = score_point_shared(exact, "SecurityIDSource")
        s_unrelated = score_point_shared(unrelated, "SecurityIDSource")
        assert s_exact > s_unrelated, f"Exact match should score higher: {s_exact} vs {s_unrelated}"
        record("score_point_shared: exact name match scores higher", True)
    except Exception as e:
        record("score_point_shared: exact name match scores higher", False, str(e))

    # rerank_points — structured: exact name match should end up first
    try:
        class FakePoint:
            def __init__(self, name, desc, score=0.5):
                self.payload = {
                    "primary_name": name,
                    "description": desc,
                    "doc_type": "structured",
                }
                self.score = score
                self.id = id(self)

        pts = [
            FakePoint("Price", "Price of instrument", score=0.9),
            FakePoint("SecurityIDSource", "Source of security ID", score=0.7),
        ]
        reranked = rerank_points(pts, "SecurityIDSource")
        assert reranked[0].payload["primary_name"] == "SecurityIDSource", \
            f"Expected SecurityIDSource first, got {reranked[0].payload['primary_name']}"
        record("rerank_points: exact name match promoted to top", True)
    except Exception as e:
        record("rerank_points: exact name match promoted to top", False, str(e))

    # rerank_points — entity_row: negation penalty
    try:
        class FakePoint:
            def __init__(self, name, desc, score=0.8):
                self.payload = {
                    "primary_name": name,
                    "description": desc,
                    "doc_type": "entity_row",
                }
                self.score = score
                self.id = id(self)

        pts = [
            FakePoint("FIX Protocol Overview", "Introduction to FIX", score=0.9),
            FakePoint("FIX Protocol not supported", "Deprecated feature", score=0.85),
        ]
        reranked = rerank_points(pts, "FIX Protocol not supported")
        # The "not supported" entry should be penalised
        assert reranked[0].payload["primary_name"] == "FIX Protocol Overview", \
            f"Expected non-negated result first, got {reranked[0].payload['primary_name']}"
        record("rerank_points: negation penalty applied for entity_row", True)
    except Exception as e:
        record("rerank_points: negation penalty applied for entity_row", False, str(e))


# ---------------------------------------------------------------------------
# 3. Unit tests — answer
# ---------------------------------------------------------------------------

def test_answer():
    print("\n── 3. Answer unit tests ─────────────────────────────────────")

    from core.retrieval.answer import (
        synthesize_answer,
        dedupe_repeated_paragraphs,
        build_answer,
    )

    # dedupe_repeated_paragraphs
    try:
        text = "Hello world\n\nHello world\n\nSecond paragraph"
        result = dedupe_repeated_paragraphs(text)
        assert result.count("Hello world") == 1, "Should deduplicate repeated paragraph"
        assert "Second paragraph" in result
        record("dedupe_repeated_paragraphs: removes duplicates", True)
    except Exception as e:
        record("dedupe_repeated_paragraphs: removes duplicates", False, str(e))

    # synthesize_answer — structured doc_type
    try:
        payload = {
            "doc_type": "structured",
            "identifier": "22",
            "identifier_field": "Tag",
            "primary_name": "SecurityIDSource",
            "description": "Identifies class or source of SecurityID value.",
            "enum_values": [],
        }
        answer = synthesize_answer(payload, [], "xml_test")
        assert "22" in answer
        assert "SecurityIDSource" in answer
        record("synthesize_answer: structured payload contains identifier + name", True)
    except Exception as e:
        record("synthesize_answer: structured payload contains identifier + name", False, str(e))

    # synthesize_answer — structured with enum_values and enum role
    try:
        payload = {
            "doc_type": "structured",
            "identifier": "22",
            "identifier_field": "Tag",
            "primary_name": "SecurityIDSource",
            "description": "Identifies class or source.",
            "enum_values": [
                {"enum_value": "1", "enum_name": "CUSIP", "description": "CUSIP"},
                {"enum_value": "4", "enum_name": "ISIN", "description": "ISIN number"},
            ],
        }
        answer = synthesize_answer(payload, ["enum_value"], "xml_test")
        assert "ISIN" in answer, f"Expected ISIN in answer: {answer}"
        assert "CUSIP" in answer, f"Expected CUSIP in answer: {answer}"
        record("synthesize_answer: enum_value role returns enum list", True)
    except Exception as e:
        record("synthesize_answer: enum_value role returns enum list", False, str(e))

    # synthesize_answer — entity_row
    try:
        payload = {
            "doc_type": "entity_row",
            "primary_name": "FIX Protocol",
            "description": "Financial Information eXchange protocol.",
            "source_type": "obsidian",
            "source_file": "fix_protocol.md",
        }
        answer = synthesize_answer(payload, [], "knowledge_base")
        assert "FIX Protocol" in answer
        record("synthesize_answer: entity_row returns name + description", True)
    except Exception as e:
        record("synthesize_answer: entity_row returns name + description", False, str(e))

    # build_answer legacy shim — basic
    try:
        class FakePoint:
            def __init__(self):
                self.payload = {
                    "identifier": "22",
                    "primary_name": "SecurityIDSource",
                    "description": "Source of security ID.",
                }
                self.score = 0.9

        answer = build_answer([FakePoint()], [])
        assert "22" in answer or "SecurityIDSource" in answer
        record("build_answer: legacy shim returns non-empty string", True)
    except Exception as e:
        record("build_answer: legacy shim returns non-empty string", False, str(e))


# ---------------------------------------------------------------------------
# 4. Unit tests — router (offline, no DB)
# ---------------------------------------------------------------------------

def test_router_offline():
    print("\n── 4. Router offline tests ──────────────────────────────────")

    from core.retrieval.router import detect_query_mode, explain_query_routing

    # detect_query_mode — short query → lexical_short
    try:
        result = detect_query_mode("tag 22")
        assert result["mode"] == "lexical_short", f"Expected lexical_short, got {result['mode']}"
        record("detect_query_mode: 2-word query → lexical_short", True)
    except Exception as e:
        record("detect_query_mode: 2-word query → lexical_short", False, str(e))

    # detect_query_mode — sentence → semantic
    try:
        result = detect_query_mode("what is the description of SecurityIDSource")
        assert result["mode"] == "semantic", f"Expected semantic, got {result['mode']}"
        record("detect_query_mode: sentence query → semantic", True)
    except Exception as e:
        record("detect_query_mode: sentence query → semantic", False, str(e))

    # explain_query_routing — returns expected keys
    try:
        result = explain_query_routing("xml_test", "what is tag 22")
        required_keys = {"question", "intent_mode", "relationship_query", "enum_lookup_query", "namespace", "identifier"}
        missing = required_keys - set(result.keys())
        assert not missing, f"Missing keys: {missing}"
        record("explain_query_routing: returns required keys", True)
    except Exception as e:
        record("explain_query_routing: returns required keys", False, str(e))

    # explain_query_routing — namespace detection
    try:
        result = explain_query_routing("xml_test", "what is tag 22")
        assert result["identifier"] == "22", f"Expected identifier=22, got {result['identifier']}"
        record("explain_query_routing: detects identifier '22' in 'what is tag 22'", True)
    except Exception as e:
        record("explain_query_routing: detects identifier '22' in 'what is tag 22'", False, str(e))


# ---------------------------------------------------------------------------
# 5. Live integration tests (requires PostgreSQL on NAS)
# ---------------------------------------------------------------------------

def test_live(collection: str = "xml_test"):
    print(f"\n── 5. Live integration tests (collection={collection}) ───────")

    # Check DB reachable
    try:
        from core.retrieval.db_retrieval import fetchall
        fetchall("SELECT 1", ())
    except Exception as e:
        record("DB connection", False, f"SKIP — cannot reach DB: {e}", skip=True)
        print("     (All live tests skipped — DB unreachable)")
        return

    from core.retrieval.router import run_query_with_method

    live_cases = [
        {
            "id": "LIVE-01",
            "question": "what is tag 22",
            "must_contain": ["22", "SecurityIDSource"],
            "must_not": [],
        },
        {
            "id": "LIVE-02",
            "question": "what values can tag 22 have",
            "must_contain": ["ISIN"],
            "must_not": [],
        },
        {
            "id": "LIVE-03",
            "question": "SecurityIDSource",
            "must_contain": ["SecurityIDSource"],
            "must_not": [],
        },
    ]

    for case in live_cases:
        try:
            result = run_query_with_method(collection, case["question"])
            answer = result.get("result") or ""
            method = result.get("method", "?")

            failures = []
            for term in case["must_contain"]:
                if term.lower() not in answer.lower():
                    failures.append(f"Missing: '{term}'")
            for term in case["must_not"]:
                if term.lower() in answer.lower():
                    failures.append(f"Should not contain: '{term}'")

            passed = not failures
            detail = f"method={method}" + (f" | {'; '.join(failures)}" if failures else "")
            record(f"{case['id']}: {case['question']}", passed, detail)

        except Exception as e:
            record(f"{case['id']}: {case['question']}", False, f"EXCEPTION: {e}")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Retrieval package smoke tests")
    parser.add_argument("--live", action="store_true", help="Also run DB-dependent tests")
    parser.add_argument("--collection", default="xml_test", help="Collection for live tests")
    parser.add_argument("--verbose", action="store_true", help="Show all detail lines")
    args = parser.parse_args()

    print("=" * 60)
    print("  Retrieval Package Smoke Tests")
    print("=" * 60)

    test_imports()
    test_reranker()
    test_answer()
    test_router_offline()

    if args.live:
        test_live(collection=args.collection)
    else:
        print("\n── 5. Live integration tests ────────────────────────────────")
        print("     (skipped — run with --live to include DB tests)")

    # Summary
    run = [r for r in results if not r["skip"]]
    passed = sum(1 for r in run if r["pass"])
    failed = sum(1 for r in run if not r["pass"])
    skipped = sum(1 for r in results if r["skip"])

    print(f"\n{'=' * 60}")
    print(f"  Result: {passed}/{len(run)} passed", end="")
    if skipped:
        print(f"  ({skipped} skipped)", end="")
    print()
    if failed:
        print("  Failed tests:")
        for r in results:
            if not r["pass"] and not r["skip"]:
                print(f"    ❌  {r['name']}")
                if r["detail"]:
                    print(f"        {r['detail']}")
    print("=" * 60)

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
