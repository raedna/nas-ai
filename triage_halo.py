"""
triage_halo.py — "have we solved this before?" for a Halo ticket.

Pilot flow: ticket JSON -> normalized text -> 14B distills 1-3 search
queries capturing the PROBLEM -> each runs through the standard chat
pipeline (routing + retrieval + arbitration) -> suggested answers with
sources. Read-only: nothing is ingested by this script.

Run on the Mac:
    python3 triage_halo.py haloitsm_jsons/halo_ticket_61643.json \
                           haloitsm_jsons/halo_actions_61643.json
"""
import sys
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")

from HALO.halo_normalizer import normalize_halo_ticket
from core.local_llm_client import call_local_llm_json

_QUERIES_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "triage_queries",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "queries": {"type": "array", "items": {"type": "string"},
                            "minItems": 1, "maxItems": 3},
            },
            "required": ["queries"],
            "additionalProperties": False,
        },
    },
}


def distill_queries(doc_text: str):
    system = (
        "You read an IT support ticket and produce 1-3 short knowledge-base "
        "search queries that would find existing solutions to the ticket's "
        "PROBLEM. Focus on the underlying issue (systems, files, jobs, error "
        "types) — ignore greetings, signatures, disclaimers, and ticket "
        "process chatter. Return ONLY JSON: {queries: [...]}.")
    r = call_local_llm_json(system, doc_text[:4000], temperature=0.0,
                            response_format=_QUERIES_FORMAT)
    qs = [str(q).strip() for q in (r or {}).get("queries", []) if str(q).strip()]
    return qs[:3]


def main(ticket_path, actions_path=None):
    doc = normalize_halo_ticket(ticket_path, actions_path)
    print(f"=== Ticket {doc['identifier']}: {doc['title']}")
    print(f"    team={doc['meta']['team']} client={doc['meta']['client_name']}\n")

    queries = distill_queries(doc["text"])
    if not queries:
        print("No queries distilled — ticket text may be empty/boilerplate.")
        return
    print("Distilled queries:", queries, "\n")

    import core.chat_engine as ce
    from core.ui_data import collection_stats
    cols = [r["name"] for r in collection_stats() if r["chunks"]]

    for q in queries:
        print(f"----- QUERY: {q}")
        selected = ce.select_collections(q, [], cols)
        if not selected:
            print("  (no collections routed)\n")
            continue
        run = ce.run_parallel_queries(selected, q)
        answer = ce._result_to_text(run.get("result", ""))
        print(f"  routed: {selected} | answered from: {run.get('collection')} "
              f"| method: {run.get('method')}")
        print("  " + answer[:500].replace("\n", "\n  "))
        print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)
