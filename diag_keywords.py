"""
diag_keywords.py — keyword/about extraction dry run (NO writes). Pulls the
full text of a few kb_docs entries, runs the proposed ingestion-time
extraction prompt, prints what would be stored. The critical contrast:
the CTM guide must get 'ctm'; 'Message Broadcaster Down' must NOT.

Run on the Mac:
  python3 diag_keywords.py
  python3 diag_keywords.py --doc "Some Title" --col kb_docs
"""
import argparse
import sys
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")

from core.db import fetchall
from core.local_llm_client import call_local_llm_json

_ap = argparse.ArgumentParser()
_ap.add_argument("--doc", action="append",
                 help="primary_name (repeatable); default = the 4 exhibit docs")
_ap.add_argument("--col", default="kb_docs")
_args = _ap.parse_args()

DOCS = _args.doc or [
    "CTM Trade Transmission Failures – Investigation Guide",
    "Message Broadcaster Down",
    "Moore Contact Emails and Description for Omni Users",
    "Granting FRA Trading Permission",
    "Charles River Environment Recovery & Middle Tier Troubleshooting Guide",
]

SYSTEM = (
    "You are indexing a document for retrieval. Read the ENTIRE document, "
    "then return STRICT JSON: "
    "{\"about\": \"...\", \"keywords\": [...], \"related\": [...]}.\n"
    "- about: 1-2 sentences stating what this document IS ABOUT — its "
    "subject and purpose, not a summary of every detail.\n"
    "- keywords: 5-15 lowercase keywords/phrases naming the document's "
    "SUBJECTS: the systems, entities, processes and error conditions it is "
    "about. Each keyword must answer yes to: 'is this document about "
    "this?'\n"
    "- related: lowercase terms the document MENTIONS as context, cause, "
    "cross-reference or example, but is NOT about. A troubleshooting guide "
    "for system X that says 'check system Y traffic as a possible cause' "
    "is about X; Y goes in related. When unsure which list a term belongs "
    "in, put it in related."
)

for name in DOCS:
    rows = fetchall(
        "SELECT nlp_text FROM chunks WHERE collection_name = %s "
        "AND primary_name = %s ORDER BY id", (_args.col, name))
    if not rows:
        print(f"\n=== {name}: NOT FOUND in {_args.col}")
        continue
    full = "\n\n".join(r["nlp_text"] or "" for r in rows)
    print(f"\n{'='*70}\n=== {name}  ({len(rows)} chunks, {len(full)} chars)")
    try:
        out = call_local_llm_json(
            SYSTEM, f"Document title: {name}\n\n{full[:12000]}",
            temperature=0.0)
        print("about   :", (out or {}).get("about"))
        print("keywords:", (out or {}).get("keywords"))
        print("related :", (out or {}).get("related"))
    except Exception as e:
        print(f"ERROR {type(e).__name__}: {e}")
