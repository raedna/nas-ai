"""
backfill_keywords.py — CLI wrapper around the same about/keywords scan the
UI runs (ingestion tab: 'Run About Scan' button / 'About scan after ingest'
switch). Groups doc-shaped chunks by entry, one LLM call per entry, updates
payload in place — no re-embed. Resumable: annotated entries skip.

Per-collection eligibility is declared in collections.json (about_scan:
all | auto | off) — see core/keyword_extractor.py.

Run on the Mac:
  python3 backfill_keywords.py --col kb_docs
  python3 backfill_keywords.py --col kb_docs --col halo_tickets
"""
import argparse
import sys
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")

from core.keyword_extractor import scan_collection

_ap = argparse.ArgumentParser()
_ap.add_argument("--col", action="append", required=True,
                 help="collection to scan (repeatable)")
_args = _ap.parse_args()

for col in _args.col:
    res = scan_collection(col)
    print(f"=== {col}: {res}")
