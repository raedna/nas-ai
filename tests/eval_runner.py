"""
NAS-AI Retrieval Eval Runner
============================
Automates NAS_AI_Retrieval_Eval_v1.md: fires every question through BOTH
surfaces — Ask (run_query_with_method against the expected collection, as a
user picking it in the tab) and Chat (chat_turn, auto-routed) — and writes a
results markdown + json. Correctness verdicts stay human: fill the ☐ boxes.

Usage:
    python tests/eval_runner.py               # all 50 questions, both surfaces
    python tests/eval_runner.py AG MI         # only these categories
    python tests/eval_runner.py --chat-only   # skip Ask surface
    python tests/eval_runner.py --ask-only    # skip Chat surface

NOTE: full run = ~100 pipeline invocations with several LLM calls each.
Expect 30-60+ minutes on the M1. Don't run while ingesting.
"""

import json
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# (id, question, expected ground truth note, ask_collection)
# ask_collection = what a user would pick in the Ask tab (eval doc's expected
# collection; first one when the expectation spans collections).
QUESTIONS = [
    # --- Category 1: Direct Lookup ---
    ("DL-01", "what is FIX tag 22", "SecurityIDSource definition", "xml_test"),
    ("DL-02", "what is gsact.txt", "Goldman RECON mapping record", "recon_assist_file"),
    ("DL-03", "what is tag 35 in FIX", "MsgType definition", "xml_test"),
    ("DL-04", "jpm_activity.xlsx details", "JPM mapping record (verify filename)", "recon_assist_file"),
    ("DL-05", "what is the PB filename for gsact.txt", "SRPB_4000_..._Custody_Tra alias", "recon_assist_file"),
    ("DL-06", "what is the Whirlpool galaxy", "M51/NGC5194 catalog entry", "astro_catalog"),
    ("DL-07", "ARD_OPERATING_EXP_PER_ASM_ASK", "BBG field A0356 definition", "bbg_fields"),
    ("DL-08", "message broadcaster down", "Broadcaster Down / Alert Checklist KB article", "kb_docs"),
    ("DL-09", "how to merge a ticket in HaloITSM", "Merge ticket KB article", "kb_docs"),
    ("DL-10", "NGC 2064", "catalog entry (type, coordinates)", "astro_catalog"),
    # --- Category 2: Paraphrase / messy phrasing ---
    ("PP-01", "that goldman activity file, whats the tidal job for it", "gsact.txt -> 019_W_RECON_GOLDMAN_PRIO_PULL", "recon_assist_file"),
    ("PP-02", "whats teh fix tag for order quantity", "OrderQty tag 38", "xml_test"),
    ("PP-03", "brodcaster acting up agian", "broadcaster troubleshooting", "kb_docs"),
    ("PP-04", "CR wont let me cancel a fix trade", "Cancel FIX Trades in CRD When Blocked", "kb_docs"),
    ("PP-05", "the us1 proc server thing for recon", "4.2 Checking Files on us1-proc02 / KB 2.2", "obsidian"),
    ("PP-06", "goldman prio pull job", "019_W_RECON_GOLDMAN_PRIO_PULL record", "recon_assist_file"),
    ("PP-07", "jennison morning stuff failing", "Jennison Morning Batch escalation/runbook", "kb_docs"),
    ("PP-08", "that lock file error on recon jobs", "2.5 Clearing Lock Files", "kb_docs"),
    # --- Category 3: Procedural ---
    ("PR-01", "how can I check if gsact.txt is on the sftp", "4.3.1 Checking sFTP / KB 2.3 (procedure, not mapping)", "obsidian"),
    ("PR-02", "recon file missing, what do I do", "Bad Dates workflow steps 1-4", "kb_docs"),
    ("PR-03", "how to rerun a tidal recon job", "4.1 Checking the Tidal Recon Job", "obsidian"),
    ("PR-04", "how to manually load a file in the recon tool", "5. Manual File Loading in RECON Tool", "obsidian"),
    ("PR-05", "weekend restart procedure for moore prod", "Automated 21R2 Weekend Restart (PROD)", "kb_docs"),
    ("PR-06", "how do I copy FIX logs to my machine", "How to copy FIX Logs to local machine", "kb_docs"),
    ("PR-07", "steps for one madison data load failure", "One Madison Data Load Status FAILURE article", "kb_docs"),
    ("PR-08", "how to check charles river logs on a user machine", "CR Log Folder for Errors note", "obsidian"),
    # --- Category 4: Aggregation ---
    ("AG-01", "how many KB articles are there", "178 active articles (NOT 348 chunks)", "kb_docs"),
    ("AG-02", "how many articles mention FIX", "SQL truth: distinct articles with fix in nlp_text", "kb_docs"),
    ("AG-03", "how many FIX tags are there", "947 (namespace=tag)", "xml_test"),
    ("AG-04", "how many Goldman files are in the recon mapping", "16 (15 if NULL row uncounted — CODE-024)", "recon_assist_file"),
    ("AG-05", "how many images do I have for M42", "verify astro_test", "astro_test"),
    ("AG-06", "how many galaxies are in the catalog", "count type=Gx in astro_catalog", "astro_catalog"),
    ("AG-07", "list all prime brokers in the recon file", "BOA, BONY, CHASE, CITCO, CITI, CS, DB, Goldman, JPM, MIZUHO, Morgan", "recon_assist_file"),
    ("AG-08", "which broker has the most recon files", "group-by type, Goldman likely", "recon_assist_file"),
    ("AG-09", "how many images with gain 100", "verify astro_test file_gain", "astro_test"),
    ("AG-10", "how many fields are in FIX 4.4", "Fields_FIX44 count", "xml_test"),
    # --- Category 5: Cross-collection ---
    ("XC-01", "I'm missing gsact.txt from Goldman, what can I do", "mapping record + Tidal/sFTP procedure", "recon_assist_file"),
    ("XC-02", "what tidal job pulls jpm files and how do I check if it ran", "JPM job name + 4.1 checking procedure", "recon_assist_file"),
    ("XC-03", "what is tag 38 and are there KB articles about FIX order issues", "tag def + FIX-related KB articles", "xml_test"),
    ("XC-04", "show me the M51 catalog entry and do I have images of it", "catalog entry + M51 images", "astro_catalog"),
    ("XC-05", "bad dates alert for citi, which file and what steps", "citi mapping + bad dates workflow", "recon_assist_file"),
    ("XC-06", "broadcaster is down, who do I contact and what do I check", "checklist article + contacts", "kb_docs"),
    # --- Category 6: No-answer traps ---
    ("NA-01", "what is FIX tag 99999", "not found — tag doesn't exist", "xml_test"),
    ("NA-02", "what is the recon mapping for barclays_fx_swap.txt", "not found — must not invent a job", "recon_assist_file"),
    ("NA-03", "how do I restart the Bloomberg terminal server", "not found — no such procedure", "kb_docs"),
    ("NA-04", "what are the FIX 5.0 SP2 changes", "not found — only 4.2/4.4 ingested", "xml_test"),
    # --- Category 7: Multi-item ---
    ("MI-01", "what are tags 22, 35 and 54", "SecurityIDSource + MsgType + Side", "xml_test"),
    ("MI-02", "give me the tidal jobs for gsact.txt and gspos.txt", "both Goldman job names", "recon_assist_file"),
    ("MI-03", "compare FIX tag 38 and tag 152", "OrderQty vs CashOrderQty", "xml_test"),
    ("MI-04", "what are the moore filenames for goldman and jpm activity", "both mappings (known gate gap: no identifier tokens)", "recon_assist_file"),
]


def _trunc(text, n=500):
    t = " ".join(str(text or "").split())
    return t[:n] + ("…" if len(t) > n else "")


def main():
    args = [a for a in sys.argv[1:]]
    ask_on = "--chat-only" not in args
    chat_on = "--ask-only" not in args
    cats = {a.upper() for a in args if not a.startswith("--")}

    from core.retrieval.router import run_query_with_method
    from core.chat_engine import chat_turn
    from core.ui_data import collection_stats

    available = [r["name"] for r in collection_stats() if r["chunks"]]
    questions = [q for q in QUESTIONS if not cats or q[0].split("-")[0] in cats]

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = PROJECT_ROOT / "results"
    out_dir.mkdir(exist_ok=True)
    md_path = out_dir / f"eval_run_{ts}.md"
    json_path = out_dir / f"eval_run_{ts}.json"

    records = []
    md = [f"# Eval run {ts}",
          f"questions: {len(questions)} | surfaces: "
          f"{'Ask+Chat' if ask_on and chat_on else 'Ask' if ask_on else 'Chat'}",
          f"collections: {available}", ""]

    t_start = time.time()
    for i, (qid, question, expected, ask_coll) in enumerate(questions, 1):
        print(f"[{i}/{len(questions)}] {qid}: {question}")
        rec = {"id": qid, "question": question, "expected": expected}
        md += [f"## {qid} — {question}", f"*Expected:* {expected}", ""]

        if ask_on and ask_coll:
            try:
                t0 = time.time()
                r = run_query_with_method(ask_coll, question)
                rec["ask"] = {
                    "collection": ask_coll,
                    "method": r.get("method"),
                    "answer": str(r.get("result", "")),
                    "seconds": round(time.time() - t0, 1),
                }
                md += [f"**Ask** [{ask_coll} · {r.get('method')} · {rec['ask']['seconds']}s]",
                       f"> {_trunc(r.get('result'))}", ""]
            except Exception as e:
                rec["ask"] = {"error": f"{type(e).__name__}: {e}"}
                md += [f"**Ask** EXCEPTION: {type(e).__name__}: {e}", ""]
                traceback.print_exc()

        if chat_on:
            try:
                t0 = time.time()
                r = chat_turn(question, [], available)
                rec["chat"] = {
                    "collections_queried": r.get("collections_queried"),
                    "collection": r.get("collection"),
                    "method": r.get("method"),
                    "answer_kind": r.get("answer_kind"),
                    "answer": str(r.get("content", "")),
                    "seconds": round(time.time() - t0, 1),
                }
                md += [f"**Chat** [routed: {r.get('collections_queried')} · "
                       f"answered from: {r.get('collection')} · {r.get('answer_kind')} · "
                       f"{rec['chat']['seconds']}s]",
                       f"> {_trunc(r.get('content'))}", ""]
            except Exception as e:
                rec["chat"] = {"error": f"{type(e).__name__}: {e}"}
                md += [f"**Chat** EXCEPTION: {type(e).__name__}: {e}", ""]
                traceback.print_exc()

        md += ["Verdict — Ask: ☐   Chat: ☐", "", "---", ""]
        records.append(rec)
        # incremental save so a crash/interrupt loses nothing
        md_path.write_text("\n".join(md))
        json_path.write_text(json.dumps(records, indent=1))

    # Latency summary — avg / max per surface + slowest five questions.
    md += ["## Latency summary", ""]
    for surface in ("ask", "chat"):
        secs = [(r["id"], r[surface]["seconds"]) for r in records
                if isinstance(r.get(surface), dict) and "seconds" in r[surface]]
        if not secs:
            continue
        vals = sorted(s for _, s in secs)
        avg = sum(vals) / len(vals)
        p95 = vals[int(len(vals) * 0.95) - 1] if len(vals) > 1 else vals[-1]
        slowest = sorted(secs, key=lambda x: -x[1])[:5]
        md += [f"**{surface.title()}**: avg {avg:.1f}s · p95 {p95:.1f}s · max {vals[-1]:.1f}s",
               "slowest: " + ", ".join(f"{i} ({s:.0f}s)" for i, s in slowest), ""]

    md += [f"\nTotal runtime: {round((time.time() - t_start) / 60, 1)} min"]
    md_path.write_text("\n".join(md))
    print(f"\nDone. Results: {md_path}\nJSON (for run-to-run diffing): {json_path}")


if __name__ == "__main__":
    main()
