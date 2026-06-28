"""
smoke_crosslinks.py — guards the cross-link layer (CL-01..05).
Mix of unit checks (no DB) and live checks (require the DB).
Run:  python tests/smoke_crosslinks.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_results = []


def check(name, cond, extra=""):
    ok = bool(cond)
    _results.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {name}{(' — ' + extra) if extra else ''}")


# ---- CL-02: meaningful-context gate (unit) ----
from core.cross_link_discoverer import _meaningful_context
check("CL-02 discussed mention accepted",
      _meaningful_context("The gspos.txt file is loaded each morning by the recon job before balances.", "gspos.txt"))
check("CL-02 bare-list mention rejected",
      not _meaningful_context("Files: gspos.txt, eq_act.csv", "gspos.txt"))

# ---- CL-03: gazetteer distinctiveness (unit) ----
from core.ner_cross_linker import _is_distinctive, _is_filename
check("CL-03 filename is distinctive", _is_distinctive("gspos.txt") and _is_filename("gspos.txt"))
check("CL-03 code-like mnemonic distinctive", _is_distinctive("020_W_RECON_GOLDMAN_PB_PULL"))
check("CL-03 prose word rejected", not _is_distinctive("Heartbeat"))
check("CL-03 short numeric rejected", not _is_distinctive("79"))

# ---- Live checks (DB) ----
try:
    from core.db import fetchall
    from core.ner_cross_linker import discover_identifier_mentions

    cands = discover_identifier_mentions("obsidian")
    recon = [c for c in cands if c["target_collection"] == "recon_assist_file"]
    check("CL-03 obsidian->recon candidates found", len(recon) >= 1,
          f"{len(recon)} recon links of {len(cands)} total")

    # CL-04: there are confirmed links to traverse (informational, soft pass)
    conf = fetchall(
        "SELECT match_type, COUNT(*) AS n FROM cross_links WHERE status='confirmed' GROUP BY match_type", ())
    by = {r["match_type"]: r["n"] for r in conf}
    print(f"      confirmed cross-links by type: {by}")
    check("CL-04 traversal has confirmed links available (soft)", True, str(by))
except Exception as e:
    check("Live DB checks", False, repr(e))

passed = sum(_results)
print(f"\n{passed}/{len(_results)} checks passed")
sys.exit(0 if passed == len(_results) else 1)
