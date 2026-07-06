"""
Schema inference A/B — old pure-LLM vs current guarded pipeline
================================================================
A: llm_infer_schema exactly as of commit ccc50e7 (pre-SCHEMA-01: no filename
   pre-pass, no role constraints, no tie-break, original prompt) — loaded
   straight from git history at runtime, nothing reconstructed by hand.
C: current llm_infer_schema (constraints + cardinality + tie-break).

Each variant runs N times on the REAL recon xlsx (parsed via the real pipeline
parser) and is scored against the known-correct schema. Dry-run: nothing saved.

Usage:
    python diag_schema_ab.py [runs_per_variant=3]
"""

import importlib.util
import json
import subprocess
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

XLSX = "/Volumes/raedsync/Documents/OmniVista/Support Desk/RECON/RECON_Moore-PB Mapping_100225.xlsx"
OLD_COMMIT = "ccc50e7"
RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 3

# Known-correct schema (ground truth, human-verified this session)
EXPECTED = {
    "identifier": {"Moore file name"},
    "primary_name": {"Tidal Job Name"},
    "aliases": {"Prime Broker file name"},
    "reference_identifier": {"Move Script (K:/Recon/FTP)"},
    "type": {"Prime Broker", "Recon Tool Data Source", "Active", "Included in Email Alert"},
}

# ---------------------------------------------------------------- load variant A
_old_src = subprocess.run(
    ["git", "show", f"{OLD_COMMIT}:core/schema_inference.py"],
    cwd=PROJECT_ROOT, capture_output=True, text=True, check=True).stdout
_tmp = Path(tempfile.mkdtemp()) / "old_schema_inference.py"
_tmp.write_text(_old_src)
_spec = importlib.util.spec_from_file_location("old_schema_inference", _tmp)
old_si = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(old_si)

from core.schema_inference import llm_infer_schema as current_llm_infer_schema
from core.schema_inference import load_roles_config
from core.paths import CONFIG_DIR
from TABLES.table_parser import parse_table

roles = load_roles_config(CONFIG_DIR / "structured_roles.json")
rows = parse_table(XLSX)
rows = rows["rows"] if isinstance(rows, dict) else rows
print(f"parsed {len(rows)} rows from real file\n")


def score(schema):
    """Return (n_correct_roles, detail) vs EXPECTED over the 5 core roles."""
    if not schema:
        return 0, "LLM returned None"
    hits, detail = 0, []
    for role, exp in EXPECTED.items():
        got = set(schema.get(role) or [])
        # type: only compare against expected members (other cols may also land there)
        ok = (got & exp == exp) if role == "type" else (got == exp)
        hits += ok
        detail.append(f"{role}:{'OK' if ok else 'X ' + str(sorted(got))}")
    return hits, " | ".join(detail)


VARIANTS = [
    (f"A old pure-LLM ({OLD_COMMIT})", old_si.llm_infer_schema),
    ("C current guarded pipeline", current_llm_infer_schema),
]

results = {}
for name, fn in VARIANTS:
    print("=" * 70)
    print(name)
    scores = []
    for i in range(RUNS):
        try:
            schema = fn(rows, roles)
        except Exception as e:
            print(f"  run {i+1}: EXCEPTION {type(e).__name__}: {e}")
            scores.append(0)
            continue
        s, detail = score(schema)
        scores.append(s)
        print(f"  run {i+1}: {s}/5  {detail}")
    results[name] = scores

print("=" * 70)
print("SUMMARY (correct roles out of 5, per run):")
for name, scores in results.items():
    stable = "stable" if len(set(scores)) == 1 else "UNSTABLE"
    print(f"  {name}: {scores}  ({stable})")
