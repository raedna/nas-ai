"""
convert_pilot_tickets.py — one-time: convert the 3 pilot ticket JSON pairs
to the combined shape in the tickets folder, and remove the pilot markdowns.

Run on the Mac:  python3 convert_pilot_tickets.py /path/to/halo_tickets
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")

SRC = Path("/Users/raednasr/RaedsMacM1/nas-ai/claude/haloitsm_jsons")
OUT = Path(sys.argv[1] if len(sys.argv) > 1 else
           "/Users/raednasr/RaedsMacM1/nas-ai/halo_tickets")
OUT.mkdir(parents=True, exist_ok=True)

for tf in sorted(SRC.glob("halo_ticket_*.json")):
    tid = tf.stem.replace("halo_ticket_", "")
    af = SRC / f"halo_actions_{tid}.json"
    t = json.load(open(tf))
    a = json.load(open(af)) if af.exists() else {}
    actions = a.get("actions", a if isinstance(a, list) else [])
    combined = {"ticket": t, "actions": actions, "images": []}
    out = OUT / f"halo_ticket_{tid}.json"
    json.dump(combined, open(out, "w"), indent=1)
    print(f"wrote {out}")

for md in OUT.glob("halo_ticket_*.md"):
    md.unlink()
    print(f"removed pilot markdown {md.name}")
print("done — force re-ingest halo_tickets in the UI")
