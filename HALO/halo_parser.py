"""
HALO/halo_parser.py — parser for combined Halo ticket JSON files.

One file per ticket (the fetcher writes it): {"ticket": {...}, "actions":
[...], "images": [{"path","name"}...]}. The file is the sync/hash unit —
an updated ticket rewrites its file and re-ingests alone.
"""
import json
from pathlib import Path


def parse_halo(file_path, template_config=None):
    p = Path(file_path)
    try:
        data = json.load(open(p))
    except Exception as e:
        print(f"[HALO PARSER] {p.name}: unreadable JSON ({e})")
        return None
    ticket = data.get("ticket")
    if not isinstance(ticket, dict) or ticket.get("id") is None:
        print(f"[HALO PARSER] {p.name}: no ticket object — skipped")
        return None
    actions = data.get("actions") or []
    if isinstance(actions, dict):        # raw API shape tolerated
        actions = actions.get("actions", [])
    images = data.get("images") or []
    print(f"[HALO PARSER] ticket {ticket.get('id')}: "
          f"{len(actions)} actions, {len(images)} images")
    return {"ticket": ticket, "actions": actions, "images": images}
