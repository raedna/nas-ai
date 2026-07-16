"""
HALO/halo_normalizer.py — Halo ITSM ticket JSON -> clean searchable document.

Pilot shape: the normalizer writes a markdown file per ticket into a folder
that a `halo_tickets` collection points at — the EXISTING doc ingestion
pipeline (chunking, embedding, vocab, cross-links) handles it from there.
No new pipeline stages; a ticket becomes a document like any obsidian note.

Curation rules (config system.json -> "halo"):
- noise_outcomes: action outcomes that are process noise (Rule Applied,
  Re-Assign, ...) — dropped from the thread.
- noise_authors: system authors (System, Automation) — dropped.
Both lists are config, seeded with defaults observed in real exports.
"""
import json
import re
from pathlib import Path


def _cfg():
    try:
        from core.system_config import load_system_config
        c = load_system_config().get("halo", {}) or {}
    except Exception:
        c = {}
    return {
        "noise_outcomes": [str(x).lower() for x in c.get("noise_outcomes", [
            "rule applied", "re-assign", "assigned to support desk",
            "sla hold", "status change", "change priority", "triaged",
            "took ownership", "responded", "other ticket merged",
            "assign to mcm team"])],
        "noise_authors": [str(x).lower() for x in c.get("noise_authors", [
            "system", "automation"])],
        # Lines STARTING with any of these are dropped wherever they occur —
        # email boilerplate repeated in every external message.
        "boilerplate_prefixes": [str(x) for x in c.get("boilerplate_prefixes", [
            "CAUTION: This email originated",
            "OmniVista Solutions Inc. Disclaimer",
            "If this ticket has been resolved in error",
        ])],
        # TRUNCATE markers: disclaimers are email TAILS, often glued mid-line
        # ("Many thanks ... Chris *** Moore Europe Legal Disclaimer...") —
        # everything FROM the marker TO THE END of the text is cut.
        "boilerplate_truncate": [str(x) for x in c.get("boilerplate_truncate", [
            "*** Moore Europe Legal Disclaimer",
        ])],
    }


def _clean_text(t: str, boilerplate_prefixes=(), truncate_markers=()) -> str:
    t = str(t or "")
    t = re.sub(r"\r\n?", "\n", t)
    for _m in truncate_markers or ():
        _i = t.find(_m)
        if _i != -1:
            t = t[:_i]
    if boilerplate_prefixes:
        kept = []
        for ln in t.split("\n"):
            if any(ln.strip().startswith(bp) for bp in boilerplate_prefixes):
                continue
            kept.append(ln)
        t = "\n".join(kept)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def normalize_halo_ticket(ticket_json_path, actions_json_path=None):
    """Returns {"identifier", "title", "text", "meta"} for one ticket."""
    cfg = _cfg()
    t = json.load(open(ticket_json_path))
    actions = []
    if actions_json_path and Path(actions_json_path).exists():
        a = json.load(open(actions_json_path))
        actions = a.get("actions", []) if isinstance(a, dict) else a

    tid = str(t.get("id"))
    summary = _clean_text(t.get("summary") or f"Ticket {tid}")
    details = _clean_text(t.get("details") or "",
                          cfg["boilerplate_prefixes"])

    meta = {
        "ticket_id": tid,
        "status_id": t.get("status_id"),
        "team": t.get("team"),
        "client_name": t.get("client_name"),
        "user_name": t.get("user_name"),
        "priority_id": t.get("priority_id"),
        "dateoccurred": t.get("dateoccurred"),
        "categories": [c for c in (t.get(f"category_{i}") for i in range(1, 5)) if c],
    }

    # Thread: human actions, chronological, deduped against the body
    kept = []
    for act in sorted(actions, key=lambda x: str(x.get("datetime") or "")):
        outcome = str(act.get("outcome") or "").strip()
        who = str(act.get("who") or "").strip()
        note = _clean_text(act.get("note") or "",
                           cfg["boilerplate_prefixes"])
        if not note:
            continue
        if outcome.lower() in cfg["noise_outcomes"]:
            continue
        if who.lower() in cfg["noise_authors"]:
            continue
        # the first email is routinely a copy of the ticket details
        if details and (note == details or note[:200] == details[:200]):
            continue
        kept.append(f"**{outcome}** — {who} ({str(act.get('datetime',''))[:16]}):\n{note}")

    lines = [f"# {summary}", ""]
    lines.append(f"Ticket: {tid} | Team: {meta['team']} | Client: {meta['client_name']} "
                 f"| Opened: {str(meta['dateoccurred'])[:10]}")
    if meta["categories"]:
        lines.append("Categories: " + " / ".join(str(c) for c in meta["categories"]))
    lines += ["", details]
    if kept:
        lines += ["", "---", "## Ticket thread", ""] + kept
    return {"identifier": tid, "title": summary,
            "text": "\n".join(lines).strip(), "meta": meta}


def write_ticket_markdown(ticket_json_path, actions_json_path, out_dir):
    """Normalize and write halo_ticket_<id>.md into out_dir (the folder the
    halo_tickets collection ingests from). Returns the written path."""
    doc = normalize_halo_ticket(ticket_json_path, actions_json_path)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    p = out / f"halo_ticket_{doc['identifier']}.md"
    p.write_text(doc["text"], encoding="utf-8")
    print(f"[HALO] wrote {p} ({len(doc['text'])} chars)")
    return str(p)
