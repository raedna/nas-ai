"""
HALO/halo_serializer.py — tickets as DATA, not documents (HALO-03).

One chunk per meaningful item:
  * header chunk  — summary + details; payload carries the ticket's facts
    (team, client, status, categories, opened_by, dates, priority)
  * action chunks — one per KEPT thread action (noise filtered by the same
    config the normalizer uses); payload carries who / action_type /
    action_datetime / ticket_id

Both faces on every chunk: nlp_text feeds retrieval (BM25 + vectors),
payload feeds the metadata SQL path ("who resolved 44539", "how many open
tickets for Moore", "tickets Andrew replied to").

The schema is KNOWN — defined here, saved once under the fixed stem
'halo_ticket' — the first collection whose schema is a fact, not an
inference.
"""
from HALO.halo_normalizer import _cfg, _clean_text

_SCHEMA = {
    "identifier": ["ticket_id"],
    "primary_name": ["summary"],
    "type": ["action_type"],
    "description": ["details"],
    "tags": ["categories"],
    "other": ["team", "client_name", "status", "opened_by", "who",
              "action_datetime", "dateoccurred", "priority"],
}
_schema_saved = set()


def _ensure_schema(collection_name):
    if collection_name in _schema_saved:
        return
    try:
        from core.schema_inference import save_schema_to_db
        save_schema_to_db(_SCHEMA, collection_name, "halo_ticket")
        _schema_saved.add(collection_name)
    except Exception as e:
        print(f"[HALO SERIALIZER] schema save failed: {e}")


def halo_serializer(parsed, file_path, template_config, file_tags, collection_name):
    cfg = _cfg()
    t = parsed["ticket"]
    tid = str(t.get("id"))
    summary = _clean_text(t.get("summary") or f"Ticket {tid}")
    details = _clean_text(t.get("details") or "", cfg["boilerplate_prefixes"])
    categories = [c for c in (t.get(f"category_{i}") for i in range(1, 5)) if c]

    # Names over ids wherever the data offers them: 'resolved'/'critical'
    # are what questions say; ids can never ground. status_name is injected
    # by the fetcher (from /api/Status); config halo.status_map bridges
    # pilot files without API access.
    _status = t.get("status_name")
    if not _status:
        try:
            from core.system_config import load_system_config
            _status = (load_system_config().get("halo", {})
                       .get("status_map", {}).get(str(t.get("status_id"))))
        except Exception:
            _status = None
    _prio = (t.get("priority") or {}).get("name") if isinstance(
        t.get("priority"), dict) else None
    base = {
        "ticket_id": tid,
        "summary": summary,
        "team": t.get("team"),
        "client_name": t.get("client_name"),
        "status": str(_status or t.get("status_id")),
        "priority": str(_prio or t.get("priority_id")),
        "dateoccurred": str(t.get("dateoccurred") or "")[:16],
        "categories": categories,
        "doc_type": "ticket",
        "source_type": "halo",
        "source_file": f"halo_ticket_{tid}.json",
    }

    items = []

    # ---- header chunk --------------------------------------------------
    header_text = (f"Ticket {tid}: {summary}\n\n"
                   f"Team: {base['team']} | Client: {base['client_name']} "
                   f"| Opened: {base['dateoccurred'][:10]} by {t.get('user_name')}\n"
                   + (f"Categories: {' / '.join(categories)}\n" if categories else "")
                   + f"\n{details}")
    items.append({
        "text": header_text,
        **base,
        "identifier": tid,
        "identifier_field": "ticket_id",
        "identifier_namespace": "ticket",
        "identifier_kind": "source",
        "primary_name": summary,
        "description": details,
        "opened_by": t.get("user_name"),
        "link_keys": [f"ticket:{tid}"],
        "related_link_keys": [],
        **(file_tags or {}),
    })

    # ---- action chunks --------------------------------------------------
    kept = 0
    for act in sorted(parsed.get("actions", []),
                      key=lambda x: str(x.get("datetime") or "")):
        outcome = str(act.get("outcome") or "").strip()
        who = str(act.get("who") or "").strip()
        note = _clean_text(act.get("note") or "", cfg["boilerplate_prefixes"])
        if not note:
            continue
        if outcome.lower() in cfg["noise_outcomes"]:
            continue
        if who.lower() in cfg["noise_authors"]:
            continue
        if details and (note == details or note[:200] == details[:200]):
            continue
        kept += 1
        aid = f"{tid}-a{act.get('id')}"
        when = str(act.get("datetime") or "")[:16]
        items.append({
            "text": (f"Ticket {tid} ({summary}) — {outcome} by {who} ({when}):\n\n"
                     f"{note}"),
            **base,
            "identifier": aid,
            "identifier_field": "action_id",
            "identifier_namespace": "ticket_action",
            "identifier_kind": "source",
            "primary_name": f"{summary} — {outcome} ({who})",
            "description": note,
            "who": who,
            "action_type": outcome,
            "action_datetime": when,
            "link_keys": [f"ticket:{tid}", f"ticket_action:{aid}"],
            "related_link_keys": [f"ticket:{tid}"],
            **(file_tags or {}),
        })

    # ---- images on the header chunk --------------------------------------
    imgs = parsed.get("images") or []
    if imgs:
        items[0]["embedded_image_paths"] = [i.get("path") for i in imgs]
        items[0]["embedded_image_targets"] = [i.get("name") for i in imgs]
        items[0]["has_embedded_image_ocr"] = False

    _ensure_schema(collection_name)
    print(f"[HALO SERIALIZER] ticket {tid} -> 1 header + {kept} action chunks")
    return items
