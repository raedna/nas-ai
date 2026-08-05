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
from HALO.halo_normalizer import _cfg, _clean_text, _prune_quoted

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
    details = _clean_text(t.get("details") or "", cfg["boilerplate_prefixes"],
                          cfg.get("boilerplate_truncate", ()))
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
        # singular category = first entry — feeds the concept-vector group
        # chooser (rank 4) the way obsidian's folder category does
        "category": (categories[0] if categories else None),
        "doc_type": "ticket",
        "source_type": "halo",
        "source_file": f"halo_ticket_{tid}.json",
    }

    items = []

    # ---- merge relationships (mined BEFORE the noise filter drops the
    # system actions that carry them) --------------------------------------
    import re as _re_m
    merged_in = []
    _pat = _re_m.compile(cfg.get("merge_id_pattern", r"ticket\s*id:?\s*(\d+)"),
                         _re_m.IGNORECASE)
    for act in parsed.get("actions", []):
        if str(act.get("outcome") or "").strip().lower() in cfg.get(
                "merge_outcomes", []):
            for mid in _pat.findall(str(act.get("note") or "")):
                if mid not in merged_in:
                    merged_in.append(mid)
    _mi_raw = t.get("merged_into_id")
    merged_into = (str(_mi_raw) if _mi_raw and str(_mi_raw) not in ("0", tid)
                   else None)
    if merged_in or merged_into:
        base["merged_tickets"] = merged_in
        if merged_into:
            base["merged_into"] = merged_into
        print(f"[HALO SERIALIZER] ticket {tid} merges: in={merged_in} "
              f"into={merged_into}")

    # ---- parent/child (user decision 2026-08-02; fields resolved by the
    # fetcher, no network here) ---------------------------------------------
    _par_raw = t.get("parent_id")
    parent_ticket = (str(_par_raw) if _par_raw
                     and str(_par_raw) not in ("0", tid) else None)
    child_tickets = [str(c) for c in (parsed.get("child_ticket_ids") or [])]
    if parent_ticket:
        base["parent_ticket"] = parent_ticket
    if child_tickets:
        base["child_tickets"] = child_tickets
    if parent_ticket or child_tickets:
        print(f"[HALO SERIALIZER] ticket {tid} family: parent="
              f"{parent_ticket} children={child_tickets}")

    # ---- header chunk --------------------------------------------------
    header_text = (f"Ticket {tid}: {summary}\n\n"
                   f"Team: {base['team']} | Client: {base['client_name']} "
                   f"| Opened: {base['dateoccurred'][:10]} by {t.get('user_name')}\n"
                   + (f"Categories: {' / '.join(categories)}\n" if categories else "")
                   + (f"Merged tickets: {', '.join(merged_in)}\n" if merged_in else "")
                   + (f"Merged into ticket: {merged_into}\n" if merged_into else "")
                   + (f"Parent ticket: {parent_ticket}\n" if parent_ticket else "")
                   + (f"Child tickets: {', '.join(child_tickets)}\n" if child_tickets else "")
                   + f"\n{details}")
    items.append({
        "text": header_text,
        **base,
        "identifier": tid,
        "identifier_field": "ticket_id",
        "identifier_namespace": "ticket",
        "identifier_kind": "source",
        "primary_name": summary,
        # Merge relationships ride on the DESCRIPTION so every answer that
        # shows the ticket mentions them — users won't ask "what was
        # merged"; the ticket should say it when selected.
        "description": (details
                        + (f"\n\nMerged tickets: {', '.join(merged_in)}"
                           if merged_in else "")
                        + (f"\nMerged into ticket: {merged_into}"
                           if merged_into else "")
                        + (f"\nParent ticket: {parent_ticket}"
                           if parent_ticket else "")
                        + (f"\nChild tickets: {', '.join(child_tickets)}"
                           if child_tickets else "")),
        "opened_by": t.get("user_name"),
        "link_keys": [f"ticket:{tid}"],
        # merged tickets become link edges when/if those tickets are ingested
        "related_link_keys": ([f"ticket:{m}" for m in merged_in]
                              + ([f"ticket:{merged_into}"] if merged_into
                                 else [])
                              + ([f"ticket:{parent_ticket}"] if parent_ticket
                                 else [])
                              + [f"ticket:{c}" for c in child_tickets]),
        **(file_tags or {}),
    })

    # header is the ISSUE section
    items[0]["section"] = "issue"

    # ---- action chunks --------------------------------------------------
    kept = 0
    _solution_act = None
    _kept_actions = []
    for act in sorted(parsed.get("actions", []),
                      key=lambda x: str(x.get("datetime") or "")):
        outcome = str(act.get("outcome") or "").strip()
        who = str(act.get("who") or "").strip()
        note = _clean_text(act.get("note") or "", cfg["boilerplate_prefixes"],
                           cfg.get("boilerplate_truncate", ()))
        note = _prune_quoted(note, cfg.get("quoted_markers", ()))
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
        # Section role (declared in config halo.resolution_outcomes): the
        # resolution outcome is CLOSURE — usually administrative sign-off,
        # not the fix (user correction 2026-08-02: the actual solution may
        # live in an email or internal note; an LLM extracts it below).
        if outcome.lower() in cfg.get("resolution_outcomes", []):
            section = "closure"
        elif act.get("hiddenfromuser"):
            section = "internal_notes"
        else:
            section = "responses"
        _sec_label = ""
        who_role = cfg.get("who_type_map", {}).get(
            str(act.get("who_type")), "")
        _who_disp = f"{who} ({who_role})" if who_role else who
        items.append({
            "text": (f"Ticket {tid} ({summary}) — {outcome} by {_who_disp} "
                     f"({when}){_sec_label}:\n\n{note}"),
            **base,
            "identifier": aid,
            "identifier_field": "action_id",
            "identifier_namespace": "ticket_action",
            "identifier_kind": "source",
            "primary_name": f"{summary} — {outcome} ({who})"
                            + _sec_label,
            "description": note,
            "who": who,
            "who_role": who_role,
            "action_type": outcome,
            "action_datetime": when,
            "section": section,
            "link_keys": [f"ticket:{tid}", f"ticket_action:{aid}"],
            "related_link_keys": [f"ticket:{tid}"],
            **(file_tags or {}),
        })
        if section == "closure":
            _solution_act = (who, when, note)
        _kept_actions.append({"id": act.get("id"), "who": who,
                              "when": when, "outcome": outcome,
                              "section": section, "note": note})

    # ---- LLM solution extraction (user decision 2026-08-02) ---------------
    # The closure action is sign-off, not the fix — the actual solution may
    # live in an email or internal note. One LLM call per ticket, whole
    # thread in view, quoting the fix and NAMING its source actions
    # (auditable; source-anchored feedback can then penalize exactly this
    # chunk if the extraction is ever wrong). has_solution=false or any
    # failure -> no solution chunk, no invention. Config switch:
    # halo.solution_extraction (default on).
    _sol = None
    try:
        try:
            from core.system_config import load_system_config as _lsc_sol
            _sol_on = bool(_lsc_sol().get("halo", {})
                           .get("solution_extraction", True))
        except Exception:
            _sol_on = True
        if _sol_on and _kept_actions:
            from core.local_llm_client import call_local_llm_json
            _thread = "\n\n".join(
                f"[action {a['id']} | {a['section']} | {a['outcome']} by "
                f"{a['who']} {a['when']}]\n{a['note'][:1500]}"
                for a in _kept_actions)
            _sys_sol = (
                "You are reading a support ticket. Determine whether the "
                "thread contains an ACTUAL SOLUTION or fix for the reported "
                "issue — not an acknowledgement, escalation or closing "
                "note. Return STRICT JSON: {\"has_solution\": true/false, "
                "\"solution\": \"1-3 sentences stating the fix, quoting "
                "the thread where possible\", \"source_action_ids\": "
                "[ids of the actions the solution comes from]}. If the "
                "ticket was closed without a stated fix, has_solution is "
                "false.")
            _usr_sol = (f"Ticket {tid}: {summary}\n\nIssue:\n"
                        f"{details[:2000]}\n\nThread:\n{_thread}")
            _out_sol = call_local_llm_json(_sys_sol, _usr_sol,
                                           temperature=0.0)
            if (isinstance(_out_sol, dict) and _out_sol.get("has_solution")
                    and str(_out_sol.get("solution") or "").strip()):
                _sol = {
                    "text": str(_out_sol["solution"]).strip(),
                    "sources": [str(x) for x in
                                (_out_sol.get("source_action_ids") or [])],
                }
                print(f"[HALO SERIALIZER] ticket {tid} solution extracted "
                      f"(sources: {_sol['sources']})")
    except Exception as _se:
        print(f"[HALO SERIALIZER] solution extraction skipped: {_se}")
    if _sol:
        _sol_txt = (f"Ticket {tid} ({summary}) — Solution:\n\n"
                    f"{_sol['text']}")
        items.append({
            "text": _sol_txt,
            **base,
            "identifier": f"{tid}-solution",
            "identifier_field": "action_id",
            "identifier_namespace": "ticket_solution",
            "identifier_kind": "derived",
            "primary_name": f"{summary} — Solution",
            "description": _sol["text"],
            "section": "solution",
            "solution_sources": _sol["sources"],
            "link_keys": [f"ticket:{tid}"],
            "related_link_keys": [f"ticket:{tid}"] + [
                f"ticket_action:{tid}-a{x}" for x in _sol["sources"]],
            **(file_tags or {}),
        })

    # ---- synthetic section chunks (user decision 2026-08-02): status and
    # merges get their own precise retrieval targets --------------------------
    _status_txt = (f"Ticket {tid} ({summary}) — Status: {base['status']}, "
                   f"Priority: {base['priority']}."
                   + (f" Resolved by {_solution_act[0]} on "
                      f"{_solution_act[1]}." if _solution_act else ""))
    items.append({
        "text": _status_txt,
        **base,
        "identifier": f"{tid}-status",
        "identifier_field": "action_id",
        "identifier_namespace": "ticket_status",
        "identifier_kind": "derived",
        "primary_name": f"{summary} — Status",
        "description": _status_txt,
        "section": "status",
        "link_keys": [f"ticket:{tid}"],
        "related_link_keys": [f"ticket:{tid}"],
        **(file_tags or {}),
    })
    if merged_in or merged_into:
        _merge_txt = (f"Ticket {tid} ({summary}) — Merged tickets:"
                      + (f" {', '.join(merged_in)} were merged into this "
                         f"ticket." if merged_in else "")
                      + (f" This ticket was merged into {merged_into}."
                         if merged_into else ""))
        items.append({
            "text": _merge_txt,
            **base,
            "identifier": f"{tid}-merges",
            "identifier_field": "action_id",
            "identifier_namespace": "ticket_merges",
            "identifier_kind": "derived",
            "primary_name": f"{summary} — Merged tickets",
            "description": _merge_txt,
            "section": "merges",
            "link_keys": [f"ticket:{tid}"],
            "related_link_keys": ([f"ticket:{m}" for m in merged_in]
                                  + ([f"ticket:{merged_into}"]
                                     if merged_into else [])),
            **(file_tags or {}),
        })

    # ---- images on the header chunk --------------------------------------
    imgs = parsed.get("images") or []
    if imgs:
        items[0]["embedded_image_paths"] = [i.get("path") for i in imgs]
        items[0]["embedded_image_targets"] = [i.get("name") for i in imgs]
        items[0]["has_embedded_image_ocr"] = False
        # The renderer inlines images at [image: name] MARKERS in the text —
        # paths in the payload alone display nothing. Same convention as the
        # doc pipeline.
        items[0]["text"] += "\n\n" + "\n".join(
            f"[image: {i.get('name')}]" for i in imgs if i.get("name"))
        # OCR (user decision 2026-08-03): screenshots in the originating
        # request often carry the actual error text — run the doc
        # pipeline's OCR over the downloaded assets and make it
        # SEARCHABLE on the header. Config switch halo.image_ocr.
        try:
            from core.system_config import load_system_config as _lsc_ocr
            _ocr_on = bool(_lsc_ocr().get("halo", {}).get("image_ocr", True))
        except Exception:
            _ocr_on = True
        if _ocr_on:
            try:
                from PIL import Image as _PILImage
                from IMAGES.image_parser import _run_ocr
                _ocr_map = {}
                for i in imgs:
                    _pth, _nm = i.get("path"), i.get("name")
                    if not (_pth and _nm):
                        continue
                    try:
                        _txt_ocr = _run_ocr(_PILImage.open(_pth),
                                            enable_ocr=True).strip()
                    except Exception:
                        _txt_ocr = ""
                    if _txt_ocr:
                        _ocr_map[_nm] = _txt_ocr[:2000]
                if _ocr_map:
                    # doc-pipeline convention: LIST of {image_target,
                    # ocr_text} dicts (the dict form crashed the renderer,
                    # 2026-08-03)
                    items[0]["embedded_image_ocr_map"] = [
                        {"image_target": n, "ocr_text": t}
                        for n, t in _ocr_map.items()]
                    items[0]["has_embedded_image_ocr"] = True
                    items[0]["text"] += "\n\n" + "\n\n".join(
                        f"[Embedded image OCR from: {n}]\n{t}"
                        for n, t in _ocr_map.items())
                    print(f"[HALO SERIALIZER] ticket {tid} OCR: "
                          f"{len(_ocr_map)} image(s), "
                          f"{sum(len(v) for v in _ocr_map.values())} chars")
            except Exception as _oe:
                print(f"[HALO SERIALIZER] OCR skipped: {_oe}")

    _ensure_schema(collection_name)
    print(f"[HALO SERIALIZER] ticket {tid} -> {len(items)} chunks "
          f"(1 issue + {kept} actions + {len(items) - kept - 1} derived)")
    return items
