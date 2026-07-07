"""ui/entry_page.py — standalone full-article page, openable in a new tab.

/entry/{chunk_id} renders the FULL document the chunk belongs to: all sibling
chunks merged (same identifier for entity_row articles, same source_file for
notes), with embedded images inline. Related-section links point here.
"""
import json

from nicegui import ui

from core.db import fetchall
from ui.render import render_answer, build_image_items


def _payload(row):
    p = row["payload"]
    return p if isinstance(p, dict) else json.loads(p)


@ui.page("/entry/{chunk_id}")
def entry_page(chunk_id: str):
    rows = fetchall(
        "SELECT id, collection_name, source_file, identifier, primary_name, doc_type, payload "
        "FROM chunks WHERE id = %s", (chunk_id,))
    if not rows:
        ui.label("Entry not found.").classes("text-red-600 m-4")
        return
    r = rows[0]
    p = _payload(r)
    title = p.get("primary_name") or r["primary_name"] or r["source_file"] or r["identifier"]

    # Structured records are single-row — render just this chunk. entity_row
    # articles (kb) merge by identifier (their source_file is the shared CSV —
    # grouping by it would merge the whole collection). Notes/docs (obsidian,
    # docx, pdf) merge by source_file — one note = one file, chunk identifiers
    # are per-section there.
    dt = str(r["doc_type"] or p.get("doc_type") or "").lower()
    if dt == "structured":
        doc_rows = rows
    elif dt == "entity_row" and r["identifier"]:
        doc_rows = fetchall(
            "SELECT payload FROM chunks WHERE collection_name = %s AND identifier = %s "
            "ORDER BY id LIMIT 100",
            (r["collection_name"], r["identifier"]))
    else:
        doc_rows = fetchall(
            "SELECT payload FROM chunks WHERE collection_name = %s AND source_file = %s "
            "ORDER BY id LIMIT 100",
            (r["collection_name"], r["source_file"]))

    texts, img_paths, img_targets, ocr_map = [], [], [], []
    for dr in doc_rows:
        dp = _payload(dr)
        t = str(dp.get("text") or "").strip()
        if t and t not in texts:
            texts.append(t)
        img_paths += dp.get("embedded_image_paths") or []
        img_targets += dp.get("embedded_image_targets") or []
        ocr_map += dp.get("embedded_image_ocr_map") or []

    merged = dict(p)
    merged["embedded_image_paths"] = img_paths
    merged["embedded_image_targets"] = img_targets
    merged["embedded_image_ocr_map"] = ocr_map

    with ui.column().classes("w-full max-w-4xl mx-auto p-4"):
        ui.label(str(title)).classes("text-xl font-bold")
        ui.label(f"{r['collection_name']} · {r['source_file'] or ''}").classes(
            "text-xs text-gray-500")
        box = ui.column().classes("w-full")
        render_answer(box, "\n\n".join(texts), build_image_items(merged), show_ocr=True)
