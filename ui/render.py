"""
ui/render.py — shared answer rendering for the NiceGUI app.

Renders an answer string with embedded images inline at their marker positions and
OCR text in collapsible expanders. Unlike the Streamlit version this controls the DOM
directly, so images render reliably (no marker-vs-payload plumbing gap).
"""
import re
from pathlib import Path

from nicegui import ui

_MARKER = re.compile(r"\[Embedded image OCR from:\s*([^\]]+)\]")


def build_image_items(payload):
    """Turn an answer payload's embedded-image fields into [{path, caption, ocr}]."""
    if not payload:
        return []
    ocr_map = {e.get("image_target"): e.get("ocr_text", "")
               for e in (payload.get("embedded_image_ocr_map") or [])}
    targets = payload.get("embedded_image_targets") or []
    paths = payload.get("embedded_image_paths") or []
    items = []
    for i, p in enumerate(paths):
        tgt = targets[i] if i < len(targets) else None
        items.append({
            "path": p,
            "caption": tgt or (Path(p).name if p else ""),
            "ocr": ocr_map.get(tgt, ""),
        })
    return items


def render_answer(container, text, image_items=None, show_ocr=True):
    """Render `text` into `container`, inlining images at their markers."""
    container.clear()
    text = str(text or "")
    image_items = image_items or []
    by_name = {Path(it["caption"]).name.lower(): it for it in image_items if it.get("caption")}

    with container:
        if not text.strip():
            ui.label("No answer found.").classes("text-gray-500")
            return

        pos = 0
        found = False
        for m in _MARKER.finditer(text):
            found = True
            before = text[pos:m.start()].strip()
            if before:
                ui.markdown(before)

            name = m.group(1).strip()
            it = by_name.get(Path(name).name.lower())
            if it and it.get("path") and Path(it["path"]).exists():
                ui.image(it["path"]).classes("max-w-2xl rounded border my-2")
                if it.get("caption"):
                    ui.label(it["caption"]).classes("text-xs text-gray-500")
                ocr = (it.get("ocr") or "").strip()
                if show_ocr and ocr:
                    with ui.expansion(f"OCR / extracted text: {it['caption']}").classes("w-full"):
                        ui.label(ocr).classes("whitespace-pre-wrap text-sm")
            else:
                ui.label(f"[image: {name}]").classes("text-xs text-gray-400")

            pos = m.end()
            # Skip the raw OCR text that often follows the marker (we show it cleanly above)
            known = (it.get("ocr") if it else "") or ""
            known = known.strip()
            if known:
                rem = text[pos:]
                idx = rem.find(known[:80])
                if idx != -1:
                    pos = pos + idx + len(known)

        if not found:
            ui.markdown(text)
        else:
            tail = text[pos:].strip()
            if tail:
                ui.markdown(tail)
