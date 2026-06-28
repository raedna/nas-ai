"""
core/chunking.py
================
Char-aware text splitter for ingestion (P0: prevent silent embedding truncation).

The embed model (bge-large) ignores everything past ~2,500 chars. This splits long
text into windows that fit comfortably under that cap, breaking on paragraph then
sentence boundaries, with a small overlap so context isn't lost at the seams.

Single entry point: split_text(text) -> list[str].
A short text returns a single-element list (callers can treat 1 chunk as "no split").
"""
import re

# Target body size per chunk. Cap is ~2,500 chars; leave headroom for a repeated
# title (added by the caller) plus overlap so the final embedded text stays < cap.
DEFAULT_MAX_CHARS = 1800
DEFAULT_OVERLAP = 150

_SENTENCE_SPLIT = re.compile(r'(?<=[.!?])\s+')
_PARA_SPLIT = re.compile(r'\n{2,}')


def _sentence_pack(paragraph, budget):
    """Split an oversized paragraph into <=budget pieces on sentence boundaries."""
    pieces, buf = [], ""
    for sent in _SENTENCE_SPLIT.split(paragraph):
        sent = sent.strip()
        if not sent:
            continue
        if len(sent) > budget:                      # pathological single sentence
            if buf:
                pieces.append(buf); buf = ""
            for i in range(0, len(sent), budget):
                pieces.append(sent[i:i + budget])
            continue
        if len(buf) + len(sent) + 1 <= budget:
            buf = (buf + " " + sent).strip()
        else:
            if buf:
                pieces.append(buf)
            buf = sent
    if buf:
        pieces.append(buf)
    return pieces


def split_text(text, max_chars=DEFAULT_MAX_CHARS, overlap=DEFAULT_OVERLAP):
    """Return a list of text chunks, each <= max_chars (plus ~overlap from the
    previous chunk). Short text -> [text]. Empty -> []."""
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]

    # 1. break into segments no larger than the budget
    segments = []
    for para in _PARA_SPLIT.split(text):
        para = para.strip()
        if not para:
            continue
        if len(para) <= max_chars:
            segments.append(para)
        else:
            segments.extend(_sentence_pack(para, max_chars))

    # 2. greedily pack segments into chunks
    chunks, buf = [], ""
    for seg in segments:
        if buf and len(buf) + len(seg) + 2 > max_chars:
            chunks.append(buf)
            buf = seg
        else:
            buf = (buf + "\n\n" + seg).strip() if buf else seg
    if buf:
        chunks.append(buf)

    if len(chunks) <= 1:
        return chunks or [text]

    # 3. prepend a small overlap from the tail of the previous chunk
    if overlap:
        out = [chunks[0]]
        for i in range(1, len(chunks)):
            tail = chunks[i - 1][-overlap:].strip()
            out.append((tail + " … " + chunks[i]).strip())
        return out
    return chunks


def split_oversized_chunks(items, text_key="text",
                           max_chars=DEFAULT_MAX_CHARS, overlap=DEFAULT_OVERLAP):
    """P0b: post-process a list of chunk dicts (doc/PDF serializer output) and split
    any whose embedded text exceeds the cap into multiple sub-chunks. Sub-chunks keep
    the same identifier/payload (storage id is seq-based, so they don't collide) and
    repeat the title/heading so each embeds self-contained. Short chunks pass through."""
    out = []
    for item in items or []:
        # Keep chunks with embedded images intact: splitting would separate the
        # "[Embedded image OCR from: ...]" marker from its image payload and break
        # inline rendering. Full OCR is still preserved in embedded_image_ocr_map.
        if item.get("embedded_image_paths") or item.get("embedded_image_ocr_map"):
            out.append(item)
            continue
        parts = split_text(item.get(text_key) or "", max_chars=max_chars, overlap=overlap)
        if len(parts) <= 1:
            out.append(item)
            continue
        title = (item.get("primary_name") or item.get("section_heading") or "").strip()
        for i, body in enumerate(parts):
            if title and not body.lstrip().startswith(title[:20]):
                body = f"{title}\n\n{body}"
            d = dict(item)
            d[text_key] = body
            d["chunk_part"] = i + 1
            d["chunk_part_total"] = len(parts)
            out.append(d)
    return out
