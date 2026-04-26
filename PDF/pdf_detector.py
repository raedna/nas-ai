from collections import Counter
import re

DEBUG = True


def _count_block_types(blocks):
    counts = Counter()
    for block in blocks or []:
        counts[block.get("block_type", "unknown")] += 1
    return counts


def detect_pdf_mode(text, page_count=0, template_config=None):
    text = str(text or "").strip()
    text_len = len(text)
    avg_chars_per_page = text_len / max(page_count, 1)

    if text_len < 80 or avg_chars_per_page < 40:
        return "scanned_pdf"

    return "readable_pdf"


def _ocr_layout_signals(blocks):
    page_ocr_blocks = [b for b in (blocks or []) if b.get("block_type") == "page_ocr"]

    if not page_ocr_blocks:
        return {
            "line_count": 0,
            "bullet_like_count": 0,
            "short_upper_count": 0,
            "dense_paragraph_count": 0,
        }

    bullet_like_count = 0
    short_upper_count = 0
    dense_paragraph_count = 0
    line_count = 0

    for block in page_ocr_blocks:
        text = str(block.get("text") or "")
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        line_count += len(lines)

        for ln in lines:
            if re.match(r"^[-*•]\s+", ln) or re.match(r"^\d+[\.\)]\s+", ln):
                bullet_like_count += 1

            if len(ln) <= 80 and ln.upper() == ln and any(c.isalpha() for c in ln):
                short_upper_count += 1

            if len(ln) > 100:
                dense_paragraph_count += 1

    return {
        "line_count": line_count,
        "bullet_like_count": bullet_like_count,
        "short_upper_count": short_upper_count,
        "dense_paragraph_count": dense_paragraph_count,
    }


def detect_pdf_doc_type(text, blocks=None, pdf_mode="readable_pdf", template_config=None):
    blocks = blocks or []
    counts = _count_block_types(blocks)

    # readable pdf path
    if pdf_mode == "readable_pdf":
        heading_count = counts.get("heading", 0)
        bullet_count = counts.get("bullet", 0)
        paragraph_count = counts.get("paragraph", 0)

        if not heading_count and not bullet_count and not paragraph_count:
            return "narrative"

        if bullet_count >= 3 and bullet_count >= paragraph_count:
            return "procedural"

        if heading_count >= 2 and paragraph_count >= 2:
            if bullet_count >= 2:
                return "mixed"
            return "reference"

        if paragraph_count >= 3:
            return "reference"

        return "narrative"

    # scanned pdf path
    signals = _ocr_layout_signals(blocks)

    if signals["bullet_like_count"] >= 4:
        return "procedural"

    if signals["short_upper_count"] >= 2 and signals["dense_paragraph_count"] >= 2:
        return "reference"

    if signals["dense_paragraph_count"] >= 3:
        return "reference"

    return "narrative"