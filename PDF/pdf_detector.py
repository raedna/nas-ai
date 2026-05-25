from collections import Counter
import json
import re

from core.paths import CONFIG_DIR

DEBUG = True

def load_pdf_detection_hints():
    path = CONFIG_DIR / "pdf_detection_hints.json"

    defaults = {
        "mode": {
            "scanned_text_len_min": 80,
            "scanned_avg_chars_per_page_min": 40
        },
        "doc_type": {
            "procedural_keywords": [],
            "reference_keywords": [],
            "thresholds": {}
        }
    }

    if not path.exists():
        return defaults

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        mode = {
            **defaults["mode"],
            **data.get("mode", {})
        }

        doc_type = {
            **defaults["doc_type"],
            **data.get("doc_type", {})
        }

        doc_type["thresholds"] = {
            **defaults["doc_type"].get("thresholds", {}),
            **data.get("doc_type", {}).get("thresholds", {})
        }

        return {
            "mode": mode,
            "doc_type": doc_type
        }

    except Exception:
        return defaults

def _count_block_types(blocks):
    counts = Counter()
    for block in blocks or []:
        counts[block.get("block_type", "unknown")] += 1
    return counts


def detect_pdf_mode(text, page_count=0, template_config=None):
    text = str(text or "").strip()
    text_len = len(text)
    avg_chars_per_page = text_len / max(page_count, 1)

    hints = load_pdf_detection_hints()
    mode_cfg = hints.get("mode", {})

    if (
        text_len < mode_cfg.get("scanned_text_len_min", 80)
        or avg_chars_per_page < mode_cfg.get("scanned_avg_chars_per_page_min", 40)
    ):
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

def _contains_any(text, keywords):
    t = str(text or "").lower()
    return any(str(k or "").lower() in t for k in keywords if str(k or "").strip())

def detect_pdf_doc_type(text, blocks=None, pdf_mode="readable_pdf", template_config=None):
    blocks = blocks or []
    counts = _count_block_types(blocks)

    hints = load_pdf_detection_hints()
    doc_cfg = hints.get("doc_type", {})
    thresholds = doc_cfg.get("thresholds", {})

    procedural_keywords = doc_cfg.get("procedural_keywords", [])
    reference_keywords = doc_cfg.get("reference_keywords", [])

    procedural_score = 0
    reference_score = 0

    if _contains_any(text, procedural_keywords):
        procedural_score += thresholds.get("procedural_keyword_score", 3)

    if _contains_any(text, reference_keywords):
        reference_score += thresholds.get("reference_keyword_score", 4)

    if pdf_mode == "readable_pdf":
        heading_count = counts.get("heading", 0)
        bullet_count = counts.get("bullet", 0)
        paragraph_count = counts.get("paragraph", 0)

        if not heading_count and not bullet_count and not paragraph_count:
            return "narrative"

        if bullet_count >= thresholds.get("readable_bullet_procedural_min", 3):
            procedural_score += thresholds.get("bullet_procedural_score", 3)

        if heading_count >= thresholds.get("readable_heading_reference_min", 2):
            reference_score += thresholds.get("heading_reference_score", 2)

        if paragraph_count >= thresholds.get("readable_paragraph_reference_min", 3):
            reference_score += thresholds.get("paragraph_reference_score", 2)

    else:
        signals = _ocr_layout_signals(blocks)

        if signals["bullet_like_count"] >= thresholds.get("scanned_bullet_procedural_min", 4):
            procedural_score += thresholds.get("bullet_procedural_score", 3)

        if signals["short_upper_count"] >= thresholds.get("scanned_short_upper_reference_min", 2):
            reference_score += thresholds.get("short_upper_reference_score", 2)

        if signals["dense_paragraph_count"] >= thresholds.get("scanned_dense_paragraph_reference_min", 3):
            reference_score += thresholds.get("dense_paragraph_reference_score", 2)

    if reference_score > procedural_score:
        return "reference"

    if procedural_score > reference_score:
        return "procedural"

    return "narrative"