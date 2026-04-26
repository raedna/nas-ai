from __future__ import annotations

import re
from typing import Any, Dict, Optional


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _tokenize(text: str) -> list[str]:
    return re.findall(r"\b\w+\b", text.lower())


def _nonempty_lines(text: str) -> list[str]:
    if not text:
        return []
    return [line.strip() for line in text.splitlines() if line.strip()]


def _signal_stats(text: str) -> Dict[str, Any]:
    lines = _nonempty_lines(text)
    tokens = _tokenize(text)

    words = len(tokens)
    line_count = len(lines)

    words_per_line = []
    short_lines = 0
    medium_lines = 0
    long_lines = 0

    for line in lines:
        wc = len(_tokenize(line))
        if wc > 0:
            words_per_line.append(wc)
        if wc <= 4:
            short_lines += 1
        elif wc <= 10:
            medium_lines += 1
        else:
            long_lines += 1

    avg_words_per_line = sum(words_per_line) / len(words_per_line) if words_per_line else 0

    return {
        "words": words,
        "line_count": line_count,
        "avg_words_per_line": avg_words_per_line,
        "short_lines": short_lines,
        "medium_lines": medium_lines,
        "long_lines": long_lines,
        "tokens": set(tokens),
    }


def detect_image_mode(
    file_name: str,
    meta: Optional[Dict[str, Any]] = None,
    ocr_text: str = "",
    template_config: Optional[Dict[str, Any]] = None,
    caption: Optional[str] = None,
    extra_signals: Optional[Dict[str, Any]] = None,
) -> str:
    meta = meta or {}
    text = _safe_text(ocr_text)

    width = int(meta.get("width", 0) or 0)
    height = int(meta.get("height", 0) or 0)
    ratio = (width / height) if width and height else 0

    stats = _signal_stats(text)
    words = stats["words"]
    line_count = stats["line_count"]
    avg_words_per_line = stats["avg_words_per_line"]
    short_lines = stats["short_lines"]
    long_lines = stats["long_lines"]

    lower_name = _safe_text(file_name).lower()

    screenshot_name_hints = {"screenshot", "screen", "capture", "snip"}
    if any(hint in lower_name for hint in screenshot_name_hints):
        return "screenshot"

    # Very little readable text usually means photo / non-document image
    if words <= 10 and line_count <= 3:
        return "photo"

    # Fragmented text layout tends to be screenshot / diagram / workflow
    if (
        words >= 20
        and short_lines >= max(5, long_lines)
        and avg_words_per_line <= 9
    ):
        if ratio >= 1.2:
            return "screenshot"
        return "chart_like"

    # Wide images with moderate or heavy text often behave like screenshots/diagrams
    if (
        width >= 1000
        and ratio >= 1.6
        and words >= 20
        and short_lines >= 4
    ):
        return "screenshot"

    # Paragraph-heavy scan / article / document page
    if (
        words >= 80
        and long_lines >= 4
        and avg_words_per_line >= 7
        and ratio < 1.6
    ):
        return "text_heavy_scan"

    # Fragmented text layout tends to be screenshot / diagram / workflow
    if (
        words >= 20
        and short_lines >= max(5, long_lines * 2)
        and avg_words_per_line <= 6
    ):
        if ratio >= 1.2:
            return "screenshot"
        return "chart_like"

    # Wide images with moderate text often behave like screenshots/diagrams
    if (
        width >= 1000
        and ratio >= 1.4
        and words >= 15
        and avg_words_per_line <= 8
    ):
        return "screenshot"

    chart_terms = {
        "chart", "graph", "axis", "figure", "table",
        "total", "percent", "rate", "amount"
    }
    if len(stats["tokens"] & chart_terms) >= 2:
        return "chart_like"

    if words >= 40:
        return "text_heavy_scan"

    return "unknown"


def detect_image_doc_type(
    image_mode: str,
    ocr_text: str = "",
    meta: Optional[Dict[str, Any]] = None,
    template_config: Optional[Dict[str, Any]] = None,
    caption: Optional[str] = None,
    extra_signals: Optional[Dict[str, Any]] = None,
) -> str:
    text = _safe_text(ocr_text).lower()
    stats = _signal_stats(text)
    words = stats["words"]

    procedural_terms = {
        "step", "steps", "click", "select", "open", "go", "run",
        "install", "configure", "check", "queue", "escalate", "timeout"
    }
    structured_terms = {
        "field", "tag", "id", "code", "value", "values",
        "type", "description", "enum"
    }
    reference_terms = {
        "definition", "includes", "advantages", "overview",
        "introduction", "derived", "explains"
    }

    procedural_score = sum(1 for term in procedural_terms if term in text)
    structured_score = sum(1 for term in structured_terms if term in text)
    reference_score = sum(1 for term in reference_terms if term in text)

    if image_mode == "chart_like":
        return "structured"

    if image_mode == "screenshot":
        if procedural_score >= 1:
            return "procedural"
        return "reference"

    if image_mode == "text_heavy_scan":
        if structured_score >= 3:
            return "structured"
        if reference_score >= 1:
            return "reference"
        if words >= 120:
            return "narrative"
        return "reference"

    if image_mode == "photo":
        return "reference"

    if structured_score >= 3:
        return "structured"

    if procedural_score >= 2:
        return "procedural"

    return "reference"