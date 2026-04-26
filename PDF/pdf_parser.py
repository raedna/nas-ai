from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import re

import fitz  # PyMuPDF

import io
from PIL import Image, ImageOps, ImageFilter

try:
    import pytesseract
except Exception:
    pytesseract = None

try:
    from PDF.pdf_detector import detect_pdf_mode, detect_pdf_doc_type
except Exception:
    detect_pdf_mode = None
    detect_pdf_doc_type = None


DEBUG = True


def _clean_text(text: str) -> str:
    text = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _is_heading_line(line: str) -> bool:
    line = line.strip()
    if not line:
        return False

    words = line.split()
    if len(words) > 12:
        return False

    if len(line) <= 120 and not line.endswith((".", "!", "?")):
        alpha_words = [w for w in words if any(c.isalpha() for c in w)]
        if not alpha_words:
            return False
        titlecase_like = sum(1 for w in alpha_words if w[:1].isupper())
        return (titlecase_like / len(alpha_words)) >= 0.6

    return False


def _is_bullet_line(line: str) -> bool:
    line = line.strip()
    return bool(re.match(r"^[-*•]\s+", line)) or bool(re.match(r"^\d+[\.\)]\s+", line))


def _split_text_into_blocks(text: str) -> List[Dict[str, Any]]:
    text = _clean_text(text)
    if not text:
        return []

    blocks: List[Dict[str, Any]] = []
    block_id = 1
    current_paragraph: List[str] = []

    def flush_paragraph():
        nonlocal block_id, current_paragraph
        if not current_paragraph:
            return
        para_text = _clean_text("\n".join(current_paragraph))
        if para_text:
            blocks.append({
                "block_id": block_id,
                "block_type": "paragraph",
                "text": para_text
            })
            block_id += 1
        current_paragraph = []

    for raw_line in text.split("\n"):
        line = raw_line.strip()

        if not line:
            flush_paragraph()
            continue

        if _is_heading_line(line):
            flush_paragraph()
            blocks.append({
                "block_id": block_id,
                "block_type": "heading",
                "text": line
            })
            block_id += 1
            continue

        if _is_bullet_line(line):
            flush_paragraph()
            blocks.append({
                "block_id": block_id,
                "block_type": "bullet",
                "text": line
            })
            block_id += 1
            continue

        current_paragraph.append(line)

    flush_paragraph()
    return blocks


def _extract_pdf_text(path: Path) -> tuple[str, int]:
    doc = fitz.open(path)
    try:
        page_count = len(doc)
        pages = []

        for page in doc:
            page_text = page.get_text("text") or ""
            page_text = _clean_text(page_text)
            if page_text:
                pages.append(page_text)

        full_text = "\n\n".join(pages).strip()
        return full_text, page_count
    finally:
        doc.close()

def _prepare_image_for_ocr(img: Image.Image) -> Image.Image:
    try:
        prepared = img.convert("L")
        prepared = ImageOps.autocontrast(prepared)

        width, height = prepared.size
        if width < 1800:
            prepared = prepared.resize(
                (width * 2, height * 2),
                Image.Resampling.LANCZOS
            )

        prepared = prepared.filter(ImageFilter.SHARPEN)
        return prepared
    except Exception:
        return img


def _run_ocr_on_image(img: Image.Image) -> str:
    if pytesseract is None:
        return ""

    try:
        prepared = _prepare_image_for_ocr(img)

        # thresholded version often helps dense scanned text
        bw = prepared.point(lambda p: 255 if p > 170 else 0)

        text1 = pytesseract.image_to_string(prepared, config="--psm 6")
        text2 = pytesseract.image_to_string(bw, config="--psm 6")

        text1 = _clean_text(text1)
        text2 = _clean_text(text2)

        # keep the fuller result
        return text1 if len(text1) >= len(text2) else text2
    except Exception:
        return ""


def _extract_scanned_pdf_ocr(path: Path) -> tuple[str, List[Dict[str, Any]]]:
    doc = fitz.open(path)
    try:
        pages_text = []
        page_items = []

        for i, page in enumerate(doc, start=1):
            pix = page.get_pixmap(matrix=fitz.Matrix(3, 3), alpha=False)
            img = Image.open(io.BytesIO(pix.tobytes("png")))

            ocr_text = _run_ocr_on_image(img)
            if ocr_text:
                pages_text.append(ocr_text)
                page_items.append({
                    "page_num": i,
                    "ocr_text": ocr_text
                })

        full_text = "\n\n".join(pages_text).strip()
        return full_text, page_items
    finally:
        doc.close()

def _build_page_blocks_from_ocr_pages(page_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    block_id = 1

    for page in page_items or []:
        page_num = page.get("page_num")
        ocr_text = _clean_text(page.get("ocr_text") or "")

        if not ocr_text:
            continue

        blocks.append({
            "block_id": block_id,
            "block_type": "page_ocr",
            "text": ocr_text,
            "page_num": page_num,
        })
        block_id += 1

    return blocks


def parse_pdf(
    file_path: str | Path,
    template_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    path = Path(file_path)
    template_config = template_config or {}

    full_text, page_count = _extract_pdf_text(path)

    pdf_mode = "readable_pdf"
    if callable(detect_pdf_mode):
        try:
            pdf_mode = detect_pdf_mode(
                text=full_text,
                page_count=page_count,
                template_config=template_config,
            ) or "readable_pdf"
        except Exception:
            pdf_mode = "readable_pdf"

    page_items = []

    if pdf_mode == "scanned_pdf":
        ocr_text, page_items = _extract_scanned_pdf_ocr(path)
        if ocr_text:
            full_text = ocr_text

    if pdf_mode == "scanned_pdf":
        blocks = _build_page_blocks_from_ocr_pages(page_items)
    else:
        blocks = _split_text_into_blocks(full_text)

    doc_type = "reference"
    if callable(detect_pdf_doc_type):
        try:
            doc_type = detect_pdf_doc_type(
                text=full_text,
                blocks=blocks,
                pdf_mode=pdf_mode,
                template_config=template_config,
            ) or "reference"
        except Exception:
            doc_type = "reference"

    result = {
        "file_type": "pdf",
        "source_type": "pdf",
        "pdf_mode": pdf_mode,
        "file_name": path.name,
        "file_path": str(path),
        "doc_type": doc_type,
        "blocks": blocks,
        "text": full_text,
        "page_count": page_count,
        "pages": page_items
    }

    if DEBUG:
        print(f"[PDF PARSER] Loaded {page_count} pages from {path.name}")
        print(f"[PDF PARSER] Blocks: {len(blocks)}")
        if blocks:
            print(f"[PDF PARSER] First block type: {blocks[0]['block_type']}")
            print(f"[PDF PARSER] First block text: {blocks[0]['text'][:200]}")

    return result