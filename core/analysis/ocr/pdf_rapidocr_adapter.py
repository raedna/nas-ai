from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, Any, List

import fitz  # PyMuPDF

from core.analysis.ocr.rapidocr_adapter import ocr_image_with_rapidocr


def _extract_embedded_pdf_text(pdf_path: str) -> str:
    """
    Extract embedded PDF text directly.

    This is preferred over OCR when available because it is cleaner
    and avoids OCR mistakes.
    """
    text_parts: List[str] = []

    with fitz.open(pdf_path) as doc:
        for page_index, page in enumerate(doc):
            text = page.get_text("text") or ""
            text = text.strip()

            if text:
                text_parts.append(text)

    return "\n\n".join(text_parts).strip()


def _looks_like_usable_fix_text(text: str) -> bool:
    """
    Decide whether embedded PDF text is good enough to use directly.

    For FIX analysis, usable text usually contains either:
    - tag=value pairs
    - table-like FIX fields such as MsgType, SenderCompID, TargetCompID
    """
    if not text or len(text.strip()) < 30:
        return False

    lowered = text.lower()

    if "msgtype" in lowered or "sendercompid" in lowered or "targetcompid" in lowered:
        return True

    # tag=value style
    if "35=" in text or "8=FIX" in text or "8=FIX." in text:
        return True

    return False


def _render_pdf_pages_to_images(pdf_path: str, output_dir: str, dpi: int = 300) -> List[str]:
    """
    Render each PDF page to a PNG image for OCR.
    """
    image_paths: List[str] = []

    zoom = dpi / 72.0
    matrix = fitz.Matrix(zoom, zoom)

    with fitz.open(pdf_path) as doc:
        for page_index, page in enumerate(doc):
            pix = page.get_pixmap(matrix=matrix, alpha=False)

            image_path = str(Path(output_dir) / f"page_{page_index + 1:03d}.png")
            pix.save(image_path)

            image_paths.append(image_path)

    return image_paths


def ocr_pdf_with_rapidocr(pdf_path: str, prefer_embedded_text: bool = True) -> Dict[str, Any]:
    """
    Analysis-only PDF OCR.

    Flow:
    1. Try embedded PDF text first.
    2. If usable, return it.
    3. Otherwise render each page to image.
    4. Run RapidOCR on each rendered page.
    """
    pdf_path = str(pdf_path)

    if prefer_embedded_text:
        embedded_text = _extract_embedded_pdf_text(pdf_path)

        if _looks_like_usable_fix_text(embedded_text):
            return {
                "text": embedded_text,
                "engine": "pdf_embedded_text",
                "pages": [],
            }

    page_results: List[Dict[str, Any]] = []
    page_texts: List[str] = []

    with TemporaryDirectory() as tmp_dir:
        image_paths = _render_pdf_pages_to_images(pdf_path, tmp_dir)

        for page_number, image_path in enumerate(image_paths, start=1):
            result = ocr_image_with_rapidocr(image_path)
            text = (result.get("text") or "").strip()

            page_results.append({
                "page": page_number,
                "image_path": image_path,
                "engine": result.get("engine"),
                "elapsed": result.get("elapsed"),
                "text": text,
                "blocks": result.get("blocks") or [],
            })

            if text:
                page_texts.append(f"--- Page {page_number} ---\n{text}")

    return {
        "text": "\n\n".join(page_texts).strip(),
        "engine": "pdf_rapidocr",
        "pages": page_results,
    }
