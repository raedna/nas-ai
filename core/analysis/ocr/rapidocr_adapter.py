from typing import List, Dict, Any

from rapidocr_onnxruntime import RapidOCR


_ocr_engine = None


def _get_engine() -> RapidOCR:
    global _ocr_engine

    if _ocr_engine is None:
        _ocr_engine = RapidOCR()

    return _ocr_engine


def ocr_image_with_rapidocr(image_path: str) -> Dict[str, Any]:
    """
    OCR image using RapidOCR.

    This is Analysis-only OCR and does not modify existing ingestion OCR.
    """
    engine = _get_engine()
    result, elapsed = engine(image_path)

    blocks: List[Dict[str, Any]] = []
    lines: List[str] = []

    for item in result or []:
        box, text, score = item

        text = str(text or "").strip()
        if not text:
            continue

        blocks.append({
            "text": text,
            "score": float(score or 0),
            "box": box,
        })
        lines.append(text)

    return {
        "text": "\n".join(lines),
        "blocks": blocks,
        "elapsed": elapsed,
        "engine": "rapidocr",
    }