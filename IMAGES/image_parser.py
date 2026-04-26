from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from PIL import Image, ImageOps, ImageFilter

try:
    import pytesseract
except Exception:
    pytesseract = None

try:
    from IMAGES.image_detector import detect_image_mode, detect_image_doc_type
except Exception:
    detect_image_mode = None
    detect_image_doc_type = None


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _extract_basic_meta(img: Image.Image) -> Dict[str, Any]:
    width, height = img.size
    return {
        "format": _safe_str(getattr(img, "format", "")),
        "width": width,
        "height": height,
        "color_mode": _safe_str(getattr(img, "mode", "")),
    }

def _prepare_image_for_ocr(img: Image.Image) -> Image.Image:
    """
    Light, safe OCR preprocessing.
    Keeps existing workflow intact.
    """
    try:
        prepared = img.convert("L")  # grayscale

        # autocontrast helps screenshots/scans
        prepared = ImageOps.autocontrast(prepared)

        # upscale small/medium images for OCR
        width, height = prepared.size
        if width < 1800:
            scale = 2
            prepared = prepared.resize(
                (width * scale, height * scale),
                Image.Resampling.LANCZOS
            )

        # light sharpen for text edges
        prepared = prepared.filter(ImageFilter.SHARPEN)

        return prepared
    except Exception:
        return img


def _run_ocr(img: Image.Image, enable_ocr: bool = False) -> str:
    if not enable_ocr:
        return ""

    if pytesseract is None:
        return ""

    try:
        prepared = _prepare_image_for_ocr(img)

        text = pytesseract.image_to_string(
            prepared,
            config="--psm 6"
        )
        return _safe_str(text)
    except Exception:
        return ""


def parse_image(
    file_path: str | Path,
    template_config: Optional[Dict[str, Any]] = None,
    enable_ocr: bool = False,
) -> Dict[str, Any]:
    """
    Phase 1 parser for standalone image files only.

    Returns a normalized parser result.
    Does not create serializer-ready documents.
    Does not create schema output.
    """

    path = Path(file_path)
    template_config = template_config or {}

    with Image.open(path) as img:
        img.load()
        meta = _extract_basic_meta(img)
        ocr_enabled = enable_ocr or bool(template_config.get("enable_ocr", False))
        ocr_text = _run_ocr(img, enable_ocr=ocr_enabled)

    image_mode = "unknown"
    if callable(detect_image_mode):
        try:
            image_mode = detect_image_mode(
                file_name=path.name,
                meta=meta,
                ocr_text=ocr_text,
                template_config=template_config,
            ) or "unknown"
        except Exception:
            image_mode = "unknown"

    doc_type = "reference"
    if callable(detect_image_doc_type):
        try:
            doc_type = detect_image_doc_type(
                image_mode=image_mode,
                ocr_text=ocr_text,
                meta=meta,
                template_config=template_config,
            ) or "reference"
        except Exception:
            doc_type = "reference"

    blocks = []
    if ocr_text:
        blocks.append({
            "type": "ocr_text",
            "text": ocr_text,
        })

    return {
        "file_type": "image",
        "source_type": "standalone_image",
        "file_name": path.name,
        "file_path": str(path),
        "doc_type": doc_type,
        "image_mode": image_mode,
        "meta": meta,
        "content": {
            "ocr_text": ocr_text,
            "caption": None,
        },
        "blocks": blocks,
    }