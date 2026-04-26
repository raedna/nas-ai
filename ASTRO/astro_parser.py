from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, Optional
import xml.etree.ElementTree as ET
import re

from astropy.io import fits

DEBUG = True


ASTRO_KEYS = {
    "OBJECT",
    "RA",
    "DEC",
    "DATE-OBS",
    "EXPTIME",
    "EXPOSURE",
    "IMAGETYP",
    "FILTER",
    "INSTRUME",
    "GAIN",
    "CCD-TEMP",
    "FOCALLEN",
    "XPIXSZ",
    "YPIXSZ",
    "ROTATOR",
    "IMAGEW",
    "IMAGEH",
    "NAXIS1",
    "NAXIS2",
    "XBINNING",
    "YBINNING",
    "TELESCOP",
    "SITELAT",
    "SITELONG",
}

ASTRO_METADATA_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "astro_metadata.json"

def _load_astro_metadata_config() -> Dict[str, Any]:
    if not ASTRO_METADATA_CONFIG_PATH.exists():
        return {}

    with open(ASTRO_METADATA_CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def _parse_astro_filename(path: Path) -> Dict[str, Any]:
    stem = path.stem
    parts = stem.split("_")

    parsed: Dict[str, Any] = {
        "file_stem": stem,
        "file_target": None,
        "file_frame_type": None,
        "file_exposure_sec": None,
        "file_binning": None,
        "file_camera": None,
        "file_filter": None,
        "file_gain": None,
        "file_capture_datetime": None,
        "file_sensor_temp_c": None,
        "file_sequence_no": None,
        "file_scope_model": None,
    }

    if not parts:
        return parsed

    first = parts[0].lower()
    if first in {"light", "dark", "flat", "bias"}:
        parsed["file_frame_type"] = parts[0]
        if len(parts) > 1:
            parsed["file_target"] = parts[1]
    else:
        parsed["file_target"] = parts[0]

    for part in parts:
        p = part.lower()

        if p.endswith("s"):
            try:
                parsed["file_exposure_sec"] = float(p[:-1])
            except Exception:
                pass

        elif p.startswith("bin"):
            try:
                parsed["file_binning"] = int(p.replace("bin", ""))
            except Exception:
                pass

        elif p.startswith("gain"):
            try:
                parsed["file_gain"] = int(p.replace("gain", ""))
            except Exception:
                pass

        elif p.endswith("c"):
            temp = p[:-1]
            try:
                parsed["file_sensor_temp_c"] = float(temp)
            except Exception:
                pass

        elif re.fullmatch(r"\d{8}-\d{6}", part):
            parsed["file_capture_datetime"] = part

        elif re.fullmatch(r"\d{4,}", part):
            parsed["file_sequence_no"] = part

        elif re.fullmatch(r"[zZ]\d{2,3}", part):
            parsed["file_scope_model"] = part

        elif "mc" in p or "asi" in p or "533" in p or "2600" in p:
            parsed["file_camera"] = part

        elif p in {"none", "ha", "oiii", "sii", "l", "r", "g", "b", "rgb"}:
            parsed["file_filter"] = part

    return parsed


def _merge_filename_metadata(metadata: Dict[str, Any], path: Path) -> Dict[str, Any]:
    md = dict(metadata)
    fmd = _parse_astro_filename(path)

    for k, v in fmd.items():
        if v not in [None, ""]:
            md[k] = v

    if not md.get("target") and fmd.get("file_target"):
        md["target"] = str(fmd["file_target"]).lower()

    if not md.get("imagetyp") and fmd.get("file_frame_type"):
        md["imagetyp"] = fmd["file_frame_type"]

    if md.get("exposure_sec") in [None, ""] and fmd.get("file_exposure_sec") is not None:
        md["exposure_sec"] = fmd["file_exposure_sec"]

    if not md.get("camera") and fmd.get("file_camera"):
        md["camera"] = str(fmd["file_camera"]).lower()

    if not md.get("filter") and fmd.get("file_filter"):
        md["filter"] = fmd["file_filter"]

    if not md.get("scope_model") and fmd.get("file_scope_model"):
        md["scope_model"] = str(fmd["file_scope_model"]).lower()

    return md


def _derive_astro_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    md = dict(metadata)
    astro_cfg = _load_astro_metadata_config()
    fits_filter_map = astro_cfg.get("fits_filter_map", {})

    print("[ASTRO CFG]", astro_cfg)
    print("[FITS FILTER MAP]", fits_filter_map)
    print("[RAW FILTER]", md.get("filter"))

    if "object" in md and md["object"]:
        md["target"] = str(md["object"]).strip().lower()

    exptime_val = md.get("exptime") or md.get("exposure")
    if exptime_val not in [None, ""]:
        try:
            md["exposure_sec"] = float(exptime_val)
        except Exception:
            pass

    width = md.get("imagew") or md.get("naxis1")
    height = md.get("imageh") or md.get("naxis2")
    if width and height:
        md["resolution"] = f"{width}x{height}"

    if "instrume" in md and md["instrume"]:
        md["camera"] = str(md["instrume"]).strip().lower()

    if "telescop" in md and md["telescop"]:
        md["mount"] = str(md["telescop"]).strip().lower()

    if "rotator" in md and md["rotator"] not in [None, ""]:
        md["rotation_angle"] = str(md["rotator"]).strip()

    raw_filter = md.get("filter")
    if raw_filter not in [None, ""]:
        raw_filter_str = str(raw_filter).strip()
        mapped_filter = fits_filter_map.get(raw_filter_str)

        if mapped_filter:
            md["filter_name"] = mapped_filter
        else:
            md["filter_name"] = raw_filter_str

    return md


def _metadata_to_text(metadata: Dict[str, Any]) -> str:
    return "\n".join(f"{k}: {v}" for k, v in metadata.items()).strip()


def _parse_fits_metadata(path: Path) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {}

    with fits.open(path) as hdul:
        header = hdul[0].header

        for key in ASTRO_KEYS:
            if key in header and header[key] is not None:
                metadata[key.lower()] = str(header[key])

    return _derive_astro_metadata(metadata)


def _parse_xisf_metadata(path: Path) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {}

    with open(path, "rb") as f:
        data = f.read(10 * 1024 * 1024)

    start = data.lower().find(b"<xisf")
    end = data.lower().find(b"</xisf>")

    if start == -1 or end == -1:
        return {}

    xml_data = data[start:end + len(b"</xisf>")]
    root = ET.fromstring(xml_data)

    raw_meta: Dict[str, Any] = {}

    # root attrs
    for k, v in root.attrib.items():
        raw_meta[k] = str(v)

    # Image attrs
    image_node = root.find(".//{*}Image")
    if image_node is not None:
        for k, v in image_node.attrib.items():
            if k != "location":  # noisy/low-value for retrieval
                raw_meta[f"image_{k}"] = str(v)

        geometry = image_node.attrib.get("geometry")
        if geometry:
            parts = geometry.split(":")
            if len(parts) >= 2:
                raw_meta["IMAGEW"] = parts[0]
                raw_meta["IMAGEH"] = parts[1]

    # Resolution attrs
    res_node = root.find(".//{*}Resolution")
    if res_node is not None:
        for k, v in res_node.attrib.items():
            raw_meta[f"resolution_{k}"] = str(v)

    # FITS keywords if present
    for kw in root.findall(".//{*}FITSKeyword"):
        name = kw.attrib.get("name")
        value = kw.attrib.get("value")
        if name and value:
            raw_meta[name] = value

    # XISF/PixInsight properties
    for prop in root.findall(".//{*}Property"):
        pid = prop.attrib.get("id")
        val = prop.attrib.get("value")

        if val is None:
            val_node = prop.find(".//{*}Value")
            if val_node is not None and val_node.text:
                val = val_node.text.strip()

        if val is None and prop.text and prop.text.strip():
            val = prop.text.strip()

        if not pid or val in [None, ""]:
            continue

        # skip noisy processing blob
        if pid == "PixInsight:ProcessingHistory":
            continue

        raw_meta[pid] = val

    # keep valid astronomy fields
    keep_exact = ASTRO_KEYS | {"IMAGEW", "IMAGEH"}

    # keep selected XISF structural fields
    keep_exact_lower = {
        "image_sampleformat",
        "image_colorspace",
        "image_geometry",
        "image_sampleformat",
        "image_bounds",
        "image_colorspace",
        "resolution_horizontal",
        "resolution_vertical",
        "resolution_unit",
        "XISF:CreationTime",
        "XISF:CreatorApplication",
        "XISF:CreatorModule",
        "XISF:CreatorOS",
    }

    for k, v in raw_meta.items():
        if k in keep_exact or k.lower() in keep_exact_lower:
            metadata[k.lower()] = str(v)

    return _derive_astro_metadata(metadata)


def parse_astro(
    file_path: str | Path,
    template_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    path = Path(file_path)
    ext = path.suffix.lower()

    if ext in [".fit", ".fits", ".fts"]:
        astro_format = "fits"
        metadata = _parse_fits_metadata(path)
    elif ext == ".xisf":
        astro_format = "xisf"
        metadata = _parse_xisf_metadata(path)
    else:
        raise ValueError(f"Unsupported astro format: {ext}")

    metadata = _merge_filename_metadata(metadata, path)
    text = _metadata_to_text(metadata)

    result = {
        "file_type": "astro",
        "source_type": "astro",
        "file_name": path.name,
        "file_path": str(path),
        "astro_format": astro_format,
        "doc_type": "structured",
        "metadata": metadata,
        "text": text,
    }

    if DEBUG:
        print(f"[ASTRO PARSER] {path.name}")
        print(f"[ASTRO PARSER] format: {astro_format}")
        print(f"[ASTRO PARSER] metadata count: {len(metadata)}")
        if metadata:
            print(f"[ASTRO PARSER] sample keys: {list(metadata.keys())[:10]}")

    return result