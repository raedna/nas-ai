from pathlib import Path

DEBUG = True


def astro_serializer(parsed, file_path, template_config, file_tags, collection_name):
    metadata = parsed.get("metadata") or {}
    text = (parsed.get("text") or "").strip()

    if not metadata or not text:
        return []

    source_file = Path(file_path).name

    primary_name = (
        metadata.get("target")
        or metadata.get("object")
        or parsed.get("file_name")
        or source_file
    )

    description_parts = []

    if metadata.get("camera"):
        description_parts.append(f"Camera: {metadata['camera']}")
    if metadata.get("mount"):
        description_parts.append(f"Mount: {metadata['mount']}")
    if metadata.get("exposure_sec") is not None:
        description_parts.append(f"Exposure: {metadata['exposure_sec']} sec")
    if metadata.get("filter"):
        description_parts.append(f"Filter: {metadata['filter']}")
    if metadata.get("resolution"):
        description_parts.append(f"Resolution: {metadata['resolution']}")
    if metadata.get("date-obs"):
        description_parts.append(f"Observed: {metadata['date-obs']}")

    description = " | ".join(description_parts) if description_parts else text

    item = {
        "text": text,
        "identifier": None,
        "primary_name": primary_name,
        "description": description,
        "doc_type": parsed.get("doc_type") or "structured",
        "source_type": "astro",
        "source_file": source_file,
        "astro_format": parsed.get("astro_format"),
        **metadata
    }

    item.update(file_tags or {})

    if DEBUG:
        print(f"[ASTRO SERIALIZER] {source_file} -> 1 item")
        print(f"[ASTRO SERIALIZER] primary_name: {primary_name}")

    return [item]