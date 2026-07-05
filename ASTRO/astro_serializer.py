from pathlib import Path
import re

DEBUG = True

def _astro_identity(file_path, source_file):
    source_key = Path(str(file_path or source_file)).stem
    source_key = re.sub(r"[^a-zA-Z0-9_]+", "_", source_key).strip("_").lower()

    identifier = source_key
    identifier_field = "astro_file"
    identifier_namespace = "astro_file"
    identifier_kind = "generated"
    link_keys = [f"{identifier_namespace}:{identifier}"]

    return {
        "identifier": identifier,
        "identifier_field": identifier_field,
        "identifier_namespace": identifier_namespace,
        "identifier_kind": identifier_kind,
        "link_keys": link_keys,
        "related_link_keys": [],
    }

def astro_serializer(parsed, file_path, template_config, file_tags, collection_name):
    metadata = parsed.get("metadata") or {}
    text = (parsed.get("text") or "").strip()

    if not metadata or not text:
        return []

    source_file = Path(file_path).name
    source_path = str(file_path)
    identity = _astro_identity(source_path, source_file)

    schema = None
    try:
        from ASTRO.schema_inference_astro import infer_astro_schema
        schema = infer_astro_schema([metadata], collection_name, collection_name)
    except Exception as e:
        print(f"[ASTRO SCHEMA] inference failed: {e}")

    primary_name = None
    if schema:
        for f in schema.get("primary_name", []) + ["file_target"]:
            if metadata.get(f):
                primary_name = metadata[f]
                break
    if not primary_name:
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
        **identity,
        "primary_name": primary_name,
        "description": description,
        "doc_type": parsed.get("doc_type") or "structured",
        "source_type": "astro",
        "source_file": source_file,
        "file_name": source_file,
        "file_path": source_path,
        "astro_format": parsed.get("astro_format"),
        **metadata
    }

    item.update(file_tags or {})

    if DEBUG:
        print(f"[ASTRO SERIALIZER] {source_file} -> 1 item")
        print(f"[ASTRO SERIALIZER] primary_name: {primary_name}")

    from core.payload_utils import enrich_payload_with_common_fields
    enrich_payload_with_common_fields(item, source_path, template_config)

    return [item]