import re
from pathlib import Path

DEBUG = True


# =========================================================
# HELPERS
# =========================================================
def _make_chunk(
    blocks,
    chunk_id,
    doc_type,
    source_file,
    source_stem,
    is_markdown,
    file_tags,
    related_titles=None,
    file_path=None,
):

    if not blocks:
        return None

    text_parts = []
    heading = None
    block_types = []

    embedded_image_targets = []
    embedded_image_paths = []
    embedded_image_modes = []
    embedded_image_doc_types = []

    for i, block in enumerate(blocks):
        block_type = block.get("block_type")
        block_text = (block.get("text") or "").strip()

        if not block_text:
            continue

        block_types.append(block_type)

        if block_type == "heading" and heading is None:
            heading = block_text

        # do not embed raw image placeholder syntax
        if block_type == "image_placeholder":
            continue

        # collect embedded image context metadata
        if block_type == "embedded_image_text":
            image_target = (block.get("image_target") or "").strip()
            image_path = (block.get("image_path") or "").strip()
            image_mode = (block.get("image_mode") or "").strip()
            image_doc_type = (block.get("image_doc_type") or "").strip()

            if image_target and image_target not in embedded_image_targets:
                embedded_image_targets.append(image_target)

            if image_path and image_path not in embedded_image_paths:
                embedded_image_paths.append(image_path)

            if image_mode and image_mode not in embedded_image_modes:
                embedded_image_modes.append(image_mode)

            if image_doc_type and image_doc_type not in embedded_image_doc_types:
                embedded_image_doc_types.append(image_doc_type)

            # visible marker in chunk text
            marker_name = image_target or "unknown image"
            block_text = f"[Embedded image OCR from: {marker_name}]\n{block_text}"

        text_parts.append(block_text)

    # fallback: short first paragraph can act like a local title
    if heading is None and blocks:
        first_text = (blocks[0].get("text") or "").strip()
        first_type = blocks[0].get("block_type")

        if first_type == "paragraph" and first_text and len(first_text) <= 120 and "\n" not in first_text:
            heading = first_text

    text = "\n\n".join(text_parts).strip()
    if not text:
        return None

    related_titles = related_titles or []

    identifier = f"chunk_{chunk_id}"
    identifier_field = "chunk_id"
    identifier_namespace = "chunk_id"
    identifier_kind = "generated"
    link_keys = [f"{identifier_namespace}:{identifier}"]

    payload = {
        "chunk_id": chunk_id,
        "identifier": identifier,
        "identifier_field": identifier_field,
        "identifier_namespace": identifier_namespace,
        "identifier_kind": identifier_kind,
        "link_keys": link_keys,
        "related_link_keys": [],
        "file_path": str(file_path) if file_path else None,

        "primary_name": source_stem if is_markdown else heading,
        "section_heading": heading,
        "description": text,
        "block_types": block_types,
        "doc_type": doc_type,
        "source_type": "doc",
        "source_file": source_file,
        "has_embedded_image_ocr": bool(embedded_image_targets),
        "embedded_image_targets": embedded_image_targets,
        "embedded_image_paths": embedded_image_paths,
        "embedded_image_modes": embedded_image_modes,
        "embedded_image_doc_types": embedded_image_doc_types,
        "file_name": source_file,
        "related_titles": related_titles,
        "note_title": source_stem if is_markdown else None,
        **file_tags
    }

    full_text = f"Title: {source_stem}\n\n{text}" if is_markdown else text

    return {
        "text": full_text,
        **payload
    }


def _clean_obsidian_note_title(source_stem):
    text = str(source_stem or "").strip()
    return re.sub(r"~\d{8}-\d{6}$", "", text).strip()

# =========================================================
# MAIN SERIALIZER
# =========================================================
def doc_serializer(parsed, file_path, template_config, file_tags, collection_name):
    blocks = parsed.get("blocks", [])
    doc_type = parsed.get("doc_type") or "narrative"
    source_file = Path(file_path).name
    source_stem = Path(file_path).stem
    source_suffix = Path(file_path).suffix.lower()
    is_markdown = source_suffix == ".md"
    display_stem = _clean_obsidian_note_title(source_stem) if is_markdown else source_stem
    related_titles = parsed.get("related_titles") or []

    if not blocks:
        return []

    items = []
    current_chunk = []
    chunk_id = 1

    max_blocks_per_chunk = template_config.get("max_blocks_per_chunk", 4)

    def flush_current_chunk():
        nonlocal current_chunk, chunk_id, items

        if not current_chunk:
            return

        chunk = _make_chunk(current_chunk, chunk_id, doc_type, source_file, display_stem, is_markdown, file_tags, related_titles)
        if chunk:
            # fallback title for procedural chunks with no heading
            if doc_type == "procedural" and not chunk.get("primary_name"):
                chunk["primary_name"] = display_stem
                chunk["section_heading"] = display_stem
                if not chunk.get("description"):
                    chunk["description"] = chunk.get("text")

            items.append(chunk)
            chunk_id += 1

        current_chunk = []

    if doc_type == "procedural":
        for block in blocks:
            block_type = block.get("block_type")

            if block_type == "front_matter":
                continue

            # heading starts a new procedural section
            if block_type == "heading":
                flush_current_chunk()
                current_chunk = [block]
                continue

            # keep bullets/paragraphs with current heading or procedure group
            current_chunk.append(block)

        flush_current_chunk()

    else:
        for block in blocks:
            block_type = block.get("block_type")

            # front matter becomes its own chunk
            if block_type == "front_matter":
                chunk = _make_chunk(current_chunk, chunk_id, doc_type, source_file, display_stem, is_markdown, file_tags, related_titles)
                if chunk:
                    items.append(chunk)
                    chunk_id += 1
                continue

            # heading starts a new chunk boundary
            if block_type == "heading" and current_chunk:
                flush_current_chunk()
                current_chunk = [block]
                continue

            current_chunk.append(block)

            if len(current_chunk) >= max_blocks_per_chunk:
                flush_current_chunk()

        flush_current_chunk()

    if DEBUG:
        print(f"[DOC SERIALIZER] {source_file} -> {len(items)} chunks ({doc_type})")
        if items:
            print(f"[DOC SERIALIZER] First chunk: {items[0]['text'][:200]}")

    return items