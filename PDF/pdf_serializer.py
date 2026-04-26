from pathlib import Path

DEBUG = True


# =========================================================
# HELPERS
# =========================================================
def _make_chunk(blocks, chunk_id, doc_type, source_file, page_count, file_tags):
    if not blocks:
        return None

    text_parts = []
    heading = None
    block_types = []

    for block in blocks:
        block_type = block.get("block_type")
        block_text = (block.get("text") or "").strip()

        if not block_text:
            continue

        block_types.append(block_type)

        if block_type == "heading" and heading is None:
            heading = block_text

        text_parts.append(block_text)

    if heading is None and blocks:
        first_text = (blocks[0].get("text") or "").strip()
        first_type = blocks[0].get("block_type")

        if first_type == "paragraph" and first_text and len(first_text) <= 120 and "\n" not in first_text:
            heading = first_text

    text = "\n\n".join(text_parts).strip()
    if not text:
        return None

    payload = {
        "chunk_id": chunk_id,
        "primary_name": heading,
        "section_heading": heading,
        "description": text if heading else None,
        "block_types": block_types,
        "doc_type": doc_type,
        "source_type": "pdf",
        "source_file": source_file,
        "page_count": page_count,
        **file_tags
    }

    return {
        "text": text,
        **payload
    }


def _chunk_has_meaningful_body(blocks):
    non_heading_blocks = [
        b for b in (blocks or [])
        if b.get("block_type") != "heading" and (b.get("text") or "").strip()
    ]
    return len(non_heading_blocks) > 0


# =========================================================
# MAIN SERIALIZER
# =========================================================
def pdf_serializer(parsed, file_path, template_config, file_tags, collection_name):
    blocks = parsed.get("blocks", [])
    doc_type = parsed.get("doc_type") or "reference"
    source_file = Path(file_path).name
    source_stem = Path(file_path).stem
    page_count = parsed.get("page_count") or 0
    pdf_mode = parsed.get("pdf_mode") or "readable_pdf"

    if not blocks:
        return []

    if pdf_mode == "scanned_pdf":
        items = []
        chunk_id = 1

        for block in blocks:
            block_text = (block.get("text") or "").strip()
            if not block_text:
                continue

            page_num = block.get("page_num")

            text = f"[PDF OCR page {page_num}]\n{block_text}" if page_num else block_text

            items.append({
                "text": text,
                "chunk_id": chunk_id,
                "primary_name": f"Page {page_num}" if page_num else None,
                "section_heading": f"Page {page_num}" if page_num else None,
                "description": text,
                "block_types": [block.get("block_type")],
                "doc_type": doc_type,
                "source_type": "pdf",
                "source_file": source_file,
                "page_count": page_count,
                "page_num": page_num,
                **file_tags
            })
            chunk_id += 1

        if DEBUG:
            print(f"[PDF SERIALIZER] {source_file} -> {len(items)} chunks ({doc_type}, scanned_pdf)")
            if items:
                print(f"[PDF SERIALIZER] First chunk: {items[0]['text'][:200]}")

        return items

    items = []
    current_chunk = []
    chunk_id = 1

    max_blocks_per_chunk = template_config.get("max_blocks_per_chunk", 6)

    def flush_current_chunk(force=False):
        nonlocal current_chunk, chunk_id, items

        if not current_chunk:
            return

        # avoid tiny heading-only chunks unless forced
        if not force and len(current_chunk) == 1 and current_chunk[0].get("block_type") == "heading":
            return

        chunk = _make_chunk(
            current_chunk,
            chunk_id,
            doc_type,
            source_file,
            page_count,
            file_tags
        )

        if chunk:
            if doc_type == "procedural" and not chunk.get("primary_name"):
                chunk["primary_name"] = source_stem
                chunk["section_heading"] = source_stem
                if not chunk.get("description"):
                    chunk["description"] = chunk.get("text")

            items.append(chunk)
            chunk_id += 1

        current_chunk = []

    if doc_type == "procedural":
        for block in blocks:
            block_type = block.get("block_type")

            if block_type == "heading":
                if current_chunk:
                    flush_current_chunk(force=True)
                current_chunk = [block]
                continue

            current_chunk.append(block)

            if len(current_chunk) >= max_blocks_per_chunk:
                flush_current_chunk(force=True)

        flush_current_chunk(force=True)

    else:
        for block in blocks:
            block_type = block.get("block_type")

            if block_type == "heading":
                if current_chunk and _chunk_has_meaningful_body(current_chunk):
                    flush_current_chunk(force=True)
                    current_chunk = [block]
                else:
                    # keep consecutive/leading heading with following body
                    current_chunk.append(block)
                continue

            current_chunk.append(block)

            if len(current_chunk) >= max_blocks_per_chunk:
                flush_current_chunk(force=True)

        flush_current_chunk(force=True)

    if DEBUG:
        print(f"[PDF SERIALIZER] {source_file} -> {len(items)} chunks ({doc_type})")
        if items:
            print(f"[PDF SERIALIZER] First chunk: {items[0]['text'][:200]}")

    return items