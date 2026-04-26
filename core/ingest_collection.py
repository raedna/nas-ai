from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from core.embedder import embed_texts
from core.orchestrator import FileTask, IngestionOrchestrator
from core.qdrant_client import upsert_vectors
from core.registry_setup import registry
import json
from pathlib import Path
import traceback

DEBUG = True

def load_filetypes():
    path = Path("config/filetypes.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def discover_files(paths: List[str]) -> List[Path]:
    files: List[Path] = []

    for p in paths:
        path_obj = Path(p)

        if not path_obj.exists():
            continue

        if path_obj.is_file():
            files.append(path_obj)
            continue

        if path_obj.is_dir():
            files.extend(f for f in path_obj.rglob("*") if f.is_file())

    return files

def _get_collection_paths(collection_cfg: Dict[str, Any]) -> List[str]:
    if collection_cfg.get("paths"):
        return collection_cfg["paths"]

    if collection_cfg.get("path"):
        return [collection_cfg["path"]]

    return []


def _normalize_ext(ext: str) -> str:
    return str(ext or "").lower().lstrip(".")


def _match_filetype(file_path: Path) -> str | None:
    suffix = _normalize_ext(file_path.suffix)

    filetypes = load_filetypes()
    for filetype_name, cfg in filetypes.items():
        exts = [_normalize_ext(x) for x in cfg.get("extensions", [])]
        if suffix in exts:
            return filetype_name

    return None

def _is_allowed_filetype(filetype_name: str, collection_cfg: Dict[str, Any]) -> bool:
    allowed = collection_cfg.get("allowed_filetypes") or []

    if not allowed:
        return True

    allowed_norm = {str(x).strip().lower() for x in allowed if str(x).strip()}
    return str(filetype_name).strip().lower() in allowed_norm

def _build_tasks(
    collection_name: str,
    collection_cfg: Dict[str, Any],
    source_files: List[Path],
) -> List[FileTask]:
    tasks: List[FileTask] = []

    for file_path in source_files:
        filetype_name = _match_filetype(file_path)
        if not filetype_name:
            continue

        if not _is_allowed_filetype(filetype_name, collection_cfg):
            continue

        filetypes = load_filetypes()
        ft_cfg = filetypes.get(filetype_name, {})
        parser_name = ft_cfg.get("parser")
        serializer_name = ft_cfg.get("serializer")

        if not parser_name or not serializer_name:
            continue

        filetype_template_config = ft_cfg.get("template_config", {}) or {}
        collection_template_config = collection_cfg.get("template_config", {}) or {}

        base_template_config = {
            **filetype_template_config,
            **collection_template_config,
            "filters": collection_cfg.get("filters", {}),
        }

        for key in [
            "asset_search_roots",
            "max_blocks_per_chunk",
            "row_tag",
            "header_row",
        ]:
            if key in collection_cfg:
                base_template_config[key] = collection_cfg[key]

        tasks.append(
            FileTask(
                path=file_path,
                filetype_name=filetype_name,
                parser_name=parser_name,
                serializer_name=serializer_name,
                template_config=base_template_config,
                collection_name=collection_name,
                file_tags={},
            )
        )

    return tasks

def _is_hidden_dir_name(name: str) -> bool:
    return str(name or "").startswith(".")


def _should_exclude_file(path_obj, collection_cfg):
    allowed_extensions = [x.lower() for x in collection_cfg.get("allowed_extensions", []) if x]
    exclude_extensions = [x.lower() for x in collection_cfg.get("exclude_extensions", []) if x]
    exclude_dirs = set(collection_cfg.get("exclude_dirs", []) or [])

    suffix = path_obj.suffix.lower()

    if allowed_extensions and suffix not in allowed_extensions:
        return True

    if exclude_extensions and suffix in exclude_extensions:
        return True

    parts = set(path_obj.parts)

    for part in parts:
        if str(part).strip().lower() in exclude_dirs:
            return True

    for part in parts:
        if _is_hidden_dir_name(part):
            return True

    return False


def ingest_collection(
    collection_name,
    collection_cfg,
    force_reingest=False,
    progress_callback=None,
):
    source_files = discover_files(_get_collection_paths(collection_cfg))

    source_files = [
        f for f in source_files
        if not _should_exclude_file(f, collection_cfg)
    ]

    tasks = _build_tasks(
        collection_name=collection_name,
        collection_cfg=collection_cfg,
        source_files=source_files,
    )

    if DEBUG:
        print(f"📁 Source files found: {len(source_files)}")
        print(f"🧩 Tasks built: {len(tasks)}")

    orchestrator = IngestionOrchestrator(
        registry=registry,
        embed_texts_fn=embed_texts,
        upsert_vectors_fn=upsert_vectors,
    )

    result = orchestrator.run(
        tasks=tasks,
        force_reingest=force_reingest,
        progress_callback=progress_callback,
    )

    if DEBUG:
        print("✅ Ingestion complete")
        print("   total_files  :", result.total_files)
        print("   processed    :", result.processed_files)
        print("   skipped      :", result.skipped_files)
        print("   failed       :", result.failed_files)
        print("   total_chunks :", result.total_chunks)

    return {
        "total_files": result.total_files,
        "processed_files": result.processed_files,
        "skipped_files": result.skipped_files,
        "failed_files": result.failed_files,
        "total_chunks": result.total_chunks,
        "results": result.results,
    }




