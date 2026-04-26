# orchestrator.py

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from core.collection_merger import merge_collection_docs
from core.collection_state import update_file_state



DEBUG = True
DEBUG1 = False

# ----------------------------
# Types
# ----------------------------

ProgressCallback = Optional[Callable[[float], None]]


@dataclass
class FileTask:
    path: Path
    filetype_name: str
    parser_name: str
    serializer_name: str
    template_config: Dict[str, Any]
    collection_name: str
    file_tags: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FileResult:
    path: Path
    filetype_name: str
    success: bool
    skipped: bool = False
    chunks_created: int = 0
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OrchestratorResult:
    total_files: int
    processed_files: int
    skipped_files: int
    failed_files: int
    total_chunks: int
    results: List[FileResult] = field(default_factory=list)


# ----------------------------
# Registry
# ----------------------------

class ComponentRegistry:
    def __init__(self):
        self.parsers: Dict[str, Callable[..., Any]] = {}
        self.serializers: Dict[str, Callable[..., Any]] = {}

    def register_parser(self, name: str, fn: Callable[..., Any]) -> None:
        self.parsers[name] = fn

    def register_serializer(self, name: str, fn: Callable[..., Any]) -> None:
        self.serializers[name] = fn

    def get_parser(self, name: str) -> Callable[..., Any]:
        if name not in self.parsers:
            raise ValueError(f"Parser '{name}' is not registered.")
        return self.parsers[name]

    def get_serializer(self, name: str) -> Callable[..., Any]:
        if name not in self.serializers:
            raise ValueError(f"Serializer '{name}' is not registered.")
        return self.serializers[name]


# ----------------------------
# Orchestrator
# ----------------------------

class IngestionOrchestrator:
    def __init__(
        self,
        registry: ComponentRegistry,
        embed_texts_fn: Callable[[List[str]], List[List[float]]],
        upsert_vectors_fn: Callable[..., int],
    ):
        self.registry = registry
        self.embed_texts = embed_texts_fn
        self.upsert_vectors = upsert_vectors_fn

    def run(
        self,
        tasks: List[FileTask],
        force_reingest: bool = False,
        progress_callback: ProgressCallback = None,
    ) -> OrchestratorResult:
        results: List[FileResult] = []
        total_chunks = 0
        processed = 0
        skipped = 0
        failed = 0

        total = len(tasks)

        for i, task in enumerate(tasks, start=1):
            try:
                result = self._process_file(task, force_reingest=force_reingest)
                results.append(result)

                update_file_state(
                    collection_name=task.collection_name,
                    file_path=task.path,
                    filetype_name=task.filetype_name,
                    result=result
                )

                if result.skipped:
                    skipped += 1
                elif result.success:
                    processed += 1
                    total_chunks += result.chunks_created or 0
                else:
                    failed += 1

            except Exception as e:
                error_result = FileResult(
                    path=task.path,
                    filetype_name=task.filetype_name,
                    success=False,
                    error=str(e),
                )

                results.append(error_result)

                update_file_state(
                    collection_name=task.collection_name,
                    file_path=task.path,
                    filetype_name=task.filetype_name,
                    result=error_result
                )

                failed += 1

            if progress_callback:
                progress_callback(i / total if total else 1.0)

        return OrchestratorResult(
            total_files=total,
            processed_files=processed,
            skipped_files=skipped,
            failed_files=failed,
            total_chunks=total_chunks,
            results=results,
        )

    def _process_file(
        self,
        task: FileTask,
        force_reingest: bool = False,
    ) -> FileResult:
        parser = self.registry.get_parser(task.parser_name)
        serializer = self.registry.get_serializer(task.serializer_name)

        # parse
        parsed = parser(
            file_path=task.path,
            template_config=task.template_config,
        )

        if not parsed:
            return FileResult(
                path=task.path,
                filetype_name=task.filetype_name,
                success=True,
                skipped=True,
                chunks_created=0,
                metadata={"reason": "parser_returned_no_content"},
            )

        # serialize
        items = serializer(
            parsed=parsed,
            file_path=task.path,
            template_config=task.template_config,
            file_tags=task.file_tags,
            collection_name=task.collection_name,
        )

        if DEBUG1:
            print("DEBUG items type:", type(items))
            print("DEBUG first item:", items[0] if items else None)

        if not items:
            # try finalize if supported
            if hasattr(serializer, "finalize"):
                items = serializer.finalize(
                    file_path=task.path,
                    collection_name=task.collection_name,
                    file_tags=task.file_tags
                )

                if not items:
                    return FileResult(
                        path=task.path,
                        filetype_name=task.filetype_name,
                        success=True,
                        skipped=False,
                        chunks_created=0,
                        metadata={"reason": "buffered_waiting_for_finalize"},
                    )

            else:
                return FileResult(
                    path=task.path,
                    filetype_name=task.filetype_name,
                    success=True,
                    skipped=True,
                    chunks_created=0,
                    metadata={"reason": "serializer_returned_no_items"},
                )

        items = merge_collection_docs(items)

        if DEBUG:
            print("POST-MERGE FIRST ITEM:", items[0] if items else None)

        texts = []
        payloads = []

        for item in items:
            text = item.get("text", "").strip()
            if not text:
                continue

            texts.append(text)
            payloads.append(item)

        if not texts:
            return FileResult(
                path=task.path,
                filetype_name=task.filetype_name,
                success=True,
                skipped=True,
                chunks_created=0,
                metadata={"reason": "no_text_after_serialization"},
            )

        vectors = self.embed_texts(texts)

        if len(vectors) != len(payloads):
            raise ValueError(
                f"Embedding count mismatch for {task.path.name}: "
                f"{len(vectors)} vectors vs {len(payloads)} payloads."
            )

        upserted = self.upsert_vectors(
            collection_name=task.collection_name,
            vectors=vectors,
            payloads=payloads,
            source_file=str(task.path),
            force_reingest=force_reingest,
        )

        return FileResult(
            path=task.path,
            filetype_name=task.filetype_name,
            success=True,
            skipped=False,
            chunks_created=upserted,
            metadata={"items_serialized": len(payloads)},
        )