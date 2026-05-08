import json
from datetime import datetime
from pathlib import Path
from core.paths import COLLECTION_STATE_DIR


STATE_DIR = COLLECTION_STATE_DIR


def _state_path(collection_name):
    return STATE_DIR / f"{collection_name}.json"


def load_collection_state(collection_name):
    path = _state_path(collection_name)

    if not path.exists():
        return {"files": {}}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_collection_state(collection_name, state):
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    path = _state_path(collection_name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def update_file_state(
    collection_name,
    file_path,
    filetype_name,
    result,
    extra_metadata=None
):
    state = load_collection_state(collection_name)

    path_obj = Path(file_path)
    file_key = str(path_obj.resolve())
    stat = path_obj.stat() if path_obj.exists() else None

    entry = {
        "path": str(file_path),
        "filetype": filetype_name,
        "status": "failed" if not result.success else (
            "skipped" if result.skipped else "ingested"
        ),
        "chunks_created": result.chunks_created,
        "error": result.error,
        "metadata": result.metadata or {},
        "last_ingested": datetime.utcnow().isoformat(),
        "mtime": stat.st_mtime if stat else None,
        "size": stat.st_size if stat else None,
    }

    if extra_metadata:
        entry.update(extra_metadata)

    state.setdefault("files", {})
    state["files"][file_key] = entry

    save_collection_state(collection_name, state)

def should_skip_file(collection_name, file_path):
    state = load_collection_state(collection_name)

    path_obj = Path(file_path)
    file_key = str(path_obj.resolve())

    entry = state.get("files", {}).get(file_key)

    if not entry:
        return False, "not_previously_ingested"

    if entry.get("status") != "ingested":
        return False, f"previous_status_{entry.get('status')}"

    if not path_obj.exists():
        return False, "file_missing"

    stat = path_obj.stat()

    previous_mtime = entry.get("mtime")
    previous_size = entry.get("size")

    if previous_mtime == stat.st_mtime and previous_size == stat.st_size:
        return True, "unchanged"

    return False, "modified"

def remove_file_state(collection_name, file_path):
    state = load_collection_state(collection_name)

    file_key = str(Path(file_path).resolve())

    if file_key in state.get("files", {}):
        del state["files"][file_key]
        save_collection_state(collection_name, state)

        