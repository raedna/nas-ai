import json
from core.paths import COLLECTIONS_PATH


def load_collections():
    if not COLLECTIONS_PATH.exists():
        return {}

    with open(COLLECTIONS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_collections(collections):
    COLLECTIONS_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(COLLECTIONS_PATH, "w", encoding="utf-8") as f:
        json.dump(collections, f, indent=2, ensure_ascii=False)


def get_collection(collection_name):
    collections = load_collections()
    return collections.get(collection_name)


def upsert_collection(collection_name, collection_data):
    collections = load_collections()
    collections[collection_name] = collection_data
    save_collections(collections)


def delete_collection(collection_name):
    collections = load_collections()

    if collection_name in collections:
        del collections[collection_name]
        save_collections(collections)


def ensure_collection_defaults(collection_name, collection_data):
    data = dict(collection_data)

    data.setdefault("path", "")
    data.setdefault("profile", "auto")
    data.setdefault("source_label", "")
    data.setdefault("options", {})
    data["options"].setdefault("cleanup", True)
    data["options"].setdefault("reingest", False)

    data.setdefault("allowed_filetypes", [])
    data.setdefault("asset_search_roots", [])

    data.setdefault("filters", {})
    data["filters"].setdefault("version_filter", [])
    data["filters"].setdefault("row_filter", {})
    data["filters"]["row_filter"].setdefault("enabled", False)
    data["filters"]["row_filter"].setdefault("column", "")
    data["filters"]["row_filter"].setdefault("exclude_values", [])

    data.setdefault("notes", "")

    return data