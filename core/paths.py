from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
SCHEMAS_DIR = PROJECT_ROOT / "schemas"
COLLECTION_STATE_DIR = CONFIG_DIR / "collection_state"

SYSTEM_CONFIG_PATH = CONFIG_DIR / "system.json"
FILETYPES_PATH = CONFIG_DIR / "filetypes.json"
COLLECTIONS_PATH = CONFIG_DIR / "collections.json"
SCHEMA_OVERRIDES_PATH = CONFIG_DIR / "schema_overrides.json"