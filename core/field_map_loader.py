import json
from pathlib import Path
from core.paths import CONFIG_DIR

def load_field_maps():
    path = CONFIG_DIR / "field_maps.json"

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)