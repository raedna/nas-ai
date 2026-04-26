import json
from pathlib import Path

def load_field_maps():
    path = Path("config/field_maps.json")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)