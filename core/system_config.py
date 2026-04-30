import json
from core.paths import SYSTEM_CONFIG_PATH


def load_system_config():
    with open(SYSTEM_CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)