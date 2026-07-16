"""
core/config_store.py — safe, journaled edits to whitelisted config keys.

Automated-but-controllable: code (UI actions, future suggestion flows) may
modify ONLY the keys declared here; every write backs up the previous file
to config/history/ and appends a journal line. Credentials, paths, and
model settings are deliberately NOT editable through this door.
"""
import json
import shutil
from datetime import datetime
from pathlib import Path

from core.paths import CONFIG_DIR

SYSTEM_JSON = Path(CONFIG_DIR) / "system.json"
HISTORY_DIR = Path(CONFIG_DIR) / "history"

# key paths (dot notation) that may be edited programmatically
EDITABLE = {
    "value_aliases",          # value_aliases.<collection>.<token> = <value>
    "halo.status_map",
    "memory.triggers",
    "memory.filler_words",
    "front_followup_markers",
    "concept_links",
}


def _allowed(path: str) -> bool:
    return any(path == e or path.startswith(e + ".") for e in EDITABLE)


def _backup():
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = HISTORY_DIR / f"system_{stamp}.json"
    shutil.copy2(SYSTEM_JSON, dst)
    # keep the newest 50 backups
    backups = sorted(HISTORY_DIR.glob("system_*.json"))
    for old in backups[:-50]:
        old.unlink()
    return dst


def _journal(line: str):
    with open(HISTORY_DIR / "journal.log", "a") as f:
        f.write(f"{datetime.now().isoformat(timespec='seconds')} {line}\n")


def get_key(path: str):
    cfg = json.load(open(SYSTEM_JSON))
    node = cfg
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def set_key(path: str, value, actor: str = "ui") -> None:
    """Set a whitelisted dotted key. Creates intermediate dicts."""
    if not _allowed(path):
        raise PermissionError(f"config key '{path}' is not editable "
                              f"programmatically (whitelist: {sorted(EDITABLE)})")
    cfg = json.load(open(SYSTEM_JSON))
    _backup()
    node = cfg
    parts = path.split(".")
    for part in parts[:-1]:
        node = node.setdefault(part, {})
        if not isinstance(node, dict):
            raise ValueError(f"'{part}' in '{path}' is not an object")
    node[parts[-1]] = value
    json.dump(cfg, open(SYSTEM_JSON, "w"), indent=2)
    _journal(f"[{actor}] set {path} = {json.dumps(value)[:200]}")


def delete_key(path: str, actor: str = "ui") -> None:
    if not _allowed(path):
        raise PermissionError(f"config key '{path}' is not editable")
    cfg = json.load(open(SYSTEM_JSON))
    _backup()
    node = cfg
    parts = path.split(".")
    for part in parts[:-1]:
        if not isinstance(node, dict) or part not in node:
            return
        node = node[part]
    node.pop(parts[-1], None)
    json.dump(cfg, open(SYSTEM_JSON, "w"), indent=2)
    _journal(f"[{actor}] deleted {path}")
