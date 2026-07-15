"""
HALO/halo_fetcher.py — Halo ITSM API client + incremental sync (HALO-02).

Credentials live OUTSIDE the repo in a file only the user touches; config
holds a pointer:

  config/system.json:
    "halo": {
      "credentials_file": "/Users/you/.nasai/halo_creds.json",
      "tickets_dir": "/Users/you/RaedsMacM1/nas-ai/halo_tickets"
    }

  credentials file (created by the user, never read by tooling):
    {"base_url": "https://agent.omnivista.com",
     "auth_url": "https://agent.omnivista.com/auth/token",   # optional
     "client_id": "...", "client_secret": "...", "scope": "all"}

Sync: pull tickets updated since the last run (state in
<tickets_dir>/.halo_sync.json), fetch actions per ticket, download embedded
images AT SYNC TIME (their URLs carry expiring JWTs), render markdown via
the normalizer. Ingestion picks up changed files by hash as usual.

Run:  python3 HALO/halo_fetcher.py            # incremental
      python3 HALO/halo_fetcher.py --full     # ignore sync state
      python3 HALO/halo_fetcher.py --ticket 44539   # one ticket
"""
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests



# ---------------------------------------------------------------- config --
def _halo_cfg():
    from core.system_config import load_system_config
    c = load_system_config().get("halo", {}) or {}
    cred_path = c.get("credentials_file")
    if not cred_path or not Path(cred_path).exists():
        raise RuntimeError(
            "halo.credentials_file missing or not found — create the file "
            "(see module docstring) and point config/system.json at it.")
    creds = json.load(open(cred_path))
    tickets_dir = c.get("tickets_dir")
    if not tickets_dir:
        raise RuntimeError("halo.tickets_dir not set in config/system.json")
    return creds, Path(tickets_dir), c


# ------------------------------------------------------------------ auth --
_TOKEN = {"value": None, "expires": 0.0}


def _get_token(creds):
    if _TOKEN["value"] and time.time() < _TOKEN["expires"] - 60:
        return _TOKEN["value"]
    auth_url = creds.get("auth_url") or creds["base_url"].rstrip("/") + "/auth/token"
    r = requests.post(auth_url, data={
        "grant_type": "client_credentials",
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
        "scope": creds.get("scope", "all"),
    }, timeout=30)
    r.raise_for_status()
    try:
        tok = r.json()
    except Exception:
        raise RuntimeError(
            f"token endpoint returned non-JSON (status {r.status_code}, "
            f"url {auth_url}) — first 200 chars: {r.text[:200]!r}. "
            "Check creds file auth_url against Postman's token request URL "
            "(hosted Halo often needs .../auth/token?tenant=<name>).")
    if "access_token" not in tok:
        raise RuntimeError(f"token response missing access_token: "
                           f"{str(tok)[:300]}")
    _TOKEN["value"] = tok["access_token"]
    _TOKEN["expires"] = time.time() + int(tok.get("expires_in", 3600))
    print(f"[HALO API] token acquired (expires in {tok.get('expires_in', '?')}s)")
    return _TOKEN["value"]


def _get(creds, path, params=None):
    url = creds["base_url"].rstrip("/") + path
    for attempt in (1, 2):
        r = requests.get(url, params=params or {}, timeout=60, headers={
            "Authorization": f"Bearer {_get_token(creds)}"})
        if r.status_code == 401 and attempt == 1:
            _TOKEN["value"] = None      # token expired server-side — refresh once
            continue
        r.raise_for_status()
        return r.json()


# ------------------------------------------------------------------ sync --
def _sync_state_path(tickets_dir):
    return tickets_dir / ".halo_sync.json"


def _load_sync_state(tickets_dir):
    p = _sync_state_path(tickets_dir)
    if p.exists():
        return json.load(open(p))
    return {"last_sync": None}


def _save_sync_state(tickets_dir, state):
    json.dump(state, open(_sync_state_path(tickets_dir), "w"), indent=2)


def _download_images(creds, details_html, ticket_id, tickets_dir):
    """Embedded <img> URLs carry expiring JWTs — download NOW, return
    [(local_path, marker_name)] for the normalizer to reference."""
    out = []
    if not details_html:
        return out
    asset_dir = tickets_dir / "assets" / str(ticket_id)
    srcs = re.findall(r'<img[^>]+src="([^"]+)"', str(details_html))
    for i, src in enumerate(srcs, 1):
        try:
            r = requests.get(src, timeout=60, headers={
                "Authorization": f"Bearer {_get_token(creds)}"})
            r.raise_for_status()
            ext = "png"
            ctype = r.headers.get("content-type", "")
            if "jpeg" in ctype or "jpg" in ctype:
                ext = "jpg"
            asset_dir.mkdir(parents=True, exist_ok=True)
            p = asset_dir / f"ticket_{ticket_id}_img{i}.{ext}"
            p.write_bytes(r.content)
            out.append((str(p), p.name))
            print(f"[HALO API]   image saved: {p.name} ({len(r.content)} bytes)")
        except Exception as e:
            print(f"[HALO API]   image {i} failed: {e}")
    return out


_STATUS_NAMES = {}


def _status_names(creds):
    """id -> name from /api/Status, fetched once per run."""
    if not _STATUS_NAMES:
        try:
            for st in _get(creds, "/api/Status") or []:
                _STATUS_NAMES[str(st.get("id"))] = st.get("name")
            print(f"[HALO API] status names loaded: {len(_STATUS_NAMES)}")
        except Exception as e:
            print(f"[HALO API] status lookup failed: {e}")
    return _STATUS_NAMES


def fetch_ticket(creds, ticket_id, tickets_dir):
    """Fetch one ticket + actions, download media, write the COMBINED JSON
    (the ingestion unit — parsed by HALO/halo_parser, chunked per-item by
    HALO/halo_serializer)."""
    t = _get(creds, f"/api/Tickets/{ticket_id}", {"includedetails": "true"})
    a = _get(creds, "/api/Actions", {"ticket_id": ticket_id, "count": 200})
    actions = a.get("actions", a if isinstance(a, list) else [])

    images = _download_images(creds, t.get("details_html"), ticket_id, tickets_dir)

    t["status_name"] = _status_names(creds).get(str(t.get("status_id")))
    combined = {
        "ticket": t,
        "actions": actions,
        "images": [{"path": p, "name": n} for p, n in images],
    }
    out = tickets_dir / f"halo_ticket_{ticket_id}.json"
    json.dump(combined, open(out, "w"), indent=1)
    print(f"[HALO API] wrote {out.name} ({len(actions)} actions, "
          f"{len(images)} images)")
    return str(out)


def sync(full=False, only_ticket=None):
    creds, tickets_dir, cfg = _halo_cfg()
    tickets_dir.mkdir(parents=True, exist_ok=True)

    if only_ticket:
        fetch_ticket(creds, only_ticket, tickets_dir)
        return

    state = {"last_sync": None} if full else _load_sync_state(tickets_dir)
    params = {"count": int(cfg.get("page_size", 100)), "open_only": "false"}
    if state.get("last_sync"):
        params["lastupdatefromdate"] = state["last_sync"]
    listing = _get(creds, "/api/Tickets", params)
    tickets = listing.get("tickets", listing if isinstance(listing, list) else [])
    print(f"[HALO API] {len(tickets)} ticket(s) to sync")
    for t in tickets:
        try:
            fetch_ticket(creds, t["id"], tickets_dir)
        except Exception as e:
            print(f"[HALO API] ticket {t.get('id')} failed: {e}")
    _save_sync_state(tickets_dir, {
        "last_sync": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")})
    print("[HALO API] sync complete")


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--ticket" in args:
        sync(only_ticket=args[args.index("--ticket") + 1])
    else:
        sync(full="--full" in args)
