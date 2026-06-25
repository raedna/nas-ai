import json
from pathlib import Path

import requests


def load_nlp_config():
    path = Path("config/nlp_config.json")

    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_local_llm_config():
    cfg = load_nlp_config()
    llm_cfg = cfg.get("local_llm", {})

    return {
        "base_url": llm_cfg.get("base_url", "http://localhost:1234"),
        "model": llm_cfg.get("model", "meta-llama-3.1-8b-instruct"),
        "timeout": int(llm_cfg.get("timeout", 60)),
    }

def call_local_llm_json(system_prompt, user_prompt, temperature=0.0):
    llm_cfg = get_local_llm_config()

    url = llm_cfg["base_url"].rstrip("/") + "/v1/chat/completions"

    payload = {
        "model": llm_cfg["model"],
        "messages": [
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": user_prompt,
            },
        ],
        "temperature": temperature,
        "stream": False,
    }

    try:
        response = requests.post(
            url,
            json=payload,
            timeout=llm_cfg["timeout"],
        )
        response.raise_for_status()
        data = response.json()
        content = data["choices"][0]["message"]["content"]
        return _parse_json_response(content)
    except Exception:
        return None


def _parse_json_response(content):
    text = str(content or "").strip()

    if text.startswith("```"):
        text = text.strip("`").strip()

        if text.lower().startswith("json"):
            text = text[4:].strip()

    start = text.find("{")
    end = text.rfind("}")

    if start >= 0 and end >= start:
        text = text[start:end + 1]

    return json.loads(text)