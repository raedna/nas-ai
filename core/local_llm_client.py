import json
import requests

from core.system_config import load_system_config


def get_local_llm_config():
    cfg = load_system_config()

    return {
        "base_url": cfg.get("local_llm_base_url", "http://127.0.0.1:1234"),
        "model": cfg.get("local_llm_model", "meta-llama-3.1-8b-instruct"),
        "timeout": int(cfg.get("local_llm_timeout", 60)),
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

    response = requests.post(
        url,
        json=payload,
        timeout=llm_cfg["timeout"],
    )
    response.raise_for_status()

    data = response.json()
    content = data["choices"][0]["message"]["content"]

    return _parse_json_response(content)


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