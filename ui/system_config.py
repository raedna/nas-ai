"""ui/system_config.py — System Config tab.

Three independently-saved blocks:
  1. Structured Planner  (config/nlp_config.json -> structured_planner)
  2. Retrieval / Embeddings / PostgreSQL / Qdrant / LLM / Cross-Encoder
     (config/system.json, plus local_llm in config/nlp_config.json)

Ported from the Streamlit "System Config" tab (core/ui_app.py). Password
fields (Postgres password) are preserved as-is and never shown/edited here —
edit config/system.json directly for that.
"""
import json

from nicegui import ui

from core.paths import SYSTEM_CONFIG_PATH, CONFIG_DIR

NLP_CONFIG_PATH = CONFIG_DIR / "nlp_config.json"


def _load_json(path, default_obj):
    if not path.exists():
        return default_obj
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default_obj


def _save_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def render_system_config_panel():
    _system_settings_section()


# ===========================================================================
# Retrieval / Embeddings / PostgreSQL / Qdrant / LLM / Cross-Encoder
# ===========================================================================
def _system_settings_section():
    system_cfg = _load_json(SYSTEM_CONFIG_PATH, {})
    nlp_cfg = _load_json(NLP_CONFIG_PATH, {})
    llm_cfg = nlp_cfg.get("local_llm", {})
    pg_cfg = system_cfg.get("pgvector", {})
    ce_cfg = system_cfg.get("cross_encoder", {})

    ui.label("Retrieval Settings").classes("text-lg font-bold")
    confidence_threshold = ui.number(
        "Confidence threshold", value=float(system_cfg.get("retrieval_confidence_threshold", 0.105)),
        min=0.0, max=1.0, step=0.005, format="%.3f",
    ).props("outlined dense").classes("w-64")
    ui.label("RRF score below this returns top 5 candidates instead of a single answer.").classes(
        "text-xs text-gray-500 -mt-2 mb-2")

    ui.separator().classes("my-3")
    ui.label("Embeddings").classes("text-lg font-bold")
    embeddings_url = ui.input(
        "Embeddings URL", value=system_cfg.get("embeddings_url", "http://localhost:1234/v1/embeddings"),
    ).props("outlined dense").classes("w-full max-w-xl")
    embeddings_model = ui.input(
        "Embeddings model", value=system_cfg.get("embeddings_model", "text-embedding-bge-large-en-v1.5"),
    ).props("outlined dense").classes("w-full max-w-xl")
    vector_size = ui.number(
        "Vector size", value=int(system_cfg.get("vector_size", 1024)), min=1, max=4096, step=1,
    ).props("outlined dense").classes("w-48")

    ui.separator().classes("my-3")
    ui.label("PostgreSQL").classes("text-lg font-bold")
    with ui.row().classes("w-full gap-2"):
        pg_host = ui.input("Host", value=pg_cfg.get("host", "")).props("outlined dense").classes("w-64")
        pg_port = ui.number("Port", value=int(pg_cfg.get("port", 5433)), min=1, max=65535, step=1).props(
            "outlined dense").classes("w-32")
        pg_dbname = ui.input("Database", value=pg_cfg.get("dbname", "")).props("outlined dense").classes("w-48")
        pg_user = ui.input("User", value=pg_cfg.get("user", "")).props("outlined dense").classes("w-48")
    ui.label("Password is preserved as-is; edit config/system.json directly to change it.").classes(
        "text-xs text-gray-500")

    ui.separator().classes("my-3")
    ui.label("Qdrant (admin reference)").classes("text-lg font-bold")
    qdrant_url = ui.input("Qdrant URL", value=system_cfg.get("qdrant_url", "")).props(
        "outlined dense").classes("w-full max-w-xl")

    ui.separator().classes("my-3")
    ui.label("Language Model (LLM)").classes("text-lg font-bold")
    llm_model = ui.input(
        "LLM model name", value=llm_cfg.get("model", "meta-llama-3.1-8b-instruct"),
    ).props("outlined dense").classes("w-full max-w-xl")
    ui.label("Must match exactly the model identifier in LM Studio.").classes(
        "text-xs text-gray-500 -mt-2 mb-1")
    llm_base_url = ui.input(
        "LLM base URL", value=llm_cfg.get("base_url", "http://localhost:1234"),
    ).props("outlined dense").classes("w-full max-w-xl")
    llm_timeout = ui.number(
        "LLM timeout (seconds)", value=int(llm_cfg.get("timeout", 60)), min=10, max=300, step=10,
    ).props("outlined dense").classes("w-48")
    llm_schema_model = ui.input(
        "Schema model (ingestion)", value=llm_cfg.get("schema_model", ""),
    ).props("outlined dense").classes("w-full max-w-xl")
    ui.label("Bigger model for schema inference at ingest (e.g. qwen3-vl-32b-instruct). "
             "Empty = use the default LLM model.").classes("text-xs text-gray-500 -mt-2 mb-1")
    llm_fast_model = ui.input(
        "Fast model (front-of-pipe)", value=llm_cfg.get("fast_model", ""),
    ).props("outlined dense").classes("w-full max-w-xl")
    ui.label("Small model for intent classification and follow-up rewriting "
             "(e.g. llama-3.2-3b-instruct). Empty = use the default LLM model.").classes(
        "text-xs text-gray-500 -mt-2 mb-1")

    ui.separator().classes("my-3")
    ui.label("Cross-Encoder Reranker").classes("text-lg font-bold")
    ce_enabled = ui.checkbox("Enable cross-encoder reranking", value=bool(ce_cfg.get("enabled", False)))
    ui.label("Uses MiniLM to rerank top-K results for entity_row and docs.").classes(
        "text-xs text-gray-500 -mt-2 mb-1")
    ce_model = ui.input(
        "Cross-encoder model", value=ce_cfg.get("model", "cross-encoder/ms-marco-MiniLM-L-6-v2"),
    ).props("outlined dense").classes("w-full max-w-xl")
    ce_top_k = ui.number(
        "Top-K candidates to rerank", value=int(ce_cfg.get("top_k", 10)), min=3, max=20, step=1,
    ).props("outlined dense").classes("w-48")

    def do_save():
        cfg = _load_json(SYSTEM_CONFIG_PATH, {})
        cfg["retrieval_confidence_threshold"] = confidence_threshold.value
        cfg["embeddings_url"] = embeddings_url.value
        cfg["embeddings_model"] = embeddings_model.value
        cfg["vector_size"] = int(vector_size.value)
        cfg["qdrant_url"] = qdrant_url.value
        cfg["pgvector"] = {
            "host": pg_host.value,
            "port": int(pg_port.value),
            "dbname": pg_dbname.value,
            "user": pg_user.value,
            "password": pg_cfg.get("password", ""),
        }
        cfg["cross_encoder"] = {
            "enabled": ce_enabled.value,
            "model": ce_model.value,
            "apply_to_doc_types": ["entity_row", "procedural", "reference", "mixed"],
            "top_k": int(ce_top_k.value),
        }
        _save_json(SYSTEM_CONFIG_PATH, cfg)

        nlp = _load_json(NLP_CONFIG_PATH, {})
        # MERGE into local_llm — replacing the dict deletes keys this tab
        # doesn't display (schema_model, fast_model, ...). Same failure mode
        # as the schema-save dropping the 'other' role: save must never
        # discard what it doesn't show.
        _llm = dict(nlp.get("local_llm") or {})
        _llm.update({
            "base_url": llm_base_url.value,
            "model": llm_model.value,
            "timeout": int(llm_timeout.value),
        })
        # Optional model overrides: empty input = remove the key (revert to
        # the default model) — never save empty strings.
        for _k, _v in (("schema_model", llm_schema_model.value),
                       ("fast_model", llm_fast_model.value)):
            _v = (_v or "").strip()
            if _v:
                _llm[_k] = _v
            else:
                _llm.pop(_k, None)
        nlp["local_llm"] = _llm
        _save_json(NLP_CONFIG_PATH, nlp)

        ui.notify("System settings saved.", type="positive")

    ui.button("Save system settings", on_click=do_save).props("unelevated").classes("mt-3")