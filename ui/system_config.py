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
    _structured_planner_section()
    ui.separator().classes("my-4")
    _system_settings_section()


# ===========================================================================
# Structured Planner
# ===========================================================================
def _structured_planner_section():
    ui.label("Structured Planner").classes("text-lg font-bold")

    nlp_cfg = _load_json(NLP_CONFIG_PATH, {})
    planner_cfg = nlp_cfg.get("structured_planner", {})

    enabled = ui.checkbox("Enable structured planner", value=bool(planner_cfg.get("enabled", True)))
    ui.label("Allow the LLM planner to generate structured retrieval plans.").classes(
        "text-xs text-gray-500 -mt-2 mb-1")

    dry_run = ui.checkbox("Dry run only", value=bool(planner_cfg.get("dry_run", True)))
    ui.label("Generate and show the plan, but do not let it produce the final answer.").classes(
        "text-xs text-gray-500 -mt-2 mb-1")

    execute = ui.checkbox("Allow planner execution", value=bool(planner_cfg.get("execute", False)))
    ui.label("Allow the structured planner + executor to produce the final answer.").classes(
        "text-xs text-gray-500 -mt-2 mb-1")

    min_confidence = ui.number(
        "Minimum planner confidence", value=float(planner_cfg.get("min_confidence", 0.7)),
        min=0.0, max=1.0, step=0.05,
    ).props("outlined dense").classes("w-64")

    debug_raw = ui.checkbox("Debug raw LLM plan", value=bool(planner_cfg.get("debug_raw_plan", False)))
    ui.label("Print/show raw LLM planner output for troubleshooting.").classes(
        "text-xs text-gray-500 -mt-2 mb-2")

    warn_box = ui.column().classes("w-full")

    def _recompute_warnings():
        warn_box.clear()
        with warn_box:
            if execute.value and dry_run.value:
                ui.label("⚠ Planner execution is enabled, but dry-run is also enabled. "
                          "Dry-run prevents final-answer execution.").classes("text-amber-700")
            if execute.value and not dry_run.value:
                ui.label("⚠ Planner execution is active. Structured planner results may "
                          "become final answers.").classes("text-amber-700")

    execute.on_value_change(lambda: _recompute_warnings())
    dry_run.on_value_change(lambda: _recompute_warnings())
    _recompute_warnings()

    def do_save():
        cfg = _load_json(NLP_CONFIG_PATH, {})
        cfg.setdefault("structured_planner", {})
        cfg["structured_planner"].update({
            "enabled": enabled.value,
            "dry_run": dry_run.value,
            "execute": execute.value,
            "min_confidence": min_confidence.value,
            "debug_raw_plan": debug_raw.value,
        })
        _save_json(NLP_CONFIG_PATH, cfg)
        ui.notify("Structured planner settings saved. Restart the UI if changes don't apply immediately.",
                   type="positive")

    ui.button("Save structured planner settings", on_click=do_save).props("unelevated").classes("mt-2")


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
        nlp["local_llm"] = {
            "base_url": llm_base_url.value,
            "model": llm_model.value,
            "timeout": int(llm_timeout.value),
        }
        _save_json(NLP_CONFIG_PATH, nlp)

        ui.notify("System settings saved.", type="positive")

    ui.button("Save system settings", on_click=do_save).props("unelevated").classes("mt-3")