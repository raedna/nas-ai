import threading
from core.db import execute, fetchall


def _is_cancelled(task_id):
    """Cooperative cancellation: True if the task's row is no longer 'running'
    (e.g. the kill switch set it to 'cancelled')."""
    if not task_id:
        return False
    rows = fetchall("SELECT status FROM background_tasks WHERE id=%s", (task_id,))
    return bool(rows) and rows[0]['status'] != 'running'


def _run_cross_link_task(collection_name, task_id):
    try:
        from core.cross_link_discoverer import discover_cross_links
        from core.cross_link_store import ensure_cross_links_table, save_cross_link_candidates
        from core.concept_vector_builder import build_concept_vectors

        def _abort():
            print(f"[BACKGROUND] Cancelled for {collection_name}")
            return True

        # Clear existing cross-links for this collection as source
        execute(
            "DELETE FROM cross_links WHERE source_collection = %s",
            (collection_name,)
        )
        if _is_cancelled(task_id):
            return _abort()

        # Discover and save new cross-links
        ensure_cross_links_table()
        candidates = discover_cross_links(collection_name)
        if _is_cancelled(task_id):
            return _abort()
        save_cross_link_candidates(candidates)
        if _is_cancelled(task_id):
            return _abort()

        # Rebuild concept vectors
        execute(
            "DELETE FROM concept_vectors WHERE collection = %s",
            (collection_name,)
        )
        build_concept_vectors(collection_name)
        if _is_cancelled(task_id):
            return _abort()

        # Persist wikilinks for doc collections
        from core.related_titles_linker import persist_related_titles_as_crosslinks
        try:
            persist_related_titles_as_crosslinks(collection_name)
        except Exception as e:
            print(f"[BACKGROUND] Wikilink persistence failed: {e}")

        if _is_cancelled(task_id):
            return _abort()

        # CL-03: identifier-mention cross-links (scan this collection's text for
        # known identifiers from other collections, e.g. obsidian -> recon filenames)
        from core.ner_cross_linker import run_identifier_ner
        try:
            run_identifier_ner(collection_name)
        except Exception as e:
            print(f"[BACKGROUND] NER cross-linking failed: {e}")

        # VOCAB-01: refresh this collection's vocabulary (spell-correction
        # lexicon) from the freshly ingested tsvectors.
        try:
            from core.vocab import build_collection_vocab
            build_collection_vocab(collection_name)
        except Exception as e:
            print(f"[BACKGROUND] vocab build failed: {e}")

        # Mark done — only if not cancelled in the meantime
        execute(
            "UPDATE background_tasks SET status='done', finished_at=NOW() WHERE id=%s AND status='running'",
            (task_id,)
        )
        print(f"[BACKGROUND] Cross-link discovery complete for {collection_name}")

    except Exception as e:
        execute(
            "UPDATE background_tasks SET status='failed', finished_at=NOW() WHERE id=%s AND status='running'",
            (task_id,)
        )
        print(f"[BACKGROUND] Cross-link discovery failed for {collection_name}: {e}")


def launch_cross_link_discovery(collection_name):
    """Launch background cross-link discovery + concept vector rebuild after ingest."""
    from core.db import fetchall
    execute(
        "INSERT INTO background_tasks (task_name, collection, status) VALUES (%s, %s, 'running')",
        ('cross_link_discovery', collection_name)
    )
    rows = fetchall(
        "SELECT id FROM background_tasks WHERE collection=%s AND status='running' ORDER BY id DESC LIMIT 1",
        (collection_name,)
    )
    task_id = rows[0]['id'] if rows else None

    thread = threading.Thread(
        target=_run_cross_link_task,
        args=(collection_name, task_id),
        daemon=True
    )
    thread.start()
    print(f"[BACKGROUND] Launched cross-link discovery for {collection_name}")


def is_cross_link_running():
    """Check if any cross-link discovery is currently running."""
    rows = fetchall(
        "SELECT COUNT(*) as n FROM background_tasks WHERE status='running'",
        ()
    )
    return (rows[0]['n'] if rows else 0) > 0


def cancel_running_tasks():
    """Kill switch: mark all running background tasks as cancelled and clear the alert.

    Daemon threads can't be force-killed, so a still-running thread stops cooperatively
    at its next step boundary (see _is_cancelled). If Streamlit already exited, the
    thread is already dead and this simply clears the stale 'running' rows that were
    keeping the alert stuck. Returns the number of tasks cancelled."""
    rows = fetchall("SELECT COUNT(*) AS n FROM background_tasks WHERE status='running'", ())
    n = rows[0]['n'] if rows else 0
    execute(
        "UPDATE background_tasks SET status='cancelled', finished_at=NOW() WHERE status='running'",
        ()
    )
    return n


def get_running_tasks():
    """Get list of currently running tasks."""
    return fetchall(
        "SELECT collection, task_name, started_at FROM background_tasks WHERE status='running' ORDER BY started_at DESC",
        ()
    )