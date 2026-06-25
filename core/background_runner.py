import threading
from core.db import execute, fetchall


def _run_cross_link_task(collection_name, task_id):
    try:
        from core.cross_link_discoverer import discover_cross_links
        from core.cross_link_store import ensure_cross_links_table, save_cross_link_candidates
        from core.concept_vector_builder import build_concept_vectors

        # Clear existing cross-links for this collection as source
        execute(
            "DELETE FROM cross_links WHERE source_collection = %s",
            (collection_name,)
        )

        # Discover and save new cross-links
        ensure_cross_links_table()
        candidates = discover_cross_links(collection_name)
        save_cross_link_candidates(candidates)

        # Rebuild concept vectors
        execute(
            "DELETE FROM concept_vectors WHERE collection = %s",
            (collection_name,)
        )
        build_concept_vectors(collection_name)

        # Mark done
        execute(
            "UPDATE background_tasks SET status='done', finished_at=NOW() WHERE id=%s",
            (task_id,)
        )
        print(f"[BACKGROUND] Cross-link discovery complete for {collection_name}")

    except Exception as e:
        execute(
            "UPDATE background_tasks SET status='failed', finished_at=NOW() WHERE id=%s",
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


def get_running_tasks():
    """Get list of currently running tasks."""
    return fetchall(
        "SELECT collection, task_name, started_at FROM background_tasks WHERE status='running' ORDER BY started_at DESC",
        ()
    )