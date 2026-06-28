"""ui/knowledge_graph.py — cross-link review (confirm/reject) + concept inspector."""
from nicegui import ui

from core.ui_data import (
    collection_stats, crosslink_review_groups, set_crosslink_status,
    ignore_crosslink_term, concept_clusters, crosslink_graph,
)


def render_kg_panel():
    names = [r["name"] for r in collection_stats()]

    with ui.tabs().props("dense").classes("w-full") as sub:
        t_graph = ui.tab("Graph")
        t_links = ui.tab("Cross-Links")
        t_concepts = ui.tab("Concept Vectors")
    with ui.tab_panels(sub, value=t_graph).classes("w-full"):
        with ui.tab_panel(t_graph):
            _graph_section()
        with ui.tab_panel(t_links):
            _crosslinks_section(names)
        with ui.tab_panel(t_concepts):
            _concepts_section(names)


def _graph_section():
    with ui.row().classes("w-full items-center gap-3"):
        ui.label("Collection cross-link graph").classes("text-sm font-medium")
        status = ui.toggle(["(all)", "confirmed", "pending_review"], value="(all)").props("dense")
        ui.button(icon="refresh", on_click=lambda: load()).props("flat round dense")
    info = ui.label("").classes("text-xs text-gray-500")
    chart = ui.echart({
        "tooltip": {},
        "legend": [{"data": [], "top": 0}],
        "series": [{
            "type": "graph", "layout": "force", "roam": True,
            "draggable": True, "label": {"show": True, "position": "right"},
            "force": {"repulsion": 280, "edgeLength": 130, "gravity": 0.1},
            "emphasis": {"focus": "adjacency"},
            "data": [], "links": [], "categories": [],
        }],
    }).classes("w-full").style("height: 600px")

    def load():
        g = crosslink_graph(status.value)
        s = chart.options["series"][0]
        s["data"] = g["nodes"]
        s["links"] = g["links"]
        s["categories"] = g["categories"]
        chart.options["legend"][0]["data"] = g["legend"]
        chart.update()
        info.text = f"{len(g['nodes'])} collections · {len(g['links'])} inter-collection link groups"

    status.on_value_change(lambda: load())
    load()


def _crosslinks_section(names):
    with ui.row().classes("w-full items-center gap-3"):
        status = ui.toggle(["pending_review", "confirmed", "rejected"],
                           value="pending_review").props("dense")
        coll = ui.select(["(all)"] + names, value="(all)", label="Collection").props("outlined dense").classes("w-56")
        direction = ui.toggle(["outgoing", "incoming", "both"], value="both").props("dense")
        ui.button(icon="refresh", on_click=lambda: load()).props("flat round dense")
    info = ui.label("").classes("text-sm text-gray-500")
    box = ui.column().classes("w-full gap-1")

    def load():
        box.clear()
        try:
            groups = crosslink_review_groups(status.value, coll.value, direction.value)
        except Exception as exc:
            info.text = f"⚠ {exc}"
            return
        info.text = f"{len(groups)} target groups · status={status.value}"
        with box:
            for g in groups:
                header = (f"→ [{g['target_collection']}] {g['target_display']}  ·  "
                          f"{g['match_type']} avg {g['avg_conf']:.2f}  ·  {g['n']} links")
                with ui.expansion(header).classes("w-full"):
                    src_note = ", ".join(sorted({s["collection"] for s in g["sources"]}))
                    ui.label(f"Sources ({g['n']}) from {src_note}:").classes("text-xs text-gray-500")
                    for s in g["sources"]:
                        ui.label(f"• {s['name']}  (#{s['id']})").classes("text-sm")
                    if g["more"]:
                        ui.label(f"…and {g['more']} more").classes("text-xs text-gray-400")
                    with ui.row().classes("gap-2 mt-1"):
                        if status.value != "confirmed":
                            ui.button("✓ Confirm All",
                                      on_click=lambda g=g: _act(g, status.value, "confirmed")).props("dense color=positive outline")
                        if status.value != "rejected":
                            ui.button("✗ Reject All",
                                      on_click=lambda g=g: _act(g, status.value, "rejected")).props("dense color=negative outline")
                        if status.value == "rejected":
                            ui.button("↩ Pending",
                                      on_click=lambda g=g: _act(g, status.value, "pending_review")).props("dense outline")
                        ui.button("✗ + Ignore term",
                                  on_click=lambda g=g: _act(g, status.value, "rejected", ignore=True)).props("dense flat")

    def _act(g, from_status, to_status, ignore=False):
        set_crosslink_status(g["target_collection"], g["target_identifier"],
                             g["match_type"], from_status, to_status)
        if ignore:
            ignore_crosslink_term(g["target_identifier"])
        ui.notify(f"{to_status}: {g['target_display']}" + (" (+ignored)" if ignore else ""),
                  type="positive")
        load()

    load()


def _concepts_section(names):
    with ui.row().classes("w-full items-center gap-3"):
        coll = ui.select(names, value=(names[0] if names else None),
                         label="Collection").props("outlined dense").classes("w-56")
        ui.button(icon="refresh", on_click=lambda: load()).props("flat round dense")
    info = ui.label("").classes("text-sm text-gray-500")
    box = ui.column().classes("w-full gap-1")

    def load():
        box.clear()
        if not coll.value:
            return
        rows = concept_clusters(coll.value)
        info.text = f"{len(rows)} clusters · grouped by {rows[0]['group_field'] if rows else '—'}"
        with box:
            for r in rows:
                with ui.expansion(f"{r['group_value']}  ·  cluster {r['cluster_id']}").classes("w-full"):
                    ui.label(str(r["preview"] or "")).classes("text-sm whitespace-pre-wrap text-gray-600")

    load()
