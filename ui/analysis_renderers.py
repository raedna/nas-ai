import json

from nicegui import ui

def render_compare_result(result: dict):
    relationship = result.get("relationship") or {}
    counts = result.get("difference_counts_by_category") or {}
    difference_rows = result.get("difference_rows") or []
    comparison_rows = result.get("comparison_rows") or []

    comparison_rows = [
        {
            **row,
            "_seq": index,
            "_ignored": False,
            "_tag_sort": int(str(row.get("tag", "999999")).split("#")[0])
            if str(row.get("tag", "")).split("#")[0].isdigit()
            else 999999,
        }
        for index, row in enumerate(comparison_rows)
    ]

    ui.label("Comparison Summary").classes("text-lg font-bold")

    summary_text = str(result.get("summary") or "").strip()

    ui.markdown(summary_text or "No summary generated.").classes(
        "p-3 bg-gray-100 rounded w-full"
    )

    with ui.card().classes("w-full mt-4"):
        ui.label("Relationship").classes("text-md font-bold")

        rel = relationship.get("relationship") or "Unknown"
        interpretation = relationship.get("interpretation") or ""
        reasons = relationship.get("reasons") or []
        time_diff = relationship.get("time_difference_seconds")

        ui.label(f"Relationship: {rel}")

        if rel == "Weak / unrelated":
            ui.label(
                "These messages do not appear to belong to the same order/trade sequence. "
                "The table below is a raw tag-by-tag comparison, not an execution lifecycle comparison."
            ).classes("text-red-600 font-bold")

        if reasons:
            ui.label("Reasons: " + ", ".join(str(r) for r in reasons))

        if interpretation:
            ui.label("Interpretation: " + interpretation)

        if time_diff is not None:
            ui.label(f"Time difference: {time_diff} seconds")

        if relationship.get("routing_reversed"):
            ui.label("Routing: reversed").classes("text-orange-700 font-bold")
        else:
            ui.label("Routing: not reversed")

    if counts:
        with ui.card().classes("w-full mt-4"):
            ui.label("Difference Counts").classes("text-md font-bold")
            count_text = " | ".join(
                f"{category}: {count}"
                for category, count in sorted(counts.items())
            )
            ui.label(count_text)

    ui.label("Compared Values").classes("text-lg font-bold mt-4")

    if not comparison_rows:
        ui.label("No compared values found.").classes("text-orange-700")
        return

    if not difference_rows:
        ui.label("No differences found. Showing all compared tags below.").classes("text-green-700")

    columns = [
        {"name": "_seq", "label": "#", "field": "_seq", "align": "left", "sortable": True},
        {"name": "_ignored", "label": "Ignore", "field": "_ignored", "align": "center"},
        {"name": "display_key", "label": "Tag", "field": "_tag_sort", "align": "left", "sortable": True},
        {"name": "tag_name", "label": "Tag Name", "field": "tag_name", "align": "left", "sortable": True},
        {"name": "category", "label": "Category", "field": "category", "align": "left", "sortable": True},
        {"name": "display_a", "label": "Message 1 Value", "field": "display_a", "align": "left", "sortable": True},
        {"name": "display_b", "label": "Message 2 Value", "field": "display_b", "align": "left", "sortable": True},
        {"name": "status", "label": "Status", "field": "status", "align": "left", "sortable": True},
        {"name": "warning_a", "label": "Message 1 Warning", "field": "warning_a", "align": "left", "sortable": True},
        {"name": "warning_b", "label": "Message 2 Warning", "field": "warning_b", "align": "left", "sortable": True},
    ]

    ui.label(
        "Default order follows Message 1 sequence. Click column headers to sort."
    ).classes("text-sm text-gray-500")

    reset_sort_button = ui.button("Reset sorting").props("outline size=sm")

    with ui.element("div").classes("w-full border rounded decoded-values-scroll"):
        compare_table = ui.table(
            columns=columns,
            rows=comparison_rows,
            row_key="key",
            pagination={
                "rowsPerPage": 0,
            },
        ).classes("w-full")

        def reset_compare_sorting():
            compare_table.rows = sorted(
                comparison_rows,
                key=lambda row: row.get("_seq", 0),
            )
            compare_table.pagination = {
                "sortBy": "_seq",
                "descending": False,
                "rowsPerPage": 0,
            }
            compare_table.update()

        reset_sort_button.on_click(reset_compare_sorting)

        compare_table.add_slot("body", r"""
        <q-tr
          :props="props"
          :class="(props.row.status !== 'Same' && !props.row._ignored) ? 'text-red' : ''"
        >
          <q-td
            v-for="col in props.cols"
            :key="col.name"
            :props="props"
            :style="(
              col.name === 'warning_a' ||
              col.name === 'warning_b'
            )
              ? 'max-width: 160px; min-width: 90px; white-space: normal; word-break: break-word; overflow-wrap: anywhere; vertical-align: top; font-size: 11px;'
              : (
                  col.name === 'display_a' ||
                  col.name === 'display_b'
                )
                  ? 'max-width: 260px; min-width: 160px; white-space: normal; word-break: break-word; overflow-wrap: anywhere; vertical-align: top; font-size: 12px;'
                  : 'white-space: nowrap; vertical-align: top;'"
          >
            <q-checkbox
              v-if="col.name === '_ignored'"
              v-model="props.row._ignored"
              dense
            />
            <template v-else>
              {{ col.name === 'display_key' ? props.row.display_key : col.value }}
            </template>
          </q-td>
        </q-tr>
        """)

def render_sequence_result(result: dict):
    ui.separator()

    ui.label("Sequence Summary").classes("text-lg font-semibold")

    ui.textarea(
        value=result.get("summary") or "",
    ).props("readonly outlined autogrow").classes("w-full")

    timeline_summary = result.get("timeline_summary") or ""

    if timeline_summary:
        ui.label("Timeline Summary").classes("text-lg font-semibold q-mt-md")

        ui.textarea(
            value=timeline_summary,
        ).props("readonly outlined autogrow").classes("w-full")

    warnings = result.get("warnings") or []

    if warnings:
        ui.label("Warnings").classes("text-lg font-semibold q-mt-md text-red")

        for warning in warnings:
            ui.label(f"- {warning}").classes("text-red")

    messages = result.get("messages") or []

    if messages:
        ui.label("Messages").classes("text-lg font-semibold q-mt-md")

        rows = []

        for msg in messages:
            rows.append({
                "message_index": msg.get("message_index"),
                "msg_seq_num": msg.get("msg_seq_num"),
                "msg_type": msg.get("msg_type"),
                "route": f"{msg.get('sender') or ''} → {msg.get('target') or ''}",
                "time": msg.get("transact_time") or msg.get("sending_time"),
                "cl_ord_id": msg.get("cl_ord_id"),
                "order_id": msg.get("order_id"),
                "exec_type": msg.get("exec_type"),
                "ord_status": msg.get("ord_status"),
                "symbol": msg.get("symbol"),
                "order_qty": msg.get("order_qty"),
                "cum_qty": msg.get("cum_qty"),
                "leaves_qty": msg.get("leaves_qty"),
            })

        columns = [
            {"name": "message_index", "label": "#", "field": "message_index", "align": "left", "sortable": True},
            {"name": "msg_seq_num", "label": "Seq", "field": "msg_seq_num", "align": "left", "sortable": True},
            {"name": "msg_type", "label": "MsgType", "field": "msg_type", "align": "left", "sortable": True},
            {"name": "route", "label": "Route", "field": "route", "align": "left", "sortable": True},
            {"name": "time", "label": "Time", "field": "time", "align": "left", "sortable": True},
            {"name": "cl_ord_id", "label": "ClOrdID", "field": "cl_ord_id", "align": "left", "sortable": True},
            {"name": "order_id", "label": "OrderID", "field": "order_id", "align": "left", "sortable": True},
            {"name": "exec_type", "label": "ExecType", "field": "exec_type", "align": "left", "sortable": True},
            {"name": "ord_status", "label": "OrdStatus", "field": "ord_status", "align": "left", "sortable": True},
            {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left", "sortable": True},
            {"name": "order_qty", "label": "OrderQty", "field": "order_qty", "align": "left", "sortable": True},
            {"name": "cum_qty", "label": "CumQty", "field": "cum_qty", "align": "left", "sortable": True},
            {"name": "leaves_qty", "label": "LeavesQty", "field": "leaves_qty", "align": "left", "sortable": True},
        ]

        ui.table(
            columns=columns,
            rows=rows,
            row_key="message_index",
            pagination={"rowsPerPage": 0},
        ).classes("w-full")