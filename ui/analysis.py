from nicegui import ui

from core.analysis.analyzers.fix.analyzer import analyze_fix_message


def render_analysis_panel():
    ui.label("Analysis Engine").classes("text-xl font-bold mb-2")
    ui.label(
        "Analyze structured business or technical artifacts. "
        "First analyzer: FIX Message."
    ).classes("text-sm text-gray-500 mb-4")

    analyzer_select = ui.select(
        options=["FIX Message"],
        value="FIX Message",
        label="Analyzer",
    ).props("outlined").classes("w-64")

    raw_input = ui.textarea(
        label="Input",
        placeholder="Paste raw FIX, SOH-delimited FIX, pipe-delimited FIX, or copied table text...",
    ).props("outlined autogrow").classes("w-full mt-4")

    result_area = ui.column().classes("w-full mt-4")

    def run_analysis():
        result_area.clear()

        if analyzer_select.value != "FIX Message":
            ui.notify("Analyzer not implemented yet.", type="warning")
            return

        result = analyze_fix_message(raw_input.value or "")

        with result_area:
            ui.label("Plain-English Summary").classes("text-lg font-bold")
            ui.markdown(result.get("summary") or "No summary generated.").classes(
                "p-3 bg-gray-100 rounded w-full"
            )

            warnings = result.get("warnings") or []
            if warnings:
                ui.label("Warnings").classes("text-lg font-bold mt-4")
                for warning in warnings:
                    ui.label(warning).classes("text-red-600")

            ui.label("Business Object").classes("text-lg font-bold mt-4")
            ui.json_editor({"content": {"json": result.get("business_object", {})}}).classes("w-full")

            ui.label("Decoded Tags").classes("text-lg font-bold mt-4")

            rows = result.get("decoded_rows") or []

            ui.table(
                columns=[
                    {"name": "tag", "label": "Tag", "field": "tag", "align": "left"},
                    {"name": "tag_name", "label": "Tag Name", "field": "tag_name", "align": "left"},
                    {"name": "value", "label": "Value", "field": "value", "align": "left"},
                    {"name": "value_name", "label": "Value Name", "field": "value_name", "align": "left"},
                    {"name": "description", "label": "Description", "field": "description", "align": "left"},
                ],
                rows=rows,
                pagination=20,
            ).classes("w-full")

    ui.button("Analyze", on_click=run_analysis).props("color=primary").classes("mt-3")