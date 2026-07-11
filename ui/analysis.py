from pathlib import Path
from tempfile import NamedTemporaryFile
import json
from nicegui import ui
from core.analysis.analyzers.fix.fix_insights import build_sequence_insights
from core.analysis.analyzers.fix.analyzer import analyze_fix_message
from core.analysis.analyzers.fix.comparator import compare_fix_messages
from IMAGES.image_parser import parse_image
from PDF.pdf_parser import parse_pdf
import base64
from fastapi import Request
from nicegui import app
from core.analysis.ocr.rapidocr_adapter import ocr_image_with_rapidocr
from core.analysis.ocr.pdf_rapidocr_adapter import ocr_pdf_with_rapidocr
from ui.analysis_renderers import render_compare_result, render_sequence_result
from core.analysis.analyzers.fix.sequence_analyzer import analyze_fix_sequence
from core.analysis.storage.fix_repository import (
    save_fix_analysis_result,
    list_fix_analysis_sessions,
    list_fix_analysis_messages,
    list_fix_message_tags,
    find_related_saved_fix_messages,
    build_related_match_messages_from_result,
    get_fix_analysis_message,
    update_fix_analysis_session_note,
    delete_fix_analysis_session,
)


DEBUG_FIX_ANALYSIS_UI = True


def debug_print(*args, **kwargs):
    if DEBUG_FIX_ANALYSIS_UI:
        print(*args, **kwargs, flush=True)

@app.post("/analysis/clipboard-image-ocr")
async def clipboard_image_ocr(request: Request):
    try:
        data = await request.json()

        image_data_url = data.get("image_data_url", "")
        if "," not in image_data_url:
            return {"ok": False, "error": "Invalid image data."}

        header, encoded = image_data_url.split(",", 1)

        suffix = ".png"
        if "image/jpeg" in header:
            suffix = ".jpg"
        elif "image/webp" in header:
            suffix = ".webp"

        image_bytes = base64.b64decode(encoded)

        with NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(image_bytes)
            tmp_path = tmp.name

        try:
            parsed = ocr_image_with_rapidocr(tmp_path)
            extracted_text = parsed.get("text", "")

            if extracted_text:

                return {
                    "ok": True,
                    "text": extracted_text or "",
                    "engine": "rapidocr",
                }

        except Exception as rapid_ex:
            print(f"=== CLIPBOARD RAPIDOCR FAILED, FALLING BACK: {rapid_ex} ===", flush=True)

        parsed = parse_image(tmp_path, enable_ocr=True)

        extracted_text = (
            parsed.get("content", {}).get("ocr_text")
            or parsed.get("text")
            or "\n".join(
                block.get("text", "")
                for block in parsed.get("blocks", [])
                if block.get("text") or block.get("type") == "ocr_text"
            )
        )

        print("=== CLIPBOARD FALLBACK IMAGE OCR USED ===", flush=True)
        print("=== CLIPBOARD OCR TEXT START ===", flush=True)
        print(extracted_text, flush=True)
        print("=== CLIPBOARD OCR TEXT END ===", flush=True)

        return {
            "ok": True,
            "text": extracted_text or "",
            "engine": "fallback",
        }

    except Exception as ex:
        print(f"=== CLIPBOARD OCR FAILED: {ex} ===", flush=True)
        return {
            "ok": False,
            "error": str(ex),
        }

def _get_upload_filename(e) -> str:
    file_obj = getattr(e, "file", None)

    return (
        getattr(e, "name", None)
        or getattr(e, "filename", None)
        or getattr(file_obj, "name", None)
        or getattr(file_obj, "filename", None)
        or "uploaded_image.png"
    )


async def _get_upload_bytes(e) -> bytes:
    content = getattr(e, "content", None)

    if content is not None:
        if hasattr(content, "read"):
            data = content.read()
            if hasattr(data, "__await__"):
                data = await data
            return data

        if isinstance(content, bytes):
            return content

    file_obj = getattr(e, "file", None)

    if file_obj is not None and hasattr(file_obj, "read"):
        data = file_obj.read()
        if hasattr(data, "__await__"):
            data = await data
        return data

    raise ValueError("Could not read uploaded file content.")


def _extract_text_from_uploaded_file(tmp_path: str, suffix: str) -> str:
    if suffix == ".pdf":
        try:
            parsed = ocr_pdf_with_rapidocr(tmp_path)
            extracted = parsed.get("text", "")

            if extracted:
                print(f"=== PDF OCR USED: {parsed.get('engine')} ===", flush=True)
                print("=== PDF OCR TEXT START ===", flush=True)
                print(extracted, flush=True)
                print("=== PDF OCR TEXT END ===", flush=True)
                return extracted

        except Exception as pdf_rapid_ex:
            print(f"=== PDF RAPIDOCR FAILED, FALLING BACK TO parse_pdf: {pdf_rapid_ex} ===", flush=True)

        parsed = parse_pdf(tmp_path)

        extracted = (
            parsed.get("text")
            or parsed.get("content", {}).get("text")
            or "\n".join(
                block.get("text", "")
                for block in parsed.get("blocks", [])
                if block.get("text")
            )
        )

        print("=== PDF FALLBACK parse_pdf USED ===", flush=True)
        return extracted

    try:
        parsed = ocr_image_with_rapidocr(tmp_path)
        extracted = parsed.get("text", "")

        if extracted:
            print("=== RAPIDOCR USED ===", flush=True)
            return extracted

    except Exception as ex:
        print(f"=== RAPIDOCR FAILED, FALLING BACK TO EXISTING IMAGE OCR: {ex} ===", flush=True)

    parsed = parse_image(tmp_path, enable_ocr=True)
    return (
        parsed.get("content", {}).get("ocr_text")
        or parsed.get("text")
        or "\n".join(
            block.get("text", "")
            for block in parsed.get("blocks", [])
            if block.get("text") or block.get("type") == "ocr_text"
        )
    )

def render_analysis_panel():

    ui.add_head_html("""
    <style>
    .decoded-row-known {
        background-color: #ecfdf3;
    }

    .decoded-row-custom {
        background-color: #f4ecff;
    }

    .decoded-row-warning {
        background-color: #fff4e5;
    }

    .decoded-row-known:hover,
    .decoded-row-custom:hover,
    .decoded-row-warning:hover {
        filter: brightness(0.97);
    }
    </style>
    """)

    MAX_COMPARE_MESSAGES = 10

    # --- FIX UI REDESIGN WORKSPACE SKELETON ---
    with ui.row().classes("w-full no-wrap items-start"):

        # 1. CONTROL SECTION
        with ui.column().classes("q-pa-md bg-grey-2").style(
            "width: 260px; min-width: 260px; position: sticky; top: 0; height: 100vh; overflow-y: auto;"
        ):
            ui.label("FIX Workspace").classes("text-xl font-bold")
            ui.label(f"Compare basket: up to {MAX_COMPARE_MESSAGES} messages").classes(
                "text-xs text-grey-7"
            )
            ui.separator().classes("q-my-md")

            ui.label("Control").classes("text-md font-semibold")
            control_area = ui.column().classes("w-full q-gutter-sm")

        # Main workspace
        with ui.column().classes("w-full q-pa-md"):

            # 2. CONTEXT / SELECTION SECTION
            with ui.card().classes("w-full q-pa-md q-mb-md"):
                ui.label("Context / Selection").classes("text-lg font-semibold")
                ui.label(
                    "Saved sessions, message selector, Message A/B, and comparison basket will move here."
                ).classes("text-sm text-grey-7")
                context_area = ui.column().classes("w-full q-mt-md")

            # 3. WORKING WINDOWS SECTION
            with ui.card().classes("w-full q-pa-md q-mb-md"):
                ui.label("Working Windows").classes("text-lg font-semibold")
                ui.label(
                    "Raw input, decoded tags, selected tag details, and message windows will move here."
                ).classes("text-sm text-grey-7")
                working_area = ui.column().classes("w-full q-mt-md")

            # 4. INFO / REPORTING SECTION
            with ui.card().classes("w-full q-pa-md q-mb-md"):
                ui.label("Info / Reporting").classes("text-lg font-semibold")
                ui.label(
                    "Analysis output, warnings, insights, related messages, and future Ask integration will move here."
                ).classes("text-sm text-grey-7")
                reporting_area = ui.column().classes("w-full q-mt-md")

    result_area = working_area
    saved_area = context_area

    ui.label("Analysis Engine").classes("text-xl font-bold mb-2")
    with control_area:
        analyzer_select = ui.select(
            options=["FIX Message"],
            value="FIX Message",
            label="Analyzer",
        ).props("outlined dense").classes("w-full")

        analysis_mode = ui.select(
            ["Single Message", "Compare Messages", "Multi-Message Sequence"],
            value="Single Message",
            label="Analysis Mode",
        ).props("outlined dense").classes("w-full")


    with working_area:
        ui.label(
            "Paste FIX text below, upload an image/PDF, or paste a screenshot into the screenshot box. "
            "Extracted text will appear in the input box for review before analysis."
        ).classes("text-sm text-gray-500 mb-4")

        raw_input = ui.textarea(
            label="FIX message(s) / extracted OCR text",
            placeholder="Paste one FIX message, multiple FIX messages, OCR text, or upload image/PDF...",
        ).props("outlined").classes("w-full mt-4 analysis-fix-input max-h-64 overflow-auto")

        ui.label(
            "For Multi-Message Sequence, paste or OCR multiple FIX messages into this same box."
        ).classes("text-xs text-grey-7")

        compare_input_box = ui.textarea(
            label="Message B / Compare Against",
            placeholder="Paste the second FIX message here for comparison...",
        ).props("outlined").classes("w-full mt-4")

        compare_input_box.visible = False


    with context_area:
        save_note_input = ui.textarea(
            label="Save note / reason *",
            placeholder="Required. Example: 76250 - CFD Issue Citi batch 13",
        ).props("outlined autogrow").classes("w-full")

    async def handle_analysis_upload(e, target_box=None):
        print("=== ANALYSIS UPLOAD CALLED ===", flush=True)

        try:
            filename = _get_upload_filename(e)
            suffix = Path(filename).suffix.lower() or ".png"

            print(f"Uploaded filename: {filename}", flush=True)
            print(f"Detected suffix: {suffix}", flush=True)

            file_bytes = await _get_upload_bytes(e)

            with NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(file_bytes)
                tmp_path = tmp.name

            print(f"Temporary file: {tmp_path}", flush=True)

            extracted_text = _extract_text_from_uploaded_file(tmp_path, suffix)

            print("=== EXTRACTED TEXT START ===", flush=True)
            print(extracted_text, flush=True)
            print("=== EXTRACTED TEXT END ===", flush=True)

            if not extracted_text:
                ui.notify("No text was extracted from the uploaded file.", type="warning")
                return

            target = target_box or raw_input
            target.value = extracted_text
            target.update()
            result_area.clear()
            ui.notify("Text extracted. Review it, then click Analyze.", type="positive")

        except Exception as ex:
            ui.notify(f"OCR failed: {ex}", type="negative")
            print(f"=== ANALYSIS UPLOAD FAILED: {ex} ===", flush=True)

    ui.upload(
        label="Upload FIX screenshot/image/PDF into main input",
        on_upload=lambda e: handle_analysis_upload(e, target_box=raw_input),
        auto_upload=True,
        max_files=1,
    ).props("accept=image/*,.pdf").classes("w-full mt-2")

    upload_b = ui.upload(
        label="Upload Message B screenshot/image or PDF",
        on_upload=lambda e: handle_analysis_upload(e, target_box=compare_input_box),
        auto_upload=True,
        max_files=1,
    ).props("accept=image/*,.pdf").classes("w-full mt-2")

    upload_b.visible = False

    def update_analysis_mode_visibility():
        is_compare = analysis_mode.value == "Compare Messages"
        compare_input_box.visible = is_compare
        upload_b.visible = is_compare

    analysis_mode.on_value_change(lambda _: update_analysis_mode_visibility())
    update_analysis_mode_visibility()

    ui.html("""
    <div id="analysis-screenshot-paste-box-a"
        tabindex="0"
        style="
            border: 2px dashed #aaa;
            border-radius: 8px;
            padding: 18px;
            margin-top: 12px;
            text-align: center;
            color: #666;
            cursor: pointer;
            background: #fafafa;
         ">
        Click here, then paste Message A screenshot with Cmd+V / Ctrl+V
    </div>

    <div id="analysis-screenshot-paste-box-b"
        tabindex="0"
        style="
            display: none;
            border: 2px dashed #aaa;
            border-radius: 8px;
            padding: 18px;
            margin-top: 12px;
            text-align: center;
            color: #666;
            cursor: pointer;
            background: #fafafa;
         ">
        Click here, then paste Message B screenshot with Cmd+V / Ctrl+V
    </div>
    """).classes("w-full")

    ui.add_body_html("""
    <script>
    setTimeout(() => {
        const boxA = document.getElementById('analysis-screenshot-paste-box-a');
        const boxB = document.getElementById('analysis-screenshot-paste-box-b');

        if (!boxA && !boxB) return;

        let activeBox = null;

        function idleText(box) {
            if (box.id === 'analysis-screenshot-paste-box-b') {
                return 'Click here, then paste Message B screenshot with Cmd+V / Ctrl+V';
            }
            return 'Click here, then paste Message A screenshot with Cmd+V / Ctrl+V';
        }

        function setPasteBoxIdle(box) {
            if (!box) return;
            box.style.borderColor = '#aaa';
            box.style.background = '#fafafa';
            box.innerText = idleText(box);
        }

        function isCompareMode() {
            const text = document.body.innerText || '';
            return text.includes('Compare Messages') && text.includes('Message B / Compare Against');
        }

        function updateBoxVisibility() {
            if (!boxB) return;

            const messageBTextareaVisible = Array.from(document.querySelectorAll('textarea')).some(t => {
                const label = t.closest('.q-field')?.innerText || '';
                return label.includes('Message B') && t.offsetParent !== null;
            });

            boxB.style.display = messageBTextareaVisible ? 'block' : 'none';
        }

        function setAllIdle() {
            activeBox = null;
            updateBoxVisibility();
            setPasteBoxIdle(boxA);
            setPasteBoxIdle(boxB);
        }

        setInterval(updateBoxVisibility, 500);

        function setPasteBoxActive(box) {
            activeBox = box;
            box.focus();
            box.style.borderColor = '#1976d2';
            box.style.background = '#eef6ff';
            box.innerText = 'Paste now with Cmd+V / Ctrl+V';
        }

        async function setTextareaValue(text, target) {
            let textarea = null;

            if (target === 'b') {
                const textareas = Array.from(document.querySelectorAll('textarea'));
                textarea = textareas.find(t => {
                    const label = t.closest('.q-field')?.innerText || '';
                    return label.includes('Message B');
                });
            } else {
                textarea = document.querySelector('textarea.analysis-fix-input');
            }

            if (!textarea) {
                const textareas = Array.from(document.querySelectorAll('textarea'));
                textarea = textareas.find(t => t.offsetParent !== null);
            }

            if (!textarea) {
                alert('Could not find target analysis input box.');
                return;
            }

            textarea.value = text;
            textarea.dispatchEvent(new Event('input', { bubbles: true }));
            textarea.dispatchEvent(new Event('change', { bubbles: true }));
        }

        function wireBox(box) {
            if (!box) return;

            box.addEventListener('click', (event) => {
                event.stopPropagation();
                setInterval(updateBoxVisibility, 500);
                setAllIdle();
                setPasteBoxActive(box);
            });
        }

        wireBox(boxA);
        wireBox(boxB);

        document.addEventListener('click', (event) => {
            if (
                (!boxA || !boxA.contains(event.target)) &&
                (!boxB || !boxB.contains(event.target))
            ) {
                setAllIdle();
            }
        });

        document.addEventListener('paste', async (event) => {
            if (!activeBox) return;

            const items = event.clipboardData && event.clipboardData.items;
            if (!items) {
                alert('No clipboard items found.');
                return;
            }

            for (const item of items) {
                if (item.type && item.type.startsWith('image/')) {
                    event.preventDefault();

                    activeBox.innerText = 'Running OCR on pasted screenshot...';
                    activeBox.style.borderColor = '#f59e0b';
                    activeBox.style.background = '#fff7ed';

                    const file = item.getAsFile();
                    const reader = new FileReader();

                    reader.onload = async () => {
                        try {
                            const response = await fetch('/analysis/clipboard-image-ocr', {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json',
                                },
                                body: JSON.stringify({
                                    image_data_url: reader.result,
                                }),
                            });

                            const result = await response.json();

                            if (!result.ok) {
                                alert('OCR failed: ' + (result.error || 'Unknown error'));
                                setPasteBoxActive(activeBox);
                                return;
                            }

                            if (!result.text) {
                                alert('No OCR text was extracted from the pasted screenshot.');
                                setPasteBoxActive(activeBox);
                                return;
                            }

                            const target = activeBox.id === 'analysis-screenshot-paste-box-b' ? 'b' : 'a';

                            await setTextareaValue(result.text, target);

                            activeBox.innerText = 'OCR text inserted. Review it, then click Analyze.';
                            activeBox.style.borderColor = '#16a34a';
                            activeBox.style.background = '#f0fdf4';

                        } catch (err) {
                            alert('OCR request failed: ' + err);
                            setPasteBoxActive(activeBox);
                        }
                    };

                    reader.readAsDataURL(file);
                    return;
                }
            }

            alert('No image found in clipboard.');
        });

        setAllIdle();
    }, 500);
    </script>
    """)

    def render_related_saved_matches(result):
        messages = build_related_match_messages_from_result(result)

        if not messages:
            return

        related_rows = []

        for message in messages:
            message_index = message.get("message_index")

            matches = find_related_saved_fix_messages(
                message,
                exclude_session_id=None,
                limit=20,
            )

            for match in matches:
                related_rows.append({
                    "source_message": message_index,
                    "message_id": match.get("message_id"),
                    "current_raw_text": message.get("raw_text") or result.get("raw_text") or "",
                    "match_strength": match.get("match_strength"),
                    "match_reason": match.get("match_reason"),
                    "session_id": match.get("session_id"),
                    "message_index": match.get("message_index"),
                    "msg_type": match.get("msg_type"),
                    "route": f"{match.get('sender') or ''} → {match.get('target') or ''}",
                    "cl_ord_id": match.get("cl_ord_id"),
                    "order_id": match.get("order_id"),
                    "exec_id": match.get("exec_id"),
                    "symbol": match.get("symbol"),
                    "security_id": match.get("security_id"),
                    "security_exchange": match.get("security_exchange"),
                    "security_type": match.get("security_type"),
                    "ex_destination": match.get("ex_destination"),
                    "created_at": str(match.get("created_at") or ""),
                })

        ui.separator().classes("q-mt-md")

        ui.label("Related Saved FIX Messages").classes("text-lg font-bold q-mt-md")

        if not related_rows:
            ui.label("No related saved FIX messages found.").classes("text-sm text-grey-7")
            return

        compare_related_area = ui.column().classes("w-full q-mt-md")

        def compare_selected_related_message():
            selected_rows = list(related_table.selected or [])

            if len(selected_rows) != 1:
                ui.notify("Select one related saved message first.", color="warning")
                return

            selected = selected_rows[0]

            saved_message_id = selected.get("message_id")
            current_raw_text = selected.get("current_raw_text") or ""

            if not saved_message_id:
                ui.notify("Could not read selected saved message id.", color="negative")
                return

            saved_message = get_fix_analysis_message(saved_message_id)

            if not saved_message:
                ui.notify("Could not load selected saved message.", color="negative")
                return

            saved_raw_text = saved_message.get("raw_text") or ""

            if not current_raw_text or not saved_raw_text:
                ui.notify("Current or saved message raw text is missing.", color="negative")
                return

            comparison = compare_fix_messages(
                current_raw_text,
                saved_raw_text,
            )

            compare_related_area.clear()

            with compare_related_area:
                ui.label(
                    f"Comparison: Current Message {selected.get('source_message')} "
                    f"vs Saved Session {selected.get('session_id')} "
                    f"Message {selected.get('message_index')}"
                ).classes("text-lg font-semibold q-mt-md")

                render_compare_result(comparison)

        ui.label(
            "Select one related saved message, then click Compare Selected Related Message."
        ).classes("text-sm text-blue-7 font-bold q-mt-md")

        ui.button(
            "Compare Selected Related Message",
            on_click=compare_selected_related_message,
        ).props("color=primary outline")

        related_table = ui.table(
            columns=[
                {"name": "source_message", "label": "Current Msg", "field": "source_message", "align": "left", "sortable": True},
                {"name": "match_strength", "label": "Strength", "field": "match_strength", "align": "left", "sortable": True},
                {"name": "match_reason", "label": "Reason", "field": "match_reason", "align": "left", "sortable": True},
                {"name": "session_id", "label": "Saved Session", "field": "session_id", "align": "left", "sortable": True},
                {"name": "message_index", "label": "Saved Msg", "field": "message_index", "align": "left", "sortable": True},
                {"name": "msg_type", "label": "MsgType", "field": "msg_type", "align": "left", "sortable": True},
                {"name": "route", "label": "Route", "field": "route", "align": "left", "sortable": True},
                {"name": "cl_ord_id", "label": "ClOrdID", "field": "cl_ord_id", "align": "left", "sortable": True},
                {"name": "order_id", "label": "OrderID", "field": "order_id", "align": "left", "sortable": True},
                {"name": "exec_id", "label": "ExecID", "field": "exec_id", "align": "left", "sortable": True},
                {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left", "sortable": True},
                {"name": "security_id", "label": "SecurityID", "field": "security_id", "align": "left", "sortable": True},
                {"name": "security_exchange", "label": "Exchange", "field": "security_exchange", "align": "left", "sortable": True},
                {"name": "security_type", "label": "SecType", "field": "security_type", "align": "left", "sortable": True},
                {"name": "ex_destination", "label": "ExDestination", "field": "ex_destination", "align": "left", "sortable": True},
                {"name": "created_at", "label": "Saved At", "field": "created_at", "align": "left", "sortable": True},
            ],
            rows=related_rows,
            row_key="message_id",
            selection="single",
            pagination=10,
        ).classes("w-full")

def render_analysis_panel():
    MAX_COMPARE_MESSAGES = 10

    # --- FIX UI REDESIGN WORKSPACE SKELETON ---
    with ui.row().classes("w-full no-wrap items-start"):

        # 1. CONTROL SECTION
        with ui.column().classes("q-pa-md bg-grey-2").style(
            "width: 260px; min-width: 260px; position: sticky; top: 0; height: 100vh; overflow-y: auto;"
        ):
            ui.label("FIX Workspace").classes("text-xl font-bold")
            ui.label(f"Compare basket: up to {MAX_COMPARE_MESSAGES} messages").classes(
                "text-xs text-grey-7"
            )
            ui.separator().classes("q-my-md")

            ui.label("Control").classes("text-md font-semibold")
            control_area = ui.column().classes("w-full q-gutter-sm")

        # Main workspace
        with ui.column().classes("w-full q-pa-md"):

            # 2. CONTEXT / SELECTION SECTION
            with ui.card().classes("w-full q-pa-md q-mb-md"):
                ui.label("Context / Selection").classes("text-lg font-semibold")
                ui.label(
                    "Saved sessions, message selector, Message A/B, and comparison basket will move here."
                ).classes("text-sm text-grey-7")
                context_area = ui.column().classes("w-full q-mt-md")

            # 3. WORKING WINDOWS SECTION
            with ui.card().classes("w-full q-pa-md q-mb-md"):
                ui.label("Working Windows").classes("text-lg font-semibold")
                ui.label(
                    "Raw input, decoded tags, selected tag details, and message windows will move here."
                ).classes("text-sm text-grey-7")
                working_area = ui.column().classes("w-full q-mt-md")

            # 4. INFO / REPORTING SECTION
            with ui.card().classes("w-full q-pa-md q-mb-md"):
                ui.label("Info / Reporting").classes("text-lg font-semibold")
                ui.label(
                    "Analysis output, warnings, insights, related messages, and future Ask integration will move here."
                ).classes("text-sm text-grey-7")
                reporting_area = ui.column().classes("w-full q-mt-md")

    # Temporary compatibility aliases.
    # Existing code can keep using these while we migrate gradually.
    result_area = working_area
    saved_area = context_area

    saved_compare_selection = []
    
    def render_sequence_insights(insights):
        if not insights:
            return

        ui.separator().classes("q-my-md")
        ui.label("Sequence Insights").classes("text-lg font-semibold")

        ui.label(insights.get("summary") or "").classes("text-sm text-grey-8")

        warnings = insights.get("warnings") or []
        changes = insights.get("changes") or []

        info_changes = [
            change for change in changes
            if change.get("severity") == "info"
        ]

        if warnings:
            ui.label("Potential Issues").classes("text-md font-semibold text-red-7 q-mt-md")
            for change in warnings:
                ui.label(f"- {change.get('summary')}").classes("text-red-7")

        if info_changes:
            with ui.expansion("Informational Changes", value=False).classes("w-full q-mt-md"):
                for change in info_changes:
                    ui.label(f"- {change.get('summary')}").classes("text-grey-8")

    def run_analysis():
        result_area.clear()

        if analyzer_select.value != "FIX Message":
            ui.notify("Analyzer not implemented yet.", type="warning")
            return

        raw_a = raw_input.value or ""
        raw_b = compare_input_box.value or ""
        mode = analysis_mode.value

        if mode == "Multi-Message Sequence":
            raw_preview = raw_a.strip()

            if len(raw_preview) < 30 or ("8=FIX" not in raw_preview and "BeginString" not in raw_preview):
                ui.notify(
                    "Input does not look like extracted FIX text. OCR may have failed. Try raw FIX text or a clearer/smaller image.",
                    color="warning",
                )
                return

        if mode == "Compare Messages":
            if not raw_a.strip() or not raw_b.strip():
                ui.notify("Please provide both Message A and Message B for comparison.", color="warning")
                return

        elif mode == "Multi-Message Sequence":
            if not raw_a.strip():
                ui.notify("Please provide one or more FIX messages or OCR text to analyze.", color="warning")
                return

        else:
            if not raw_a.strip():
                ui.notify("Please provide a FIX message or OCR text to analyze.", color="warning")
                return

        with result_area:
            ui.label("Analyzing... please wait.").classes("text-blue-7 font-bold")
            ui.spinner(size="lg")

        def do_analysis():
            if mode == "Compare Messages":
                result = compare_fix_messages(raw_a, raw_b)

            elif mode == "Multi-Message Sequence":
                result = analyze_fix_sequence(raw_a)

            else:
                result = analyze_fix_message(raw_a)

            if result.get("input_type") != "fix_compare" and result.get("input_type") != "fix_sequence":
                result["raw_text"] = raw_a

            result_area.clear()

            with result_area:
                if result.get("input_type") == "fix_compare":
                    render_compare_result(result)
                    return

                if result.get("input_type") == "fix_sequence":
                    render_sequence_result(result)
                    insights = build_sequence_insights(result.get("messages") or [])
                    render_sequence_insights(insights)
                    render_related_saved_matches(result)

                    def save_current_sequence_result():
                        save_note = (save_note_input.value or "").strip()
                        
                        debug_print("=== SAVE CLICKED ===")
                        debug_print("analysis mode:", "Multi-Message Sequence")
                        debug_print("result input_type:", result.get("input_type"))
                        debug_print("summary first 200:", str(result.get("summary") or "")[:200])
                        debug_print("save note:", save_note_input.value or "")
                        debug_print("message count:", len(result.get("messages") or []))

                        for msg in (result.get("messages") or [])[:3]:
                            debug_print(
                                "seq msg:",
                                msg.get("message_index"),
                                str(msg.get("raw_text") or "")[:120],
                            )

                        if not save_note:
                            ui.notify("Save note is required before saving the analysis.", color="warning")
                            return

                        session_id, created = save_fix_analysis_result(
                            result,
                            analysis_mode="Multi-Message Sequence",
                            source_type="ui",
                            source_name="Analysis tab",
                            save_note=save_note,
                        )

                        if created:
                            ui.notify(f"Saved new FIX analysis session {session_id}.", color="positive")
                        else:
                            if save_note_input.value:
                                ui.notify(
                                    f"Analysis already exists as session {session_id}. Save skipped, note updated.",
                                    color="info",
                                )
                            else:
                                ui.notify(
                                    f"Analysis already exists as session {session_id}. Save skipped.",
                                    color="info",
                                )

                        refresh_saved_analyses()

                    ui.button(
                        "Save Analysis",
                        on_click=save_current_sequence_result,
                    ).props("color=primary outline").classes("q-mt-md")

                    return

                ui.label("Plain-English Summary").classes("text-lg font-bold")

                summary_text = str(result.get("summary") or "").strip()

                ui.textarea(
                    value=summary_text or "No summary generated.",
                ).props(
                    "readonly outlined autogrow"
                ).classes(
                    "w-full bg-gray-100"
                )

                ui.label(
                    f"Parsed tags: {result.get('parsed_count', 0)} | "
                    f"Dictionary hits: {result.get('dictionary_hits', 0)} | "
                    f"Misses: {result.get('dictionary_misses', 0)} | "
                    f"Enum hits: {result.get('enum_hits', 0)}"
                ).classes("text-sm text-gray-500")

                warnings = result.get("warnings") or []
                if warnings:
                    ui.label("Warnings").classes("text-lg font-bold mt-4")
                    for warning in warnings:
                        ui.label(warning).classes("text-red-600")

                business_object = result.get("business_object", {}) or {}

                parties = business_object.get("parties") or []

                if parties:
                    ui.label("Parties").classes("text-lg font-bold mt-4")

                    ui.table(
                        columns=[
                            {"name": "party_id", "label": "Party ID", "field": "party_id", "align": "left"},
                            {"name": "party_id_source", "label": "Source", "field": "party_id_source", "align": "left"},
                            {"name": "party_role", "label": "Role", "field": "party_role", "align": "left"},
                            {"name": "party_role_name", "label": "Role Meaning", "field": "party_role_name", "align": "left"},
                        ],
                        rows=parties,
                        row_key="party_id",
                        pagination=10,
                    ).classes("w-full max-h-80 overflow-auto")

                ui.label("Decoded Values").classes("text-lg font-bold mt-4")

                rows = result.get("decoded_rows") or []

                def decoded_row_class(row):
                    tag_warning = str(row.get("tag_warning") or "").strip()
                    enum_warning = str(row.get("enum_warning") or "").strip()
                    tag_status = str(row.get("tag_status") or "").strip().lower()
                    tag_name = str(row.get("tag_name") or "").strip()

                    if tag_warning or enum_warning:
                        return "decoded-row-warning"

                    if tag_status in ("custom", "unknown", "missing", "not_found", "not found"):
                        return "decoded-row-custom"

                    if tag_name:
                        return "decoded-row-known"

                    return "decoded-row-custom"


                rows = [
                    {
                        **row,
                        "tag": int(str(row.get("tag", "")).split("#")[0])
                        if str(row.get("tag", "")).split("#")[0].isdigit()
                        else row.get("tag", ""),
                        "_seq": index,
                        "_row_class": decoded_row_class(row),
                        "_tag_sort": int(str(row.get("tag", "999999")).split("#")[0])
                        if str(row.get("tag", "")).split("#")[0].isdigit()
                        else 999999,
                    }
                    for index, row in enumerate(rows)
                ]

                review_rows = [
                    row for row in rows
                    if str(row.get("enum_valid", "")).lower() in {"false", "review", "parse review", "ocr review"}
                    or row.get("enum_warning")
                    or row.get("tag_warning")
                ]

                review_tags = []
                for row in review_rows:
                    tag = str(row.get("tag", "")).strip()
                    if tag and tag not in review_tags:
                        review_tags.append(tag)

                if review_tags:
                    ui.label(
                        "Check values for tags "
                        + ", ".join(review_tags)
                        + " in the Decoded Values table. Their values do not match listed dictionary values or need review."
                    ).classes("text-red-600 font-bold mt-4")

                ui.add_head_html("""
                <style>
                .decoded-values-scroll .q-table__middle {
                    max-height: 650px;
                    overflow: auto;
                }

                .decoded-values-scroll thead tr th {
                    position: sticky;
                    top: 0;
                    z-index: 5;
                    background: white;
                }

                .decoded-values-scroll thead tr:first-child th {
                    top: 0;
                }
                </style>
                """)

                ui.label(
                    "Default order follows the message sequence. Click column headers to sort."
                ).classes("text-sm text-gray-500")

                reset_decoded_sort_button = ui.button("Reset sorting").props("outline size=sm")

                with ui.element("div").classes("w-full border rounded decoded-values-scroll"):
                    decoded_table = ui.table(
                        columns=[
                            {"name": "_seq", "label": "#", "field": "_seq", "align": "left", "sortable": True},
                            {"name": "tag", "label": "Tag", "field": "_tag_sort", "align": "left", "sortable": True},
                            {"name": "tag_name", "label": "Tag Name", "field": "tag_name", "align": "left", "sortable": True},
                            {"name": "tag_status", "label": "Tag Status", "field": "tag_status", "align": "left", "sortable": True},
                            {"name": "tag_warning", "label": "Tag Warning", "field": "tag_warning", "align": "left"},
                            {"name": "ocr_repair_warning", "label": "OCR Repair", "field": "ocr_repair_warning", "align": "left"},
                            {"name": "value", "label": "Value", "field": "value", "align": "left", "sortable": True},
                            {"name": "value_name", "label": "Value Name", "field": "value_name", "align": "left", "sortable": True},
                            {"name": "has_enums", "label": "Has Enums", "field": "has_enums", "align": "left", "sortable": True},
                            {"name": "enum_valid", "label": "Enum Valid", "field": "enum_valid", "align": "left", "sortable": True},
                            {"name": "enum_warning", "label": "Enum Warning", "field": "enum_warning", "align": "left"},
                            {"name": "description", "label": "Description", "field": "description", "align": "left"},
                            {"name": "ocr_inferred", "label": "OCR Inferred", "field": "ocr_inferred", "align": "left", "sortable": True},
                            {"name": "ocr_score", "label": "OCR Score", "field": "ocr_score", "align": "left", "sortable": True},
                        ],
                        rows=rows,
                        pagination={
                            "sortBy": "_seq",
                            "descending": False,
                            "rowsPerPage": 0,
                        },
                    ).classes("w-full")

                    def reset_decoded_sorting():
                        decoded_table.rows = sorted(
                            rows,
                            key=lambda row: row.get("_seq", 0),
                        )
                        decoded_table.pagination = {
                            "sortBy": "_seq",
                            "descending": False,
                            "rowsPerPage": 0,
                        }
                        decoded_table.update()

                    reset_decoded_sort_button.on_click(reset_decoded_sorting)

                    decoded_table.add_slot("body", r"""
                    <q-tr
                      :props="props"
                      :class="(
                        props.row.enum_valid === false ||
                        props.row.enum_valid === 'Review' ||
                        props.row.enum_valid === 'Parse Review' ||
                        props.row.enum_valid === 'OCR Review' ||
                        props.row.enum_warning ||
                        props.row.tag_warning ||
                        props.row.ocr_repair_warning
                      ) ? 'text-red' : ''"
                    >
                      <q-td
                        v-for="col in props.cols"
                        :key="col.name"
                        :props="props"
                        :style="(
                          col.name === 'ocr_repair_warning' ||
                          col.name === 'tag_warning' ||
                          col.name === 'enum_warning'
                        )
                          ? 'max-width: 160px; min-width: 90px; white-space: normal; word-break: break-word; overflow-wrap: anywhere; vertical-align: top; font-size: 11px;'
                          : (
                              col.name === 'description'
                            )
                              ? 'max-width: 320px; min-width: 220px; white-space: normal; word-break: break-word; overflow-wrap: anywhere; vertical-align: top; font-size: 12px;'
                              : 'white-space: nowrap; vertical-align: top;'"
                      >
                        {{ col.name === 'tag' ? props.row.tag : col.value }}
                      </q-td>
                    </q-tr>
                    """)

                parties = business_object.get("parties") or []

                if parties:
                    ui.label("Parties").classes("text-lg font-bold mt-4")

                    with ui.element("div").classes("w-full max-h-80 overflow-auto border rounded"):
                        ui.table(
                            columns=[
                                {"name": "party_id", "label": "Party ID", "field": "party_id", "align": "left"},
                                {"name": "party_id_source", "label": "Source", "field": "party_id_source", "align": "left"},
                                {"name": "party_role", "label": "Role", "field": "party_role", "align": "left"},
                                {"name": "party_role_name", "label": "Role Meaning", "field": "party_role_name", "align": "left"},
                            ],
                            rows=parties,
                            row_key="party_id",
                            pagination=False,
                        ).classes("w-full")

                ui.label("Business Object").classes("text-lg font-bold mt-4")

                ui.code(
                    json.dumps(business_object, indent=2, ensure_ascii=False),
                    language="json",
                ).classes("w-full max-h-96 overflow-auto")

                def save_current_single_result():
                    save_note = (save_note_input.value or "").strip()

                    if not save_note:
                        ui.notify("Save note is required before saving the analysis.", color="warning")
                        return

                    session_id, created = save_fix_analysis_result(
                        result,
                        analysis_mode="Single Message",
                        source_type="ui",
                        source_name="Analysis tab",
                        save_note=save_note,
                    )

                    if created:
                        ui.notify(f"Saved new FIX analysis session {session_id}.", color="positive")
                    else:
                        ui.notify(f"Analysis already exists as session {session_id}. Save skipped.", color="info")

                render_related_saved_matches(result)

                ui.button(
                    "Save Analysis",
                    on_click=save_current_single_result,
                ).props("color=primary outline").classes("q-mt-md")

        ui.timer(0.1, do_analysis, once=True)

    def clear_analysis():
        raw_input.value = ""
        raw_input.update()

        compare_input_box.value = ""
        compare_input_box.update()

        result_area.clear()

    def refresh_saved_analyses():
        saved_area.clear()

        sessions = list_fix_analysis_sessions(20)

        with saved_area:
            ui.label("Saved Analyses").classes("text-lg font-bold mt-4")

            if not sessions:
                ui.label("No saved analyses yet.").classes("text-sm text-gray-500")
                return

            rows = []

            for session in sessions:
                rows.append({
                    "id": session.get("id"),
                    "analysis_mode": session.get("analysis_mode"),
                    "save_note": session.get("save_note") or "",
                    "message_count": session.get("message_count"),
                    "group_count": session.get("group_count"),
                    "warning_count": session.get("warning_count"),
                    "source_name": session.get("source_name"),
                    "created_at": str(session.get("created_at")),
                    "summary": (session.get("summary") or "")[:160],
                })

            session_options = {}

            for row in rows:
                note = row.get("save_note") or "No note"
                note_preview = note[:50] + "..." if len(note) > 50 else note

                label = (
                    f"Session {row.get('id')} | "
                    f"{note_preview} | "
                    f"{row.get('message_count') or 0} msgs | "
                    f"{row.get('warning_count') or 0} warnings"
                )

                session_options[label] = row.get("id")

            selected_session_dropdown = ui.select(
                options=session_options,
                label="Saved Session",
                with_input=True,
            ).props("outlined dense").classes("w-full q-mt-md")

            def open_selected_session():
                selected_rows = list(table.selected or [])

                if len(selected_rows) != 1:
                    ui.notify("Select one saved analysis first.", color="warning")
                    return

                session_id = selected_rows[0].get("id")

                if not session_id:
                    ui.notify("Could not read selected session id.", color="negative")
                    return

                messages = list_fix_analysis_messages(session_id)

                result_area.clear()

                with result_area:
                    ui.label(
                        f"Saved Analysis Session {session_id}"
                    ).classes("text-lg font-bold")

                    if not messages:
                        ui.label("No messages found for this saved session.").classes("text-red")
                        return

                    rows = []

                    for msg in messages:
                        rows.append({
                            "id": msg.get("id"),
                            "session_id": msg.get("session_id"),
                            "message_index": msg.get("message_index"),
                            "msg_seq_num": msg.get("msg_seq_num"),
                            "msg_type": msg.get("msg_type"),
                            "route": f"{msg.get('sender') or ''} → {msg.get('target') or ''}",
                            "cl_ord_id": msg.get("cl_ord_id"),
                            "order_id": msg.get("order_id"),
                            "secondary_order_id": msg.get("secondary_order_id"),
                            "exec_id": msg.get("exec_id"),
                            "symbol": msg.get("symbol"),
                            "order_qty": msg.get("order_qty"),
                            "last_qty": msg.get("last_qty"),
                            "cum_qty": msg.get("cum_qty"),
                            "leaves_qty": msg.get("leaves_qty"),
                        })

                    tag_result_area = ui.column().classes("w-full q-mt-md")

                    compare_saved_area = ui.column().classes("w-full q-mt-md")

                    def add_selected_saved_message_to_compare():
                        selected_rows = list(message_table.selected or [])

                        if len(selected_rows) != 1:
                            ui.notify("Select one saved message to add to compare.", color="warning")
                            return

                        selected = selected_rows[0]

                        message_id = selected.get("id")
                        session_id = selected.get("session_id")
                        message_index = selected.get("message_index")

                        if not message_id:
                            ui.notify("Could not read selected message id.", color="negative")
                            return

                        # Avoid duplicates
                        for item in saved_compare_selection:
                            if item.get("message_id") == message_id:
                                ui.notify("This message is already in the compare basket.", color="info")
                                return

                        if len(saved_compare_selection) >= 2:
                            saved_compare_selection.clear()

                        saved_compare_selection.append({
                            "message_id": message_id,
                            "session_id": session_id,
                            "message_index": message_index,
                        })

                        ui.notify(
                            f"Added session {session_id} message {message_index} to compare basket "
                            f"({len(saved_compare_selection)}/2).",
                            color="positive",
                        )


                    def compare_saved_message_basket():
                        if len(saved_compare_selection) != 2:
                            ui.notify("Add exactly two saved messages to the compare basket first.", color="warning")
                            return

                        first = saved_compare_selection[0]
                        second = saved_compare_selection[1]

                        first_message = get_fix_analysis_message(first["message_id"])
                        second_message = get_fix_analysis_message(second["message_id"])

                        if not first_message or not second_message:
                            ui.notify("Could not load one of the saved messages.", color="negative")
                            return

                        first_raw_text = first_message.get("raw_text") or ""
                        second_raw_text = second_message.get("raw_text") or ""

                        if not first_raw_text or not second_raw_text:
                            ui.notify("One of the selected saved messages is missing raw text.", color="negative")
                            return

                        comparison = compare_fix_messages(first_raw_text, second_raw_text)

                        compare_saved_area.clear()

                        with compare_saved_area:
                            ui.label(
                                f"Comparison: Session {first.get('session_id')} Message {first.get('message_index')} "
                                f"vs Session {second.get('session_id')} Message {second.get('message_index')}"
                            ).classes("text-lg font-semibold q-mt-md")

                            render_compare_result(comparison)

                    def compare_selected_saved_messages():
                        selected_rows = list(message_table.selected or [])

                        if len(selected_rows) != 2:
                            ui.notify("Select exactly two saved messages to compare.", color="warning")
                            return

                        first_message_id = selected_rows[0].get("id")
                        second_message_id = selected_rows[1].get("id")

                        if not first_message_id or not second_message_id:
                            ui.notify("Could not read selected message ids.", color="negative")
                            return

                        first_message = get_fix_analysis_message(first_message_id)
                        second_message = get_fix_analysis_message(second_message_id)

                        if not first_message or not second_message:
                            ui.notify("Could not load selected saved messages.", color="negative")
                            return

                        first_raw_text = first_message.get("raw_text") or ""
                        second_raw_text = second_message.get("raw_text") or ""

                        if not first_raw_text or not second_raw_text:
                            ui.notify("One of the selected saved messages is missing raw text.", color="negative")
                            return

                        comparison = compare_fix_messages(first_raw_text, second_raw_text)

                        compare_saved_area.clear()

                        with compare_saved_area:
                            ui.label(
                                f"Comparison: Saved Message {first_message.get('message_index')} "
                                f"vs Saved Message {second_message.get('message_index')}"
                            ).classes("text-lg font-semibold q-mt-md")

                            render_compare_result(comparison)

                    def view_selected_message_tags():
                        selected_rows = list(message_table.selected or [])

                        if len(selected_rows) != 1:
                            ui.notify("Select one saved message first.", color="warning")
                            return

                        message_id = selected_rows[0].get("id")
                        message_index = selected_rows[0].get("message_index")

                        if not message_id:
                            ui.notify("Could not read selected message id.", color="negative")
                            return

                        tags = list_fix_message_tags(message_id)

                        tag_result_area.clear()

                        with tag_result_area:
                            ui.label(
                                f"Decoded Tags for Message {message_index}"
                            ).classes("text-lg font-semibold q-mt-md")

                            if not tags:
                                ui.label("No decoded tags found for this message.").classes("text-red")
                                return

                            tag_rows = []

                            def saved_tag_row_class(tag):
                                tag_warning = str(tag.get("tag_warning") or "").strip()
                                enum_warning = str(tag.get("enum_warning") or "").strip()
                                tag_status = str(tag.get("tag_status") or "").strip().lower()
                                tag_name = str(tag.get("tag_name") or "").strip()

                                if tag_warning or enum_warning:
                                    return "decoded-row-warning"

                                if tag_status in ("custom", "unknown", "missing", "not_found", "not found"):
                                    return "decoded-row-custom"

                                if tag_name:
                                    return "decoded-row-known"

                                return "decoded-row-custom"


                            tag_rows = []

                            for tag in tags:
                                tag_value = str(tag.get("tag") or "").strip()

                                tag_rows.append({
                                    "position_index": tag.get("position_index"),
                                    "tag": int(tag_value) if tag_value.isdigit() else tag_value,
                                    "tag_name": tag.get("tag_name"),
                                    "value": tag.get("value"),
                                    "value_name": tag.get("value_name"),
                                    "description": tag.get("description"),
                                    "tag_warning": tag.get("tag_warning"),
                                    "enum_warning": tag.get("enum_warning"),
                                    "_row_class": saved_tag_row_class(tag),
                                })

                            decoded_table = ui.table(
                                columns=[
                                    {"name": "position_index", "label": "#", "field": "position_index", "align": "left", "sortable": True},
                                    {"name": "tag", "label": "Tag", "field": "tag", "align": "left", "sortable": True},
                                    {"name": "tag_name", "label": "Name", "field": "tag_name", "align": "left", "sortable": True},
                                    {"name": "value", "label": "Value", "field": "value", "align": "left", "sortable": True},
                                    {"name": "value_name", "label": "Value Name", "field": "value_name", "align": "left", "sortable": True},
                                    {"name": "description", "label": "Description", "field": "description", "align": "left"},
                                    {"name": "tag_warning", "label": "Tag Warning", "field": "tag_warning", "align": "left"},
                                    {"name": "enum_warning", "label": "Enum Warning", "field": "enum_warning", "align": "left"},
                                ],
                                rows=tag_rows,
                                row_key="position_index",
                                pagination={
                                    "rowsPerPage": 0,
                                    "sortBy": "position_index",
                                    "descending": False,
                                },
                            ).classes("w-full")

                            def reset_saved_tag_sorting():
                                decoded_table.pagination = {
                                    "rowsPerPage": 0,
                                    "sortBy": "position_index",
                                    "descending": False,
                                }
                                decoded_table.update()

                            ui.button(
                                "Reset Sorting",
                                on_click=reset_saved_tag_sorting,
                            ).props("outline size=sm").classes("q-mt-sm")

                            decoded_table.add_slot(
                                "body",
                                """
                                <q-tr :props="props" :class="props.row._row_class">
                                    <q-td v-for="col in props.cols" :key="col.name" :props="props">
                                        {{ col.value }}
                                    </q-td>
                                </q-tr>
                                """
                            )

                    ui.label(
                        "Select one saved message row, then click View Selected Message Tags."
                    ).classes("text-sm text-blue-7 font-bold q-mt-md")

                    with ui.row().classes("q-gutter-sm"):
                        ui.button(
                            "View Selected Message Tags",
                            on_click=view_selected_message_tags,
                        ).props("color=primary outline")

                        ui.button(
                            "Compare Selected Saved Messages",
                            on_click=compare_selected_saved_messages,
                        ).props("color=primary outline")

                        ui.button(
                            "Add Selected to Cross-Session Compare",
                            on_click=add_selected_saved_message_to_compare,
                        ).props("outline color=secondary")

                        ui.button(
                            "Compare Cross-Session Basket",
                            on_click=compare_saved_message_basket,
                        ).props("outline color=secondary")

                    message_table = ui.table(
                        columns=[
                            {"name": "message_index", "label": "#", "field": "message_index", "align": "left", "sortable": True},
                            {"name": "msg_seq_num", "label": "Seq", "field": "msg_seq_num", "align": "left", "sortable": True},
                            {"name": "msg_type", "label": "MsgType", "field": "msg_type", "align": "left", "sortable": True},
                            {"name": "route", "label": "Route", "field": "route", "align": "left", "sortable": True},
                            {"name": "cl_ord_id", "label": "ClOrdID", "field": "cl_ord_id", "align": "left", "sortable": True},
                            {"name": "order_id", "label": "OrderID", "field": "order_id", "align": "left", "sortable": True},
                            {"name": "secondary_order_id", "label": "SecondaryOrderID", "field": "secondary_order_id", "align": "left", "sortable": True},
                            {"name": "exec_id", "label": "ExecID", "field": "exec_id", "align": "left", "sortable": True},
                            {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left", "sortable": True},
                            {"name": "order_qty", "label": "OrderQty", "field": "order_qty", "align": "left", "sortable": True},
                            {"name": "last_qty", "label": "LastQty", "field": "last_qty", "align": "left", "sortable": True},
                            {"name": "cum_qty", "label": "CumQty", "field": "cum_qty", "align": "left", "sortable": True},
                            {"name": "leaves_qty", "label": "LeavesQty", "field": "leaves_qty", "align": "left", "sortable": True},
                        ],
                        rows=rows,
                        row_key="id",
                        selection="multiple",
                        pagination={"rowsPerPage": 0},
                    ).classes("w-full")

            def update_selected_session_note():
                selected_rows = list(table.selected or [])

                if len(selected_rows) != 1:
                    ui.notify("Select one saved analysis first.", color="warning")
                    return

                session_id = selected_rows[0].get("id")

                if not session_id:
                    ui.notify("Could not read selected session id.", color="negative")
                    return

                update_fix_analysis_session_note(
                    session_id,
                    save_note_input.value or "",
                )

                ui.notify(f"Updated save note for session {session_id}.", color="positive")
                refresh_saved_analyses()

            def confirm_delete_selected_session():
                selected_rows = list(table.selected or [])

                if len(selected_rows) != 1:
                    ui.notify("Select one saved analysis to delete.", color="warning")
                    return

                session_id = selected_rows[0].get("id")

                if not session_id:
                    ui.notify("Could not read selected session id.", color="negative")
                    return

                with ui.dialog() as dialog, ui.card():
                    ui.label(f"Delete saved analysis session {session_id}?").classes("text-lg font-semibold")
                    ui.label(
                        "This will delete the saved session, its messages, and message tags. This cannot be undone."
                    ).classes("text-red-7")

                    with ui.row().classes("q-gutter-sm q-mt-md"):
                        ui.button("Cancel", on_click=dialog.close).props("outline")

                        def do_delete():
                            delete_fix_analysis_session(session_id)
                            dialog.close()
                            ui.notify(f"Deleted saved analysis session {session_id}.", color="positive")
                            refresh_saved_analyses()

                        ui.button("Delete", on_click=do_delete).props("color=negative")

                dialog.open()

            with ui.row().classes("q-gutter-sm"):
                ui.button("Open Selected Saved Analysis", on_click=open_selected_session).props("outline")

                ui.button(
                    "Update Selected Save Note",
                    on_click=update_selected_session_note,
                ).props("outline color=primary")

                ui.button(
                    "Delete Selected Saved Analysis",
                    on_click=confirm_delete_selected_session,
                ).props("outline color=negative")

            table = ui.table(
                columns=[
                    {"name": "id", "label": "Session", "field": "id", "align": "left", "sortable": True},
                    {"name": "analysis_mode", "label": "Mode", "field": "analysis_mode", "align": "left", "sortable": True},
                    {"name": "save_note", "label": "Save Note", "field": "save_note", "align": "left", "sortable": True},
                    {"name": "message_count", "label": "Messages", "field": "message_count", "align": "left", "sortable": True},
                    {"name": "group_count", "label": "Groups", "field": "group_count", "align": "left", "sortable": True},
                    {"name": "warning_count", "label": "Warnings", "field": "warning_count", "align": "left", "sortable": True},
                    {"name": "created_at", "label": "Created", "field": "created_at", "align": "left", "sortable": True},
                    {"name": "summary", "label": "Summary", "field": "summary", "align": "left"},
                ],
                rows=rows,
                row_key="id",
                selection="single",
                pagination=10,
            ).classes("w-full")

        with control_area:
            ui.separator().classes("q-my-md")

            ui.button("Analyze", on_click=run_analysis).props("color=primary").classes("w-full")
            ui.button("Refresh Saved Analyses", on_click=refresh_saved_analyses).props("outline").classes("w-full")
            ui.button("Clear", on_click=clear_analysis).props("outline color=secondary").classes("w-full")

    refresh_saved_analyses()

        