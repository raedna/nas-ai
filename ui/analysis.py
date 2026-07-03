from pathlib import Path
from tempfile import NamedTemporaryFile
import json
from nicegui import ui

from core.analysis.analyzers.fix.analyzer import analyze_fix_message
from core.analysis.analyzers.fix.comparator import compare_fix_messages
from IMAGES.image_parser import parse_image
from PDF.pdf_parser import parse_pdf
import base64
from fastapi import Request
from nicegui import app
from core.analysis.ocr.rapidocr_adapter import ocr_image_with_rapidocr
from core.analysis.ocr.pdf_rapidocr_adapter import ocr_pdf_with_rapidocr


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
                print("=== CLIPBOARD RAPIDOCR USED ===", flush=True)
                print("=== CLIPBOARD OCR TEXT START ===", flush=True)
                print(extracted_text, flush=True)
                print("=== CLIPBOARD OCR TEXT END ===", flush=True)

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

def render_compare_result(result: dict):
    relationship = result.get("relationship") or {}
    counts = result.get("difference_counts_by_category") or {}
    difference_rows = result.get("difference_rows") or []
    comparison_rows = result.get("comparison_rows") or []

    comparison_rows = [
        {**row, "_seq": index}
        for index, row in enumerate(comparison_rows)
    ]

    ui.label("Comparison Summary").classes("text-lg font-bold")
    ui.markdown(result.get("summary") or "No summary generated.").classes(
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
        {"name": "display_key", "label": "Tag", "field": "display_key", "align": "left", "sortable": True},
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
                "sortBy": "_seq",
                "descending": False,
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
          :class="props.row.status !== 'Same' ? 'text-red' : ''"
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
            {{ col.value }}
          </q-td>
        </q-tr>
        """)

def render_analysis_panel():
    ui.label("Analysis Engine").classes("text-xl font-bold mb-2")
    ui.label(
        "Paste FIX text below, upload an image/PDF, or paste a screenshot into the screenshot box. "
        "Extracted text will appear in the input box for review before analysis."
    ).classes("text-sm text-gray-500 mb-4")

    analyzer_select = ui.select(
        options=["FIX Message"],
        value="FIX Message",
        label="Analyzer",
    ).props("outlined").classes("w-64")

    analysis_mode = ui.select(
        ["Single Message", "Compare Messages"],
        value="Single Message",
        label="Analysis Mode",
    ).classes("w-64")

    raw_input = ui.textarea(
        label="FIX message / extracted OCR text",
        placeholder="Paste FIX message, OCR text, or upload image/PDF...",
    ).props("outlined").classes("w-full mt-4 analysis-fix-input max-h-64 overflow-auto")

    compare_input_box = ui.textarea(
        label="Message B / Compare Against",
        placeholder="Paste the second FIX message here for comparison...",
    ).classes("w-full")

    compare_input_box.visible = False

    result_area = ui.column().classes("w-full mt-4")

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
        label="Upload Message A screenshot/image or PDF",
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

    def run_analysis():
        result_area.clear()

        if analyzer_select.value != "FIX Message":
            ui.notify("Analyzer not implemented yet.", type="warning")
            return

        raw_a = raw_input.value or ""

        if analysis_mode.value == "Compare Messages":
            raw_b = compare_input_box.value or ""

            if not raw_a.strip() or not raw_b.strip():
                ui.notify("Please provide both Message A and Message B for comparison.", color="warning")
                return

            result = compare_fix_messages(raw_a, raw_b)
        else:
            if not raw_a.strip():
                ui.notify("Please provide a FIX message or OCR text to analyze.", color="warning")
                return

            result = analyze_fix_message(raw_a)

        with result_area:
            if result.get("input_type") == "fix_compare":
                render_compare_result(result)
                return

            ui.label("Plain-English Summary").classes("text-lg font-bold")

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

            rows = [
                {**row, "_seq": index}
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
                        {"name": "tag", "label": "Tag", "field": "tag", "align": "left"},
                        {"name": "tag_name", "label": "Tag Name", "field": "tag_name", "align": "left"},
                        {"name": "tag_status", "label": "Tag Status", "field": "tag_status", "align": "left"},
                        {"name": "tag_warning", "label": "Tag Warning", "field": "tag_warning", "align": "left"},
                        {"name": "ocr_repair_warning", "label": "OCR Repair", "field": "ocr_repair_warning", "align": "left"},
                        {"name": "value", "label": "Value", "field": "value", "align": "left"},
                        {"name": "value_name", "label": "Value Name", "field": "value_name", "align": "left"},
                        {"name": "has_enums", "label": "Has Enums", "field": "has_enums", "align": "left"},
                        {"name": "enum_valid", "label": "Enum Valid", "field": "enum_valid", "align": "left"},
                        {"name": "enum_warning", "label": "Enum Warning", "field": "enum_warning", "align": "left"},
                        {"name": "description", "label": "Description", "field": "description", "align": "left"},
                        {"name": "ocr_inferred", "label": "OCR Inferred", "field": "ocr_inferred", "align": "left"},
                        {"name": "ocr_score", "label": "OCR Score", "field": "ocr_score", "align": "left"},
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
                    {{ col.value }}
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

    def clear_analysis():
        raw_input.value = ""
        raw_input.update()

        compare_input_box.value = ""
        compare_input_box.update()

        result_area.clear()

    with ui.row().classes("mt-3 gap-2"):
        ui.button("Analyze", on_click=run_analysis).props("color=primary")
        ui.button("Clear", on_click=clear_analysis).props("outline color=secondary")