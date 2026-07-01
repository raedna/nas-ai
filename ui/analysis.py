from pathlib import Path
from tempfile import NamedTemporaryFile
import json
from nicegui import ui

from core.analysis.analyzers.fix.analyzer import analyze_fix_message
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

    raw_input = ui.textarea(
        label="FIX message / extracted OCR text",
        placeholder="Paste FIX message, OCR text, or upload image/PDF...",
    ).props("outlined").classes("w-full mt-4 analysis-fix-input max-h-64 overflow-auto")

    result_area = ui.column().classes("w-full mt-4")

    async def handle_analysis_upload(e):
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

            raw_input.value = extracted_text
            result_area.clear()
            ui.notify("Text extracted. Review it, then click Analyze.", type="positive")

        except Exception as ex:
            ui.notify(f"OCR failed: {ex}", type="negative")
            print(f"=== ANALYSIS UPLOAD FAILED: {ex} ===", flush=True)

    ui.upload(
        label="Upload screenshot/image or PDF",
        on_upload=handle_analysis_upload,
        auto_upload=True,
        max_files=1,
    ).props("accept=image/*,.pdf").classes("w-full mt-2")

    ui.html("""
    <div id="analysis-screenshot-paste-box"
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
        Click here, then paste screenshot with Cmd+V / Ctrl+V
    </div>
    """).classes("w-full")

    ui.add_body_html("""
    <script>
    setTimeout(() => {
        const box = document.getElementById('analysis-screenshot-paste-box');
        if (!box) return;

        let pasteBoxActive = false;

        function setPasteBoxIdle() {
            pasteBoxActive = false;
            box.style.borderColor = '#aaa';
            box.style.background = '#fafafa';
            box.innerText = 'Click here, then paste screenshot with Cmd+V / Ctrl+V';
        }

        function setPasteBoxActive() {
            pasteBoxActive = true;
            box.focus();
            box.style.borderColor = '#1976d2';
            box.style.background = '#eef6ff';
            box.innerText = 'Paste now with Cmd+V / Ctrl+V';
        }

        async function setTextareaValue(text) {
            let textarea = document.querySelector('textarea.analysis-fix-input');

            if (!textarea) {
                const textareas = Array.from(document.querySelectorAll('textarea'));
                textarea = textareas.find(t => t.offsetParent !== null);
            }

            if (!textarea) {
                alert('Could not find analysis input box.');
                return;
            }

            textarea.value = text;
            textarea.dispatchEvent(new Event('input', { bubbles: true }));
            textarea.dispatchEvent(new Event('change', { bubbles: true }));
        }

        box.addEventListener('click', (event) => {
            event.stopPropagation();
            setPasteBoxActive();
        });

        document.addEventListener('click', (event) => {
            if (!box.contains(event.target)) {
                setPasteBoxIdle();
            }
        });

        document.addEventListener('paste', async (event) => {
            if (!pasteBoxActive) return;

            const items = event.clipboardData && event.clipboardData.items;
            if (!items) {
                alert('No clipboard items found.');
                return;
            }

            for (const item of items) {
                if (item.type && item.type.startsWith('image/')) {
                    event.preventDefault();

                    box.innerText = 'Running OCR on pasted screenshot...';
                    box.style.borderColor = '#f59e0b';
                    box.style.background = '#fff7ed';

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
                                setPasteBoxActive();
                                return;
                            }

                            if (!result.text) {
                                alert('No OCR text was extracted from the pasted screenshot.');
                                setPasteBoxActive();
                                return;
                            }

                            await setTextareaValue(result.text);

                            box.innerText = 'OCR text inserted. Review it, then click Analyze.';
                            box.style.borderColor = '#16a34a';
                            box.style.background = '#f0fdf4';

                        } catch (err) {
                            alert('OCR request failed: ' + err);
                            setPasteBoxActive();
                        }
                    };

                    reader.readAsDataURL(file);
                    return;
                }
            }

            alert('No image found in clipboard.');
        });
    }, 500);
    </script>
    """)

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

            #key_rows = []
#
            #message = business_object.get("message") or {}
            #trade = business_object.get("trade") or {}
            #order = business_object.get("order") or {}
#
            #def add_key(section, field, value):
            #    if value not in (None, ""):
            #        key_rows.append({
            #            "section": section,
            #            "field": field,
            #            "value": value,
            #        })

            #add_key("Message", "Message Type", message.get("type"))
            #add_key("Message", "Begin String", message.get("begin_string"))
            #add_key("Message", "Sequence Number", message.get("message_sequence_number"))
            #add_key("Message", "Sender", message.get("sender"))
            #add_key("Message", "Target", message.get("target"))
            #add_key("Message", "On Behalf Of", message.get("on_behalf_of"))
            #add_key("Message", "Deliver To", message.get("deliver_to"))
            #add_key("Message", "Sender Location", message.get("sender_location"))
            #add_key("Message", "On Behalf Of Location", message.get("on_behalf_of_location"))
            #add_key("Message", "Sending Time", message.get("sending_time"))
            #add_key("Message", "Message Encoding", message.get("message_encoding"))
#
            #add_key("Trade", "Side", trade.get("side"))
            #add_key("Trade", "Symbol", trade.get("symbol"))
            #add_key("Trade", "Security ID", trade.get("security_id"))
            #add_key("Trade", "Security ID Source", trade.get("security_id_source"))
            #add_key("Trade", "Security Type", trade.get("security_type"))
            #add_key("Trade", "Last Quantity", trade.get("last_quantity"))
            #add_key("Trade", "Last Price", trade.get("last_price"))
            #add_key("Trade", "Average Price", trade.get("average_price"))
            #add_key("Trade", "Cumulative Quantity", trade.get("cumulative_quantity"))
            #add_key("Trade", "Leaves Quantity", trade.get("leaves_quantity"))
            #add_key("Trade", "Order Quantity", trade.get("order_quantity"))
            #add_key("Trade", "Order Price", trade.get("order_price"))
            #add_key("Trade", "Currency", trade.get("currency"))
            #add_key("Trade", "Trade Date", trade.get("trade_date"))
            #add_key("Trade", "Settlement Date", trade.get("settlement_date"))
            #add_key("Trade", "Settlement Type", trade.get("settlement_type"))
            #add_key("Trade", "Transaction Time", trade.get("transaction_time"))
            #add_key("Trade", "Last Market", trade.get("last_market"))
            #add_key("Trade", "Coupon Rate", trade.get("coupon_rate"))
            #add_key("Trade", "Maturity Date", trade.get("maturity_date"))
            #add_key("Trade", "Contract Multiplier", trade.get("contract_multiplier"))
#
            #add_key("Order", "Client Order ID", order.get("client_order_id"))
            #add_key("Order", "Secondary Client Order ID", order.get("secondary_client_order_id"))
            #add_key("Order", "Order ID", order.get("order_id"))
            #add_key("Order", "Secondary Order ID", order.get("secondary_order_id"))
            #add_key("Order", "Execution ID", order.get("execution_id"))
            #add_key("Order", "Execution Ref ID", order.get("execution_ref_id"))
            #add_key("Order", "Execution Type", order.get("execution_type"))
            #add_key("Order", "Order Status", order.get("order_status"))
            #add_key("Order", "Account", order.get("account"))
#
            #if key_rows:
            #    ui.label("Key Fields").classes("text-lg font-bold mt-4")
#
            #    ui.table(
            #        columns=[
            #            {"name": "section", "label": "Section", "field": "section", "align": "left"},
            #            {"name": "field", "label": "Field", "field": "field", "align": "left"},
            #            {"name": "value", "label": "Value", "field": "value", "align": "left"},
            #        ],
            #        rows=key_rows,
            #        row_key="field",
            #        pagination=15,
            #    ).classes("w-full max-h-96 overflow-auto")

            ui.label("Decoded Values").classes("text-lg font-bold mt-4")

            rows = result.get("decoded_rows") or []

            with ui.element("div").classes("w-full max-h-[500px] overflow-auto border rounded"):
                ui.table(
                    columns=[
                        {"name": "tag", "label": "Tag", "field": "tag", "align": "left"},
                        {"name": "tag_name", "label": "Tag Name", "field": "tag_name", "align": "left"},
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
                    pagination=False,
                ).classes("w-full")

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
        result_area.clear()

    with ui.row().classes("mt-3 gap-2"):
        ui.button("Analyze", on_click=run_analysis).props("color=primary")
        ui.button("Clear", on_click=clear_analysis).props("outline color=secondary")