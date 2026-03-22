"""
Receipt Scanner App - Uses Google Gemini to analyze receipt images and extract
a table with Item, Quantity, Price (ISK), Category, plus merchant detection.
"""
import json
import re
import io
import csv
import hashlib
import uuid
from pathlib import Path
from datetime import date, datetime, timedelta
from collections import defaultdict, Counter
from typing import Optional, Iterable, Any
import streamlit as st
from PIL import Image
import google.generativeai as genai
from google.oauth2.service_account import Credentials
import gspread

APP_DIR = Path(__file__).resolve().parent
HISTORY_CSV_PATH = APP_DIR / "history.csv"
BUDGETS_CSV_PATH = APP_DIR / "budgets.csv"

HISTORY_COLUMNS = [
    "Row_ID",
    "Merchant",
    "Date",
    "Item",
    "Standard_Name",
    "Quantity",
    "Price_ISK",
    "Category",
]

BUDGET_COLUMNS = ["YearMonth", "Category", "Budget_ISK"]

GSHEET_HISTORY_TAB = "history"
GSHEET_BUDGETS_TAB = "budgets"


def fmt_isk(value: Any) -> str:
    try:
        n = int(round(float(value)))
    except Exception:
        n = 0
    return f"{n:,}".replace(",", ".") + " ISK"


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(round(float(value)))
    except Exception:
        return default


def yearmonth(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"

def _get_gsheets_client_and_sheet():
    """
    Uses Streamlit secrets:
      - GSHEETS_SHEET_ID: Google Sheet ID
      - GOOGLE_SERVICE_ACCOUNT_JSON: service account JSON (string) OR a TOML dict via st.secrets
    """
    sheet_id = None
    try:
        sheet_id = (st.secrets.get("GSHEETS_SHEET_ID") or "").strip()
    except Exception:
        sheet_id = ""
    if not sheet_id:
        return None

    sa = None
    try:
        sa = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    except Exception:
        sa = None
    if isinstance(sa, str):
        try:
            sa_info = json.loads(sa)
        except Exception:
            sa_info = None
    elif isinstance(sa, dict):
        sa_info = sa
    else:
        sa_info = None

    if not isinstance(sa_info, dict):
        return None

    try:
        creds = Credentials.from_service_account_info(
            sa_info,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ],
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        return sh
    except Exception as e:
        st.error(f"GSheets internal error: {e}")
        return None


def _ensure_ws(sh, title: str, header: list[str]):
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=1000, cols=max(8, len(header)))
    # Ensure header row exists
    existing = ws.row_values(1)
    if [h.strip() for h in existing] != header:
        ws.clear()
        ws.append_row(header, value_input_option="RAW")
    return ws


def _gsheets_load_history_rows() -> list[dict]:
    sh = _get_gsheets_client_and_sheet()
    if sh is None:
        return []
    ws = _ensure_ws(sh, GSHEET_HISTORY_TAB, HISTORY_COLUMNS)
    records = ws.get_all_records()
    rows: list[dict] = []
    for idx, row in enumerate(records):
        try:
            purchased_on = datetime.strptime(str(row.get("Date", "")), "%Y-%m-%d").date()
        except Exception:
            continue
        row_id = str(row.get("Row_ID", "")).strip()
        if not row_id:
            row_id = hashlib.sha1(
                f"{row.get('Merchant','')}-{row.get('Date')}-{row.get('Item','')}-{row.get('Price_ISK',0)}-{idx}".encode(
                    "utf-8"
                )
            ).hexdigest()[:16]
        rows.append(
            {
                "Row_ID": row_id,
                "Merchant": (row.get("Merchant") or "").strip() or "Unknown",
                "Date": purchased_on,
                "Item": (row.get("Item") or "").strip(),
                "Standard_Name": (row.get("Standard_Name") or "").strip()
                or (row.get("Item") or "").strip(),
                "Quantity": as_int(row.get("Quantity", 1), default=1),
                "Price_ISK": as_int(row.get("Price_ISK", 0), default=0),
                "Category": (row.get("Category") or "Other").strip() or "Other",
            }
        )
    return rows


def _gsheets_save_history_rows(rows: list[dict]) -> None:
    sh = _get_gsheets_client_and_sheet()
    if sh is None:
        raise RuntimeError("Google Sheets is not configured.")
    ws = _ensure_ws(sh, GSHEET_HISTORY_TAB, HISTORY_COLUMNS)
    values = [HISTORY_COLUMNS]
    for r in rows:
        item = (r.get("Item") or "").strip()
        if not item:
            continue
        purchased_on: date = r["Date"]
        values.append(
            [
                (r.get("Row_ID") or "").strip() or uuid.uuid4().hex,
                (r.get("Merchant") or "").strip() or "Unknown",
                purchased_on.isoformat(),
                item,
                (r.get("Standard_Name") or "").strip() or item,
                max(1, as_int(r.get("Quantity", 1), default=1)),
                as_int(r.get("Price_ISK", 0), default=0),
                (r.get("Category") or "Other").strip() or "Other",
            ]
        )
    ws.clear()
    ws.update(values, value_input_option="RAW")


def _gsheets_append_history_rows(*, merchant: str, purchased_on: date, items: Iterable[dict]) -> int:
    sh = _get_gsheets_client_and_sheet()
    if sh is None:
        raise RuntimeError("Could not connect to Google Sheets — check secrets.")
    ws = _ensure_ws(sh, GSHEET_HISTORY_TAB, HISTORY_COLUMNS)
    out_rows = []
    for it in items:
        item_name = (it.get("item") or "").strip()
        if not item_name:
            continue
        std_name = (it.get("standard_name") or it.get("Standard_Name") or "").strip() or item_name
        out_rows.append(
            [
                uuid.uuid4().hex,
                merchant,
                purchased_on.isoformat(),
                item_name,
                std_name,
                as_int(it.get("quantity", 1), default=1),
                as_int(it.get("price_isk", 0), default=0),
                (it.get("category") or "Other").strip() or "Other",
            ]
        )
    if not out_rows:
        return 0
    ws.append_rows(out_rows, value_input_option="RAW")
    return len(out_rows)


def _gsheets_load_budgets() -> list[dict]:
    sh = _get_gsheets_client_and_sheet()
    if sh is None:
        return []
    ws = _ensure_ws(sh, GSHEET_BUDGETS_TAB, BUDGET_COLUMNS)
    records = ws.get_all_records()
    out = []
    for row in records:
        ym = (row.get("YearMonth") or "").strip()
        cat = (row.get("Category") or "").strip()
        if not ym or not cat:
            continue
        out.append(
            {
                "YearMonth": ym,
                "Category": cat,
                "Budget_ISK": as_int(row.get("Budget_ISK", 0), default=0),
            }
        )
    return out


def _gsheets_save_budgets(rows: list[dict]) -> None:
    sh = _get_gsheets_client_and_sheet()
    if sh is None:
        raise RuntimeError("Google Sheets is not configured.")
    ws = _ensure_ws(sh, GSHEET_BUDGETS_TAB, BUDGET_COLUMNS)
    values = [BUDGET_COLUMNS]
    for r in rows:
        ym = (r.get("YearMonth") or "").strip()
        cat = (r.get("Category") or "").strip()
        if not ym or not cat:
            continue
        values.append([ym, cat, as_int(r.get("Budget_ISK", 0), default=0)])
    ws.clear()
    ws.update(values, value_input_option="RAW")


def using_gsheets() -> bool:
    try:
        return bool((st.secrets.get("GSHEETS_SHEET_ID") or "").strip()) and bool(st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON"))
    except Exception:
        return False


def ensure_budgets_csv_exists() -> None:
    if BUDGETS_CSV_PATH.exists():
        return
    with BUDGETS_CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=BUDGET_COLUMNS)
        w.writeheader()


def load_budgets() -> list[dict]:
    if using_gsheets():
        return _gsheets_load_budgets()
    if not BUDGETS_CSV_PATH.exists():
        return []
    with BUDGETS_CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        out = []
        for row in r:
            ym = (row.get("YearMonth") or "").strip()
            cat = (row.get("Category") or "").strip()
            if not ym or not cat:
                continue
            out.append(
                {
                    "YearMonth": ym,
                    "Category": cat,
                    "Budget_ISK": as_int(row.get("Budget_ISK", 0), default=0),
                }
            )
        return out


def save_budgets(rows: list[dict]) -> None:
    if using_gsheets():
        _gsheets_save_budgets(rows)
        return
    ensure_budgets_csv_exists()
    with BUDGETS_CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=BUDGET_COLUMNS)
        w.writeheader()
        for r in rows:
            ym = (r.get("YearMonth") or "").strip()
            cat = (r.get("Category") or "").strip()
            if not ym or not cat:
                continue
            w.writerow(
                {
                    "YearMonth": ym,
                    "Category": cat,
                    "Budget_ISK": as_int(r.get("Budget_ISK", 0), default=0),
                }
            )


def ensure_history_csv_exists_and_up_to_date() -> None:
    if not HISTORY_CSV_PATH.exists():
        with HISTORY_CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=HISTORY_COLUMNS)
            w.writeheader()
        return

    with HISTORY_CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        header = next(r, None)
    if not header:
        with HISTORY_CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=HISTORY_COLUMNS)
            w.writeheader()
        return

    header_set = {h.strip() for h in header}
    if all(col in header_set for col in HISTORY_COLUMNS):
        return

    old_rows = load_history_rows(allow_upgrade=False)
    upgraded = []
    for idx, row in enumerate(old_rows):
        row_id = (row.get("Row_ID") or "").strip() or hashlib.sha1(
            f"{row.get('Merchant','')}-{row.get('Date')}-{row.get('Item','')}-{row.get('Price_ISK',0)}-{idx}".encode(
                "utf-8"
            )
        ).hexdigest()[:16]
        std = (row.get("Standard_Name") or "").strip() or (row.get("Item") or "").strip()
        upgraded.append(
            {
                "Row_ID": row_id,
                "Merchant": row.get("Merchant", "Unknown"),
                "Date": row.get("Date"),
                "Item": row.get("Item", ""),
                "Standard_Name": std,
                "Quantity": as_int(row.get("Quantity", 1), default=1),
                "Price_ISK": as_int(row.get("Price_ISK", 0), default=0),
                "Category": row.get("Category", "Other"),
            }
        )
    _write_history_rows_no_upgrade(upgraded)


def append_history_rows(
    *,
    merchant: str,
    purchased_on: date,
    items: Iterable[dict],
) -> int:
    if using_gsheets():
        return _gsheets_append_history_rows(merchant=merchant, purchased_on=purchased_on, items=items)
    ensure_history_csv_exists_and_up_to_date()
    wrote = 0
    with HISTORY_CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HISTORY_COLUMNS)
        for it in items:
            item_name = (it.get("item") or "").strip()
            if not item_name:
                continue
            std_name = (it.get("standard_name") or it.get("Standard_Name") or "").strip()
            if not std_name:
                std_name = item_name
            w.writerow(
                {
                    "Row_ID": uuid.uuid4().hex,
                    "Merchant": merchant,
                    "Date": purchased_on.isoformat(),
                    "Item": item_name,
                    "Standard_Name": std_name,
                    "Quantity": as_int(it.get("quantity", 1), default=1),
                    "Price_ISK": as_int(it.get("price_isk", 0), default=0),
                    "Category": (it.get("category") or "Other").strip() or "Other",
                }
            )
            wrote += 1
    return wrote


def _write_history_rows_no_upgrade(rows: list[dict]) -> None:
    with HISTORY_CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HISTORY_COLUMNS)
        w.writeheader()
        for r in rows:
            row_id = (r.get("Row_ID") or "").strip() or uuid.uuid4().hex
            purchased_on: date = r["Date"]
            item = (r.get("Item") or "").strip()
            if not item:
                continue
            std = (r.get("Standard_Name") or "").strip() or item
            w.writerow(
                {
                    "Row_ID": row_id,
                    "Merchant": (r.get("Merchant") or "").strip() or "Unknown",
                    "Date": purchased_on.isoformat(),
                    "Item": item,
                    "Standard_Name": std,
                    "Quantity": max(1, as_int(r.get("Quantity", 1), default=1)),
                    "Price_ISK": as_int(r.get("Price_ISK", 0), default=0),
                    "Category": (r.get("Category") or "Other").strip() or "Other",
                }
            )


def save_history_rows(rows: list[dict]) -> None:
    if using_gsheets():
        _gsheets_save_history_rows(rows)
        return
    ensure_history_csv_exists_and_up_to_date()
    _write_history_rows_no_upgrade(rows)


def load_history_rows(*, allow_upgrade: bool = True) -> list[dict]:
    if using_gsheets():
        return _gsheets_load_history_rows()
    if allow_upgrade:
        ensure_history_csv_exists_and_up_to_date()
    if not HISTORY_CSV_PATH.exists():
        return []
    with HISTORY_CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = []
        for idx, row in enumerate(r):
            try:
                purchased_on = datetime.strptime(row.get("Date", ""), "%Y-%m-%d").date()
            except Exception:
                continue
            row_id = (row.get("Row_ID") or "").strip()
            if not row_id:
                row_id = hashlib.sha1(
                    f"{row.get('Merchant','')}-{row.get('Date')}-{row.get('Item','')}-{row.get('Price_ISK',0)}-{idx}".encode(
                        "utf-8"
                    )
                ).hexdigest()[:16]
            rows.append(
                {
                    "Row_ID": row_id,
                    "Merchant": (row.get("Merchant") or "").strip() or "Unknown",
                    "Date": purchased_on,
                    "Item": (row.get("Item") or "").strip(),
                    "Standard_Name": (row.get("Standard_Name") or "").strip()
                    or (row.get("Item") or "").strip(),
                    "Quantity": as_int(row.get("Quantity", 1), default=1),
                    "Price_ISK": as_int(row.get("Price_ISK", 0), default=0),
                    "Category": (row.get("Category") or "Other").strip() or "Other",
                }
            )
        return rows


st.set_page_config(page_title="Receipt Scanner", page_icon="🧾", layout="centered")
st.title("🧾 Receipt Scanner")
st.write("Using GSheets:", using_gsheets())
try:
    sh = _get_gsheets_client_and_sheet()
    st.write("Sheet connected:", sh is not None)
    if sh is not None:
        st.write("Sheet title:", sh.title)
except Exception as e:
    st.error(f"GSheets connection error: {e}")
st.caption("Scan receipts and build a spending history you can filter and summarize.")

# --- API key: from secrets or sidebar ---
def get_api_key() -> Optional[str]:
    try:
        return st.secrets.get("GEMINI_API_KEY") or st.secrets.get("gemini_api_key")
    except Exception:
        return None


# Sidebar for API key if not in secrets
api_key = get_api_key()
if not api_key:
    with st.sidebar:
        api_key = st.text_input(
            "Google Gemini API key",
            type="password",
            help="Get a key at https://aistudio.google.com/apikey",
        )
if not api_key:
    st.info("Enter your Gemini API key in the sidebar to continue.")
    st.stop()

genai.configure(api_key=api_key)

# --- Gemini receipt analysis ---
PROMPT = """You are analyzing a receipt image (often from Iceland; prices in ISK/kr).

Do the following:
1. Detect the merchant/store name (e.g. H&M, Bónus, Krónan, or the exact name shown).
2. Extract every line item that has a price. For each line item provide:
   - item: product/description name.
   - standard_name: normalize staples for tracking nutrients.
       * If an item is any kind of Skyr, set standard_name to "Skyr".
       * If it is chicken breast or thighs, set standard_name to "Chicken".
       * If it is eggs or a carton of eggs, set standard_name to "Eggs".
       * Otherwise, set standard_name to the receipt text (same as item).
   - quantity: number of units (e.g. 1, 2; use 1 if not stated)
   - price_isk: total price in ISK as a number (no dots/commas in the number; e.g. 24015 not 24.015)
   - category: spending category. Use one of:
       * Protein: for high‑protein main foods like Skyr, chicken, fish, beef, pork, lamb, fish, seafood, protein powder, protein bars, harðfiskur and similar.
       * Vegetables: for fresh, frozen, or canned vegetables and salads. In Icelandic, words like "agúrka", "agúrkur", "gúrka", "grænmeti" should be Vegetables.
       * Fruits: for berries and fruit, including Icelandic names like "bláber", "jarðarber", "jarðaber".
       * Beverages: for drinks such as soda, juice, energy drinks, coffee, tea, alcohol.
       * Food: for other general groceries and snacks that are not clearly Protein, Vegetables, Fruits, or Beverages.
       * Clothing, Household, Health & Beauty, Electronics, Other: for non‑food items as appropriate.

Vegetables and Fruits should be treated as sub‑categories of Food when reasoning about overall food spend.

Return ONLY a single valid JSON object, no markdown or code fences, with this exact structure:
{"merchant": "Merchant Name", "items": [{"item": "...", "standard_name": "...", "quantity": 1, "price_isk": 0, "category": "..."}, ...]}"""


def analyze_receipt_with_gemini(image_bytes: bytes) -> dict:
    """Send receipt image to Gemini and return parsed JSON with merchant and items."""
    image = Image.open(io.BytesIO(image_bytes))
    model = genai.GenerativeModel("gemini-2.5-flash")
    response = model.generate_content([PROMPT, image])
    text = response.text.strip()

    # Remove markdown code block if present
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON object in response
        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            return json.loads(match.group(0))
        raise ValueError(f"Gemini did not return valid JSON. Response: {text[:500]}")


tab_scanner, tab_insights = st.tabs(["Scanner", "Insights"])

with tab_scanner:
    # --- UI: file upload or camera ---
    source = st.radio("Choose input", ["Upload an image", "Take a photo"], horizontal=True)
    image_data = None

    if source == "Upload an image":
        uploaded = st.file_uploader(
            "Upload receipt image",
            type=["png", "jpg", "jpeg"],
            label_visibility="collapsed",
        )
        if uploaded:
            image_data = uploaded.read()
    elif source == "Take a photo":
        cam = st.camera_input("Take a photo of your receipt")
        if cam:
            image_data = cam.getvalue()

    if image_data:
        with st.spinner("Analyzing receipt with Gemini…"):
            try:
                data = analyze_receipt_with_gemini(image_data)
            except Exception as e:
                st.error(f"Analysis failed: {e}")
                st.stop()

        merchant = (data.get("merchant") or "Unknown").strip() or "Unknown"
        items = data.get("items") or []

        # Show image
        st.subheader("Receipt image")
        st.image(image_data, use_container_width=True)

        # Merchant + date
        st.subheader("Purchase details")
        col_a, col_b = st.columns([2, 1])
        with col_a:
            st.write(f"**Merchant:** {merchant}")
        with col_b:
            purchased_on = st.date_input("Date", value=date.today())

        # Table: Item, Quantity, Price (ISK), Category
        st.subheader("Line items")
        if items:
            rows = [
                {
                    "Item": (it.get("item") or "").strip(),
                    "Standard name": (it.get("standard_name") or it.get("Standard_Name") or "").strip()
                    or (it.get("item") or "").strip(),
                    "Quantity": as_int(it.get("quantity", 1), default=1),
                    "Price (ISK)": fmt_isk(as_int(it.get("price_isk", 0), default=0)),
                    "Category": (it.get("category") or "Other").strip() or "Other",
                }
                for it in items
                if (it.get("item") or "").strip()
            ]
            st.dataframe(rows, use_container_width=True, hide_index=True)

            total_isk = sum(as_int(it.get("price_isk", 0), default=0) for it in items)
            st.metric("Total", fmt_isk(total_isk))

            receipt_id = hashlib.sha1(image_data).hexdigest()
            already_saved = st.session_state.get("last_accepted_receipt_id") == receipt_id
            accept = st.button("Accept & save to history", type="primary", disabled=already_saved)
            if already_saved:
                st.caption("Already saved for this receipt in this session.")
            if accept:
                try:
                    wrote = append_history_rows(merchant=merchant, purchased_on=purchased_on, items=items)
                    st.session_state["last_accepted_receipt_id"] = receipt_id
                    if wrote:
                        st.success(f"Saved {wrote} items to history.")
                    else:
                        st.warning("Nothing was saved (no valid line items found).")
                except Exception as e:
                    st.error(f"Save failed: {e}")
        else:
            st.info("No line items were extracted from this receipt.")
    else:
        st.info("Upload an image or take a photo to get started.")

with tab_insights:
    st.subheader("Insights")
    rows = load_history_rows()
    if not rows:
        st.info("No history yet. Scan a receipt and click **Accept & save to history**.")
        st.stop()

    today = date.today()
    preset = st.selectbox(
        "Date range",
        ["Last 7 days", "Last 30 days", "Last 90 days", "Year to date", "All time", "Custom"],
        index=1,
    )

    if preset == "Last 7 days":
        start, end = today - timedelta(days=7), today
    elif preset == "Last 30 days":
        start, end = today - timedelta(days=30), today
    elif preset == "Last 90 days":
        start, end = today - timedelta(days=90), today
    elif preset == "Year to date":
        start, end = date(today.year, 1, 1), today
    elif preset == "All time":
        start, end = min(r["Date"] for r in rows), max(r["Date"] for r in rows)
    else:
        start, end = st.date_input(
            "Custom range",
            value=(today - timedelta(days=30), today),
        )

    filtered = [r for r in rows if start <= r["Date"] <= end]
    if not filtered:
        st.warning("No purchases found in that date range.")
        st.stop()

    total_spend = sum(r["Price_ISK"] for r in filtered)
    st.metric("Total spend", fmt_isk(total_spend))

    # Manual additions for forgotten receipts
    with st.expander("Add manual purchase (no receipt)"):
        m_date = st.date_input("Manual purchase date", value=today, key="manual_date")
        m_merchant = st.text_input("Merchant (optional)", value="", key="manual_merchant")
        m_total = st.number_input("Total amount (ISK)", min_value=0, step=100, key="manual_total")
        st.caption(
            "Optionally break the total into staples or categories (for example 3.000 ISK Skyr out of 10.000 ISK total)."
        )
        breakdown_default = [
            {"Standard_Name": "Skyr", "Category": "Protein", "Amount_ISK": 0},
            {"Standard_Name": "Eggs", "Category": "Protein", "Amount_ISK": 0},
            {"Standard_Name": "Vegetables", "Category": "Vegetables", "Amount_ISK": 0},
        ]
        breakdown = st.data_editor(
            breakdown_default,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config={
                "Amount_ISK": st.column_config.NumberColumn(format="%d"),
            },
            key="manual_breakdown",
        )
        if st.button("Save manual purchase"):
            if m_total <= 0:
                st.error("Total amount must be greater than 0.")
            else:
                items_for_append = []
                allocated = 0
                for row in breakdown:
                    amt = as_int(row.get("Amount_ISK", 0), default=0)
                    if amt <= 0:
                        continue
                    std = (row.get("Standard_Name") or "").strip()
                    cat = (row.get("Category") or "Food").strip() or "Food"
                    item_name = std or cat or "Manual item"
                    items_for_append.append(
                        {
                            "item": item_name,
                            "standard_name": std or item_name,
                            "quantity": 1,
                            "price_isk": amt,
                            "category": cat,
                        }
                    )
                    allocated += amt

                remaining = max(0, m_total - allocated)
                if remaining > 0:
                    items_for_append.append(
                        {
                            "item": "Manual remainder",
                            "standard_name": "Other Food",
                            "quantity": 1,
                            "price_isk": remaining,
                            "category": "Food",
                        }
                    )

                if not items_for_append:
                    items_for_append.append(
                        {
                            "item": "Manual total",
                            "standard_name": "Manual total",
                            "quantity": 1,
                            "price_isk": m_total,
                            "category": "Food",
                        }
                    )

                wrote = append_history_rows(
                    merchant=(m_merchant or "Manual").strip(),
                    purchased_on=m_date,
                    items=items_for_append,
                )
                if wrote:
                    st.success(f"Saved {wrote} manual items to history.")
                    st.rerun()
                else:
                    st.error("Nothing was saved. Please check the amounts and try again.")

    # Aggregated spending by category (Food groups Vegetables & Fruits)
    def parent_category(cat: str) -> str:
        base = (cat or "Other").strip()
        if base in {"Vegetables", "Fruits"}:
            return "Food"
        return base

    parent_totals: dict[str, int] = defaultdict(int)
    items_by_parent: dict[str, list[dict]] = defaultdict(list)
    for r in filtered:
        p = parent_category(r["Category"])
        parent_totals[p] += r["Price_ISK"]
        items_by_parent[p].append(r)

    st.subheader("Spending by category")
    for parent, total in sorted(parent_totals.items(), key=lambda x: x[1], reverse=True):
        with st.expander(f"{parent}: {fmt_isk(total)}", expanded=False):
            detail_rows = [
                {
                    "Date": row["Date"].isoformat(),
                    "Merchant": row["Merchant"],
                    "Category": row["Category"],
                    "Item": row["Item"],
                    "Standard_Name": row.get("Standard_Name", row["Item"]),
                    "Quantity": row["Quantity"],
                    "Price (ISK)": fmt_isk(row["Price_ISK"]),
                }
                for row in sorted(items_by_parent[parent], key=lambda x: (x["Date"], x["Merchant"], x["Item"]))
            ]
            st.dataframe(detail_rows, use_container_width=True, hide_index=True)

    # Top 5 most purchased items (by quantity, then by spend)
    item_qty: Counter[str] = Counter()
    item_spend: defaultdict[str, int] = defaultdict(int)
    display_name: dict[str, str] = {}
    for r in filtered:
        key = (r.get("Standard_Name") or r.get("Item") or "").strip().lower()
        if not key:
            continue
        display_name.setdefault(key, (r.get("Standard_Name") or r.get("Item") or "").strip())
        qty = max(1, r["Quantity"])
        item_qty[key] += qty
        item_spend[key] += r["Price_ISK"]

    top_keys = sorted(item_qty.keys(), key=lambda k: (item_qty[k], item_spend[k]), reverse=True)[:5]
    top_items = [
        {
            "Item": display_name[k],
            "Total quantity": item_qty[k],
            "Total spend (ISK)": fmt_isk(item_spend[k]),
        }
        for k in top_keys
    ]

    st.subheader("Top 5 most purchased items")
    st.dataframe(top_items, use_container_width=True, hide_index=True)

    st.subheader("Edit / delete transactions")
    st.caption("Edit values and click **Save changes**. Mark rows for deletion using the checkbox.")

    all_rows = rows
    by_id = {r["Row_ID"]: r for r in all_rows}

    editable = [
        {
            "Delete": False,
            "Row_ID": r["Row_ID"],
            "Date": r["Date"],
            "Merchant": r["Merchant"],
            "Item": r["Item"],
            "Standard_Name": r.get("Standard_Name", r["Item"]),
            "Quantity": r["Quantity"],
            "Price_ISK": r["Price_ISK"],
            "Category": r["Category"],
        }
        for r in sorted(filtered, key=lambda x: (x["Date"], x["Merchant"], x["Item"]))
    ]

    edited = st.data_editor(
        editable,
        use_container_width=True,
        hide_index=True,
        disabled=["Row_ID"],
        column_config={
            "Delete": st.column_config.CheckboxColumn(),
            "Date": st.column_config.DateColumn(format="YYYY-MM-DD"),
            "Price_ISK": st.column_config.NumberColumn(format="%d"),
            "Quantity": st.column_config.NumberColumn(format="%d"),
        },
        key="history_editor",
    )

    col_save, col_reload = st.columns([1, 1])
    with col_save:
        save_changes = st.button("Save changes", type="primary")
    with col_reload:
        st.button("Reload history")

    if save_changes:
        keep_ids = set()
        updated_rows = {}
        errors = []
        for r in edited:
            row_id = (r.get("Row_ID") or "").strip()
            if not row_id:
                continue
            if r.get("Delete"):
                continue
            d_val = r.get("Date")
            if isinstance(d_val, date):
                d = d_val
            else:
                try:
                    d = datetime.strptime(str(d_val), "%Y-%m-%d").date()
                except Exception:
                    errors.append(f"Invalid date for Row_ID {row_id}")
                    continue
            item = (r.get("Item") or "").strip()
            if not item:
                errors.append(f"Empty Item for Row_ID {row_id}")
                continue
            std = (r.get("Standard_Name") or "").strip() or item
            qty = max(1, as_int(r.get("Quantity", 1), default=1))
            price = as_int(r.get("Price_ISK", 0), default=0)
            cat = (r.get("Category") or "Other").strip() or "Other"
            merch = (r.get("Merchant") or "").strip() or "Unknown"
            keep_ids.add(row_id)
            updated_rows[row_id] = {
                "Row_ID": row_id,
                "Merchant": merch,
                "Date": d,
                "Item": item,
                "Standard_Name": std,
                "Quantity": qty,
                "Price_ISK": price,
                "Category": cat,
            }

        if errors:
            st.error("Could not save due to validation errors:\n- " + "\n- ".join(errors))
        else:
            # Apply updates to full history and drop deletions for this filtered range.
            new_all = []
            deleted_ids = {str(r.get("Row_ID")).strip() for r in edited if r.get("Delete")}
            for row in all_rows:
                rid = row["Row_ID"]
                if rid in deleted_ids:
                    continue
                if rid in updated_rows:
                    new_all.append(updated_rows[rid])
                else:
                    new_all.append(row)
            save_history_rows(new_all)
            st.success("History saved.")
            st.rerun()

    # --- Budgets ---
    st.subheader("Monthly budgets")
    history_months = sorted({yearmonth(r["Date"]) for r in rows})
    default_month = yearmonth(date.today())
    if default_month not in history_months:
        history_months.append(default_month)
        history_months = sorted(history_months)

    selected_month = st.selectbox("Month", history_months, index=history_months.index(default_month))
    y, m = [int(x) for x in selected_month.split("-")]
    month_start = date(y, m, 1)
    month_end = (date(y + 1, 1, 1) - timedelta(days=1)) if m == 12 else (date(y, m + 1, 1) - timedelta(days=1))

    month_rows = [r for r in rows if month_start <= r["Date"] <= month_end]
    month_total = sum(r["Price_ISK"] for r in month_rows)
    st.metric("This month spend", fmt_isk(month_total))

    month_cat_totals: dict[str, int] = defaultdict(int)
    for r in month_rows:
        month_cat_totals[r["Category"]] += r["Price_ISK"]

    budgets_all = load_budgets()
    budgets_for_month = [b for b in budgets_all if b["YearMonth"] == selected_month]
    budget_by_cat = {b["Category"]: b["Budget_ISK"] for b in budgets_for_month}

    known_categories = sorted(set(month_cat_totals.keys()) | set(budget_by_cat.keys()))
    if "Overall" not in known_categories:
        known_categories.insert(0, "Overall")

    budget_editor_rows = []
    for cat in known_categories:
        budget_editor_rows.append(
            {
                "Category": cat,
                "Budget_ISK": budget_by_cat.get(cat, 0),
                "Spent_ISK": month_total if cat == "Overall" else month_cat_totals.get(cat, 0),
            }
        )

    budgets_edited = st.data_editor(
        budget_editor_rows,
        use_container_width=True,
        hide_index=True,
        disabled=["Spent_ISK"],
        column_config={
            "Budget_ISK": st.column_config.NumberColumn(format="%d"),
            "Spent_ISK": st.column_config.NumberColumn(format="%d"),
        },
        key="budget_editor",
    )

    if st.button("Save budgets"):
        # remove old month budgets, then add new
        budgets_all = [b for b in budgets_all if b["YearMonth"] != selected_month]
        for r in budgets_edited:
            cat = (r.get("Category") or "").strip()
            if not cat:
                continue
            budgets_all.append(
                {
                    "YearMonth": selected_month,
                    "Category": cat,
                    "Budget_ISK": as_int(r.get("Budget_ISK", 0), default=0),
                }
            )
        save_budgets(budgets_all)
        st.success("Budgets saved.")
        st.rerun()

    # Over/under tracking
    overall_budget = budget_by_cat.get("Overall", 0)
    if overall_budget:
        diff = overall_budget - month_total
        if diff >= 0:
            st.success(f"Under budget by {fmt_isk(diff)} (Overall).")
        else:
            st.error(f"Over budget by {fmt_isk(abs(diff))} (Overall).")
