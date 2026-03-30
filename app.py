"""
Receipt Scanner App - Uses Google Gemini to analyze receipt images and extract
a table with Item, Quantity, Price (ISK), Category, plus merchant detection.

Features:
- Enhanced prompt tuned for Bónus, Krónan, Costco Icelandic receipts
- Editable review table before saving (fix errors before they enter your data)
- Product dictionary that learns from your corrections (auto-corrects future scans)
- Post-processing to filter junk lines (discounts, subtotals, payment lines)
- Me / AG / Shared spending attribution
- Clean categories: Food, Fast Food, Candy & Snacks, Drinks, Alcohol,
  Clothing, Household, Health & Beauty, Other
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
DICTIONARY_CSV_PATH = APP_DIR / "product_dictionary.csv"

# ── Fixed categories ──────────────────────────────────────────────────
CATEGORIES = [
    "Food",
    "Fast Food",
    "Candy & Snacks",
    "Drinks",
    "Alcohol",
    "Clothing",
    "Household",
    "Health & Beauty",
    "Other",
]

FOR_OPTIONS = ["Shared", "Me", "AG"]

HISTORY_COLUMNS = [
    "Row_ID",
    "Merchant",
    "Date",
    "Item",
    "Standard_Name",
    "Quantity",
    "Price_ISK",
    "Category",
    "For",
]

BUDGET_COLUMNS = ["YearMonth", "Category", "Budget_ISK"]

DICTIONARY_COLUMNS = ["OCR_Name", "Corrected_Name", "Standard_Name", "Category", "For", "Merchant"]

GSHEET_HISTORY_TAB = "history"
GSHEET_BUDGETS_TAB = "budgets"
GSHEET_DICTIONARY_TAB = "dictionary"


# ── Icelandic junk-line patterns ──────────────────────────────────────
JUNK_PATTERNS = [
    r"(?i)^samtals",
    r"(?i)^alls\b",
    r"(?i)^afsl[aá]tt",
    r"(?i)^greiðsla",
    r"(?i)^debetkort",
    r"(?i)^kreditkort",
    r"(?i)^kort\b",
    r"(?i)^mynt\b",
    r"(?i)^innborgun",
    r"(?i)^skilagjald",
    r"(?i)^poki\b",
    r"(?i)^plastpoki",
    r"(?i)^burðarpoki",
    r"(?i)^v(ir)?ðisaukaskattur",
    r"(?i)^vsk\b",
    r"(?i)^breyting",
    r"(?i)^til baka",
    r"(?i)^millisamtala",
    r"(?i)^fjöldi vara",
    r"(?i)^línur\b",
    r"(?i)^kennitala",
    r"(?i)^dags\b",
    r"(?i)^kl\.\s*\d",
    r"(?i)^kvittun",
    r"(?i)^auðkenni",
    r"(?i)^afgreiðslu",
    r"(?i)^kassi\b",
    r"(?i)^takk\b",
    r"(?i)^opnunart",
    r"(?i)^s[ií]mi\b",
]
JUNK_RE = [re.compile(p) for p in JUNK_PATTERNS]


def is_junk_line(item_name: str) -> bool:
    name = item_name.strip()
    if not name:
        return True
    return any(rx.search(name) for rx in JUNK_RE)


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


# ── Google Sheets helpers ─────────────────────────────────────────────

def _get_gsheets_client_and_sheet():
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
    elif hasattr(sa, 'items'):
        sa_info = dict(sa)
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
        ws = sh.add_worksheet(title=title, rows=1000, cols=max(10, len(header)))
    existing = ws.row_values(1)
    if [h.strip() for h in existing] != header:
        # If only missing the new "For" column, migrate instead of clearing
        existing_stripped = [h.strip() for h in existing]
        if title == GSHEET_HISTORY_TAB and "For" not in existing_stripped and len(existing_stripped) >= 8:
            # Add the For column header and default existing rows
            ws.update_cell(1, len(existing_stripped) + 1, "For")
            # Fill default "Shared" for all existing data rows
            all_vals = ws.get_all_values()
            if len(all_vals) > 1:
                for row_idx in range(2, len(all_vals) + 1):
                    ws.update_cell(row_idx, len(existing_stripped) + 1, "Shared")
        elif not existing_stripped:
            ws.clear()
            ws.append_row(header, value_input_option="RAW")
        else:
            # Full reset only if header is very different
            ws.clear()
            ws.append_row(header, value_input_option="RAW")
    return ws


def using_gsheets() -> bool:
    try:
        return bool((st.secrets.get("GSHEETS_SHEET_ID") or "").strip()) and bool(st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON"))
    except Exception:
        return False


# ── Session-state cache to avoid GSheets rate limits ──────────────────
# Each key stores {"data": ..., "ts": datetime}. Reads are served from
# cache if less than CACHE_TTL seconds old. Writes clear the relevant key.
CACHE_TTL = 120  # seconds

def _cache_get(key: str) -> Optional[Any]:
    """Return cached value if fresh, else None."""
    entry = st.session_state.get(f"_cache_{key}")
    if entry and (datetime.now() - entry["ts"]).total_seconds() < CACHE_TTL:
        return entry["data"]
    return None

def _cache_set(key: str, data: Any) -> None:
    st.session_state[f"_cache_{key}"] = {"data": data, "ts": datetime.now()}

def _cache_clear(key: str) -> None:
    st.session_state.pop(f"_cache_{key}", None)


# ── Product Dictionary ────────────────────────────────────────────────

def load_dictionary() -> list[dict]:
    cached = _cache_get("dictionary")
    if cached is not None:
        return cached
    result = _load_dictionary_uncached()
    _cache_set("dictionary", result)
    return result

def _load_dictionary_uncached() -> list[dict]:
    if using_gsheets():
        sh = _get_gsheets_client_and_sheet()
        if sh is None:
            return []
        ws = _ensure_ws(sh, GSHEET_DICTIONARY_TAB, DICTIONARY_COLUMNS)
        records = ws.get_all_records()
        return [
            {
                "OCR_Name": (r.get("OCR_Name") or "").strip().lower(),
                "Corrected_Name": (r.get("Corrected_Name") or "").strip(),
                "Standard_Name": (r.get("Standard_Name") or "").strip(),
                "Category": (r.get("Category") or "").strip(),
                "For": (r.get("For") or "").strip(),
                "Merchant": (r.get("Merchant") or "").strip(),
            }
            for r in records
            if (r.get("OCR_Name") or "").strip()
        ]
    if not DICTIONARY_CSV_PATH.exists():
        return []
    with DICTIONARY_CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [
            {
                "OCR_Name": (r.get("OCR_Name") or "").strip().lower(),
                "Corrected_Name": (r.get("Corrected_Name") or "").strip(),
                "Standard_Name": (r.get("Standard_Name") or "").strip(),
                "Category": (r.get("Category") or "").strip(),
                "For": (r.get("For") or "").strip(),
                "Merchant": (r.get("Merchant") or "").strip(),
            }
            for r in reader
            if (r.get("OCR_Name") or "").strip()
        ]


def save_dictionary(entries: list[dict]) -> None:
    _cache_clear("dictionary")
    if using_gsheets():
        sh = _get_gsheets_client_and_sheet()
        if sh is None:
            return
        ws = _ensure_ws(sh, GSHEET_DICTIONARY_TAB, DICTIONARY_COLUMNS)
        values = [DICTIONARY_COLUMNS]
        for e in entries:
            values.append([
                (e.get("OCR_Name") or "").strip().lower(),
                (e.get("Corrected_Name") or "").strip(),
                (e.get("Standard_Name") or "").strip(),
                (e.get("Category") or "").strip(),
                (e.get("For") or "").strip(),
                (e.get("Merchant") or "").strip(),
            ])
        ws.clear()
        ws.update(values, value_input_option="RAW")
        return
    with DICTIONARY_CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DICTIONARY_COLUMNS)
        w.writeheader()
        for e in entries:
            w.writerow({
                "OCR_Name": (e.get("OCR_Name") or "").strip().lower(),
                "Corrected_Name": (e.get("Corrected_Name") or "").strip(),
                "Standard_Name": (e.get("Standard_Name") or "").strip(),
                "Category": (e.get("Category") or "").strip(),
                "For": (e.get("For") or "").strip(),
                "Merchant": (e.get("Merchant") or "").strip(),
            })


def apply_dictionary(items: list[dict], merchant: str) -> list[dict]:
    dictionary = load_dictionary()
    if not dictionary:
        return items
    lookup: dict[str, dict] = {}
    for entry in dictionary:
        key = entry["OCR_Name"]
        existing = lookup.get(key)
        if existing is None:
            lookup[key] = entry
        elif entry["Merchant"].lower() == merchant.lower() and existing["Merchant"].lower() != merchant.lower():
            lookup[key] = entry

    corrected = []
    for item in items:
        ocr_key = (item.get("item") or "").strip().lower()
        match = lookup.get(ocr_key)
        if match:
            item = dict(item)
            if match["Corrected_Name"]:
                item["item"] = match["Corrected_Name"]
            if match["Standard_Name"]:
                item["standard_name"] = match["Standard_Name"]
            if match["Category"]:
                item["category"] = match["Category"]
            if match["For"]:
                item["for_whom"] = match["For"]
        corrected.append(item)
    return corrected


def learn_from_corrections(
    original_items: list[dict],
    edited_items: list[dict],
    merchant: str,
) -> int:
    if len(original_items) != len(edited_items):
        return 0
    dictionary = load_dictionary()
    existing_keys = {(e["OCR_Name"], e["Merchant"].lower()) for e in dictionary}
    new_entries = 0
    for orig, edited in zip(original_items, edited_items):
        ocr_name = (orig.get("item") or "").strip().lower()
        if not ocr_name:
            continue
        edited_item = (edited.get("Item") or "").strip()
        edited_std = (edited.get("Standard_Name") or "").strip()
        edited_cat = (edited.get("Category") or "").strip()
        edited_for = (edited.get("For") or "").strip()
        orig_item = (orig.get("item") or "").strip()
        orig_std = (orig.get("standard_name") or "").strip()
        orig_cat = (orig.get("category") or "").strip()
        orig_for = (orig.get("for_whom") or "Shared").strip()
        changed = (
            edited_item != orig_item
            or edited_std != orig_std
            or edited_cat != orig_cat
            or edited_for != orig_for
        )
        if changed and (ocr_name, merchant.lower()) not in existing_keys:
            dictionary.append({
                "OCR_Name": ocr_name,
                "Corrected_Name": edited_item,
                "Standard_Name": edited_std or edited_item,
                "Category": edited_cat or "Other",
                "For": edited_for or "Shared",
                "Merchant": merchant,
            })
            existing_keys.add((ocr_name, merchant.lower()))
            new_entries += 1
    if new_entries > 0:
        save_dictionary(dictionary)
    return new_entries


# ── History persistence ───────────────────────────────────────────────

def _parse_for_value(val: Any) -> str:
    """Safely parse the 'For' field, defaulting to Shared."""
    s = (str(val) if val is not None else "").strip()
    if s in FOR_OPTIONS:
        return s
    return "Shared"


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
                f"{row.get('Merchant','')}-{row.get('Date')}-{row.get('Item','')}-{row.get('Price_ISK',0)}-{idx}".encode("utf-8")
            ).hexdigest()[:16]
        rows.append({
            "Row_ID": row_id,
            "Merchant": (row.get("Merchant") or "").strip() or "Unknown",
            "Date": purchased_on,
            "Item": (row.get("Item") or "").strip(),
            "Standard_Name": (row.get("Standard_Name") or "").strip() or (row.get("Item") or "").strip(),
            "Quantity": as_int(row.get("Quantity", 1), default=1),
            "Price_ISK": as_int(row.get("Price_ISK", 0), default=0),
            "Category": (row.get("Category") or "Other").strip() or "Other",
            "For": _parse_for_value(row.get("For")),
        })
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
        values.append([
            (r.get("Row_ID") or "").strip() or uuid.uuid4().hex,
            (r.get("Merchant") or "").strip() or "Unknown",
            purchased_on.isoformat(),
            item,
            (r.get("Standard_Name") or "").strip() or item,
            max(1, as_int(r.get("Quantity", 1), default=1)),
            as_int(r.get("Price_ISK", 0), default=0),
            (r.get("Category") or "Other").strip() or "Other",
            _parse_for_value(r.get("For")),
        ])
    ws.clear()
    ws.update(values, value_input_option="RAW")


def _gsheets_append_history_rows(*, merchant: str, purchased_on: date, items: Iterable[dict]) -> int:
    sh = _get_gsheets_client_and_sheet()
    if sh is None:
        raise RuntimeError("Could not connect to Google Sheets — check secrets.")
    ws = _ensure_ws(sh, GSHEET_HISTORY_TAB, HISTORY_COLUMNS)
    out_rows = []
    for it in items:
        item_name = (it.get("item") or it.get("Item") or "").strip()
        if not item_name:
            continue
        std_name = (it.get("standard_name") or it.get("Standard_Name") or "").strip() or item_name
        out_rows.append([
            uuid.uuid4().hex,
            merchant,
            purchased_on.isoformat(),
            item_name,
            std_name,
            as_int(it.get("quantity") or it.get("Quantity", 1), default=1),
            as_int(it.get("price_isk") or it.get("Price_ISK", 0), default=0),
            (it.get("category") or it.get("Category") or "Other").strip() or "Other",
            _parse_for_value(it.get("for_whom") or it.get("For")),
        ])
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
        out.append({"YearMonth": ym, "Category": cat, "Budget_ISK": as_int(row.get("Budget_ISK", 0), default=0)})
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


def ensure_budgets_csv_exists() -> None:
    if BUDGETS_CSV_PATH.exists():
        return
    with BUDGETS_CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=BUDGET_COLUMNS)
        w.writeheader()


def load_budgets() -> list[dict]:
    cached = _cache_get("budgets")
    if cached is not None:
        return cached
    result = _load_budgets_uncached()
    _cache_set("budgets", result)
    return result

def _load_budgets_uncached() -> list[dict]:
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
            out.append({"YearMonth": ym, "Category": cat, "Budget_ISK": as_int(row.get("Budget_ISK", 0), default=0)})
        return out


def save_budgets(rows: list[dict]) -> None:
    _cache_clear("budgets")
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
            w.writerow({"YearMonth": ym, "Category": cat, "Budget_ISK": as_int(r.get("Budget_ISK", 0), default=0)})


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
            f"{row.get('Merchant','')}-{row.get('Date')}-{row.get('Item','')}-{row.get('Price_ISK',0)}-{idx}".encode("utf-8")
        ).hexdigest()[:16]
        std = (row.get("Standard_Name") or "").strip() or (row.get("Item") or "").strip()
        upgraded.append({
            "Row_ID": row_id,
            "Merchant": row.get("Merchant", "Unknown"),
            "Date": row.get("Date"),
            "Item": row.get("Item", ""),
            "Standard_Name": std,
            "Quantity": as_int(row.get("Quantity", 1), default=1),
            "Price_ISK": as_int(row.get("Price_ISK", 0), default=0),
            "Category": row.get("Category", "Other"),
            "For": _parse_for_value(row.get("For")),
        })
    _write_history_rows_no_upgrade(upgraded)


def append_history_rows(*, merchant: str, purchased_on: date, items: Iterable[dict]) -> int:
    _cache_clear("history")
    if using_gsheets():
        return _gsheets_append_history_rows(merchant=merchant, purchased_on=purchased_on, items=items)
    ensure_history_csv_exists_and_up_to_date()
    wrote = 0
    with HISTORY_CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HISTORY_COLUMNS)
        for it in items:
            item_name = (it.get("item") or it.get("Item") or "").strip()
            if not item_name:
                continue
            std_name = (it.get("standard_name") or it.get("Standard_Name") or "").strip()
            if not std_name:
                std_name = item_name
            w.writerow({
                "Row_ID": uuid.uuid4().hex,
                "Merchant": merchant,
                "Date": purchased_on.isoformat(),
                "Item": item_name,
                "Standard_Name": std_name,
                "Quantity": as_int(it.get("quantity") or it.get("Quantity", 1), default=1),
                "Price_ISK": as_int(it.get("price_isk") or it.get("Price_ISK", 0), default=0),
                "Category": (it.get("category") or it.get("Category") or "Other").strip() or "Other",
                "For": _parse_for_value(it.get("for_whom") or it.get("For")),
            })
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
            w.writerow({
                "Row_ID": row_id,
                "Merchant": (r.get("Merchant") or "").strip() or "Unknown",
                "Date": purchased_on.isoformat(),
                "Item": item,
                "Standard_Name": std,
                "Quantity": max(1, as_int(r.get("Quantity", 1), default=1)),
                "Price_ISK": as_int(r.get("Price_ISK", 0), default=0),
                "Category": (r.get("Category") or "Other").strip() or "Other",
                "For": _parse_for_value(r.get("For")),
            })


def save_history_rows(rows: list[dict]) -> None:
    _cache_clear("history")
    if using_gsheets():
        _gsheets_save_history_rows(rows)
        return
    ensure_history_csv_exists_and_up_to_date()
    _write_history_rows_no_upgrade(rows)


def load_history_rows(*, allow_upgrade: bool = True) -> list[dict]:
    cached = _cache_get("history")
    if cached is not None:
        return cached
    result = _load_history_rows_uncached(allow_upgrade=allow_upgrade)
    _cache_set("history", result)
    return result

def _load_history_rows_uncached(*, allow_upgrade: bool = True) -> list[dict]:
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
                    f"{row.get('Merchant','')}-{row.get('Date')}-{row.get('Item','')}-{row.get('Price_ISK',0)}-{idx}".encode("utf-8")
                ).hexdigest()[:16]
            rows.append({
                "Row_ID": row_id,
                "Merchant": (row.get("Merchant") or "").strip() or "Unknown",
                "Date": purchased_on,
                "Item": (row.get("Item") or "").strip(),
                "Standard_Name": (row.get("Standard_Name") or "").strip() or (row.get("Item") or "").strip(),
                "Quantity": as_int(row.get("Quantity", 1), default=1),
                "Price_ISK": as_int(row.get("Price_ISK", 0), default=0),
                "Category": (row.get("Category") or "Other").strip() or "Other",
                "For": _parse_for_value(row.get("For")),
            })
        return rows


# ══════════════════════════════════════════════════════════════════════
#  GEMINI PROMPT
# ══════════════════════════════════════════════════════════════════════

PROMPT = """You are analyzing a photograph of a receipt from an Icelandic store.
Prices are in ISK (Icelandic króna). The receipt text is in Icelandic.

IMPORTANT RULES FOR ICELANDIC RECEIPTS:
1. ONLY extract actual purchased products. NEVER include any of these:
   - "Samtals", "Alls", "Millisamtala" (totals/subtotals)
   - "Afsláttur", "Tilboð" (discounts — but DO adjust the product price if a discount applies to a specific item)
   - "Skilagjald" (deposit fee — skip entirely)
   - "Greiðsla", "Debetkort", "Kreditkort", "Kort" (payment lines)
   - "Poki", "Plastpoki", "Burðarpoki" (bag charges)
   - "Breyting", "Til baka" (change given)
   - "VSK", "Virðisaukaskattur" (VAT lines)
   - Lines showing only a date, time, register number, or cashier name

2. PRICE PARSING:
   - Icelandic receipts use periods as thousands separators: "1.299" means 1299 ISK
   - "2 x 399" means quantity=2, price_isk=798 (the TOTAL paid)
   - If a discount line applies to the item above, subtract it from the item price
   - price_isk must be a positive integer

3. WEIGHT-BASED ITEMS:
   - "0,456 kg x 1.299 kr/kg = 592" → use final price 592, quantity = 1

4. ICELANDIC CHARACTERS — preserve exactly:
   á, ð, é, í, ó, ú, ý, þ, æ, ö, Á, Ð, É, Í, Ó, Ú, Ý, Þ, Æ, Ö
   Common items: Nýmjólk, Léttmjólk, Smjör, Brauð, Hrísgrjón, Kartöflur,
   Laukur, Tómatar, Agúrkur, Bananar, Epli, Appelsínur, Pylsur, Kjúklingur

5. MERCHANT DETECTION:
   - Bónus: yellow-pink branding, pig logo
   - Krónan: blue/white
   - Costco: large quantities, English names mixed with Icelandic

6. CATEGORIES — assign exactly one of these nine:
   - Food: groceries for cooking/eating at home — meat, fish, dairy, eggs, skyr,
     vegetables, fruits, bread, rice, pasta, frozen meals, cooking oil, spices, etc.
   - Fast Food: takeaway meals, restaurant food, Domino's, Subway, pylsur from stands, etc.
   - Candy & Snacks: sælgæti, súkkulaði, chips, kex, ís (ice cream), nammi, snarl, hnetur
   - Drinks: non-alcoholic — gosdrykk, safi, kaffi, te, vatn, orkudrykk, Coca Cola, Pepsi, Egils Appelsín
   - Alcohol: bjór, vín, áfengi, vodka, gin, anything from Vínbúðin/ÁTVR
   - Clothing: föt, sokkar, bolur, jakki, skór
   - Household: hreinsiefni, þvottaefni, tuttpappír, eldhúsrúlla, disk-, þvotta-, gler-efni,
     poki, vökvadiskefni, rakari, ljós, plásttaska
   - Health & Beauty: sápa, sjampó, tannkrem, tannbursti, rakblöð, deodorant, lyf, plástur
   - Other: anything that does not clearly fit above (electronics, gifts, etc.)

7. STANDARD NAMES — normalize for tracking:
   - Any Skyr → "Skyr"
   - Any chicken → "Kjúklingur"
   - Any egg carton → "Egg"
   - Any milk → "Mjólk"
   - Any bread → "Brauð"
   - Any banana → "Bananar"
   - Any rice → "Hrísgrjón"
   - Any butter → "Smjör"
   - Any potato → "Kartöflur"
   - Any tomato → "Tómatar"
   - Otherwise use the item name

Return ONLY a single valid JSON object, no markdown fences:
{"merchant": "Store Name", "items": [{"item": "Exact receipt text", "standard_name": "Normalized", "quantity": 1, "price_isk": 0, "category": "..."}, ...]}"""


def analyze_receipt_with_gemini(image_bytes: bytes) -> dict:
    image = Image.open(io.BytesIO(image_bytes))
    model = genai.GenerativeModel("gemini-2.5-flash")
    response = model.generate_content([PROMPT, image])
    text = response.text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            return json.loads(match.group(0))
        raise ValueError(f"Gemini did not return valid JSON. Response: {text[:500]}")


def postprocess_items(items: list[dict]) -> list[dict]:
    cleaned = []
    for item in items:
        name = (item.get("item") or "").strip()
        if not name:
            continue
        if is_junk_line(name):
            continue
        price = as_int(item.get("price_isk", 0), default=0)
        if price <= 0:
            continue
        item = dict(item)
        item["price_isk"] = price
        item["quantity"] = max(1, as_int(item.get("quantity", 1), default=1))
        cat = (item.get("category") or "Other").strip()
        if cat not in CATEGORIES:
            cat = "Other"
        item["category"] = cat
        item["standard_name"] = (item.get("standard_name") or name).strip()
        if "for_whom" not in item:
            item["for_whom"] = "Shared"
        cleaned.append(item)
    return cleaned


# ══════════════════════════════════════════════════════════════════════
#  STREAMLIT UI
# ══════════════════════════════════════════════════════════════════════

st.set_page_config(page_title="Receipt Scanner", page_icon="🧾", layout="centered")
st.title("🧾 Receipt Scanner")
st.caption("Scan receipts · Track spending · Me / AG / Shared")


def get_api_key() -> Optional[str]:
    try:
        return st.secrets.get("GEMINI_API_KEY") or st.secrets.get("gemini_api_key")
    except Exception:
        return None


api_key = get_api_key()
if not api_key:
    with st.sidebar:
        api_key = st.text_input("Google Gemini API key", type="password", help="Get a key at https://aistudio.google.com/apikey")
if not api_key:
    st.info("Enter your Gemini API key in the sidebar to continue.")
    st.stop()

genai.configure(api_key=api_key)

tab_scanner, tab_insights, tab_dictionary = st.tabs(["Scanner", "Insights", "Dictionary"])


# ── Scanner Tab ───────────────────────────────────────────────────────

with tab_scanner:
    source = st.radio("Choose input", ["Upload an image", "Take a photo"], horizontal=True)
    image_data = None

    if source == "Upload an image":
        uploaded = st.file_uploader("Upload receipt image", type=["png", "jpg", "jpeg"], label_visibility="collapsed")
        if uploaded:
            image_data = uploaded.read()
    elif source == "Take a photo":
        cam = st.camera_input("Take a photo of your receipt")
        if cam:
            image_data = cam.getvalue()

    if image_data:
        receipt_id = hashlib.sha1(image_data).hexdigest()

        if st.session_state.get("current_receipt_id") != receipt_id:
            with st.spinner("Analyzing receipt with Gemini…"):
                try:
                    data = analyze_receipt_with_gemini(image_data)
                except Exception as e:
                    st.error(f"Analysis failed: {e}")
                    st.stop()

            merchant = (data.get("merchant") or "Unknown").strip() or "Unknown"
            raw_items = data.get("items") or []
            clean_items = postprocess_items(raw_items)
            corrected_items = apply_dictionary(clean_items, merchant)

            st.session_state["current_receipt_id"] = receipt_id
            st.session_state["current_merchant"] = merchant
            st.session_state["current_raw_items"] = clean_items
            st.session_state["current_items"] = corrected_items
            st.session_state["current_image"] = image_data

        merchant = st.session_state.get("current_merchant", "Unknown")
        items = st.session_state.get("current_items", [])
        raw_items = st.session_state.get("current_raw_items", [])

        st.subheader("Receipt image")
        st.image(image_data, use_container_width=True)

        st.subheader("Purchase details")
        col_a, col_b = st.columns([2, 1])
        with col_a:
            merchant = st.text_input("Merchant", value=merchant, key="merchant_input")
        with col_b:
            purchased_on = st.date_input("Date", value=date.today())

        # ── EDITABLE review table ─────────────────────────────────────
        st.subheader("Review & edit items")
        st.caption("✏️ Fix errors, set **For** (Me/AG/Shared), then save. Corrections are remembered!")

        if items:
            editable_items = [
                {
                    "Keep": True,
                    "Item": (it.get("item") or "").strip(),
                    "Standard_Name": (it.get("standard_name") or it.get("item") or "").strip(),
                    "Quantity": max(1, as_int(it.get("quantity", 1), default=1)),
                    "Price_ISK": as_int(it.get("price_isk", 0), default=0),
                    "Category": (it.get("category") or "Other").strip(),
                    "For": (it.get("for_whom") or "Shared").strip(),
                }
                for it in items
                if (it.get("item") or "").strip()
            ]

            edited = st.data_editor(
                editable_items,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                column_config={
                    "Keep": st.column_config.CheckboxColumn("Keep", default=True),
                    "Item": st.column_config.TextColumn("Item (receipt text)"),
                    "Standard_Name": st.column_config.TextColumn("Standard name"),
                    "Quantity": st.column_config.NumberColumn("Qty", format="%d", min_value=1),
                    "Price_ISK": st.column_config.NumberColumn("Price (ISK)", format="%d", min_value=0),
                    "Category": st.column_config.SelectboxColumn("Category", options=CATEGORIES),
                    "For": st.column_config.SelectboxColumn("For", options=FOR_OPTIONS),
                },
                key="item_editor",
            )

            kept = [r for r in edited if r.get("Keep", True)]
            total_isk = sum(as_int(r.get("Price_ISK", 0)) for r in kept)

            # Show total + breakdown
            col_t, col_me, col_ag, col_sh = st.columns(4)
            with col_t:
                st.metric("Total", fmt_isk(total_isk))
            me_total = sum(as_int(r.get("Price_ISK", 0)) for r in kept if r.get("For") == "Me")
            ag_total = sum(as_int(r.get("Price_ISK", 0)) for r in kept if r.get("For") == "AG")
            shared_total = sum(as_int(r.get("Price_ISK", 0)) for r in kept if r.get("For") == "Shared")
            with col_me:
                st.metric("Me", fmt_isk(me_total))
            with col_ag:
                st.metric("AG", fmt_isk(ag_total))
            with col_sh:
                st.metric("Shared", fmt_isk(shared_total))

            already_saved = st.session_state.get("last_accepted_receipt_id") == receipt_id
            accept = st.button("Accept & save to history", type="primary", disabled=already_saved)
            if already_saved:
                st.caption("✅ Already saved for this receipt.")

            if accept:
                learned = learn_from_corrections(raw_items, kept, merchant)
                if learned:
                    st.info(f"📚 Learned {learned} new correction(s) for future scans.")

                save_items = [
                    {
                        "item": (r.get("Item") or "").strip(),
                        "standard_name": (r.get("Standard_Name") or r.get("Item") or "").strip(),
                        "quantity": as_int(r.get("Quantity", 1), default=1),
                        "price_isk": as_int(r.get("Price_ISK", 0), default=0),
                        "category": (r.get("Category") or "Other").strip(),
                        "for_whom": (r.get("For") or "Shared").strip(),
                    }
                    for r in kept
                    if (r.get("Item") or "").strip()
                ]

                try:
                    wrote = append_history_rows(merchant=merchant, purchased_on=purchased_on, items=save_items)
                    st.session_state["last_accepted_receipt_id"] = receipt_id
                    if wrote:
                        st.success(f"Saved {wrote} items to history.")
                    else:
                        st.warning("Nothing saved (no valid items).")
                except Exception as e:
                    st.error(f"Save failed: {e}")
        else:
            st.info("No line items were extracted from this receipt.")
    else:
        st.info("Upload an image or take a photo to get started.")


# ── Dictionary Tab ────────────────────────────────────────────────────

with tab_dictionary:
    st.subheader("Product Dictionary")
    st.caption(
        "Auto-learned corrections from your edits. When the scanner sees an OCR name "
        "it recognizes, it auto-corrects the name, category, and For assignment."
    )

    dict_entries = load_dictionary()

    if dict_entries:
        dict_editable = [
            {
                "Delete": False,
                "OCR_Name": e["OCR_Name"],
                "Corrected_Name": e["Corrected_Name"],
                "Standard_Name": e["Standard_Name"],
                "Category": e["Category"],
                "For": e.get("For", ""),
                "Merchant": e["Merchant"],
            }
            for e in dict_entries
        ]

        dict_edited = st.data_editor(
            dict_editable,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config={
                "Delete": st.column_config.CheckboxColumn("Delete"),
                "Category": st.column_config.SelectboxColumn("Category", options=CATEGORIES),
                "For": st.column_config.SelectboxColumn("For", options=[""] + FOR_OPTIONS),
            },
            key="dict_editor",
        )

        if st.button("Save dictionary", type="primary"):
            new_dict = [
                {
                    "OCR_Name": (r.get("OCR_Name") or "").strip().lower(),
                    "Corrected_Name": (r.get("Corrected_Name") or "").strip(),
                    "Standard_Name": (r.get("Standard_Name") or "").strip(),
                    "Category": (r.get("Category") or "").strip(),
                    "For": (r.get("For") or "").strip(),
                    "Merchant": (r.get("Merchant") or "").strip(),
                }
                for r in dict_edited
                if not r.get("Delete") and (r.get("OCR_Name") or "").strip()
            ]
            save_dictionary(new_dict)
            st.success(f"Dictionary saved with {len(new_dict)} entries.")
            st.rerun()
    else:
        st.info("No entries yet. The dictionary grows as you correct items in the Scanner tab.")
        manual_dict = st.data_editor(
            [{"OCR_Name": "", "Corrected_Name": "", "Standard_Name": "", "Category": "", "For": "", "Merchant": ""}],
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config={
                "Category": st.column_config.SelectboxColumn("Category", options=CATEGORIES),
                "For": st.column_config.SelectboxColumn("For", options=[""] + FOR_OPTIONS),
            },
            key="dict_manual_add",
        )
        if st.button("Save manual entries"):
            new_entries = [
                {
                    "OCR_Name": (r.get("OCR_Name") or "").strip().lower(),
                    "Corrected_Name": (r.get("Corrected_Name") or "").strip(),
                    "Standard_Name": (r.get("Standard_Name") or "").strip(),
                    "Category": (r.get("Category") or "").strip(),
                    "For": (r.get("For") or "").strip(),
                    "Merchant": (r.get("Merchant") or "").strip(),
                }
                for r in manual_dict
                if (r.get("OCR_Name") or "").strip()
            ]
            if new_entries:
                save_dictionary(new_entries)
                st.success(f"Added {len(new_entries)} entries.")
                st.rerun()


# ── Insights Tab ──────────────────────────────────────────────────────

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
        start, end = st.date_input("Custom range", value=(today - timedelta(days=30), today))

    filtered = [r for r in rows if start <= r["Date"] <= end]
    if not filtered:
        st.warning("No purchases found in that date range.")
        st.stop()

    total_spend = sum(r["Price_ISK"] for r in filtered)

    # ── Top-level metrics with Me/AG/Shared split ─────────────────
    me_spend = sum(r["Price_ISK"] for r in filtered if r.get("For") == "Me")
    ag_spend = sum(r["Price_ISK"] for r in filtered if r.get("For") == "AG")
    shared_spend = sum(r["Price_ISK"] for r in filtered if r.get("For") == "Shared")

    col_t, col_me, col_ag, col_sh = st.columns(4)
    with col_t:
        st.metric("Total spend", fmt_isk(total_spend))
    with col_me:
        st.metric("Me", fmt_isk(me_spend))
    with col_ag:
        st.metric("AG", fmt_isk(ag_spend))
    with col_sh:
        st.metric("Shared", fmt_isk(shared_spend))

    # Manual purchase entry
    with st.expander("Add manual purchase (no receipt)"):
        m_date = st.date_input("Manual purchase date", value=today, key="manual_date")
        m_merchant = st.text_input("Merchant (optional)", value="", key="manual_merchant")
        m_total = st.number_input("Total amount (ISK)", min_value=0, step=100, key="manual_total")
        m_category = st.selectbox("Category", CATEGORIES, key="manual_category")
        m_for = st.selectbox("For", FOR_OPTIONS, key="manual_for")
        st.caption("Optionally break the total into specific items:")
        breakdown_default = [
            {"Item": "", "Standard_Name": "", "Category": m_category, "For": m_for, "Amount_ISK": 0},
        ]
        breakdown = st.data_editor(
            breakdown_default,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config={
                "Amount_ISK": st.column_config.NumberColumn(format="%d"),
                "Category": st.column_config.SelectboxColumn("Category", options=CATEGORIES),
                "For": st.column_config.SelectboxColumn("For", options=FOR_OPTIONS),
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
                    item_name = (row.get("Item") or row.get("Standard_Name") or "Manual item").strip()
                    std = (row.get("Standard_Name") or item_name).strip()
                    cat = (row.get("Category") or m_category).strip()
                    for_whom = (row.get("For") or m_for).strip()
                    items_for_append.append({
                        "item": item_name, "standard_name": std,
                        "quantity": 1, "price_isk": amt,
                        "category": cat, "for_whom": for_whom,
                    })
                    allocated += amt

                remaining = max(0, m_total - allocated)
                if remaining > 0:
                    items_for_append.append({
                        "item": "Manual remainder", "standard_name": "Manual remainder",
                        "quantity": 1, "price_isk": remaining,
                        "category": m_category, "for_whom": m_for,
                    })

                if not items_for_append:
                    items_for_append.append({
                        "item": "Manual purchase", "standard_name": "Manual purchase",
                        "quantity": 1, "price_isk": m_total,
                        "category": m_category, "for_whom": m_for,
                    })

                wrote = append_history_rows(merchant=(m_merchant or "Manual").strip(), purchased_on=m_date, items=items_for_append)
                if wrote:
                    st.success(f"Saved {wrote} manual items.")
                    st.rerun()
                else:
                    st.error("Nothing was saved.")

    # ── Spending by category ──────────────────────────────────────
    st.subheader("Spending by category")

    cat_totals: dict[str, int] = defaultdict(int)
    cat_for_totals: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    items_by_cat: dict[str, list[dict]] = defaultdict(list)

    for r in filtered:
        cat = r["Category"]
        cat_totals[cat] += r["Price_ISK"]
        cat_for_totals[cat][r.get("For", "Shared")] += r["Price_ISK"]
        items_by_cat[cat].append(r)

    for cat, total in sorted(cat_totals.items(), key=lambda x: x[1], reverse=True):
        for_breakdown = cat_for_totals[cat]
        parts = []
        if for_breakdown.get("Me", 0) > 0:
            parts.append(f"Me: {fmt_isk(for_breakdown['Me'])}")
        if for_breakdown.get("AG", 0) > 0:
            parts.append(f"AG: {fmt_isk(for_breakdown['AG'])}")
        if for_breakdown.get("Shared", 0) > 0:
            parts.append(f"Shared: {fmt_isk(for_breakdown['Shared'])}")
        subtitle = " · ".join(parts) if parts else ""

        with st.expander(f"{cat}: {fmt_isk(total)}" + (f"  ({subtitle})" if subtitle else ""), expanded=False):
            detail_rows = [
                {
                    "Date": row["Date"].isoformat(),
                    "Merchant": row["Merchant"],
                    "Item": row["Item"],
                    "Standard_Name": row.get("Standard_Name", row["Item"]),
                    "Qty": row["Quantity"],
                    "Price": fmt_isk(row["Price_ISK"]),
                    "For": row.get("For", "Shared"),
                }
                for row in sorted(items_by_cat[cat], key=lambda x: (x["Date"], x["Merchant"], x["Item"]))
            ]
            st.dataframe(detail_rows, use_container_width=True, hide_index=True)

    # ── AG spending summary ───────────────────────────────────────
    if ag_spend > 0:
        st.subheader("AG spending breakdown")
        ag_items = [r for r in filtered if r.get("For") == "AG"]
        ag_cat_totals: dict[str, int] = defaultdict(int)
        for r in ag_items:
            ag_cat_totals[r["Category"]] += r["Price_ISK"]
        ag_summary = [
            {"Category": cat, "Spend": fmt_isk(total)}
            for cat, total in sorted(ag_cat_totals.items(), key=lambda x: x[1], reverse=True)
        ]
        st.dataframe(ag_summary, use_container_width=True, hide_index=True)

    # ── Top purchased items ───────────────────────────────────────
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
        {"Item": display_name[k], "Total qty": item_qty[k], "Total spend": fmt_isk(item_spend[k])}
        for k in top_keys
    ]
    st.subheader("Top 5 most purchased items")
    st.dataframe(top_items, use_container_width=True, hide_index=True)

    # ── Edit / delete transactions ────────────────────────────────
    st.subheader("Edit / delete transactions")
    st.caption("Edit values and click **Save changes**.")

    all_rows = rows
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
            "For": r.get("For", "Shared"),
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
            "Category": st.column_config.SelectboxColumn("Category", options=CATEGORIES),
            "For": st.column_config.SelectboxColumn("For", options=FOR_OPTIONS),
        },
        key="history_editor",
    )

    col_save, col_reload = st.columns([1, 1])
    with col_save:
        save_changes = st.button("Save changes", type="primary")
    with col_reload:
        st.button("Reload history")

    if save_changes:
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
            for_val = _parse_for_value(r.get("For"))
            updated_rows[row_id] = {
                "Row_ID": row_id, "Merchant": merch, "Date": d, "Item": item,
                "Standard_Name": std, "Quantity": qty, "Price_ISK": price,
                "Category": cat, "For": for_val,
            }

        if errors:
            st.error("Validation errors:\n- " + "\n- ".join(errors))
        else:
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

    # ── Monthly budgets ───────────────────────────────────────────
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
        budget_editor_rows.append({
            "Category": cat,
            "Budget_ISK": budget_by_cat.get(cat, 0),
            "Spent_ISK": month_total if cat == "Overall" else month_cat_totals.get(cat, 0),
        })

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
        budgets_all = [b for b in budgets_all if b["YearMonth"] != selected_month]
        for r in budgets_edited:
            cat = (r.get("Category") or "").strip()
            if not cat:
                continue
            budgets_all.append({
                "YearMonth": selected_month,
                "Category": cat,
                "Budget_ISK": as_int(r.get("Budget_ISK", 0), default=0),
            })
        save_budgets(budgets_all)
        st.success("Budgets saved.")
        st.rerun()

    overall_budget = budget_by_cat.get("Overall", 0)
    if overall_budget:
        diff = overall_budget - month_total
        if diff >= 0:
            st.success(f"Under budget by {fmt_isk(diff)} (Overall).")
        else:
            st.error(f"Over budget by {fmt_isk(abs(diff))} (Overall).")
