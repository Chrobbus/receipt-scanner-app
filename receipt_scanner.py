"""
Receipt Scanner App - Uses Google Gemini to analyze receipt images and extract
a table with Item, Quantity, Price (ISK), Category, plus merchant detection.

IMPROVEMENTS:
- Enhanced prompt tuned for Bónus, Krónan, Costco Icelandic receipts
- Editable review table before saving (fix errors before they enter your data)
- Product dictionary that learns from your corrections (auto-corrects future scans)
- Post-processing to filter junk lines (discounts, subtotals, payment lines)
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

DICTIONARY_COLUMNS = ["OCR_Name", "Corrected_Name", "Standard_Name", "Category", "Merchant"]

GSHEET_HISTORY_TAB = "history"
GSHEET_BUDGETS_TAB = "budgets"
GSHEET_DICTIONARY_TAB = "dictionary"


# ── Icelandic junk-line patterns ──────────────────────────────────────
# Lines matching these are NOT real products — they're subtotals, discounts,
# payment methods, deposit fees, bag charges, etc.
JUNK_PATTERNS = [
    r"(?i)^samtals",          # Samtals / Total
    r"(?i)^alls\b",           # Alls
    r"(?i)^afsl[aá]tt",       # Afsláttur (discount)
    r"(?i)^greiðsla",         # Greiðsla (payment)
    r"(?i)^debetkort",        # Debit card
    r"(?i)^kreditkort",       # Credit card
    r"(?i)^kort\b",           # Kort (card)
    r"(?i)^mynt\b",           # Mynt (coin)
    r"(?i)^innborgun",        # Innborgun (deposit)
    r"(?i)^skilagjald",       # Skilagjald (deposit fee) — keep only if user wants
    r"(?i)^poki\b",           # Poki (bag)
    r"(?i)^plastpoki",        # Plastpoki (plastic bag)
    r"(?i)^burðarpoki",       # Burðarpoki (carrier bag)
    r"(?i)^v(ir)?ðisaukaskattur", # VAT
    r"(?i)^vsk\b",            # VSK (VAT abbreviation)
    r"(?i)^breyting",         # Breyting (change)
    r"(?i)^til baka",         # Til baka (change back)
    r"(?i)^millisamtala",     # Millisamtala (subtotal)
    r"(?i)^fjöldi vara",      # Fjöldi vara (number of items)
    r"(?i)^línur\b",          # Línur (lines count)
    r"(?i)^kennitala",        # Kennitala (ID number)
    r"(?i)^dags\b",           # Dags (date line)
    r"(?i)^kl\.\s*\d",        # Kl. 14:30 (time)
    r"(?i)^kvittun",          # Kvittun (receipt)
    r"(?i)^auðkenni",         # Auðkenni (identifier)
    r"(?i)^afgreiðslu",       # Afgreiðslumaður (cashier)
    r"(?i)^kassi\b",          # Kassi (register)
    r"(?i)^takk\b",           # Takk (thank you)
    r"(?i)^opnunart",         # Opnunartímar (opening hours)
    r"(?i)^s[ií]mi\b",        # Sími (phone)
]
JUNK_RE = [re.compile(p) for p in JUNK_PATTERNS]


def is_junk_line(item_name: str) -> bool:
    """Return True if this looks like a non-product receipt line."""
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
        ws = sh.add_worksheet(title=title, rows=1000, cols=max(8, len(header)))
    existing = ws.row_values(1)
    if [h.strip() for h in existing] != header:
        ws.clear()
        ws.append_row(header, value_input_option="RAW")
    return ws


def using_gsheets() -> bool:
    try:
        return bool((st.secrets.get("GSHEETS_SHEET_ID") or "").strip()) and bool(st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON"))
    except Exception:
        return False


# ── Product Dictionary (learning from corrections) ────────────────────

def load_dictionary() -> list[dict]:
    """Load the product dictionary (OCR→corrected name mappings)."""
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
                "Merchant": (r.get("Merchant") or "").strip(),
            }
            for r in records
            if (r.get("OCR_Name") or "").strip()
        ]
    # CSV fallback
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
                "Merchant": (r.get("Merchant") or "").strip(),
            }
            for r in reader
            if (r.get("OCR_Name") or "").strip()
        ]


def save_dictionary(entries: list[dict]) -> None:
    """Save the full product dictionary."""
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
                (e.get("Merchant") or "").strip(),
            ])
        ws.clear()
        ws.update(values, value_input_option="RAW")
        return
    # CSV fallback
    with DICTIONARY_CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DICTIONARY_COLUMNS)
        w.writeheader()
        for e in entries:
            w.writerow({
                "OCR_Name": (e.get("OCR_Name") or "").strip().lower(),
                "Corrected_Name": (e.get("Corrected_Name") or "").strip(),
                "Standard_Name": (e.get("Standard_Name") or "").strip(),
                "Category": (e.get("Category") or "").strip(),
                "Merchant": (e.get("Merchant") or "").strip(),
            })


def _normalize_for_match(name: str) -> str:
    """Lowercase and strip accents lightly for fuzzy matching."""
    return name.strip().lower()


def apply_dictionary(items: list[dict], merchant: str) -> list[dict]:
    """
    Apply dictionary corrections to Gemini output.
    If the OCR name matches a known entry, replace item name, standard_name, and category.
    """
    dictionary = load_dictionary()
    if not dictionary:
        return items

    # Build lookup: ocr_name → best match (prefer merchant-specific)
    lookup: dict[str, dict] = {}
    for entry in dictionary:
        key = entry["OCR_Name"]
        existing = lookup.get(key)
        # Merchant-specific entries take priority
        if existing is None:
            lookup[key] = entry
        elif entry["Merchant"].lower() == merchant.lower() and existing["Merchant"].lower() != merchant.lower():
            lookup[key] = entry

    corrected = []
    for item in items:
        ocr_key = _normalize_for_match(item.get("item", ""))
        match = lookup.get(ocr_key)
        if match:
            item = dict(item)  # copy
            if match["Corrected_Name"]:
                item["item"] = match["Corrected_Name"]
            if match["Standard_Name"]:
                item["standard_name"] = match["Standard_Name"]
            if match["Category"]:
                item["category"] = match["Category"]
        corrected.append(item)
    return corrected


def learn_from_corrections(
    original_items: list[dict],
    edited_items: list[dict],
    merchant: str,
) -> int:
    """
    Compare original Gemini output with user-edited version.
    If the user changed an item name, standard_name, or category, record the mapping.
    Returns number of new dictionary entries added.
    """
    if len(original_items) != len(edited_items):
        return 0  # rows were added/removed, skip learning

    dictionary = load_dictionary()
    existing_keys = {(e["OCR_Name"], e["Merchant"].lower()) for e in dictionary}

    new_entries = 0
    for orig, edited in zip(original_items, edited_items):
        ocr_name = _normalize_for_match(orig.get("item", ""))
        if not ocr_name:
            continue

        edited_item = (edited.get("Item") or "").strip()
        edited_std = (edited.get("Standard_Name") or "").strip()
        edited_cat = (edited.get("Category") or "").strip()

        orig_item = (orig.get("item") or "").strip()
        orig_std = (orig.get("standard_name") or "").strip()
        orig_cat = (orig.get("category") or "").strip()

        # Check if anything was changed
        changed = (
            edited_item != orig_item
            or edited_std != orig_std
            or edited_cat != orig_cat
        )

        if changed and (ocr_name, merchant.lower()) not in existing_keys:
            dictionary.append({
                "OCR_Name": ocr_name,
                "Corrected_Name": edited_item,
                "Standard_Name": edited_std or edited_item,
                "Category": edited_cat or "Other",
                "Merchant": merchant,
            })
            existing_keys.add((ocr_name, merchant.lower()))
            new_entries += 1

    if new_entries > 0:
        save_dictionary(dictionary)

    return new_entries


# ── History persistence (unchanged from original) ─────────────────────

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
        })
    _write_history_rows_no_upgrade(upgraded)


def append_history_rows(*, merchant: str, purchased_on: date, items: Iterable[dict]) -> int:
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
            })


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
            })
        return rows


# ══════════════════════════════════════════════════════════════════════
#  IMPROVED GEMINI PROMPT
# ══════════════════════════════════════════════════════════════════════

PROMPT = """You are analyzing a photograph of a receipt from an Icelandic store.
Prices are in ISK (Icelandic króna). The receipt text is in Icelandic.

IMPORTANT RULES FOR ICELANDIC RECEIPTS:
1. ONLY extract actual purchased products. NEVER include any of these:
   - "Samtals", "Alls", "Millisamtala" (these are totals/subtotals)
   - "Afsláttur", "Tilboð" (these are discounts — but DO adjust the product price if a discount applies to a specific item)
   - "Skilagjald" (deposit fee — skip these entirely)
   - "Greiðsla", "Debetkort", "Kreditkort", "Kort" (payment method lines)
   - "Poki", "Plastpoki", "Burðarpoki" (bag charges — skip unless they clearly look like a purchased item)
   - "Breyting", "Til baka" (change given back)
   - "VSK", "Virðisaukaskattur" (VAT lines)
   - Lines showing only a date, time, register number, or cashier name

2. PRICE PARSING:
   - Icelandic receipts use periods as thousands separators: "1.299" means 1299 ISK, not 1.299
   - Some receipts show "2 x 399" meaning 2 items at 399 each; set quantity=2 and price_isk=798 (the TOTAL)
   - If an item has a discount line right below it (e.g. "-200" or "Afsl. -200"), subtract the discount from the item price
   - Negative prices are refunds or discounts — skip them unless they clearly apply to the item above
   - price_isk must always be a positive integer (no dots, commas, or decimals)

3. WEIGHT-BASED ITEMS:
   - Items sold by weight show something like "0,456 kg x 1.299 kr/kg = 592"
   - Use the FINAL price (592 in this example), quantity = 1

4. ICELANDIC CHARACTER ACCURACY:
   - Preserve these characters exactly: á, ð, é, í, ó, ú, ý, þ, æ, ö, Á, Ð, É, Í, Ó, Ú, Ý, Þ, Æ, Ö
   - Common items: Nýmjólk, Léttmjólk, Smjör, Brauð, Hrísgrjón, Kartöflur, Laukur, Tómatar, Agúrkur, Bananar, Epli
   - "Skyr" stays "Skyr", "Pylsur" stays "Pylsur", etc.

5. MERCHANT DETECTION:
   - Bónus: yellow-pink bags, pig logo, items often abbreviated
   - Krónan: blue/white branding
   - Costco: large quantities, English product names mixed with Icelandic
   - Hagkaup, Nettó, etc.: detect from receipt header

6. CATEGORIES — assign exactly one:
   - Protein: Skyr, chicken (kjúklingur), fish (fiskur), beef (nautakjöt), pork (svínakjöt), lamb (lambakjöt), eggs (egg), harðfiskur, protein powder/bars, pylsur, hangikjöt, sardínur, túnfiskur
   - Vegetables: agúrka/agúrkur, tómatar, laukur, gulrætur, paprika, brókkólí, salat, spinat, grænmeti, spergilkál
   - Fruits: bananar, epli, appelsínur, jarðarber, bláber, hráberjasafi, ávextir, vindruvor
   - Dairy: mjólk, nýmjólk, léttmjólk, rjómi, ostur, smjör, mysingur (NOT Skyr — Skyr goes under Protein)
   - Grains & Bakery: brauð, hrísgrjón, pasta, hafragrautur, morgunkorn, tortilla, flatbaka
   - Beverages: gosdrykk, safi, kaffi, te, áfengi, bjór, vatn, orkudrykk, Coca Cola, Pepsi, Egils
   - Snacks & Sweets: sælgæti, súkkulaði, snarl, flögur, kex, chips, hnetur
   - Household: hreinsiefni, þvottaefni, tuttpappír, eldhúsrúlla, vökvadiskefni
   - Health & Beauty: sápa, sjampó, tannkrem, tannbursti, rakblöð, deodorant
   - Clothing: föt, sokkar, bolur
   - Electronics: snúra, hleðslutæki, rafhlöður
   - Other: anything that does not clearly fit above

7. STANDARD NAMES — normalize common staples for tracking:
   - Any Skyr product → standard_name = "Skyr"
   - Any chicken product → standard_name = "Kjúklingur"
   - Any egg carton → standard_name = "Egg"
   - Any milk → standard_name = "Mjólk"
   - Any bread → standard_name = "Brauð"
   - Any banana → standard_name = "Bananar"
   - Any rice → standard_name = "Hrísgrjón"
   - For everything else, use the item name as standard_name

Return ONLY a single valid JSON object, no markdown fences:
{"merchant": "Store Name", "items": [{"item": "Exact receipt text", "standard_name": "Normalized", "quantity": 1, "price_isk": 0, "category": "..."}, ...]}"""


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
        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            return json.loads(match.group(0))
        raise ValueError(f"Gemini did not return valid JSON. Response: {text[:500]}")


def postprocess_items(items: list[dict]) -> list[dict]:
    """
    Clean up Gemini output:
    - Remove junk lines that slipped through
    - Ensure prices are positive integers
    - Remove items with zero or negative price
    """
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
        item["category"] = (item.get("category") or "Other").strip() or "Other"
        item["standard_name"] = (item.get("standard_name") or name).strip()
        cleaned.append(item)
    return cleaned


# ══════════════════════════════════════════════════════════════════════
#  STREAMLIT UI
# ══════════════════════════════════════════════════════════════════════

st.set_page_config(page_title="Receipt Scanner", page_icon="🧾", layout="centered")
st.title("🧾 Receipt Scanner")
st.caption("Scan receipts and build a spending history. Now with auto-learning corrections!")


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

        # Only analyze if we haven't already (prevents re-running on every interaction)
        if st.session_state.get("current_receipt_id") != receipt_id:
            with st.spinner("Analyzing receipt with Gemini…"):
                try:
                    data = analyze_receipt_with_gemini(image_data)
                except Exception as e:
                    st.error(f"Analysis failed: {e}")
                    st.stop()

            merchant = (data.get("merchant") or "Unknown").strip() or "Unknown"
            raw_items = data.get("items") or []

            # Post-process: remove junk lines
            clean_items = postprocess_items(raw_items)

            # Apply dictionary corrections
            corrected_items = apply_dictionary(clean_items, merchant)

            # Store in session state
            st.session_state["current_receipt_id"] = receipt_id
            st.session_state["current_merchant"] = merchant
            st.session_state["current_raw_items"] = clean_items  # before dictionary
            st.session_state["current_items"] = corrected_items
            st.session_state["current_image"] = image_data

        # Retrieve from session state
        merchant = st.session_state.get("current_merchant", "Unknown")
        items = st.session_state.get("current_items", [])
        raw_items = st.session_state.get("current_raw_items", [])

        # Show image
        st.subheader("Receipt image")
        st.image(image_data, use_container_width=True)

        # Merchant + date
        st.subheader("Purchase details")
        col_a, col_b = st.columns([2, 1])
        with col_a:
            merchant = st.text_input("Merchant", value=merchant, key="merchant_input")
        with col_b:
            purchased_on = st.date_input("Date", value=date.today())

        # ── EDITABLE review table ─────────────────────────────────────
        st.subheader("Review & edit items")
        st.caption("✏️ Fix any errors below before saving. Your corrections will be remembered for future scans.")

        if items:
            CATEGORY_OPTIONS = [
                "Protein", "Vegetables", "Fruits", "Dairy", "Grains & Bakery",
                "Beverages", "Snacks & Sweets", "Food",
                "Household", "Health & Beauty", "Clothing", "Electronics", "Other",
            ]

            editable_items = [
                {
                    "Keep": True,
                    "Item": (it.get("item") or "").strip(),
                    "Standard_Name": (it.get("standard_name") or it.get("item") or "").strip(),
                    "Quantity": max(1, as_int(it.get("quantity", 1), default=1)),
                    "Price_ISK": as_int(it.get("price_isk", 0), default=0),
                    "Category": (it.get("category") or "Other").strip(),
                }
                for it in items
                if (it.get("item") or "").strip()
            ]

            edited = st.data_editor(
                editable_items,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",  # allow adding missed items
                column_config={
                    "Keep": st.column_config.CheckboxColumn("Keep", default=True),
                    "Item": st.column_config.TextColumn("Item (receipt text)"),
                    "Standard_Name": st.column_config.TextColumn("Standard name"),
                    "Quantity": st.column_config.NumberColumn("Qty", format="%d", min_value=1),
                    "Price_ISK": st.column_config.NumberColumn("Price (ISK)", format="%d", min_value=0),
                    "Category": st.column_config.SelectboxColumn("Category", options=CATEGORY_OPTIONS),
                },
                key="item_editor",
            )

            # Calculate total from kept items
            kept = [r for r in edited if r.get("Keep", True)]
            total_isk = sum(as_int(r.get("Price_ISK", 0)) for r in kept)
            st.metric("Total", fmt_isk(total_isk))

            already_saved = st.session_state.get("last_accepted_receipt_id") == receipt_id
            accept = st.button("Accept & save to history", type="primary", disabled=already_saved)
            if already_saved:
                st.caption("✅ Already saved for this receipt in this session.")

            if accept:
                # Learn from corrections before saving
                learned = learn_from_corrections(raw_items, kept, merchant)
                if learned:
                    st.info(f"📚 Learned {learned} new product correction(s) for future scans.")

                # Build items for saving
                save_items = [
                    {
                        "item": (r.get("Item") or "").strip(),
                        "standard_name": (r.get("Standard_Name") or r.get("Item") or "").strip(),
                        "quantity": as_int(r.get("Quantity", 1), default=1),
                        "price_isk": as_int(r.get("Price_ISK", 0), default=0),
                        "category": (r.get("Category") or "Other").strip(),
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
                        st.warning("Nothing was saved (no valid line items found).")
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
        "These are corrections the app has learned from your edits. "
        "When the scanner sees an OCR name it's seen before, it auto-corrects it. "
        "You can also add entries manually."
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
                    "Merchant": (r.get("Merchant") or "").strip(),
                }
                for r in dict_edited
                if not r.get("Delete") and (r.get("OCR_Name") or "").strip()
            ]
            save_dictionary(new_dict)
            st.success(f"Dictionary saved with {len(new_dict)} entries.")
            st.rerun()
    else:
        st.info(
            "No entries yet. The dictionary grows automatically as you correct items in the Scanner tab. "
            "You can also add entries manually below."
        )
        manual_dict = st.data_editor(
            [{"OCR_Name": "", "Corrected_Name": "", "Standard_Name": "", "Category": "", "Merchant": ""}],
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            key="dict_manual_add",
        )
        if st.button("Save manual entries"):
            new_entries = [
                {
                    "OCR_Name": (r.get("OCR_Name") or "").strip().lower(),
                    "Corrected_Name": (r.get("Corrected_Name") or "").strip(),
                    "Standard_Name": (r.get("Standard_Name") or "").strip(),
                    "Category": (r.get("Category") or "").strip(),
                    "Merchant": (r.get("Merchant") or "").strip(),
                }
                for r in manual_dict
                if (r.get("OCR_Name") or "").strip()
            ]
            if new_entries:
                save_dictionary(new_entries)
                st.success(f"Added {len(new_entries)} entries to dictionary.")
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
    st.metric("Total spend", fmt_isk(total_spend))

    # Manual additions for forgotten receipts
    with st.expander("Add manual purchase (no receipt)"):
        m_date = st.date_input("Manual purchase date", value=today, key="manual_date")
        m_merchant = st.text_input("Merchant (optional)", value="", key="manual_merchant")
        m_total = st.number_input("Total amount (ISK)", min_value=0, step=100, key="manual_total")
        st.caption("Optionally break the total into staples or categories.")
        breakdown_default = [
            {"Standard_Name": "Skyr", "Category": "Protein", "Amount_ISK": 0},
            {"Standard_Name": "Egg", "Category": "Protein", "Amount_ISK": 0},
            {"Standard_Name": "Vegetables", "Category": "Vegetables", "Amount_ISK": 0},
        ]
        breakdown = st.data_editor(
            breakdown_default,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config={"Amount_ISK": st.column_config.NumberColumn(format="%d")},
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
                    items_for_append.append({
                        "item": item_name, "standard_name": std or item_name,
                        "quantity": 1, "price_isk": amt, "category": cat,
                    })
                    allocated += amt

                remaining = max(0, m_total - allocated)
                if remaining > 0:
                    items_for_append.append({
                        "item": "Manual remainder", "standard_name": "Other Food",
                        "quantity": 1, "price_isk": remaining, "category": "Food",
                    })

                if not items_for_append:
                    items_for_append.append({
                        "item": "Manual total", "standard_name": "Manual total",
                        "quantity": 1, "price_isk": m_total, "category": "Food",
                    })

                wrote = append_history_rows(merchant=(m_merchant or "Manual").strip(), purchased_on=m_date, items=items_for_append)
                if wrote:
                    st.success(f"Saved {wrote} manual items to history.")
                    st.rerun()
                else:
                    st.error("Nothing was saved.")

    # Category spending
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

    # Top 5 most purchased
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
        {"Item": display_name[k], "Total quantity": item_qty[k], "Total spend (ISK)": fmt_isk(item_spend[k])}
        for k in top_keys
    ]
    st.subheader("Top 5 most purchased items")
    st.dataframe(top_items, use_container_width=True, hide_index=True)

    # Edit/delete transactions
    st.subheader("Edit / delete transactions")
    st.caption("Edit values and click **Save changes**. Mark rows for deletion using the checkbox.")

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
                "Row_ID": row_id, "Merchant": merch, "Date": d, "Item": item,
                "Standard_Name": std, "Quantity": qty, "Price_ISK": price, "Category": cat,
            }

        if errors:
            st.error("Could not save due to validation errors:\n- " + "\n- ".join(errors))
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

    # Budgets
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
