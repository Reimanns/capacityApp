
# gsheets_store.py
# Zero-cost Google Sheets persistence for Streamlit.
# Requires: gspread, google-auth
# usage in app: import gsheets_store as ds

import json
import os
from typing import List, Dict, Optional

import gspread
from google.oauth2.service_account import Credentials

# ----- Globals -----
GC = None         # gspread client
SH = None         # Spreadsheet
SHEET_ID = None

WS_PROJECTS = "projects"
WS_POTENTIAL = "potential"
WS_ACTUAL = "actual"
WS_DEPTS = "depts"

BASE_HEADERS = ["number","customer","aircraftModel","scope","induction","delivery"]

def _get_creds(credentials_dict: Optional[dict] = None):
    if credentials_dict is None:
        # Allow env var (stringified JSON) fallback
        env_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        if env_json:
            credentials_dict = json.loads(env_json)
    if credentials_dict is None:
        raise RuntimeError("Missing Google service account credentials. Put JSON in st.secrets['gcp_service_account'] or env GOOGLE_SERVICE_ACCOUNT_JSON.")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_info(credentials_dict, scopes=scopes)

def init(sheet_id: Optional[str] = None, credentials_dict: Optional[dict] = None):
    """Initialize gspread + open spreadsheet by ID. Will raise if not accessible."""
    global GC, SH, SHEET_ID
    SHEET_ID = sheet_id or os.getenv("GSHEET_ID")
    if not SHEET_ID:
        raise RuntimeError("Missing Google Sheet ID. Put in st.secrets['gsheet_id'] or env GSHEET_ID.")
    creds = _get_creds(credentials_dict)
    GC = gspread.authorize(creds)
    SH = GC.open_by_key(SHEET_ID)
    # Ensure worksheets exist
    _ensure_worksheets()

def _ensure_worksheets():
    existing = {ws.title for ws in SH.worksheets()}
    for title in (WS_PROJECTS, WS_POTENTIAL, WS_ACTUAL, WS_DEPTS):
        if title not in existing:
            SH.add_worksheet(title=title, rows=1000, cols=50)
    # Ensure headers
    _ensure_headers()

def _ensure_headers():
    depts = _read_depts_raw()
    dept_keys = [d.get("key") or d.get("name") for d in depts] if depts else []
    _ensure_sheet_header(WS_PROJECTS, dept_keys)
    _ensure_sheet_header(WS_POTENTIAL, dept_keys)
    _ensure_sheet_header(WS_ACTUAL, dept_keys)
    _ensure_depts_header()

def _ensure_sheet_header(title: str, dept_keys: List[str]):
    ws = SH.worksheet(title)
    values = ws.get_all_values()
    if not values:
        headers = BASE_HEADERS + dept_keys
        ws.update([headers])
    else:
        # Upgrade header by adding any missing dept columns to the end
        headers = values[0]
        changed = False
        for k in dept_keys:
            if k not in headers:
                headers.append(k)
                changed = True
        if changed:
            ws.resize(rows=max(ws.row_count, 2), cols=max(len(headers), ws.col_count))
            ws.update('1:1', [headers])

def _ensure_depts_header():
    ws = SH.worksheet(WS_DEPTS)
    values = ws.get_all_values()
    if not values:
        ws.update([["key","name","headcount"]])

# ----- Helpers -----

def _ws(title:str):
    return SH.worksheet(title)

def _headers(ws):
    vals = ws.row_values(1)
    return [h.strip() for h in vals]

def _rows_as_dicts(ws):
    values = ws.get_all_values()
    if not values:
        return []
    headers = [h.strip() for h in values[0]]
    rows = []
    for r in values[1:]:
        d = {}
        for i, h in enumerate(headers):
            d[h] = r[i] if i < len(r) else ""
        rows.append(d)
    return rows

def _write_dicts(ws, rows: List[Dict], headers: Optional[List[str]] = None):
    if headers is None:
        headers = _headers(ws)
    # normalize rows -> list of lists
    matrix = [headers]
    for d in rows:
        row = [str(d.get(h, "")) for h in headers]
        matrix.append(row)
    ws.clear()
    ws.update(matrix)

def _to_float(x):
    try:
        if x is None or x == "":
            return 0.0
        return float(x)
    except Exception:
        # tolerate stray text
        return 0.0

def _normalize_project_row(d: Dict, dept_keys: List[str]):
    # keep base fields
    out = {
        "number": (d.get("number") or "").strip(),
        "customer": d.get("customer") or "",
        "aircraftModel": d.get("aircraftModel") or "",
        "scope": d.get("scope") or "",
        "induction": d.get("induction") or "",
        "delivery": d.get("delivery") or "",
    }
    for k in dept_keys:
        out[k] = _to_float(d.get(k))
    return out

def _read_depts_raw():
    try:
        ws = _ws(WS_DEPTS)
    except Exception:
        return []
    rows = _rows_as_dicts(ws)
    out = []
    for r in rows:
        key = (r.get("key") or r.get("name") or "").strip()
        if not key:
            continue
        name = r.get("name") or key
        try:
            head = int(float(r.get("headcount") or 0))
        except Exception:
            head = 0
        out.append({"key": key, "name": name, "headcount": head})
    return out

def list_depts():
    return _read_depts_raw()

def save_depts(depts: List[Dict]):
    ws = _ws(WS_DEPTS)
    rows = []
    for d in depts:
        key = (d.get("key") or d.get("name") or "").strip()
        if not key:
            continue
        rows.append({"key": key, "name": d.get("name") or key, "headcount": int(d.get("headcount") or 0)})
    _write_dicts(ws, rows, headers=["key","name","headcount"])
    # Expand headers on project sheets to include any new dept keys
    _ensure_headers()

def _load_dataset(title: str, dept_keys: List[str]):
    ws = _ws(title)
    rows = _rows_as_dicts(ws)
    out = []
    for r in rows:
        out.append(_normalize_project_row(r, dept_keys))
    return out

def load_all():
    depts = list_depts()
    dept_keys = [d["key"] for d in depts]
    return {
        "projects":  _load_dataset(WS_PROJECTS, dept_keys),
        "potential": _load_dataset(WS_POTENTIAL, dept_keys),
        "actual":    _load_dataset(WS_ACTUAL, dept_keys),
        "depts":     depts,
    }

def replace_dataset(dsname: str, entries: List[Dict]):
    title = WS_PROJECTS if dsname == "projects" else WS_POTENTIAL if dsname == "potential" else WS_ACTUAL
    depts = list_depts()
    dept_keys = [d["key"] for d in depts]
    rows = [_normalize_project_row(e, dept_keys) for e in entries]
    ws = _ws(title)
    headers = BASE_HEADERS + dept_keys
    _write_dicts(ws, rows, headers=headers)

def upsert_project(dsname: str, entry: Dict):
    title = WS_PROJECTS if dsname == "projects" else WS_POTENTIAL if dsname == "potential" else WS_ACTUAL
    depts = list_depts()
    dept_keys = [d["key"] for d in depts]
    ws = _ws(title)
    rows = _rows_as_dicts(ws)
    headers = _headers(ws)
    # ensure headers have all dept keys
    missing = [k for k in dept_keys if k not in headers]
    if missing:
        headers = headers + missing
        ws.update('1:1', [headers])
        rows = _rows_as_dicts(ws)

    key = (entry.get("number") or "").strip()
    norm = _normalize_project_row(entry, dept_keys)

    updated = False
    for r in rows:
        if (r.get("number") or "").strip() == key and key:
            # update in place
            r.update({k: str(norm.get(k, "")) for k in headers})
            updated = True
            break
    if not updated:
        rows.append({k: str(norm.get(k, "")) for k in headers})

    _write_dicts(ws, rows, headers=headers)

def delete_project(dsname: str, number: str):
    title = WS_PROJECTS if dsname == "projects" else WS_POTENTIAL if dsname == "potential" else WS_ACTUAL
    ws = _ws(title)
    rows = _rows_as_dicts(ws)
    number = (number or "").strip()
    if not number:
        return
    rows = [r for r in rows if (r.get("number") or "").strip() != number]
    headers = _headers(ws)
    _write_dicts(ws, rows, headers=headers)

def seed_if_empty(projects, potential, actual, depts):
    # Only seed if the four sheets are empty (no data beyond header)
    cur = load_all()
    if any(cur[k] for k in ("projects","potential","actual","depts")):
        return  # already has data
    save_depts(depts)
    replace_dataset("projects", projects or [])
    replace_dataset("potential", potential or [])
    replace_dataset("actual", actual or [])
