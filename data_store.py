
from __future__ import annotations
import io
import math
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd

# DB beside this module when used in Streamlit; falls back to CWD if __file__ missing
try:
    DB_PATH = Path(__file__).with_name("capacity_store.db")
except NameError:
    DB_PATH = Path("capacity_store.db")

DEFAULT_DEPARTMENTS = [
    ("Maintenance", "Maintenance", 8),
    ("Structures", "Structures", 8),
    ("Avionics", "Avionics", 6),
    ("Inspection", "Inspection", 5),
    ("Interiors", "Interiors", 10),
    ("Engineering", "Engineering", 8),
    ("Cabinet", "Cabinet", 7),
    ("Upholstery", "Upholstery", 6),
    ("Finish", "Finish", 5),
]

def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")
    return con

def _init_db(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS departments (
            key TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            headcount INTEGER NOT NULL DEFAULT 0,
            sort_order INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL CHECK (category IN ('confirmed','potential','actual')),
            number TEXT,
            customer TEXT,
            aircraftModel TEXT,
            scope TEXT,
            induction TEXT,
            delivery TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS project_hours (
            project_id INTEGER NOT NULL,
            dept_key TEXT NOT NULL,
            hours REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (project_id, dept_key),
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
            FOREIGN KEY (dept_key) REFERENCES departments(key) ON UPDATE CASCADE ON DELETE CASCADE
        );
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_projects_category ON projects(category);")

def _rowdict(r: sqlite3.Row) -> Dict[str, Any]:
    return {k: r[k] for k in r.keys()}

def _now_iso() -> str:
    return pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# -------- Departments --------
def seed_if_empty() -> None:
    with _conn() as con:
        _init_db(con)
        n = con.execute("SELECT COUNT(*) AS c FROM departments;").fetchone()["c"]
        if n == 0:
            con.executemany(
                "INSERT INTO departments(key,name,headcount,sort_order) VALUES (?,?,?,?);",
                [(k, n, hc, i) for i, (k, n, hc) in enumerate(DEFAULT_DEPARTMENTS)],
            )

def list_departments() -> List[Dict[str, Any]]:
    with _conn() as con:
        rows = con.execute(
            "SELECT key, name, headcount FROM departments ORDER BY sort_order ASC, name ASC;"
        ).fetchall()
    return [_rowdict(r) for r in rows]

def upsert_departments(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    with _conn() as con:
        _init_db(con)
        for i, rec in enumerate(records):
            key = str(rec.get("key") or "").strip()
            name = str(rec.get("name") or key or "").strip() or key
            headcount = int(pd.to_numeric(rec.get("headcount"), errors="coerce") or 0)
            con.execute(
                """
                INSERT INTO departments(key, name, headcount, sort_order)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                  name = excluded.name,
                  headcount = excluded.headcount
                ;""",
                (key, name, headcount, i),
            )
    return list_departments()

# -------- Projects + hours --------
def _ensure_hours_for_all_departments(con: sqlite3.Connection, project_id: int) -> None:
    for r in con.execute("SELECT key FROM departments;").fetchall():
        dk = r["key"]
        con.execute(
            """
            INSERT INTO project_hours(project_id, dept_key, hours)
            VALUES (?, ?, 0)
            ON CONFLICT(project_id, dept_key) DO NOTHING;
            """,
            (project_id, dk),
        )

def _pivot_project(con: sqlite3.Connection, proj_row: sqlite3.Row) -> Dict[str, Any]:
    proj = _rowdict(proj_row)
    hrs = con.execute(
        "SELECT dept_key, hours FROM project_hours WHERE project_id = ?;",
        (proj["id"],),
    ).fetchall()
    by_key = {r["dept_key"]: float(r["hours"] or 0.0) for r in hrs}
    for r in con.execute("SELECT key FROM departments;").fetchall():
        k = r["key"]
        proj[k] = float(by_key.get(k, 0.0))
    return proj

def _list_projects_raw(con: sqlite3.Connection, category: str) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT id, category, number, customer, aircraftModel, scope, induction, delivery
        FROM projects
        WHERE category = ?
        ORDER BY COALESCE(induction,''), COALESCE(delivery,''), id;
        """,
        (category,),
    ).fetchall()
    return [_pivot_project(con, r) for r in rows]

def list_projects(category: str) -> List[Dict[str, Any]]:
    cat = (category or "").strip().lower()
    if cat not in {"confirmed","potential","actual"}:
        raise ValueError("category must be one of: confirmed, potential, actual")
    with _conn() as con:
        _init_db(con)
        return _list_projects_raw(con, cat)

def create_project(payload: Dict[str, Any], category: Optional[str] = None) -> Dict[str, Any]:
    cat = (category or payload.get("category") or "").strip().lower()
    if cat not in {"confirmed","potential","actual"}:
        raise ValueError("category must be one of: confirmed, potential, actual")
    number = payload.get("number")
    customer = payload.get("customer")
    aircraftModel = payload.get("aircraftModel")
    scope = payload.get("scope")
    induction = _to_iso_date(payload.get("induction"))
    delivery = _to_iso_date(payload.get("delivery"))
    with _conn() as con:
        _init_db(con)
        cur = con.execute(
            """
            INSERT INTO projects(category, number, customer, aircraftModel, scope, induction, delivery, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (cat, number, customer, aircraftModel, scope, induction, delivery, _now_iso(), _now_iso()),
        )
        pid = cur.lastrowid
        _ensure_hours_for_all_departments(con, pid)
        # write provided hours
        dept_keys = [r["key"] for r in con.execute("SELECT key FROM departments;").fetchall()]
        for dk in dept_keys:
            if dk in payload:
                con.execute(
                    """
                    INSERT INTO project_hours(project_id, dept_key, hours)
                    VALUES (?, ?, ?)
                    ON CONFLICT(project_id, dept_key) DO UPDATE SET hours=excluded.hours;
                    """,
                    (pid, dk, float(payload.get(dk) or 0.0)),
                )
        row = con.execute("SELECT * FROM projects WHERE id = ?;", (pid,)).fetchone()
        return _pivot_project(con, row)

def update_project(project_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    with _conn() as con:
        _init_db(con)
        row = con.execute("SELECT * FROM projects WHERE id = ?;", (project_id,)).fetchone()
        if not row:
            raise ValueError(f"Project id {project_id} not found")
        cat = (payload.get("category") or row["category"] or "").strip().lower()
        if cat not in {"confirmed","potential","actual"}:
            raise ValueError("category must be one of: confirmed, potential, actual")
        number = payload.get("number", row["number"])
        customer = payload.get("customer", row["customer"])
        aircraftModel = payload.get("aircraftModel", row["aircraftModel"])
        scope = payload.get("scope", row["scope"])
        induction = _to_iso_date(payload.get("induction", row["induction"]))
        delivery = _to_iso_date(payload.get("delivery", row["delivery"]))
        con.execute(
            """
            UPDATE projects
            SET category=?, number=?, customer=?, aircraftModel=?, scope=?, induction=?, delivery=?, updated_at=?
            WHERE id=?;
            """,
            (cat, number, customer, aircraftModel, scope, induction, delivery, _now_iso(), project_id),
        )
        _ensure_hours_for_all_departments(con, project_id)
        dept_keys = [r["key"] for r in con.execute("SELECT key FROM departments;").fetchall()]
        for dk in dept_keys:
            if dk in payload:
                con.execute(
                    """
                    INSERT INTO project_hours(project_id, dept_key, hours)
                    VALUES (?, ?, ?)
                    ON CONFLICT(project_id, dept_key) DO UPDATE SET hours=excluded.hours;
                    """,
                    (project_id, dk, float(payload.get(dk) or 0.0)),
                )
        row = con.execute("SELECT * FROM projects WHERE id = ?;", (project_id,)).fetchone()
        return _pivot_project(con, row)

def delete_project(project_id: int) -> None:
    with _conn() as con:
        con.execute("DELETE FROM projects WHERE id = ?;", (project_id,))

# ---- Bulk import ----
def _to_iso_date(val: Any) -> Optional[str]:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=False)
        if pd.isna(dt):
            return None
        return pd.Timestamp(dt).strftime("%Y-%m-%d")
    except Exception:
        return None

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    col_map = {}
    for c in df.columns:
        lc = str(c).strip().lower()
        if lc in {"number","project number","proj","p","p_number"}:
            col_map[c] = "number"
        elif lc in {"customer","client"}:
            col_map[c] = "customer"
        elif lc in {"aircraftmodel","aircraft model","model","aircraft"}:
            col_map[c] = "aircraftModel"
        elif lc in {"scope","work scope"}:
            col_map[c] = "scope"
        elif lc in {"induction","start","start_date","induct","induction_date"}:
            col_map[c] = "induction"
        elif lc in {"delivery","end","end_date","delivery_date"}:
            col_map[c] = "delivery"
        else:
            # try to match to a department key (case-insensitive)
            for dk, _, _ in DEFAULT_DEPARTMENTS:
                if lc.replace(" ","") == dk.lower().replace(" ",""):
                    col_map[c] = dk
                    break
    return df.rename(columns=col_map) if col_map else df

def _none_if_nan(v):
    try:
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        s = str(v).strip()
        return s if s else None
    except Exception:
        return None

def bulk_import_projects(file_like, category: str) -> Dict[str, Any]:
    cat = (category or "").strip().lower()
    if cat not in {"confirmed","potential","actual"}:
        raise ValueError("category must be one of: confirmed, potential, actual")
    data = file_like.read()
    try:
        file_like.seek(0)
    except Exception:
        pass
    name = getattr(file_like, "name", "").lower()
    try:
        if name.endswith(".csv") or (not name and isinstance(data, (bytes, bytearray)) and b"," in data[:4096]):
            df = pd.read_csv(io.BytesIO(data) if isinstance(data, (bytes, bytearray)) else file_like)
        else:
            df = pd.read_excel(io.BytesIO(data) if isinstance(data, (bytes, bytearray)) else file_like)
    except Exception as e:
        raise RuntimeError(f"Unable to read file: {e}")
    df = _normalize_columns(df)

    imported, errors = 0, 0
    for _, row in df.iterrows():
        try:
            payload = {
                "number": _none_if_nan(row.get("number")),
                "customer": _none_if_nan(row.get("customer")),
                "aircraftModel": _none_if_nan(row.get("aircraftModel")),
                "scope": _none_if_nan(row.get("scope")),
                "induction": _to_iso_date(row.get("induction")),
                "delivery": _to_iso_date(row.get("delivery")),
                "category": cat,
            }
            # include dept hours if present
            with _conn() as con:
                dks = [r["key"] for r in con.execute("SELECT key FROM departments;").fetchall()]
            for dk in dks:
                if dk in row.index:
                    payload[dk] = float(pd.to_numeric(row.get(dk), errors="coerce") or 0.0)
            create_project(payload, category=cat)
            imported += 1
        except Exception:
            errors += 1
    return {"imported": imported, "errors": errors}

def get_all_datasets() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    with _conn() as con:
        _init_db(con)
        confirmed = _list_projects_raw(con, "confirmed")
        potential = _list_projects_raw(con, "potential")
        actual    = _list_projects_raw(con, "actual")
        depts = [_rowdict(r) for r in con.execute("SELECT key, name, headcount FROM departments ORDER BY sort_order ASC, name ASC;")]
    return confirmed, potential, actual, depts

# Allow "python data_store.py" to init
if __name__ == "__main__":
    seed_if_empty()
    print(f"Initialized DB at {DB_PATH}")
