# data_store.py
from __future__ import annotations
import json
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional

DB_PATH = Path(__file__).with_name("capacity.db")

# -----------------------------
# Connection helpers
# -----------------------------
def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def _exec(con: sqlite3.Connection, sql: str, params: Tuple = ()) -> None:
    con.execute(sql, params)

def _fetchall(con: sqlite3.Connection, sql: str, params: Tuple = ()) -> List[sqlite3.Row]:
    cur = con.execute(sql, params)
    return cur.fetchall()

# -----------------------------
# Schema & migration
# -----------------------------
def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    r = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,)
    ).fetchone()
    return r is not None

def _projects_columns(con: sqlite3.Connection) -> List[str]:
    if not _table_exists(con, "projects"):
        return []
    cols = _fetchall(con, "PRAGMA table_info(projects);")
    return [c["name"] for c in cols]

def _migrate_projects_schema(con: sqlite3.Connection) -> None:
    """
    Ensure projects has columns: dataset TEXT, number TEXT, payload TEXT, PK(dataset, number).
    If the table exists but lacks 'payload' (or uses a different JSON column name),
    copy into a fresh table and swap.
    """
    cols = _projects_columns(con)
    if not cols:
        _exec(
            con,
            """
            CREATE TABLE IF NOT EXISTS projects (
                dataset TEXT NOT NULL,
                number  TEXT NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (dataset, number)
            );
            """,
        )
        return

    has_dataset = "dataset" in cols
    has_number  = "number" in cols
    has_payload = "payload" in cols

    if has_dataset and has_number and has_payload:
        return

    json_src: Optional[str] = None
    for cand in ("json", "data", "payload_json", "project", "content"):
        if cand in cols:
            json_src = cand
            break

    _exec(
        con,
        """
        CREATE TABLE IF NOT EXISTS projects_new (
            dataset TEXT NOT NULL,
            number  TEXT NOT NULL,
            payload TEXT NOT NULL,
            PRIMARY KEY (dataset, number)
        );
        """,
    )

    if has_dataset and has_number:
        if json_src:
            _exec(
                con,
                f"""
                INSERT OR IGNORE INTO projects_new (dataset, number, payload)
                SELECT dataset, number, {json_src} FROM projects;
                """,
            )
        else:
            _exec(
                con,
                """
                INSERT OR IGNORE INTO projects_new (dataset, number, payload)
                SELECT dataset, number, '{}' FROM projects;
                """,
            )

    _exec(con, "DROP TABLE projects;")
    _exec(con, "ALTER TABLE projects_new RENAME TO projects;")

def ensure_schema() -> None:
    con = _conn()
    try:
        _exec(con, "PRAGMA journal_mode=WAL;")
        _exec(con, "PRAGMA foreign_keys=ON;")

        _exec(
            con,
            """
            CREATE TABLE IF NOT EXISTS departments (
                key TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                headcount INTEGER NOT NULL DEFAULT 0
            );
            """,
        )

        _migrate_projects_schema(con)
        con.commit()
    finally:
        con.close()

# -----------------------------
# Seed data
# -----------------------------
def seed_if_empty() -> None:
    con = _conn()
    try:
        n = con.execute("SELECT COUNT(*) FROM departments").fetchone()[0] or 0
        if n == 0:
            con.executemany(
                "INSERT OR IGNORE INTO departments (key, name, headcount) VALUES (?,?,?)",
                [
                    ("Maintenance", "Maintenance", 12),
                    ("Structures", "Structures", 8),
                    ("Avionics", "Avionics", 6),
                    ("Inspection", "Inspection", 5),
                    ("Interiors", "Interiors", 10),
                    ("Engineering", "Engineering", 7),
                    ("Cabinet", "Cabinet", 6),
                    ("Upholstery", "Upholstery", 6),
                    ("Finish", "Finish", 6),
                ],
            )
            con.commit()
    finally:
        con.close()

# -----------------------------
# Normalization helpers
# -----------------------------
def _get_dept_keys(con: sqlite3.Connection | None = None) -> List[str]:
    close_me = False
    if con is None:
        con = _conn()
        close_me = True
    try:
        try:
            rows = _fetchall(con, "SELECT key FROM departments ORDER BY sort_order, name;")
        except sqlite3.OperationalError:
            rows = _fetchall(con, "SELECT key FROM departments ORDER BY name;")
        return [r["key"] for r in rows]
    finally:
        if close_me:
            con.close()

def _normalize_project_payload(p: Dict[str, Any], dept_keys: List[str]) -> Dict[str, Any]:
    """
    Flatten nested hours -> top-level dept keys, normalize field names, coerce numbers,
    and trim dates to YYYY-MM-DD.
    """
    if not isinstance(p, dict):
        return {}

    if "aircraft" in p and "aircraftModel" not in p:
        p["aircraftModel"] = p.pop("aircraft")

    hrs = p.get("hours")
    if isinstance(hrs, dict):
        for k in dept_keys:
            v = hrs.get(k, 0)
            p[k] = float(v) if v is not None else 0.0
        p.pop("hours", None)

    for k in dept_keys:
        v = p.get(k, 0.0)
        try:
            p[k] = float(v) if v is not None else 0.0
        except Exception:
            p[k] = 0.0

    def _norm_date(s):
        if not s:
            return s
        s = str(s).strip()
        return s.split("T")[0] if "T" in s else s

    p["induction"] = _norm_date(p.get("induction"))
    p["delivery"]  = _norm_date(p.get("delivery"))

    if p.get("number") is not None:
        p["number"] = str(p["number"]).strip()

    p.setdefault("customer", "")
    p.setdefault("aircraftModel", "")
    p.setdefault("scope", "")

    return p

# -----------------------------
# Department CRUD
# -----------------------------
def list_departments() -> List[Dict[str, Any]]:
    con = _conn()
    try:
        rows = _fetchall(con, "SELECT key, name, headcount FROM departments ORDER BY name ASC;")
        return [dict(r) for r in rows]
    finally:
        con.close()

def upsert_department(key: str, name: str, headcount: int) -> None:
    con = _conn()
    try:
        _exec(
            con,
            """
            INSERT INTO departments (key, name, headcount)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                name = excluded.name,
                headcount = excluded.headcount;
            """,
            (key, name, int(headcount)),
        )
        con.commit()
    finally:
        con.close()

def delete_department(key: str) -> Dict[str, Any]:
    con = _conn()
    try:
        cur = con.execute("DELETE FROM departments WHERE key = ?;", (key,))
        con.commit()
        return {"deleted": cur.rowcount or 0}
    finally:
        con.close()

# -----------------------------
# Project CRUD
# -----------------------------
_DATASETS = {"confirmed", "potential", "actual"}

def _normalize_dataset(dataset: str) -> str:
    d = (dataset or "").strip().lower()
    if d in _DATASETS:
        return d
    if d in {"projects", "confirmed_projects", "confirmedproject", "confirmed_project"}:
        return "confirmed"
    return d

def list_projects(category: str) -> List[Dict[str, Any]]:
    ds = _normalize_dataset(category)
    con = _conn()
    try:
        dept_keys = _get_dept_keys(con)
        rows = _fetchall(
            con,
            "SELECT payload FROM projects WHERE dataset = ? ORDER BY number ASC;",
            (ds,),
        )
        out: List[Dict[str, Any]] = []
        for r in rows:
            try:
                p = json.loads(r["payload"])
            except Exception:
                p = {}
            out.append(_normalize_project_payload(p, dept_keys))
        return out
    finally:
        con.close()

def list_projects_with_rowid(category: str) -> List[Dict[str, Any]]:
    ds = _normalize_dataset(category)
    con = _conn()
    try:
        dept_keys = _get_dept_keys(con)
        rows = _fetchall(
            con,
            "SELECT rowid, payload FROM projects WHERE dataset = ? ORDER BY number ASC;",
            (ds,),
        )
        out: List[Dict[str, Any]] = []
        for r in rows:
            try:
                p = json.loads(r["payload"])
            except Exception:
                p = {}
            clean = _normalize_project_payload(p, dept_keys)
            rid = int(r["rowid"])
            # Provide both keys: 'id' for your UI, and '_rowid' for compatibility
            clean["id"] = rid
            clean["_rowid"] = rid
            out.append(clean)
        return out
    finally:
        con.close()


def create_project(payload: Dict[str, Any], category: str) -> Dict[str, Any]:
    """
    Create or upsert a project by (dataset, number).
    RETURNS: {'id': rowid, 'dataset': ds, 'number': number}
    """
    ds = _normalize_dataset(category)
    con = _conn()
    try:
        dept_keys = _get_dept_keys(con)
        clean = _normalize_project_payload(dict(payload), dept_keys)

        number = str(clean.get("number") or "").strip()
        if not number:
            raise ValueError("Project 'number' is required.")

        _exec(
            con,
            """
            INSERT INTO projects (dataset, number, payload)
            VALUES (?, ?, ?)
            ON CONFLICT(dataset, number) DO UPDATE SET
              payload = excluded.payload;
            """,
            (ds, number, json.dumps(clean)),
        )
        # Fetch rowid reliably even on UPSERT
        row = con.execute(
            "SELECT rowid FROM projects WHERE dataset=? AND number=?;",
            (ds, number),
        ).fetchone()
        con.commit()
        rid = int(row["rowid"]) if row else None
        return {"id": rid, "dataset": ds, "number": number}
    finally:
        con.close()

def update_project(row_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update a project using its SQLite rowid.
    RETURNS: {'id': row_id (new or same if PK changed), 'dataset': ds, 'number': new_number}
    """
    con = _conn()
    try:
        row = con.execute("SELECT dataset, number FROM projects WHERE rowid = ?;", (row_id,)).fetchone()
        if not row:
            raise ValueError("Project not found")

        ds = row["dataset"]
        old_number = row["number"]

        dept_keys = _get_dept_keys(con)
        clean = _normalize_project_payload(dict(payload), dept_keys)
        new_number = str(clean.get("number") or old_number).strip()

        if new_number != old_number:
            _exec(
                con,
                """
                INSERT INTO projects (dataset, number, payload)
                VALUES (?, ?, ?)
                ON CONFLICT(dataset, number) DO UPDATE SET
                  payload = excluded.payload;
                """,
                (ds, new_number, json.dumps(clean)),
            )
            _exec(con, "DELETE FROM projects WHERE dataset = ? AND number = ?;", (ds, old_number))
            # get the new rowid
            row2 = con.execute(
                "SELECT rowid FROM projects WHERE dataset=? AND number=?;",
                (ds, new_number),
            ).fetchone()
            rid = int(row2["rowid"]) if row2 else row_id
        else:
            _exec(
                con,
                "UPDATE projects SET payload = ? WHERE rowid = ?;",
                (json.dumps(clean), row_id),
            )
            rid = row_id

        con.commit()
        return {"id": rid, "dataset": ds, "number": new_number}
    finally:
        con.close()

def delete_project(number: str, dataset: str) -> Dict[str, Any]:
    ds = _normalize_dataset(dataset)
    num = str(number or "").strip()
    if not num:
        return {"deleted": 0}
    con = _conn()
    try:
        cur = con.execute("DELETE FROM projects WHERE dataset = ? AND number = ?;", (ds, num))
        con.commit()
        return {"deleted": cur.rowcount or 0, "dataset": ds, "number": num}
    finally:
        con.close()

def delete_project_by_rowid(row_id: int) -> Dict[str, Any]:
    con = _conn()
    try:
        cur = con.execute("DELETE FROM projects WHERE rowid = ?;", (row_id,))
        con.commit()
        return {"deleted": cur.rowcount or 0, "id": row_id}
    finally:
        con.close()

def bulk_upsert_projects(projects: List[Dict[str, Any]], dataset: str) -> None:
    ds = _normalize_dataset(dataset)
    con = _conn()
    try:
        dept_keys = _get_dept_keys(con)
        rows = []
        for p in projects or []:
            clean = _normalize_project_payload(dict(p), dept_keys)
            number = str(clean.get("number") or "").strip()
            if not number:
                continue
            rows.append((ds, number, json.dumps(clean)))
        if rows:
            con.executemany(
                """
                INSERT INTO projects (dataset, number, payload)
                VALUES (?, ?, ?)
                ON CONFLICT(dataset, number) DO UPDATE SET
                    payload = excluded.payload;
                """,
                rows,
            )
        con.commit()
    finally:
        con.close()

# -----------------------------
# Export / Import
# -----------------------------
def export_all() -> Dict[str, Any]:
    con = _conn()
    try:
        dept_keys = _get_dept_keys(con)
        depts = list_departments()
        all_data = {"departments": depts, "confirmed": [], "potential": [], "actual": []}
        for ds in ("confirmed", "potential", "actual"):
            rows = _fetchall(con, "SELECT payload FROM projects WHERE dataset = ? ORDER BY number;", (ds,))
            all_data[ds] = [
                _normalize_project_payload(json.loads(r["payload"]), dept_keys) for r in rows
            ]
        return all_data
    finally:
        con.close()

def import_all(blob: Dict[str, Any], replace: bool = False) -> None:
    con = _conn()
    try:
        if replace:
            _exec(con, "DELETE FROM departments;")
            _exec(con, "DELETE FROM projects;")

        for d in blob.get("departments", []) or []:
            k = str(d.get("key") or d.get("name") or "").strip()
            n = str(d.get("name") or d.get("key") or "").strip() or k
            hc = int(d.get("headcount") or 0)
            _exec(
                con,
                """
                INSERT INTO departments (key, name, headcount)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                  name = excluded.name,
                  headcount = excluded.headcount;
                """,
                (k, n, hc),
            )

        dept_keys = _get_dept_keys(con)
        for ds in ("confirmed", "potential", "actual"):
            rows = blob.get(ds, []) or []
            for p in rows:
                clean = _normalize_project_payload(dict(p), dept_keys)
                number = str(clean.get("number") or "").strip()
                if not number:
                    continue
                _exec(
                    con,
                    """
                    INSERT INTO projects (dataset, number, payload)
                    VALUES (?, ?, ?)
                    ON CONFLICT(dataset, number) DO UPDATE SET
                      payload = excluded.payload;
                    """,
                    (ds, number, json.dumps(clean)),
                )
        con.commit()
    finally:
        con.close()

# -----------------------------
# Convenience used by app.py
# -----------------------------
def get_all_datasets() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    confirmed = list_projects("confirmed")
    potential = list_projects("potential")
    actual    = list_projects("actual")
    depts     = list_departments()
    return confirmed, potential, actual, depts

def replace_all_datasets(confirmed: List[Dict[str, Any]],
                         potential: List[Dict[str, Any]],
                         actual: List[Dict[str, Any]],
                         departments: List[Dict[str, Any]]) -> None:
    con = _conn()
    try:
        _exec(con, "DELETE FROM projects;")
        _exec(con, "DELETE FROM departments;")
        con.commit()

        for d in departments or []:
            upsert_department(
                d.get("key") or d.get("name"),
                d.get("name") or d.get("key"),
                int(d.get("headcount") or 0),
            )

        bulk_upsert_projects(confirmed or [], "confirmed")
        bulk_upsert_projects(potential or [], "potential")
        bulk_upsert_projects(actual or [], "actual")
    finally:
        con.close()

# -----------------------------
# Bootstrap on import
# -----------------------------
def init() -> None:
    ensure_schema()
    seed_if_empty()

try:
    init()
except Exception as e:
    print(f"[data_store] init warning: {e}")
