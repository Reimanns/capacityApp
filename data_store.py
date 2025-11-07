from __future__ import annotations
import json
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Tuple

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

def _fetchone(con: sqlite3.Connection, sql: str, params: Tuple = ()) -> sqlite3.Row | None:
    cur = con.execute(sql, params)
    return cur.fetchone()

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
    has_number  = "number"  in cols
    has_payload = "payload" in cols

    if has_dataset and has_number and has_payload:
        return  # already modern

    # Try to locate an old JSON column to carry forward
    json_src = None
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

        # Departments table (key is the PK; no numeric id)
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

        # Projects with migration support
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
        n = (_fetchone(con, "SELECT COUNT(*) AS c FROM departments") or {"c": 0})["c"]
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
# Helpers for payload hygiene
# -----------------------------
def _get_dept_keys(con: sqlite3.Connection) -> List[str]:
    rows = _fetchall(con, "SELECT key FROM departments ORDER BY key;")
    return [r["key"] for r in rows]

def _normalize_project_payload(p: Dict[str, Any], dept_keys: List[str]) -> Dict[str, Any]:
    # Basic fields
    p = dict(p or {})
    p.setdefault("number", "")
    p.setdefault("customer", "")
    p.setdefault("aircraftModel", "")
    p.setdefault("scope", "")
    p.setdefault("induction", "")
    p.setdefault("delivery", "")
    # Ensure department numeric fields exist
    for k in dept_keys:
        try:
            p[k] = float(p.get(k, 0) or 0)
        except Exception:
            p[k] = 0.0
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

def delete_department(key: str) -> None:
    con = _conn()
    try:
        _exec(con, "DELETE FROM departments WHERE key = ?;", (key,))
        con.commit()
    finally:
        con.close()

# -----------------------------
# Dataset normalization
# -----------------------------
_DATASETS = {"confirmed", "potential", "actual"}
def _normalize_dataset(dataset: str) -> str:
    d = (dataset or "").strip().lower()
    if d in _DATASETS:
        return d
    if d in {"projects", "confirmed_projects"}:
        return "confirmed"
    return d

# -----------------------------
# Project CRUD / Queries
# -----------------------------
def list_projects(dataset: str) -> List[Dict[str, Any]]:
    """
    NOTE: This returns an 'id' for each project (rowid) so the UI can reference it.
    """
    ds = _normalize_dataset(dataset)
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
            p = _normalize_project_payload(p, dept_keys)
            rid = int(r["rowid"])
            p["id"] = rid
            p["_rowid"] = rid  # back-compat
            out.append(p)
        return out
    finally:
        con.close()

def list_projects_with_rowid(dataset: str) -> List[Dict[str, Any]]:
    """
    Alias of list_projects() that explicitly ensures 'id' exists.
    """
    return list_projects(dataset)

def get_project_by_id(project_id: int) -> Dict[str, Any] | None:
    con = _conn()
    try:
        dept_keys = _get_dept_keys(con)
        r = _fetchone(con, "SELECT rowid, payload, dataset, number FROM projects WHERE rowid = ?;", (int(project_id),))
        if not r:
            return None
        p = {}
        try:
            p = json.loads(r["payload"])
        except Exception:
            pass
        p = _normalize_project_payload(p, dept_keys)
        p["id"] = int(r["rowid"])
        p["_rowid"] = int(r["rowid"])
        p["_dataset"] = r["dataset"]
        p["_number"] = r["number"]
        return p
    finally:
        con.close()

def create_project(payload: Dict[str, Any], category: str) -> Dict[str, Any]:
    """
    Inserts a new project into the given dataset. Returns the created project with 'id'.
    """
    ds = _normalize_dataset(category)
    if ds not in _DATASETS:
        raise ValueError(f"Unknown dataset: {category}")

    number = str(payload.get("number") or "").strip()
    if not number:
        raise ValueError("Project 'number' is required.")

    # Ensure minimal fields
    payload = dict(payload)
    payload.setdefault("customer", "")
    payload.setdefault("aircraftModel", "")
    payload.setdefault("scope", "")
    payload.setdefault("induction", "")
    payload.setdefault("delivery", "")

    con = _conn()
    try:
        _exec(
            con,
            """
            INSERT INTO projects (dataset, number, payload)
            VALUES (?, ?, ?)
            ON CONFLICT(dataset, number) DO UPDATE SET
              payload = excluded.payload;
            """,
            (ds, number, json.dumps(payload)),
        )
        # fetch rowid for this (dataset, number)
        row = _fetchone(con, "SELECT rowid FROM projects WHERE dataset = ? AND number = ?;", (ds, number))
        con.commit()
        if not row:
            # Extremely unlikely, but keep it safe
            return dict(payload)  # no id; avoids crashing
        rid = int(row["rowid"])
        # Return normalized result with id
        dept_keys = _get_dept_keys(con)
        out = _normalize_project_payload(payload, dept_keys)
        out["id"] = rid
        out["_rowid"] = rid
        return out
    finally:
        con.close()

def update_project(project_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Updates the payload (and keeps number in sync) for the given rowid.
    Returns the updated project with 'id'.
    """
    number = str(payload.get("number") or "").strip()
    if not number:
        raise ValueError("Project 'number' is required.")

    con = _conn()
    try:
        # We also keep the 'number' column in sync with the JSON's number
        _exec(
            con,
            """
            UPDATE projects
               SET number  = ?,
                   payload = ?
             WHERE rowid = ?;
            """,
            (number, json.dumps(payload), int(project_id)),
        )
        con.commit()
        return get_project_by_id(int(project_id)) or {"id": int(project_id), **payload}
    finally:
        con.close()

def delete_project(number: str, dataset: str) -> None:
    """
    Delete by (dataset, number) – kept for backward compatibility.
    """
    ds = _normalize_dataset(dataset)
    num = str(number or "").strip()
    if not num:
        return
    con = _conn()
    try:
        _exec(con, "DELETE FROM projects WHERE dataset = ? AND number = ?;", (ds, num))
        con.commit()
    finally:
        con.close()

def delete_project_by_id(project_id: int) -> None:
    con = _conn()
    try:
        _exec(con, "DELETE FROM projects WHERE rowid = ?;", (int(project_id),))
        con.commit()
    finally:
        con.close()

def bulk_upsert_projects(projects: List[Dict[str, Any]], dataset: str) -> None:
    ds = _normalize_dataset(dataset)
    con = _conn()
    try:
        rows = []
        for p in projects:
            number = str(p.get("number") or "").strip()
            if not number:
                continue
            rows.append((ds, number, json.dumps(p)))
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
        depts = list_departments()
        res = {"departments": depts, "confirmed": [], "potential": [], "actual": []}
        for ds in ("confirmed", "potential", "actual"):
            # include ids as well
            rows = _fetchall(con, "SELECT rowid, payload FROM projects WHERE dataset = ? ORDER BY number ASC;", (ds,))
            block = []
            dept_keys = _get_dept_keys(con)
            for r in rows:
                try:
                    p = json.loads(r["payload"])
                except Exception:
                    p = {}
                p = _normalize_project_payload(p, dept_keys)
                rid = int(r["rowid"])
                p["id"] = rid
                p["_rowid"] = rid
                block.append(p)
            res[ds] = block
        return res
    finally:
        con.close()

def import_all(blob: Dict[str, Any], replace: bool = False) -> None:
    con = _conn()
    try:
        if replace:
            _exec(con, "DELETE FROM departments;")
            _exec(con, "DELETE FROM projects;")

        # Departments
        for d in blob.get("departments", []) or []:
            k = str(d.get("key") or d.get("name") or "").strip()
            n = str(d.get("name") or d.get("key") or "").strip() or k
            hc = int(d.get("headcount") or 0)
            if not k:
                continue
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

        # Projects by dataset
        for ds in ("confirmed", "potential", "actual"):
            for p in blob.get(ds, []) or []:
                number = str(p.get("number") or "").strip()
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
                    (ds, number, json.dumps(p)),
                )

        con.commit()
    finally:
        con.close()

# -----------------------------
# Convenience used by app.py
# -----------------------------
def get_all_datasets() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (confirmed, potential, actual, departments) with each project including 'id'.
    """
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
