# data_store.py
# Lightweight SQLite-backed datastore for the Capacity app.
# - Uses COUNT(*) (no references to departments.id)
# - Stores projects as JSON payloads keyed by (dataset, number)
# - Departments keyed by 'key' (e.g., "Maintenance"), no autoincrement id

from __future__ import annotations
import json
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

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
# Schema & Seeding
# -----------------------------
def ensure_schema() -> None:
    con = _conn()
    try:
        # For better concurrency if needed
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA foreign_keys=ON;")

        # Departments: key is the primary identifier
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS departments (
                key TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                headcount INTEGER NOT NULL DEFAULT 0
            );
            """
        )

        # Projects table: store entire project dict as JSON payload,
        # partitioned by dataset: 'confirmed' | 'potential' | 'actual'
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS projects (
                dataset TEXT NOT NULL,
                number  TEXT NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (dataset, number)
            );
            """
        )
        con.commit()
    finally:
        con.close()


def seed_if_empty() -> None:
    """
    Ensure default departments exist. Uses COUNT(*) to check for emptiness.
    """
    con = _conn()
    try:
        # ✅ Use COUNT(*) — do NOT reference departments.id
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
# Project CRUD
# -----------------------------
# Valid dataset values the app expects to use
_DATASETS = {"confirmed", "potential", "actual"}

def _normalize_dataset(dataset: str) -> str:
    d = (dataset or "").strip().lower()
    if d in _DATASETS:
        return d
    # be forgiving with synonyms
    if d == "projects" or d == "confirmed_projects":
        return "confirmed"
    return d


def list_projects(dataset: str) -> List[Dict[str, Any]]:
    ds = _normalize_dataset(dataset)
    con = _conn()
    try:
        rows = _fetchall(
            con,
            "SELECT payload FROM projects WHERE dataset = ? ORDER BY number ASC;",
            (ds,),
        )
        return [json.loads(r["payload"]) for r in rows]
    finally:
        con.close()


def upsert_project(project: Dict[str, Any], dataset: str) -> None:
    """
    Upsert a single project. The project dict should include 'number'.
    """
    ds = _normalize_dataset(dataset)
    number = str(project.get("number") or "").strip()
    if not number:
        raise ValueError("Project 'number' is required for upsert.")
    con = _conn()
    try:
        payload = json.dumps(project)
        _exec(
            con,
            """
            INSERT INTO projects (dataset, number, payload)
            VALUES (?, ?, ?)
            ON CONFLICT(dataset, number) DO UPDATE SET
                payload = excluded.payload;
            """,
            (ds, number, payload),
        )
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
                # skip invalid rows quietly
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


def delete_project(number: str, dataset: str) -> None:
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


# -----------------------------
# Export / Import Utilities
# -----------------------------
def export_all() -> Dict[str, Any]:
    """
    Export departments and all project datasets for backup/download.
    """
    con = _conn()
    try:
        depts = list_departments()
        all_data = {"departments": depts, "confirmed": [], "potential": [], "actual": []}
        for ds in ("confirmed", "potential", "actual"):
            rows = _fetchall(con, "SELECT payload FROM projects WHERE dataset = ?;", (ds,))
            all_data[ds] = [json.loads(r["payload"]) for r in rows]
        return all_data
    finally:
        con.close()


def import_all(blob: Dict[str, Any], replace: bool = False) -> None:
    """
    Import a previously exported blob. If replace=True, clears existing rows.
    """
    con = _conn()
    try:
        if replace:
            _exec(con, "DELETE FROM departments;")
            _exec(con, "DELETE FROM projects;")

        # Departments
        for d in blob.get("departments", []):
            k = str(d.get("key") or "").strip()
            n = str(d.get("name") or "").strip() or k
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

        # Projects
        for ds in ("confirmed", "potential", "actual"):
            rows = blob.get(ds, []) or []
            for p in rows:
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
# Module bootstrap convenience
# -----------------------------
def init() -> None:
    """
    Call on app start.
    """
    ensure_schema()
    seed_if_empty()


# Auto-init if imported by Streamlit
try:
    init()
except Exception as e:
    # Fail-soft: app can handle an empty DB path if necessary
    print(f"[data_store] init warning: {e}")
