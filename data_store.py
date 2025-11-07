# data_store.py
"""
Lightweight persistence layer for the Capacity/Load app.

- Defaults to local SQLite file: ./capacity.db
- Optional hosted DB: set DB_URL (and optionally DB_TOKEN) in env or Streamlit secrets.toml
    .streamlit/secrets.toml
    -----------------------
    DB_URL = "postgresql+psycopg2://USER:PASS@HOST:PORT/DBNAME"
    # DB_TOKEN = "..."  (if your driver needs it; not used for SQLite/Postgres)

Tables
------
Project:
  id, number, customer, aircraft_model, scope, induction (date), delivery (date),
  category ('confirmed' | 'potential' | 'actual'),
  hours by department: maintenance, structures, avionics, inspection,
  interiors, engineering, cabinet, upholstery, finish

Department:
  id, key, name, headcount (int)

Public API (all return/accept plain dicts/lists)
------------------------------------------------
init_db()
seed_if_empty(projects=None, potential=None, actual=None, depts=None)

list_projects(category: str|None=None) -> list[dict]
get_project(project_id: int) -> dict|None
create_project(data: dict, category: str='confirmed') -> dict
update_project(project_id: int, data: dict) -> dict
delete_project(project_id: int) -> None

list_departments() -> list[dict]
upsert_departments(rows: list[dict]) -> list[dict]

bulk_import_projects(file_path_or_buffer, category: str) -> dict  # uses pandas (csv/xlsx)

get_all_datasets() -> dict  # {"projects": [...], "potential":[...], "actual":[...], "depts":[...]}

Notes
-----
- Date fields accept 'YYYY-MM-DD' (preferred) or ISO strings; they are stored as DATE.
- Returned date fields are 'YYYY-MM-DD' strings.
- Field names in dicts match your Streamlit app:
    number, customer, aircraftModel, scope, induction, delivery, and dept keys:
    "Maintenance","Structures","Avionics","Inspection","Interiors",
    "Engineering","Cabinet","Upholstery","Finish"
"""

from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

# SQLAlchemy
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Date, select, func
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# Optional: read Streamlit secrets if available
def _read_secrets(key: str, default: Optional[str] = None) -> Optional[str]:
    try:
        import streamlit as st  # type: ignore
        return st.secrets.get(key, default)
    except Exception:
        return os.environ.get(key, default)

Base = declarative_base()

# ------------------------- Models -------------------------
class Project(Base):
    __tablename__ = "projects"

    id = Column(Integer, primary_key=True, autoincrement=True)
    number = Column(String(64), nullable=True)
    customer = Column(String(128), nullable=True)
    aircraft_model = Column(String(64), nullable=True)
    scope = Column(String(256), nullable=True)
    induction = Column(Date, nullable=True)
    delivery = Column(Date, nullable=True)
    category = Column(String(16), nullable=False, default="confirmed")  # confirmed|potential|actual

    # Hours by department
    maintenance = Column(Float, default=0.0)
    structures  = Column(Float, default=0.0)
    avionics    = Column(Float, default=0.0)
    inspection  = Column(Float, default=0.0)
    interiors   = Column(Float, default=0.0)
    engineering = Column(Float, default=0.0)
    cabinet     = Column(Float, default=0.0)
    upholstery  = Column(Float, default=0.0)
    finish      = Column(Float, default=0.0)


class Department(Base):
    __tablename__ = "departments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(64), unique=True, nullable=False)   # e.g., "Maintenance"
    name = Column(String(128), nullable=False)              # display name
    headcount = Column(Integer, default=0)


# ------------------------- Engine & Session -------------------------
def _resolve_db_url() -> str:
    url = _read_secrets("DB_URL")
    if url:
        return url  # e.g., postgresql+psycopg2://... or sqlite:///...
    # default to local SQLite (works on Streamlit Cloud but is ephemeral)
    return "sqlite:///capacity.db"

_ENGINE = create_engine(_resolve_db_url(), future=True)  # autocommit=False by default
_SessionLocal = sessionmaker(bind=_ENGINE, expire_on_commit=False, class_=Session)

def init_db() -> None:
    Base.metadata.create_all(_ENGINE)

# ------------------------- Helpers -------------------------
DEPT_KEYS = [
    "Maintenance","Structures","Avionics","Inspection",
    "Interiors","Engineering","Cabinet","Upholstery","Finish"
]

def _to_date(v: Any) -> Optional[date]:
    if v in (None, "", "NaT", "nan"):
        return None
    if isinstance(v, date):
        return v
    if isinstance(v, datetime):
        return v.date()
    s = str(v).strip()
    # Accept 'YYYY-MM-DD' or full ISO
    try:
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z","")).date()
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        # Try broader ISO parse
        try:
            return datetime.fromisoformat(s).date()
        except Exception:
            return None

def _float_or_zero(v: Any) -> float:
    try:
        if v is None or v == "":
            return 0.0
        return float(v)
    except Exception:
        return 0.0

def _project_to_dict(p: Project) -> Dict[str, Any]:
    # Keep the app’s original field names/casing
    return {
        "id": p.id,
        "number": p.number,
        "customer": p.customer,
        "aircraftModel": p.aircraft_model,
        "scope": p.scope,
        "induction": p.induction.isoformat() if p.induction else None,
        "delivery":  p.delivery.isoformat()  if p.delivery  else None,
        # Hours by dept
        "Maintenance": p.maintenance or 0.0,
        "Structures":  p.structures  or 0.0,
        "Avionics":    p.avionics    or 0.0,
        "Inspection":  p.inspection  or 0.0,
        "Interiors":   p.interiors   or 0.0,
        "Engineering": p.engineering or 0.0,
        "Cabinet":     p.cabinet     or 0.0,
        "Upholstery":  p.upholstery  or 0.0,
        "Finish":      p.finish      or 0.0,
        "category":    p.category,
    }

def _dict_to_project_fields(data: Dict[str, Any]) -> Dict[str, Any]:
    # Map incoming dict to model columns (normalize keys)
    return {
        "number": data.get("number"),
        "customer": data.get("customer"),
        "aircraft_model": data.get("aircraftModel") or data.get("aircraft_model"),
        "scope": data.get("scope"),
        "induction": _to_date(data.get("induction")),
        "delivery":  _to_date(data.get("delivery")),
        "maintenance": _float_or_zero(data.get("Maintenance")),
        "structures":  _float_or_zero(data.get("Structures")),
        "avionics":    _float_or_zero(data.get("Avionics")),
        "inspection":  _float_or_zero(data.get("Inspection")),
        "interiors":   _float_or_zero(data.get("Interiors")),
        "engineering": _float_or_zero(data.get("Engineering")),
        "cabinet":     _float_or_zero(data.get("Cabinet")),
        "upholstery":  _float_or_zero(data.get("Upholstery")),
        "finish":      _float_or_zero(data.get("Finish")),
    }

# ------------------------- Project CRUD -------------------------
def list_projects(category: Optional[str] = None) -> List[Dict[str, Any]]:
    init_db()
    with _SessionLocal() as db:
        stmt = select(Project)
        if category:
            stmt = stmt.where(Project.category == category)
        stmt = stmt.order_by(Project.induction.nulls_last())
        rows = db.execute(stmt).scalars().all()
        return [_project_to_dict(p) for p in rows]

def get_project(project_id: int) -> Optional[Dict[str, Any]]:
    init_db()
    with _SessionLocal() as db:
        p = db.get(Project, project_id)
        return _project_to_dict(p) if p else None

def create_project(data: Dict[str, Any], category: str = "confirmed") -> Dict[str, Any]:
    init_db()
    fields = _dict_to_project_fields(data)
    cat = (data.get("category") or category or "confirmed").lower()
    if cat not in ("confirmed","potential","actual"):
        cat = "confirmed"
    with _SessionLocal() as db:
        p = Project(category=cat, **fields)
        db.add(p)
        db.commit()
        db.refresh(p)
        return _project_to_dict(p)

def update_project(project_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
    init_db()
    with _SessionLocal() as db:
        p: Project | None = db.get(Project, project_id)
        if not p:
            raise ValueError(f"Project id {project_id} not found")
        fields = _dict_to_project_fields(data)
        for k, v in fields.items():
            setattr(p, k, v)
        if "category" in data and data["category"]:
            cat = str(data["category"]).lower()
            if cat in ("confirmed","potential","actual"):
                p.category = cat
        db.commit()
        db.refresh(p)
        return _project_to_dict(p)

def delete_project(project_id: int) -> None:
    init_db()
    with _SessionLocal() as db:
        p = db.get(Project, project_id)
        if p:
            db.delete(p)
            db.commit()

# ------------------------- Departments -------------------------
def list_departments() -> List[Dict[str, Any]]:
    init_db()
    with _SessionLocal() as db:
        rows = db.execute(select(Department).order_by(Department.id)).scalars().all()
        return [{"id": d.id, "key": d.key, "name": d.name, "headcount": d.headcount} for d in rows]

def upsert_departments(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Upsert by 'key'. If a row has an 'id' that doesn't match, 'key' wins.
    """
    init_db()
    rows = rows or []
    with _SessionLocal() as db:
        existing_by_key = {d.key: d for d in db.execute(select(Department)).scalars().all()}
        result = []
        for r in rows:
            key = (r.get("key") or r.get("name") or "").strip() or "Unknown"
            name = r.get("name") or key
            headcount = int(r.get("headcount") or 0)
            d = existing_by_key.get(key)
            if d:
                d.name = name
                d.headcount = headcount
            else:
                d = Department(key=key, name=name, headcount=headcount)
                db.add(d)
            result.append({"key": key, "name": name, "headcount": headcount})
        db.commit()
    return list_departments()

# ------------------------- Bulk Import -------------------------
def bulk_import_projects(file_path_or_buffer: Any, category: str) -> Dict[str, Any]:
    """
    Import projects from CSV/XLSX with columns:
    number, customer, aircraftModel, scope, induction, delivery, and the DEPT_KEYS.
    Extra columns are ignored. Missing dept columns default to 0.

    Returns: {"imported": N, "errors": [row_index,...]}
    """
    import pandas as pd

    cat = (category or "confirmed").lower()
    if cat not in ("confirmed","potential","actual"):
        cat = "confirmed"

    df = None
    try:
        df = pd.read_excel(file_path_or_buffer)
    except Exception:
        try:
            df = pd.read_csv(file_path_or_buffer)
        except Exception as e:
            raise ValueError(f"Could not read file as Excel or CSV: {e}")

    # Normalize columns -> exact expected names
    colmap = {c.lower().strip(): c for c in df.columns}
    def get_col(name: str) -> Optional[str]:
        # accept camel or snake or common variants
        candidates = {
            "number": ["number","project","project number","proj","p#","p-number"],
            "customer": ["customer","client","owner"],
            "aircraftModel": ["aircraftmodel","aircraft","model","aircraft_model"],
            "scope": ["scope","work scope","description"],
            "induction": ["induction","start","start_date","induct","induction_date"],
            "delivery": ["delivery","end","finish","delivery_date","complete","completion"],
        }
        want = name if name in candidates else None
        names = candidates.get(name, [name])
        for n in names:
            key = n.lower().strip()
            if key in colmap:
                return colmap[key]
        return None

    base_cols = {
        "number": get_col("number"),
        "customer": get_col("customer"),
        "aircraftModel": get_col("aircraftModel"),
        "scope": get_col("scope"),
        "induction": get_col("induction"),
        "delivery": get_col("delivery"),
    }

    imported, errors = 0, []
    for i, row in df.iterrows():
        try:
            rec = {
                "number": row.get(base_cols["number"]) if base_cols["number"] else None,
                "customer": row.get(base_cols["customer"]) if base_cols["customer"] else None,
                "aircraftModel": row.get(base_cols["aircraftModel"]) if base_cols["aircraftModel"] else None,
                "scope": row.get(base_cols["scope"]) if base_cols["scope"] else None,
                "induction": row.get(base_cols["induction"]) if base_cols["induction"] else None,
                "delivery": row.get(base_cols["delivery"]) if base_cols["delivery"] else None,
            }
            # departments
            for k in DEPT_KEYS:
                rec[k] = row.get(k, 0.0)
            create_project(rec, category=cat)
            imported += 1
        except Exception:
            errors.append(int(i))

    return {"imported": imported, "errors": errors}

# ------------------------- Seeding -------------------------
_DEFAULT_DEPTS = [
    {"key":"Maintenance","name":"Maintenance","headcount":36},
    {"key":"Structures","name":"Structures","headcount":22},
    {"key":"Avionics","name":"Avionics","headcount":15},
    {"key":"Inspection","name":"Inspection","headcount":10},
    {"key":"Interiors","name":"Interiors","headcount":11},
    {"key":"Engineering","name":"Engineering","headcount":7},
    {"key":"Cabinet","name":"Cabinet","headcount":3},
    {"key":"Upholstery","name":"Upholstery","headcount":7},
    {"key":"Finish","name":"Finish","headcount":6},
]

def seed_if_empty(
    projects: Optional[List[Dict[str, Any]]] = None,
    potential: Optional[List[Dict[str, Any]]] = None,
    actual: Optional[List[Dict[str, Any]]] = None,
    depts: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """
    Seed DB if no rows exist. Pass in your DEFAULT_* lists from the app.
    """
    init_db()
    with _SessionLocal() as db:
        has_projects = db.execute(select(func.count(Project.id))).scalar_one() > 0
        has_depts = db.execute(select(func.count(Department.id))).scalar_one() > 0

    if not has_depts:
        upsert_departments(depts or _DEFAULT_DEPTS)

    if not has_projects:
        for arr, cat in ((projects, "confirmed"), (potential, "potential"), (actual, "actual")):
            if not arr:
                continue
            for rec in arr:
                create_project(rec, category=cat)

# ------------------------- Convenience -------------------------
def get_all_datasets() -> Dict[str, Any]:
    """
    Return everything the Streamlit app expects, grouped like session state:
    {
      "projects": [...], "potential": [...], "actual": [...], "depts":[...]
    }
    """
    return {
        "projects":  list_projects("confirmed"),
        "potential": list_projects("potential"),
        "actual":    list_projects("actual"),
        "depts":     list_departments(),
    }

# ------------------------- Run-once init -------------------------
# Safe to call multiple times; create_all is idempotent.
init_db()

if __name__ == "__main__":
    # Simple smoke test: prints counts
    init_db()
    print("Projects (confirmed):", len(list_projects("confirmed")))
    print("Projects (potential):", len(list_projects("potential")))
    print("Projects (actual):", len(list_projects("actual")))
    print("Departments:", len(list_departments()))
