# =======================
# file: data_store.py
# =======================
import json
import sqlite3
from contextlib import contextmanager
from typing import Dict, List, Optional

DB_PATH = "capacity.db"

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS depts (
  key TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  headcount INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS projects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  number TEXT,
  customer TEXT,
  aircraftModel TEXT,
  scope TEXT,
  induction TEXT,
  delivery TEXT,
  dataset TEXT CHECK(dataset IN ('confirmed','potential','actual')) NOT NULL,
  hours_json TEXT NOT NULL DEFAULT '{}',
  include_in_analysis INTEGER NOT NULL DEFAULT 1,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_unique ON projects(number, dataset);
"""


def init_db():
    with get_conn() as c:
        c.executescript(SCHEMA_SQL)


def seed_defaults(default_projects: List[dict], default_potential: List[dict], default_actual: List[dict], default_depts: List[dict]):
    """Seed DB only if tables are empty."""
    with get_conn() as c:
        # Seed depts if empty
        cur = c.execute("SELECT COUNT(1) AS n FROM depts")
        if cur.fetchone()[0] == 0:
            for d in default_depts:
                c.execute("INSERT INTO depts(key,name,headcount) VALUES (?,?,?)", (d["key"], d["name"], int(d.get("headcount", 0))))
        # Seed projects if empty
        cur = c.execute("SELECT COUNT(1) AS n FROM projects")
        if cur.fetchone()[0] == 0:
            def _insert(rows: List[dict], dataset: str):
                for p in rows:
                    hours = {k: float(p.get(k, 0) or 0.0) for k in p.keys() if k not in {"number","customer","aircraftModel","scope","induction","delivery"}}
                    c.execute(
                        """
                        INSERT INTO projects(number, customer, aircraftModel, scope, induction, delivery, dataset, hours_json, include_in_analysis)
                        VALUES (?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            str(p.get("number") or ""),
                            str(p.get("customer") or ""),
                            str(p.get("aircraftModel") or ""),
                            str(p.get("scope") or ""),
                            str(p.get("induction") or ""),
                            str(p.get("delivery") or ""),
                            dataset,
                            json.dumps(hours),
                            1,
                        )
                    )
            _insert(default_projects, "confirmed")
            _insert(default_potential, "potential")
            _insert(default_actual, "actual")


def list_depts() -> List[dict]:
    with get_conn() as c:
        rows = c.execute("SELECT key,name,headcount FROM depts ORDER BY name").fetchall()
        return [dict(r) for r in rows]


def upsert_depts(depts: List[dict]):
    with get_conn() as c:
        for d in depts:
            c.execute(
                "INSERT INTO depts(key,name,headcount) VALUES (?,?,?)\n                 ON CONFLICT(key) DO UPDATE SET name=excluded.name, headcount=excluded.headcount",
                (d["key"], d["name"], int(d.get("headcount", 0)))
            )


def _row_to_project(r: sqlite3.Row) -> dict:
    base = dict(r)
    base["hours"] = json.loads(base.pop("hours_json") or "{}")
    base["include_in_analysis"] = bool(base.get("include_in_analysis", 1))
    return base


def get_projects(dataset: Optional[str] = None) -> List[dict]:
    with get_conn() as c:
        if dataset:
            rows = c.execute("SELECT * FROM projects WHERE dataset=? ORDER BY induction, delivery, number", (dataset,)).fetchall()
        else:
            rows = c.execute("SELECT * FROM projects ORDER BY dataset, induction, delivery, number").fetchall()
        return [_row_to_project(r) for r in rows]


def upsert_project(p: dict):
    """Upsert by (number, dataset) if id not provided."""
    hours = p.get("hours") or {k: v for k, v in p.items() if k not in {"id","number","customer","aircraftModel","scope","induction","delivery","dataset","include_in_analysis"}}
    payload = (
        p.get("number") or "",
        p.get("customer") or "",
        p.get("aircraftModel") or "",
        p.get("scope") or "",
        p.get("induction") or "",
        p.get("delivery") or "",
        p.get("dataset") or "confirmed",
        json.dumps({k: float(hours.get(k, 0) or 0.0) for k in hours.keys()}),
        1 if p.get("include_in_analysis", True) else 0,
    )
    with get_conn() as c:
        if p.get("id"):
            c.execute(
                """
                UPDATE projects SET number=?, customer=?, aircraftModel=?, scope=?, induction=?, delivery=?, dataset=?, hours_json=?, include_in_analysis=?, updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                payload + (int(p["id"]),)
            )
        else:
            # try update by (number, dataset); if no row, insert
            cur = c.execute("SELECT id FROM projects WHERE number=? AND dataset=?", (p.get("number"), p.get("dataset")))
            row = cur.fetchone()
            if row:
                c.execute(
                    """
                    UPDATE projects SET customer=?, aircraftModel=?, scope=?, induction=?, delivery=?, hours_json=?, include_in_analysis=?, updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (
                        p.get("customer") or "",
                        p.get("aircraftModel") or "",
                        p.get("scope") or "",
                        p.get("induction") or "",
                        p.get("delivery") or "",
                        payload[7],
                        payload[8],
                        int(row["id"]),
                    ),
                )
            else:
                c.execute(
                    """
                    INSERT INTO projects(number, customer, aircraftModel, scope, induction, delivery, dataset, hours_json, include_in_analysis)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    payload,
                )


def delete_project(project_id: int):
    with get_conn() as c:
        c.execute("DELETE FROM projects WHERE id=?", (project_id,))


def replace_dataset(dataset: str, rows: List[dict]):
    with get_conn() as c:
        c.execute("DELETE FROM projects WHERE dataset=?", (dataset,))
        for p in rows:
            upsert_project({**p, "dataset": dataset})


# =======================
# file: app.py  (Streamlit single-app with CRUD + charts)
# =======================
import json as _json
from datetime import date
from copy import deepcopy

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd

import data_store as store

st.set_page_config(layout="wide", page_title="Labor Capacity Dashboard + Admin")
try:
    st.image("citadel_logo.png", width=200)
except Exception:
    pass

# --------------------- DEFAULT (only used for first-run seed) ---------------------
DEFAULT_PROJECTS = [
    {"number":"P7657","customer":"Kaiser","aircraftModel":"B737","scope":"Starlink","induction":"2025-11-15T00:00:00","delivery":"2025-11-25T00:00:00","Maintenance":93.57,"Structures":240.61,"Avionics":294.07,"Inspection":120.3,"Interiors":494.58,"Engineering":80.2,"Cabinet":0,"Upholstery":0,"Finish":13.37},
    {"number":"P7611","customer":"Alpha Star","aircraftModel":"A340","scope":"Mx Check","induction":"2025-10-20T00:00:00","delivery":"2025-12-04T00:00:00","Maintenance":2432.23,"Structures":1252.97,"Avionics":737.04,"Inspection":1474.08,"Interiors":1474.08,"Engineering":0.0,"Cabinet":0,"Upholstery":0,"Finish":0.0},
    {"number":"P7645","customer":"Kaiser","aircraftModel":"B737","scope":"Starlink","induction":"2025-11-30T00:00:00","delivery":"2025-12-10T00:00:00","Maintenance":93.57,"Structures":240.61,"Avionics":294.07,"Inspection":120.3,"Interiors":494.58,"Engineering":80.2,"Cabinet":0,"Upholstery":0,"Finish":13.37},
    {"number":"P7426","customer":"Celestial","aircraftModel":"B757","scope":"Post Maintenance Discrepancies","induction":"2026-01-05T00:00:00","delivery":"2026-01-15T00:00:00","Maintenance":0.0,"Structures":0.0,"Avionics":0.0,"Inspection":0.0,"Interiors":0.0,"Engineering":0.0,"Cabinet":0,"Upholstery":0,"Finish":0.0},
    {"number":"P7548","customer":"Ty Air","aircraftModel":"B737","scope":"CMS Issues","induction":"2025-10-20T00:00:00","delivery":"2025-10-30T00:00:00","Maintenance":0.0,"Structures":0.0,"Avionics":0.0,"Inspection":0.0,"Interiors":0.0,"Engineering":0.0,"Cabinet":0,"Upholstery":0,"Finish":0.0},
    {"number":"P7706","customer":"Valkyrie","aircraftModel":"B737-MAX","scope":"Starlink, Mods","induction":"2025-10-31T00:00:00","delivery":"2025-11-25T00:00:00","Maintenance":123.3,"Structures":349.4,"Avionics":493.2,"Inspection":164.4,"Interiors":698.7,"Engineering":143.8,"Cabinet":61.6,"Upholstery":0,"Finish":20.6},
    {"number":"P7685","customer":"Sands","aircraftModel":"B737-700","scope":"Starlink","induction":"2025-11-17T00:00:00","delivery":"2025-11-24T00:00:00","Maintenance":105.44,"Structures":224.1,"Avionics":303.14,"Inspection":118.62,"Interiors":474.48,"Engineering":79.08,"Cabinet":0,"Upholstery":0,"Finish":13.18},
    {"number":"P7712","customer":"Ty Air","aircraftModel":"B737","scope":"Monthly and 6 Month Check","induction":"2025-11-04T00:00:00","delivery":"2025-12-21T00:00:00","Maintenance":893.0,"Structures":893.0,"Avionics":476.3,"Inspection":238.1,"Interiors":3453.0,"Engineering":0.0,"Cabinet":0,"Upholstery":0,"Finish":0.0},
    {"number":"P7639/7711","customer":"Snap","aircraftModel":"B737","scope":"Starlink and MX Package","induction":"2025-12-01T00:00:00","delivery":"2025-12-15T00:00:00","Maintenance":132.1,"Structures":330.3,"Avionics":440.4,"Inspection":220.2,"Interiors":990.9,"Engineering":66.1,"Cabinet":0,"Upholstery":0,"Finish":22.0},
]
DEFAULT_POTENTIAL = [
    {"number":"P7661","customer":"Sands","aircraftModel":"A340-500","scope":"C Check","induction":"2026-01-29T00:00:00","delivery":"2026-02-28T00:00:00","Maintenance":2629.44,"Structures":1709.14,"Avionics":723.1,"Inspection":1248.98,"Interiors":262.94,"Engineering":0,"Cabinet":0,"Upholstery":0,"Finish":0},
    {"number":"P7669","customer":"Sands","aircraftModel":"A319-133","scope":"C Check","induction":"2025-12-08T00:00:00","delivery":"2026-01-28T00:00:00","Maintenance":2029.67,"Structures":984.08,"Avionics":535.55,"Inspection":675.56,"Interiors":1906.66,"Engineering":0,"Cabinet":0,"Upholstery":0,"Finish":0},
    {"number":None,"customer":"Sands","aircraftModel":"B767-300","scope":"C Check","induction":"2026-09-15T00:00:00","delivery":"2026-12-04T00:00:00","Maintenance":0.0,"Structures":0.0,"Avionics":0.0,"Inspection":0.0,"Interiors":0.0,"Engineering":0,"Cabinet":0,"Upholstery":0,"Finish":0},
    {"number":"P7686","customer":"Polaris","aircraftModel":"B777","scope":"1A & 3A Mx Checks","induction":"2025-12-01T00:00:00","delivery":"2025-12-09T00:00:00","Maintenance":643.15,"Structures":287.36,"Avionics":150.52,"Inspection":177.89,"Interiors":109.47,"Engineering":0,"Cabinet":0,"Upholstery":0,"Finish":0},
    {"number":"P7430","customer":"Turkmen","aircraftModel":"B777","scope":"Maint/Recon/Refub","induction":"2025-11-10T00:00:00","delivery":"2026-07-13T00:00:00","Maintenance":12720.0,"Structures":12720.0,"Avionics":3180.0,"Inspection":3180.0,"Interiors":19080.0,"Engineering":3180,"Cabinet":3180,"Upholstery":3180,"Finish":3180},
    {"number":"P7649","customer":"NEP","aircraftModel":"B767-300","scope":"Refurb","induction":"2026-02-02T00:00:00","delivery":"2026-07-13T00:00:00","Maintenance":2000.0,"Structures":2400.0,"Avionics":2800.0,"Inspection":800.0,"Interiors":4400.0,"Engineering":1800,"Cabinet":1600,"Upholstery":1200,"Finish":3000},
    {"number":"P7689","customer":"Sands","aircraftModel":"B737-700","scope":"C1,C3,C6C7 Mx","induction":"2025-09-10T00:00:00","delivery":"2026-11-07T00:00:00","Maintenance":8097.77,"Structures":1124.69,"Avionics":899.75,"Inspection":787.28,"Interiors":337.14,"Engineering":0,"Cabinet":0,"Upholstery":0,"Finish":0},
    {"number":"P7690","customer":"Sands","aircraftModel":None,"scope":"C1,C2,C7 Mx","induction":"2025-05-25T00:00:00","delivery":"2025-07-22T00:00:00","Maintenance":3227.14,"Structures":2189.85,"Avionics":922.04,"Inspection":1152.55,"Interiors":4033.92,"Engineering":0,"Cabinet":0,"Upholstery":0,"Finish":0},
    {"number":"P7691","customer":"Sands","aircraftModel":"B737-700","scope":"C1,C2,C3,C7 Mx","induction":"2026-10-13T00:00:00","delivery":"2026-12-22T00:00:00","Maintenance":4038.3,"Structures":5115.18,"Avionics":1076.88,"Inspection":1346.1,"Interiors":1884.54,"Engineering":0,"Cabinet":0,"Upholstery":0,"Finish":0},
]
DEFAULT_ACTUAL = []
DEFAULT_DEPTS = [
    {"name":"Maintenance","headcount":36,"key":"Maintenance"},
    {"name":"Structures","headcount":22,"key":"Structures"},
    {"name":"Avionics","headcount":15,"key":"Avionics"},
    {"name":"Inspection","headcount":10,"key":"Inspection"},
    {"name":"Interiors","headcount":11,"key":"Interiors"},
    {"name":"Engineering","headcount":7,"key":"Engineering"},
    {"name":"Cabinet","headcount":3,"key":"Cabinet"},
    {"name":"Upholstery","headcount":7,"key":"Upholstery"},
    {"name":"Finish","headcount":6,"key":"Finish"},
]

# --------------------- DB INIT ---------------------
store.init_db()
store.seed_defaults(DEFAULT_PROJECTS, DEFAULT_POTENTIAL, DEFAULT_ACTUAL, DEFAULT_DEPTS)

# --------------------- HELPERS ---------------------

def dept_keys():
    return [d["key"] for d in store.list_depts()]


def project_to_hours_dict(p: dict) -> dict:
    h = {k: float(v) for k, v in (p.get("hours") or {}).items()}
    # Normalize to all known dept keys
    for k in dept_keys():
        if k not in h:
            h[k] = 0.0
    return h

# --------------------- SIDEBAR: CRUD / ADMIN ---------------------
st.sidebar.header("Data Admin")
admin_tabs = st.sidebar.tabs(["Add / Edit", "Bulk Upload", "Departments"])

with admin_tabs[0]:
    st.caption("Create or update a single project (saved to SQLite immediately)")
    ds_map = {"Confirmed":"confirmed","Potential":"potential","Actual":"actual"}
    ds_choice = st.selectbox("Dataset", list(ds_map.keys()), key="admin_ds")

    dataset = ds_map[ds_choice]
    existing = store.get_projects(dataset)
    options = ["➕ New Project"] + [f"{p['number'] or '—'} — {p['customer']}" for p in existing]
    sel = st.selectbox("Project", options, key="admin_sel")

    if sel == "➕ New Project":
        proj = {"number":"PXXXX","customer":"","aircraftModel":"","scope":"",
                "induction": date(2025,11,1).isoformat(), "delivery": date(2025,11,8).isoformat(),
                "dataset": dataset, "hours": {k:0.0 for k in dept_keys()}, "include_in_analysis": True}
    else:
        idx = options.index(sel) - 1
        proj = existing[idx]

    c1, c2, c3 = st.columns(3)
    with c1:
        proj["number"] = st.text_input("Project Number", str(proj.get("number") or ""))
        proj["customer"] = st.text_input("Customer", str(proj.get("customer") or ""))
        proj["aircraftModel"] = st.text_input("Aircraft Model", str(proj.get("aircraftModel") or ""))
    with c2:
        proj["scope"] = st.text_input("Scope", str(proj.get("scope") or ""))
        proj["induction"] = st.date_input("Induction", date.fromisoformat(str(proj.get("induction", "2025-11-01"))[:10])).isoformat()
        proj["delivery"] = st.date_input("Delivery", date.fromisoformat(str(proj.get("delivery", "2025-11-08"))[:10])).isoformat()
    with c3:
        if dataset == "potential":
            proj["include_in_analysis"] = st.checkbox("Include in visualizations by default", bool(proj.get("include_in_analysis", True)))
        else:
            st.caption("\n")
            st.caption("\n")
            st.caption("\n")

    st.markdown("**Hours by department**")
    hcols = st.columns(3)
    hours = project_to_hours_dict(proj)
    keys = dept_keys()
    for i, k in enumerate(keys):
        with hcols[i % 3]:
            hours[k] = st.number_input(f"{k}", min_value=0.0, value=float(hours.get(k, 0.0)), step=1.0)
    proj["hours"] = hours
    proj["dataset"] = dataset

    cA, cB, cC = st.columns([1,1,1])
    if cA.button("Save / Update", use_container_width=True):
        store.upsert_project(proj)
        st.success("Saved to database.")
    if sel != "➕ New Project":
        if cB.button("Delete", type="primary", use_container_width=True):
            store.delete_project(existing[options.index(sel)-1]["id"])
            st.warning("Deleted. Reload sidebar to refresh list.")

with admin_tabs[1]:
    st.caption("Upload CSV or Excel with columns: dataset, number, customer, aircraftModel, scope, induction, delivery, and one column per department name (e.g., Maintenance, Structures, ...)")
    mode = st.radio("Ingest mode", ["Append", "Replace per dataset"], horizontal=True)
    up = st.file_uploader("Upload file", type=["csv", "xlsx", "xls"])
    if up:
        try:
            if up.name.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(up)
            else:
                df = pd.read_csv(up)
        except Exception as e:
            st.error(f"Could not read file: {e}")
            df = None
        if df is not None:
            # Normalize column names
            df.columns = [c.strip() for c in df.columns]
            st.dataframe(df.head(20))
            if st.button("Import", type="primary"):
                try:
                    grouped = df.groupby(df['dataset'].str.lower())
                    for ds, sub in grouped:
                        rows = []
                        for _, r in sub.iterrows():
                            hours = {k: float(r.get(k, 0) or 0.0) for k in dept_keys()}
                            rows.append({
                                "number": str(r.get("number", "")),
                                "customer": str(r.get("customer", "")),
                                "aircraftModel": str(r.get("aircraftModel", "")),
                                "scope": str(r.get("scope", "")),
                                "induction": str(r.get("induction", "")),
                                "delivery": str(r.get("delivery", "")),
                                "hours": hours,
                                "include_in_analysis": bool(r.get("include_in_analysis", True)),
                            })
                        if mode.startswith("Replace"):
                            store.replace_dataset(ds, rows)
                        else:
                            for p in rows:
                                store.upsert_project({**p, "dataset": ds})
                    st.success("Import complete.")
                except Exception as e:
                    st.error(f"Import failed: {e}")

with admin_tabs[2]:
    st.caption("Edit department names and headcounts, then save.")
    df_depts = pd.DataFrame(store.list_depts())
    df_depts = st.data_editor(df_depts, key="de_depts", height=260)
    if st.button("Save Departments", type="primary"):
        try:
            df_depts["headcount"] = pd.to_numeric(df_depts["headcount"], errors="coerce").fillna(0).astype(int)
            store.upsert_depts(df_depts.to_dict(orient="records"))
            st.success("Departments saved.")
        except Exception as e:
            st.error(f"Failed to save departments: {e}")

st.sidebar.markdown("---")

# --------------------- LOAD DATA FOR DASHBOARD ---------------------
# Pull all current rows to feed into the existing HTML/JS visual layer
all_confirmed = store.get_projects("confirmed")
all_potential = store.get_projects("potential")
all_actual    = store.get_projects("actual")
all_depts     = store.list_depts()

# Potential checklist (for visualizations only)
pot_labels = [f"{p['number'] or '—'} — {p['customer']}" for p in all_potential]
pre_selected = [i for i,p in enumerate(all_potential) if p.get("include_in_analysis", True)]

st.sidebar.subheader("Visualization Filters")
use_checklist = st.sidebar.checkbox("Use potential project checklist", value=True)
selected_idx = st.sidebar.multiselect("Potential to include", options=list(range(len(pot_labels))),
                                      default=pre_selected, format_func=lambda i: pot_labels[i],
                                      disabled=not use_checklist)

# Filter potential for injection
if use_checklist:
    filtered_potential = [all_potential[i] for i in selected_idx]
else:
    filtered_potential = [p for p in all_potential if p.get("include_in_analysis", True)]

# Shape data to match the HTML template expectations

def shape_for_html(rows: List[dict]) -> List[dict]:
    shaped = []
    for p in rows:
        base = {
            "number": p.get("number"),
            "customer": p.get("customer"),
            "aircraftModel": p.get("aircraftModel"),
            "scope": p.get("scope"),
            "induction": p.get("induction"),
            "delivery": p.get("delivery"),
        }
        for k, v in (p.get("hours") or {}).items():
            base[k] = float(v or 0.0)
        shaped.append(base)
    return shaped

HTML_PROJECTS  = shape_for_html(all_confirmed)
HTML_POTENTIAL = shape_for_html(filtered_potential)
HTML_ACTUAL    = shape_for_html(all_actual)
HTML_DEPTS     = all_depts

# --------------------- VISUAL LAYER (reuses your proven HTML/JS) ---------------------
# NOTE: potential is passed already filtered. Default Show Potential in the top graph is OFF in the HTML.

# the massive html_template was in your previous code; we keep it as-is but ensure default toggles
# For brevity, we include only the small patch that turns off defaults in the top graph and snapshot/planner

from pathlib import Path

# Load your existing template from file if you placed it externally; otherwise, embed a minimized version
# Here we inline a tiny loader that expects `html_template` variable available from your prior app.
try:
    from html_template_module import html_template  # optional: if you split HTML to a separate file
except Exception:
    # Fallback: read from a sibling file named html_template.html if you created one
    if Path("html_template.html").exists():
        html_template = Path("html_template.html").read_text(encoding="utf-8")
    else:
        # As a last resort, import the template string from a previous block in memory
        # If you still keep your long html_template string in this same file, comment this block out
        html_template = """
        <html><body>
        <p style='color:#b91c1c'>Missing html_template. Please paste your existing long HTML/JS template string into this variable.</p>
        </body></html>
        """

# --- Patch default toggles inside the HTML ---
# 1) Top graph: default Show Potential OFF (we'll set checkbox unchecked via a simple replace on the HTML)
html_template = html_template.replace(
    '<input type="checkbox" id="showPotential" checked>',
    '<input type="checkbox" id="showPotential">'
)
# 2) Snapshot defaults: potential OFF by default
html_template = html_template.replace(
    '<input type="checkbox" id="snapPotential" checked>',
    '<input type="checkbox" id="snapPotential">'
)
# 3) Hangar planner defaults: potential OFF by default
html_template = html_template.replace(
    '<input type="checkbox" id="planIncludePotential" checked>',
    '<input type="checkbox" id="planIncludePotential">'
)

# Inject live data
html_code = (
    html_template
      .replace("__PROJECTS__", _json.dumps(HTML_PROJECTS))
      .replace("__POTENTIAL__", _json.dumps(HTML_POTENTIAL))
      .replace("__ACTUAL__", _json.dumps(HTML_ACTUAL))
      .replace("__DEPTS__", _json.dumps(HTML_DEPTS))
)

components.html(html_code, height=2600, scrolling=False)

# Footer note
st.caption("Data is persisted in capacity.db (SQLite). Use the sidebar → Data Admin to add, edit, delete, or bulk-upload projects and departments. Potential project checklist only affects visualizations; it does not delete data.")
