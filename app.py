import json
from datetime import date
from copy import deepcopy
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd

st.set_page_config(layout="wide", page_title="Labor Capacity Dashboard")
try:
    st.image("citadel_logo.png", width=200)
except Exception:
    pass

# --------------------- LOCAL JS FALLBACKS ---------------------

def _read_asset(p: str) -> str:
    """Read a JS asset from disk; if missing, return a safe warning script.
    Place these files in ./assets/ :
      - chart.umd.min.js
      - chartjs-plugin-annotation.min.js
      - echarts.min.js
    """
    try:
        return Path(p).read_text(encoding="utf-8")
    except Exception:
        # Keep running even if files are missing (CDN may still succeed)
        return f"console.warn('Missing {p}; charts may not render if CDN is blocked.');"

CHART_JS_INLINE = _read_asset("assets/chart.umd.min.js")
CHART_ANNO_INLINE = _read_asset("assets/chartjs-plugin-annotation.min.js")
ECHARTS_INLINE = _read_asset("assets/echarts.min.js")

# --------------------- DEFAULT DATA (YOUR SET) ---------------------
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

# --------------------- SESSION STATE ---------------------
if "projects" not in st.session_state:
    st.session_state.projects = deepcopy(DEFAULT_PROJECTS)
if "potential" not in st.session_state:
    st.session_state.potential = deepcopy(DEFAULT_POTENTIAL)
if "actual" not in st.session_state:
    st.session_state.actual = deepcopy(DEFAULT_ACTUAL)
if "depts" not in st.session_state:
    st.session_state.depts = deepcopy(DEFAULT_DEPTS)


def dept_keys():
    return [d["key"] for d in st.session_state.depts]

# --------------------- QUICK EDIT (sidebar) ---------------------
st.sidebar.header("Quick Edit")
dataset_choice = st.sidebar.selectbox("Dataset", ["Confirmed","Potential","Actual"])
dataset_key = {"Confirmed":"projects","Potential":"potential","Actual":"actual"}[dataset_choice]
current_list = st.session_state[dataset_key]
project_ids = [f'{(p.get("number") or "—")} — {(p.get("customer") or "Unknown")}' for p in current_list]
select_existing = st.sidebar.selectbox("Project", ["➕ New Project"] + project_ids)

with st.sidebar.form("quick_edit"):
    if select_existing == "➕ New Project":
        number = st.text_input("Project Number", "PXXXX")
        customer = st.text_input("Customer", "")
        aircraft = st.text_input("Aircraft Model", "")
        scope = st.text_input("Scope", "")
        induction = st.date_input("Induction", date(2025, 11, 1)).isoformat()
        delivery  = st.date_input("Delivery",  date(2025, 11, 8)).isoformat()
        hours_inputs = {k: st.number_input(f"{k} hours", min_value=0.0, value=0.0, step=1.0) for k in dept_keys()}
    else:
        idx = project_ids.index(select_existing)
        proj = deepcopy(current_list[idx])
        number = st.text_input("Project Number", str(proj.get("number") or ""))
        customer = st.text_input("Customer", str(proj.get("customer") or ""))
        aircraft = st.text_input("Aircraft Model", str(proj.get("aircraftModel") or ""))
        scope = st.text_input("Scope", str(proj.get("scope") or ""))
        induction = st.date_input("Induction", date.fromisoformat(str(proj["induction"])[:10])).isoformat()
        delivery  = st.date_input("Delivery",  date.fromisoformat(str(proj["delivery"])[:10])).isoformat()
        hours_inputs = {k: st.number_input(f"{k} hours", min_value=0.0, value=float(proj.get(k, 0) or 0), step=1.0) for k in dept_keys()}

    colA, colB = st.columns(2)
    with colA:
        apply_btn = st.form_submit_button("Apply Changes", use_container_width=True)
    with colB:
        reset_btn = st.form_submit_button("Reset to Defaults", use_container_width=True)

if apply_btn:
    entry = {"number": number.strip(), "customer": customer.strip(), "aircraftModel": aircraft.strip(),
             "scope": scope.strip(), "induction": induction, "delivery": delivery}
    for k in dept_keys():
        entry[k] = float(hours_inputs[k] or 0.0)
    if select_existing == "➕ New Project":
        st.session_state[dataset_key].append(entry)
    else:
        st.session_state[dataset_key][idx] = entry
    st.toast("Dataset updated", icon="✅")

if reset_btn:
    st.session_state.projects  = deepcopy(DEFAULT_PROJECTS)
    st.session_state.potential = deepcopy(DEFAULT_POTENTIAL)
    st.session_state.actual    = deepcopy(DEFAULT_ACTUAL)
    st.session_state.depts     = deepcopy(DEFAULT_DEPTS)
    st.toast("All datasets reset to defaults", icon="↩️")

# --------------------- BULK EDIT ---------------------
with st.expander("Bulk Edit: Confirmed / Potential / Actual", expanded=False):
    c1, c2, c3 = st.columns(3)
    with c1:
        df_proj = st.data_editor(pd.DataFrame(st.session_state.projects), key="ed_confirmed", height=300)
        st.session_state.projects = df_proj.astype(object).to_dict(orient="records")
    with c2:
        df_pot = st.data_editor(pd.DataFrame(st.session_state.potential), key="ed_potential", height=300)
        st.session_state.potential = df_pot.astype(object).to_dict(orient="records")
    with c3:
        df_act = st.data_editor(pd.DataFrame(st.session_state.actual), key="ed_actual", height=300)
        st.session_state.actual = df_act.astype(object).to_dict(orient="records")

with st.expander("Edit Department Headcounts", expanded=False):
    df_depts = st.data_editor(pd.DataFrame(st.session_state.depts), key="ed_depts", height=240)
    df_depts["headcount"] = pd.to_numeric(df_depts["headcount"], errors="coerce").fillna(0).astype(int)
    st.session_state.depts = df_depts.to_dict(orient="records")

st.markdown("---")

# --------------------- HTML/JS ---------------------
html_template = r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <title>Labor Capacity Dashboard</title>

  <!-- Chart.js + annotation: try CDN, then inline fallback -->
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"
          onerror="window._chartCdnFailed=true;"></script>
  <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.1.2/dist/chartjs-plugin-annotation.min.js"
          onerror="window._chartAnnoCdnFailed=true;"></script>
  <script>
  (function(){
    if (!window.Chart || window._chartCdnFailed) {
      /* __CHART_JS_INLINE__ */
    }
    if (!window['chartjs-plugin-annotation'] || window._chartAnnoCdnFailed) {
      /* __CHART_ANNO_INLINE__ */
    }
    try { if (window['chartjs-plugin-annotation']) { Chart.register(window['chartjs-plugin-annotation']); } } catch(e) {}
  })();
  </script>

  <!-- ECharts: try CDN, then inline fallback -->
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"
          onerror="window._echartsCdnFailed=true;"></script>
  <script>
  if (!window.echarts || window._echartsCdnFailed) {
    /* __ECHARTS_INLINE__ */
  }
  </script>

  <!-- Tiny diagnostic banner -->
  <script>
  (function(){
    function note(msg){
      var d=document.createElement('div');
      d.style.cssText='margin:8px 14px;padding:8px;border-radius:8px;border:1px solid #eab308;background:#fef9c3;color:#713f12;font:12px/1.3 system-ui,sans-serif';
      d.textContent=msg; document.addEventListener('DOMContentLoaded', function(){ document.body.prepend(d); });
    }
    if (!window.Chart)   note('Chart.js not available — using local fallback or blocked by network.');
    if (!window.echarts) note('ECharts not available — using local fallback or blocked by network.');
  })();
  </script>

  <style>
    :root{
      --brand:#003366; --brand-20: rgba(0,51,102,0.2);
      --capacity:#d32f2f; --capacity-20: rgba(211,47,47,0.2);
      --potential:#2e7d32; --potential-20: rgba(46,125,50,0.2);
      --actual:#ef6c00; --actual-20: rgba(239,108,0,0.2);
      --muted:#6b7280;
      --confirmed:#2563eb;
      --potential2:#059669;
    }
    html, body { height:100%; }
    body { font-family: Arial, sans-serif; margin: 8px 14px 24px; overflow-x:hidden; }
    h1 { text-align:center; margin: 6px 0 4px; }
    .controls { display:flex; gap:16px; flex-wrap:wrap; align-items:center; justify-content:center; margin: 8px auto 10px; }
    .controls label { font-size:14px; display:flex; align-items:center; gap:6px; }
    .metric-bar { display:flex; gap:16px; justify-content:center; flex-wrap:wrap; margin: 8px 0 10px; }
    .metric { border:1px solid #e5e7eb; border-radius:10px; padding:10px 14px; min-width:220px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); background:#fff; }
    .metric .label { font-size:12px; color:var(--muted); margin-bottom:4px; }
    .metric .value { font-weight:700; font-size:18px; }
    .chart-wrap { width:100%; height:720px; margin-bottom: 8px; position:relative; }
    .chart-wrap.util { height:380px; margin-top: 8px; }
    .footnote { text-align:center; color:#6b7280; font-size:12px; }

    /* Anchored popover for drilldown */
    .popover { display:none; position:fixed; z-index:9999; max-width:min(92vw, 900px);
      background:#fff; border:1px solid #e5e7eb; border-radius:12px; box-shadow:0 12px 30px rgba(0,0,0,0.2); }
    .popover header { padding:10px 12px; border-bottom:1px solid #eee; font-weight:600; display:flex; justify-content:space-between; gap:10px; align-items:center; }
    .popover header button { border:none; background:#f3f4f6; border-radius:8px; padding:4px 8px; cursor:pointer; }
    .popover .content { padding:10px 12px 12px; max-height:60vh; overflow:auto; }
    .popover table { width:100%; border-collapse:collapse; }
    .popover th, .popover td { border-bottom:1px solid #eee; padding:6px 8px; text-align:left; font-size:13px; }

    /* What-If panel */
    .impact-grid{ display:grid; gap:10px; grid-template-columns: repeat(6, minmax(120px,1fr)); align-items:end; margin:10px 0 6px; }
    .impact-grid label{ font-size:12px; color:#374151; display:flex; flex-direction:column; gap:6px; }
    .impact-grid input, .impact-grid select, .impact-grid button{ padding:8px; border:1px solid #e5e7eb; border-radius:8px; font-size:13px;}
    .impact-grid button{ cursor:pointer; background:#111827; color:#fff; border-color:#111827; }
    .impact-box{ border:1px solid #e5e7eb; border-radius:10px; padding:10px 12px; background:#fff; font-size:13px;}
    .impact-table{ width:100%; border-collapse:collapse; margin-top:6px; }
    .impact-table th,.impact-table td{ border-bottom:1px solid #eee; padding:6px 8px; text-align:left; }

    details.impact{ border:1px solid #e5e7eb; border-radius:10px; padding:8px 12px; background:#fafafa; margin:8px 0 14px; }
    details.impact summary{ cursor:pointer; font-weight:600; }

    /* Manual project sub-panel */
    .manual-panel { display:none; border:1px dashed #cbd5e1; border-radius:10px; padding:10px; background:#fff; }
    .manual-grid { display:grid; gap:10px; grid-template-columns: repeat(6, minmax(120px,1fr)); margin-top:8px; }
    .manual-grid label { font-size:12px; color:#374151; display:flex; flex-direction:column; gap:6px; }
    .manual-hours { display:grid; gap:8px; grid-template-columns: repeat(6, minmax(100px,1fr)); margin-top:10px; }

    .manual-panel input,
    .manual-panel select,
    .manual-hours input { font-size: 13px; line-height: 1.25; padding: 8px 10px; border: 1px solid #e5e7eb; border-radius: 8px; width: 100%; box-sizing: border-box; }
    .manual-hours label { font-size: 12px; display: flex; flex-direction: column; gap: 6px; }
    .manual-grid, .manual-hours { align-items: end; }

    /* Hangar Bay Planner */
    .hangar-wrap { margin-top:14px; }
    .hangar-controls { display:flex; gap:12px; flex-wrap:wrap; align-items:center; margin:8px 0 10px; }
    .hangar-grid { width:100%; overflow:auto; border:1px solid #e5e7eb; border-radius:10px; background:#fff; }
    .hgrid { display:grid; min-width:800px; }
    .hcell { border-bottom:1px solid #f1f5f9; border-right:1px solid #f1f5f9; padding:8px; font-size:12px; line-height:1.15; }
    .hcell.header { background:#f8fafc; font-weight:600; position:sticky; top:0; z-index:1; }
    .hcell.rowhdr { background:#f8fafc; font-weight:600; position:sticky; left:0; z-index:1; white-space:nowrap; }
    /* Occupancy-based colors */
    .hcell.empty { background:#fef9c3; border-color:#fde68a; color:#7a6c1f; }   /* yellow: no aircraft */
    .hcell.occupied { background:#dcfce7; border-color:#bbf7d0; }               /* green: has aircraft */
    .hcell.occupied.split { border:1px dashed #86efac; }                        /* keep dashed edge for split */
    .hcell.conflict { background:#fee2e2; border-color:#fecaca; color:#991b1b; font-weight:600; } /* red */

    /* Snapshot breakdown */
    details.snapshot { border:1px solid #e5e7eb; border-radius:10px; padding:8px 12px; background:#fafafa; margin:10px 0 2px; }
    details.snapshot summary{ cursor:pointer; font-weight:600; }
    .snap-controls { display:flex; gap:12px; align-items:center; flex-wrap:wrap; margin:8px 0; }
    .snap-grid { display:grid; gap:10px; grid-template-columns: repeat(3, minmax(250px,1fr)); }
    .snap-card { background:#fff; border:1px solid #e5e7eb; border-radius:10px; padding:8px; height:360px; display:flex; flex-direction:column; }
    .snap-card h4 { margin:0 0 6px 0; font-size:14px; color:#111827; }
    .snap-legend { font-size:12px; color:#374151; display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin:4px 0 6px; }
    .chip { display:inline-flex; align-items:center; gap:6px; padding:4px 8px; border-radius:999px; border:1px solid #e5e7eb; background:#fff; }
    .dot { width:10px; height:10px; border-radius:999px; display:inline-block; }
    .snap-echart { width:100%; flex:1 1 auto; }

  </style>
</head>
<body>

<h1>Capacity-Load By Discipline</h1>

<div class="controls">
  <label for="disciplineSelect"><strong>Discipline:</strong></label>
  <select id="disciplineSelect"></select>

  <label><input type="checkbox" id="showPotential"> Show Potential</label>
  <label><input type="checkbox" id="showActual"> Show Actual</label>

  <label><strong>Timeline:</strong>
    <select id="periodSel">
      <option value="weekly" selected>Weekly</option>
      <option value="monthly">Monthly</option>
    </select>
  </label>

  <label><strong>Productivity:</strong>
    <input type="range" id="prodFactor" min="0.50" max="1.00" step="0.01" value="0.85">
    <span id="prodVal">0.85</span>
  </label>

  <label><strong>Hours / FTE / wk:</strong>
    <input type="number" id="hoursPerFTE" min="30" max="60" step="1" value="40" style="width:64px;">
  </label>

  <label><input type="checkbox" id="utilSeparate" checked> Utilization in separate chart</label>
</div>

<div class="metric-bar">
  <div class="metric"><div class="label">Peak Utilization</div><div class="value" id="peakUtil">—</div></div>
  <div class="metric"><div class="label">Worst Period (Max Over/Under)</div><div class="value" id="worstWeek">—</div></div>
  <div class="metric"><div class="label">Capacity</div><div class="value" id="weeklyCap">—</div></div>
</div>

<!-- What-If Schedule Impact -->
<details class="impact" open>
  <summary>What-If Schedule Impact</summary>
  <div class="impact-grid">
    <label>Source dataset
      <select id="impactSource">
        <option value="potential" selected>Potential</option>
        <option value="confirmed">Confirmed</option>
        <option value="manual">Manual</option>
      </select>
    </label>

    <label id="impactProjWrap">Project
      <select id="impactProject"></select>
    </label>

    <label>Scope multiplier
      <input id="impactMult" type="number" step="0.05" value="1.00">
    </label>
    <label>Min lead (days)
      <input id="impactLead" type="number" step="1" value="14">
    </label>
    <label>Overtime (+% cap)
      <input id="impactOT" type="number" step="5" value="0">
    </label>
    <label>Target util (%)
      <input id="impactTarget" type="number" step="5" value="100">
    </label>

    <label>Induction override
      <input type="date" id="impactInd">
    </label>
    <label>Delivery override
      <input type="date" id="impactDel">
    </label>

    <button id="impactRun">Calculate Impact</button>
    <button id="impactClear" style="background:#6b7280;border-color:#6b7280;">Clear What-If</button>
  </div>

  <!-- Manual project details (shown only when source = Manual) -->
  <div class="manual-panel" id="manualPanel">
    <div class="manual-grid">
      <label>Project Number
        <input id="m_number" type="text" value="P-Manual">
      </label>
      <label>Customer
        <input id="m_customer" type="text" value="Manual">
      </label>
      <label>Aircraft Model
        <input id="m_aircraft" type="text" value="">
      </label>
      <label>Scope
        <input id="m_scope" type="text" value="What-If">
      </label>
      <label>Induction
        <input id="m_ind" type="date">
      </label>
      <label>Delivery
        <input id="m_del" type="date">
      </label>
    </div>
    <div class="manual-hours" id="manualHours">
      <!-- dept hours inputs injected here -->
    </div>
  </div>

  <div id="impactResult" class="impact-box"></div>
</details>

<div class="chart-wrap"><canvas id="myChart"></canvas></div>
<div class="chart-wrap util" style="display:block;"><canvas id="utilChart"></canvas></div>

<p class="footnote">Tip: click the <em>Confirmed</em> line; if “Show Potential” is on, the popup includes both Confirmed and Potential for that period.</p>

<!-- Snapshot Breakdown (Sankey, Treemap, Pareto) -->
<details class="snapshot" open>
  <summary>Snapshot Breakdown (Projects → Dept)</summary>
  <div class="snap-controls">
    <label><input type="checkbox" id="snapConfirmed" checked> Include Confirmed</label>
    <label><input type="checkbox" id="snapPotential" checked> Include Potential</label>

    <label>Top N projects
      <input type="range" id="snapTopN" min="3" max="20" step="1" value="8" style="vertical-align:middle;">
      <span id="snapTopNVal">8</span>
    </label>

    <label>From
      <input type="date" id="snapFrom">
    </label>
    <label>To
      <input type="date" id="snapTo">
    </label>
    <button id="snapReset" style="padding:6px 10px;border:1px solid #e5e7eb;border-radius:8px;background:#fff;cursor:pointer;">Reset</button>

    <span class="snap-legend" style="margin-left:auto">
      <span class="chip"><span class="dot" style="background:var(--confirmed)"></span> Confirmed</span>
      <span class="chip"><span class="dot" style="background:var(--potential2)"></span> Potential</span>
    </span>
  </div>
  <div class="snap-grid">
    <div class="snap-card">
      <h4>Sankey: Project → Dept (by hours)</h4>
      <div id="sankeyDiv" class="snap-echart"></div>
    </div>
    <div class="snap-card">
      <h4>Treemap: Project contribution</h4>
      <div id="treemapDiv" class="snap-echart"></div>
    </div>
    <div class="snap-card">
      <h4>Pareto: Top contributors</h4>
      <canvas id="paretoCanvas"></canvas>
    </div>
  </div>
</details>

<!-- Hangar Bay Planner (beta) -->
<details class="snapshot" open>
  <summary>Hangar Bay Planner (beta)</summary>
  <div class="hangar-wrap">
    <div class="hangar-controls">
      <label><input type="checkbox" id="planIncludePotential" checked> Include Potential projects</label>
      <label>Periods to show
        <input type="number" id="planPeriods" min="4" max="52" step="1" value="12" style="width:72px;">
      </label>
      <label>Start at
        <input type="date" id="planFrom">
      </label>
      <span style="margin-left:auto;font-size:12px;color:#6b7280;">Rules: H has 2 bays (each can split into 2 small). D1 & D2 can each host 1×B757 or split into 2 small (only one of them split at a time). D3 = one slot only.</span>
    </div>
    <div id="hangarGrid" class="hangar-grid"></div>
  </div>
</details>

<!-- Anchored popover -->
<div class="popover" id="drillPopover" role="dialog" aria-modal="true" aria-labelledby="popTitle">
  <header>
    <div id="popTitle">Breakdown</div>
    <button id="closePop">Close</button>
  </header>
  <div class="content">
    <table>
      <thead id="popHead"><tr><th>Customer</th><th>Hours</th></tr></thead>
      <tbody id="popBody"></tbody>
    </table>
  </div>
</div>

<script>
// ---- LIVE DATA ----
const projects = __PROJECTS__;
const potentialProjects = __POTENTIAL__;
const projectsActual = __ACTUAL__;
const departmentCapacities = __DEPTS__;

let PRODUCTIVITY_FACTOR = 0.85;
let HOURS_PER_FTE = 40;

// -------------------- Helpers --------------------
function parseDateLocalISO(s){ if(!s) return new Date(NaN); const t=String(s).split('T')[0]; const [y,m,d]=t.split('-').map(Number); return new Date(y,(m||1)-1,d||1); }
function ymd(d){ return [d.getFullYear(), String(d.getMonth()+1).padStart(2,'0'), String(d.getDate()).padStart(2,'0')].join('-'); }
function mondayOf(d){ const t=new Date(d.getFullYear(), d.getMonth(), d.getDate()); const day=(t.getDay()+6)%7; t.setDate(t.getDate()-day); return t; }
function firstOfMonth(d){ return new Date(d.getFullYear(), d.getMonth(), 1); }
function lastOfMonth(d){ return new Date(d.getFullYear(), d.getMonth()+1, 0); }
function isWorkday(d){ const day = d.getDay(); return day >= 1 && day <= 5; }
function workdaysInclusive(a,b){ const start=new Date(a.getFullYear(), a.getMonth(), a.getDate()); const end=new Date(b.getFullYear(), b.getMonth(), b.getDate()); let c=0, d=new Date(start); while(d<=end){ if(isWorkday(d)) c++; d.setDate(d.getDate()+1);} return c; }
function workdaysInMonth(d){ return workdaysInclusive(firstOfMonth(d), lastOfMonth(d)); }
function projectLabel(p){ return `${p.number || '—'} — ${p.customer || 'Unknown'}`; }

// -------------------- Labels --------------------
function getWeekList(){ let minD=null,maxD=null; function exp(arr){ for(const p of arr){ const a=parseDateLocalISO(p.induction), b=parseDateLocalISO(p.delivery); if(!minD||a<minD)minD=a; if(!maxD||b>maxD)maxD=b; } } if(projects.length)exp(projects); if(potentialProjects.length)exp(potentialProjects); if(projectsActual.length)exp(projectsActual); if(!minD||!maxD){ const start=mondayOf(new Date()); return [ymd(start)]; } const start=mondayOf(minD); const weeks=[]; const cur=new Date(start); while(cur<=maxD){ weeks.push(new Date(cur)); cur.setDate(cur.getDate()+7);} return weeks.map(ymd); }
function getMonthList(){ let minD=null,maxD=null; function exp(arr){ for(const p of arr){ const a=parseDateLocalISO(p.induction), b=parseDateLocalISO(p.delivery); if(!minD||a<minD)minD=a; if(!maxD||b>maxD)maxD=b; } } if(projects.length)exp(projects); if(potentialProjects.length)exp(potentialProjects); if(projectsActual.length)exp(projectsActual); if(!minD||!maxD){ const start=firstOfMonth(new Date()); return [ymd(start)]; } const start=firstOfMonth(minD); const end=firstOfMonth(maxD); const months=[]; const cur=new Date(start); while(cur<=end){ months.push(new Date(cur)); cur.setMonth(cur.getMonth()+1); } return months.map(ymd); }

// -------------------- Weekly/Monthly Loads --------------------
function computeWeeklyLoadsDetailed(arr, key, labels){
  const total=new Array(labels.length).fill(0); const breakdown=labels.map(()=>[]);
  for(const p of arr){
    const hrs=p[key]||0; if(!hrs) continue;
    const a=parseDateLocalISO(p.induction), b=parseDateLocalISO(p.delivery);
    let s=-1,e=-1; for(let i=0;i<labels.length;i++){ const L=parseDateLocalISO(labels[i]); if(L>=a && s===-1) s=i; if(L<=b) e=i; }
    if(s!==-1 && e!==-1 && e>=s){
      const n=e-s+1, per=hrs/n;
      for(let w=s; w<=e; w++){ total[w]+=per; breakdown[w].push({customer:(p.customer||"Unknown"), label:projectLabel(p), hours:per}); }
    }
  }
  return {series:total, breakdown};
}
function computeWeeklyLoadsActual(arr, key, labels){
  const total=new Array(labels.length).fill(0); const breakdown=labels.map(()=>[]);
  const today=new Date();
  for(const p of arr){
    const hrs=p[key]||0; if(!hrs) continue;
    const a=parseDateLocalISO(p.induction), planned=parseDateLocalISO(p.delivery);
    const end = (a>today) ? planned : (planned<today? planned : today);
    if(end<a) continue;
    let s=-1,e=-1; for(let i=0;i<labels.length;i++){ const L=parseDateLocalISO(labels[i]); if(L>=a && s===-1) s=i; if(L<=end) e=i; }
    if(s!==-1 && e!==-1 && e>=s){
      const n=e-s+1, per=hrs/n;
      for(let w=s; w<=e; w++){ total[w]+=per; breakdown[w].push({customer:(p.customer||"Unknown"), label:projectLabel(p), hours:per}); }
    }
  }
  return {series:total, breakdown};
}
function computeMonthlyLoadsDetailed(arr, key, monthLabels){
  const total=new Array(monthLabels.length).fill(0); const breakdown=monthLabels.map(()=>[]);
  for(const p of arr){
    const hrs=p[key]||0; if(!hrs) continue;
    const start=parseDateLocalISO(p.induction), end=parseDateLocalISO(p.delivery);
    const projWD = Math.max(1, workdaysInclusive(start, end));
    for(let i=0;i<monthLabels.length;i++){
      const mStart = parseDateLocalISO(monthLabels[i]);
      const mEnd   = lastOfMonth(mStart);
      const overlapStart = new Date(Math.max(mStart, start));
      const overlapEnd   = new Date(Math.min(mEnd, end));
      if(overlapEnd >= overlapStart){
        const overlapWD = workdaysInclusive(overlapStart, overlapEnd);
        const hoursMonth = hrs * (overlapWD / projWD);
        total[i] += hoursMonth;
        breakdown[i].push({customer:(p.customer||"Unknown"), label:projectLabel(p), hours:hoursMonth});
      }
    }
  }
  return {series:total, breakdown};
}
function computeMonthlyLoadsActual(arr, key, monthLabels){
  const total=new Array(monthLabels.length).fill(0); const breakdown=monthLabels.map(()=>[]);
  const today=new Date();
  for(const p of arr){
    const hrs=p[key]||0; if(!hrs) continue;
    const a=parseDateLocalISO(p.induction), planned=parseDateLocalISO(p.delivery);
    const end = (a>today) ? planned : (planned<today? planned : today);
    if(end<a) continue;
    const projWD = Math.max(1, workdaysInclusive(a, end));
    for(let i=0;i<monthLabels.length;i++){
      const mStart = parseDateLocalISO(monthLabels[i]);
      const mEnd   = lastOfMonth(mStart);
      const overlapStart = new Date(Math.max(mStart, a));
      const overlapEnd   = new Date(Math.min(mEnd, end));
      if(overlapEnd >= overlapStart){
        const overlapWD = workdaysInclusive(overlapStart, overlapEnd);
        const hoursMonth = hrs * (overlapWD / projWD);
        total[i] += hoursMonth;
        breakdown[i].push({customer:(p.customer||"Unknown"), label:projectLabel(p), hours:hoursMonth});
      }
    }
  }
  return {series:total, breakdown};
}

// -------------------- Data Maps --------------------
const weekLabels = getWeekList();
const monthLabels = getMonthList();

const dataWConfirmed = {}, dataWPotential = {}, dataWActual = {};
const dataMConfirmed = {}, dataMPotential = {}, dataMActual = {};

departmentCapacities.forEach(d=>{
  const cw=computeWeeklyLoadsDetailed(projects, d.key, weekLabels);
  const pw=computeWeeklyLoadsDetailed(potentialProjects, d.key, weekLabels);
  const aw=computeWeeklyLoadsActual(projectsActual, d.key, weekLabels);
  dataWConfirmed[d.key]={name:d.name, series:cw.series, breakdown:cw.breakdown};
  dataWPotential[d.key]={name:d.name, series:pw.series, breakdown:pw.breakdown};
  dataWActual[d.key]   ={name:d.name, series:aw.series, breakdown:aw.breakdown};

  const cm=computeMonthlyLoadsDetailed(projects, d.key, monthLabels);
  const pm=computeMonthlyLoadsDetailed(potentialProjects, d.key, monthLabels);
  const am=computeMonthlyLoadsActual(projectsActual, d.key, monthLabels);
  dataMConfirmed[d.key]={name:d.name, series:cm.series, breakdown:cm.breakdown};
  dataMPotential[d.key]={name:d.name, series:pm.series, breakdown:pm.breakdown};
  dataMActual[d.key]   ={name:d.name, series:am.series, breakdown:am.breakdown};
});

// -------------------- UI Elm refs --------------------
const sel = document.getElementById('disciplineSelect');
departmentCapacities.forEach(d=>{ const o=document.createElement('option'); o.value=d.key; o.textContent=d.name; sel.appendChild(o); });
sel.value=departmentCapacities[0]?.key || "";

const prodSlider = document.getElementById('prodFactor');
const prodVal = document.getElementById('prodVal');
const hoursInput = document.getElementById('hoursPerFTE');
const chkPot = document.getElementById('showPotential');
const chkAct = document.getElementById('showActual');
const periodSel = document.getElementById('periodSel');
const utilSepChk = document.getElementById('utilSeparate');

// -------------------- Capacity & Util --------------------
function capacityArray(key, labels, period){
  const dept = departmentCapacities.find(x=>x.key===key);
  const capPerWeek = (dept?.headcount || 0) * HOURS_PER_FTE * PRODUCTIVITY_FACTOR;
  if(period==='weekly') return labels.map(()=>capPerWeek);
  return labels.map(lbl=>{
    const d = parseDateLocalISO(lbl);
    const wd = workdaysInMonth(d);
    return (capPerWeek / 5) * wd;
  });
}
function utilizationArray(period, key, includePotential){
  const mapC = (period==='weekly') ? dataWConfirmed : dataMConfirmed;
  const mapP = (period==='weekly') ? dataWPotential : dataMPotential;
  const labels = (period==='weekly') ? weekLabels : monthLabels;
  const conf = mapC[key]?.series || [];
  const pot  = mapP[key]?.series || [];
  const cap  = capacityArray(key, labels, period);
  return conf.map((v,i)=>{ const load = includePotential ? v + (pot[i]||0) : v; return cap[i] ? (100*load/cap[i]) : 0; });
}

const weekTodayLabel = ymd(mondayOf(new Date()));
const monthTodayLabel = ymd(firstOfMonth(new Date()));
const annos = { annotations:{ todayLine:{ type:'line', xMin: weekTodayLabel, xMax: weekTodayLabel,
  borderColor:'#9ca3af', borderWidth:1, borderDash:[4,4],
  label:{ display:true, content:'Today', position:'start', color:'#6b7280', backgroundColor:'rgba(255,255,255,0.8)' } } } };

// -------------------- Main Chart --------------------
const ctx = document.getElementById('myChart').getContext('2d');
let currentKey = sel.value;
let currentPeriod = 'weekly';
let showPotential = false; // default OFF
let showActual = false;
let utilSeparate = true;
let utilChart = null;

function currentLabels(){ return currentPeriod==='weekly' ? weekLabels : monthLabels; }
function dataMap(kind){
  if(currentPeriod==='weekly'){
    return kind==='c'?dataWConfirmed:kind==='p'?dataWPotential:dataWActual;
  } else {
    return kind==='c'?dataMConfirmed:kind==='p'?dataMPotential:dataMActual;
  }
}

let chart = new Chart(ctx,{
  type:'line',
  data:{
    labels: currentLabels(),
    datasets:[
      { label: ((dataMap('c')[currentKey]?.name)||'Dept') + ' Load (hrs)', data: (dataMap('c')[currentKey]?.series)||[],
        borderColor: getComputedStyle(document.documentElement).getPropertyValue('--brand').trim(),
        backgroundColor: getComputedStyle(document.documentElement).getPropertyValue('--brand-20').trim(),
        borderWidth:2, fill:true, tension:0.1, pointRadius:0 },
      { label: ((dataMap('c')[currentKey]?.name)||'Dept') + ' Capacity (hrs)', data: capacityArray(currentKey, currentLabels(), currentPeriod),
        borderColor: getComputedStyle(document.documentElement).getPropertyValue('--capacity').trim(),
        backgroundColor: getComputedStyle(document.documentElement).getPropertyValue('--capacity-20').trim(),
        borderWidth:2, fill:false, borderDash:[6,6], tension:0.1, pointRadius:0 },
      { label: ((dataMap('c')[currentKey]?.name)||'Dept') + ' Potential (hrs)', data: (dataMap('p')[currentKey]?.series)||[],
        borderColor: getComputedStyle(document.documentElement).getPropertyValue('--potential').trim(),
        backgroundColor: getComputedStyle(document.documentElement).getPropertyValue('--potential-20').trim(),
        borderWidth:2, fill:true, tension:0.1, pointRadius:0, hidden: !showPotential },
      { label: ((dataMap('c')[currentKey]?.name)||'Dept') + ' Actual (hrs)', data: (dataMap('a')[currentKey]?.series)||[],
        borderColor: getComputedStyle(document.documentElement).getPropertyValue('--actual').trim(),
        backgroundColor: getComputedStyle(document.documentElement).getPropertyValue('--actual-20').trim(),
        borderWidth:2, fill:true, tension:0.1, pointRadius:0, hidden: !showActual },
      { label: 'Utilization %', data: utilizationArray(currentPeriod, currentKey, showPotential),
        borderColor:'#374151', backgroundColor:'rgba(55,65,81,0.12)',
        yAxisID:'y2', borderWidth:1.5, fill:false, tension:0.1, pointRadius:0 }
    ]
  },
  options:{
    responsive:true, maintainAspectRatio:false,
    interaction:{ mode:'index', intersect:false },
    scales:{
      x:{ title:{display:true, text:'Week Starting'} },
      y:{ title:{display:true, text:'Hours'}, beginAtZero:true },
      y2:{ title:{display:true, text:'Utilization %'}, beginAtZero:true, position:'right', grid:{ drawOnChartArea:false }, suggestedMax:150 }
    },
    plugins:{
      annotation: annos,
      legend:{ position:'top' },
      title:{ display:true, text: 'Weekly Load vs. Capacity - ' + ((dataMap('c')[currentKey]?.name)||'Dept') }
    },
    onClick:(evt, elems)=>{
      if(!elems||!elems.length) return;
      const {datasetIndex, index:idx} = elems[0];
      if(datasetIndex===1 || datasetIndex===4) return;

      const labels = currentLabels();
      const name = (dataMap('c')[currentKey]?.name)||'Dept';
      const isMonthly = currentPeriod==='monthly';

      const mapC = dataMap('c')[currentKey]?.breakdown || [];
      const mapP = dataMap('p')[currentKey]?.breakdown || [];
      const mapA = dataMap('a')[currentKey]?.breakdown || [];

      const includePot = document.getElementById('showPotential').checked;

      let title='', rows=null, combined=false;
      if(datasetIndex===0){
        const bc = mapC[idx] || [];
        const bp = includePot ? (mapP[idx] || []) : [];
        if(includePot && bp.length){
          rows = mergeConfirmedPotential(bc, bp); combined=true;
          title = `${labels[idx]} · ${name} · ${isMonthly?'Monthly':'Weekly'} · Confirmed + Potential`;
        } else {
          rows = bc;
          title = `${labels[idx]} · ${name} · ${isMonthly?'Confirmed (mo, workdays)':'Confirmed (wk)'}`;
        }
      } else if(datasetIndex===2){
        rows = mapP[idx] || [];
        title = `${labels[idx]} · ${name} · ${isMonthly?'Potential (mo, workdays)':'Potential (wk)'}`;
      } else if(datasetIndex===3){
        rows = mapA[idx] || [];
        title = `${labels[idx]} · ${name} · ${isMonthly?'Actual (mo, workdays)':'Actual (wk)'}`;
      } else { return; }

      const native = evt?.native || evt?.nativeEvent || evt;
      const cx = (native?.clientX ?? 200);
      const cy = (native?.clientY ?? 200);
      if(combined) openPopoverCombined(title, rows, cx, cy);
      else openPopoverSingle(title, rows, cx, cy);
    }
  }
});

function createUtilChart(){
  const ctx2 = document.getElementById('utilChart').getContext('2d');
  const todayX = (currentPeriod==='weekly') ? weekTodayLabel : monthTodayLabel;
  utilChart = new Chart(ctx2, {
    type: 'line',
    data: {
      labels: currentLabels(),
      datasets: [{
        label: 'Utilization %',
        data: utilizationArray(currentPeriod, currentKey, showPotential),
        borderColor: '#111827',
        backgroundColor: 'rgba(17,24,39,0.10)',
        borderWidth: 2,
        pointRadius: 0,
        fill: false,
        tension: (currentPeriod==='monthly') ? 0 : 0.1,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      scales: {
        x: { title: { display: true, text: currentPeriod==='weekly' ? 'Week Starting' : 'Month Starting' } },
        y: { title: { display: true, text: 'Utilization %' }, beginAtZero: true, suggestedMax: 160, ticks: { callback: (v)=>`${v}%` } },
      },
      plugins: {
        legend: { display: false },
        title: { display: true, text: 'Utilization %' },
        annotation: {
          annotations: {
            todayLine: { type:'line', xMin: todayX, xMax: todayX, borderColor:'#9ca3af', borderWidth:1, borderDash:[4,4],
              label:{ display:true, content:'Today', position:'start', color:'#6b7280', backgroundColor:'rgba(255,255,255,0.8)' } },
            target100: { type:'line', yMin:100, yMax:100, borderColor:getComputedStyle(document.documentElement).getPropertyValue('--capacity').trim(),
              borderWidth:2, borderDash:[6,3],
              label:{ display:true, content:'100% target', position:'end', backgroundColor:'rgba(255,255,255,0.9)',
                color:getComputedStyle(document.documentElement).getPropertyValue('--capacity').trim() } }
          }
        }
      }
    }
  });
}
function rebuildUtilChart(){
  const wrap = document.querySelector('.chart-wrap.util');
  if (utilSeparate) {
    wrap.style.display = 'block';
    if (utilChart) { utilChart.destroy(); utilChart = null; }
    createUtilChart();
    chart.data.datasets[4].hidden = true;
  } else {
    wrap.style.display = 'none';
    if (utilChart) { utilChart.destroy(); utilChart = null; }
    chart.data.datasets[4].hidden = false;
  }
  chart.update();
}

// -------------------- KPIs & refresh --------------------
function updateKPIs(){
  const labels = currentLabels();
  const capArr = capacityArray(currentKey, labels, currentPeriod);
  const conf = (dataMap('c')[currentKey]?.series)||[];
  const pot  = (dataMap('p')[currentKey]?.series)||[];
  const combined = conf.map((v,i)=> v + (showPotential ? (pot[i]||0) : 0));
  let peak=0, peakIdx=0, worstDiff=-Infinity, worstIdx=0;
  for(let i=0;i<combined.length;i++){
    const u = capArr[i] ? (combined[i]/capArr[i]*100) : 0;
    if(u>peak){ peak=u; peakIdx=i; }
    const diff = combined[i] - capArr[i];
    if(diff>worstDiff){ worstDiff=diff; worstIdx=i; }
  }
  document.getElementById('peakUtil').textContent = combined.length? `${peak.toFixed(0)}% (${currentPeriod==='weekly'?'wk':'mo'} ${labels[peakIdx]})` : '—';
  const status = worstDiff>=0 ? `+${isFinite(worstDiff)?worstDiff.toFixed(0):0} hrs over` : `${isFinite(worstDiff)?(-worstDiff).toFixed(0):0} hrs under`;
  document.getElementById('worstWeek').textContent = combined.length? `${labels[worstIdx]} · ${status}` : '—';
  const capUnit = currentPeriod==='weekly' ? `${capArr[0]?.toFixed(0)||0} hrs / wk` : `~${(capArr[0]||0).toFixed(0)} hrs / mo (workdays)`;
  document.getElementById('weeklyCap').textContent = capUnit;
}
function refreshDatasets(){
  const labels = currentLabels();
  chart.data.labels = labels;

  const deptName = (dataMap('c')[currentKey]?.name)||'Dept';
  chart.data.datasets[0].label = `${deptName} Load (hrs)`;
  chart.data.datasets[0].data  = (dataMap('c')[currentKey]?.series)||[];
  chart.data.datasets[1].label = `${deptName} Capacity (hrs)`;
  chart.data.datasets[1].data  = capacityArray(currentKey, labels, currentPeriod);
  chart.data.datasets[2].label = `${deptName} Potential (hrs)`;
  chart.data.datasets[2].data  = (dataMap('p')[currentKey]?.series)||[];
  chart.data.datasets[3].label = `${deptName} Actual (hrs)`;
  chart.data.datasets[3].data  = (dataMap('a')[currentKey]?.series)||[];
  chart.data.datasets[2].hidden = !showPotential;
  chart.data.datasets[3].hidden = !showActual;

  chart.data.datasets[4].data = utilizationArray(currentPeriod, currentKey, showPotential);

  const monthly = currentPeriod==='monthly';
  chart.data.datasets.forEach((ds, i)=>{ ds.tension = monthly ? 0 : 0.1; if(i===1){ ds.stepped = monthly ? true : false; } });
  chart.options.scales.x.title.text = monthly ? 'Month Starting' : 'Week Starting';
  chart.options.plugins.title.text = (monthly ? 'Monthly (workdays)' : 'Weekly') + ' Load vs. Capacity - ' + deptName;

  chart.options.plugins.annotation.annotations.todayLine.xMin = monthly ? monthTodayLabel : weekTodayLabel;
  chart.options.plugins.annotation.annotations.todayLine.xMax = monthly ? monthTodayLabel : weekTodayLabel;

  chart.update();
  updateKPIs();

  if (utilChart) {
    utilChart.data.labels = currentLabels();
    utilChart.data.datasets[0].data = utilizationArray(currentPeriod, currentKey, showPotential);
    utilChart.options.scales.x.title.text = (currentPeriod==='weekly' ? 'Week Starting' : 'Month Starting');
    const todayX = (currentPeriod==='weekly') ? weekTodayLabel : monthTodayLabel;
    utilChart.options.plugins.annotation.annotations.todayLine.xMin = todayX;
    utilChart.options.plugins.annotation.annotations.todayLine.xMax = todayX;
    utilChart.data.datasets[0].tension = (currentPeriod==='monthly') ? 0 : 0.1;
    utilChart.update();
  }

  // keep snapshot date inputs in range whenever labels change
  syncSnapshotRangeToLabels();
  rebuildSnapshot();
  rebuildPlanner();
}

// -------------------- Popover --------------------
const pop = document.getElementById('drillPopover');
const popTitle = document.getElementById('popTitle');
const popHead = document.getElementById('popHead');
const popBody = document.getElementById('popBody');
document.getElementById('closePop').addEventListener('click', ()=>{ pop.style.display='none'; });
function placePopoverAt(x, y){ pop.style.display='block'; const rect=pop.getBoundingClientRect(); const pad=12; const vw=innerWidth; const vh=innerHeight; let left=x+14, top=y-10; if(left+rect.width+pad>vw) left=vw-rect.width-pad; if(top+rect.height+pad>vh) top=vh-rect.height-pad; if(top<pad) top=pad; if(left<pad) left=pad; pop.style.left=left+"px"; pop.style.top=top+"px"; }
function openPopoverSingle(title, rows, x, y){ popTitle.textContent=title; popHead.innerHTML="<tr><th>Customer</th><th>Hours</th></tr>"; popBody.innerHTML=(rows&&rows.length)? rows.map(r=>`<tr><td>${r.customer}</td><td>${r.hours.toFixed(1)}</td></tr>`).join(''):`<tr><td colspan="2">No data</td></tr>`; placePopoverAt(x,y); }
function mergeConfirmedPotential(bc, bp){ const map=new Map(); (bc||[]).forEach(r=>{ map.set(r.customer, {customer:r.customer, conf:(r.hours||0), pot:0}); }); (bp||[]).forEach(r=>{ if(map.has(r.customer)){ map.get(r.customer).pot += (r.hours||0);} else { map.set(r.customer, {customer:r.customer, conf:0, pot:(r.hours||0)});} }); const rows=Array.from(map.values()).map(x=>({...x,total:(x.conf+x.pot)})); rows.sort((a,b)=>b.total-a.total); return rows; }
function openPopoverCombined(title, rows, x, y){ popTitle.textContent=title; popHead.innerHTML="<tr><th>Customer</th><th>Confirmed</th><th>Potential</th><th>Total</th></tr>"; popBody.innerHTML=(rows&&rows.length)? rows.map(r=>`<tr><td>${r.customer}</td><td>${r.conf.toFixed(1)}</td><td>${r.pot.toFixed(1)}</td><td>${r.total.toFixed(1)}</td></tr>`).join(''):`<tr><td colspan="4">No data</td></tr>`; placePopoverAt(x,y); }

// ------- WHAT-IF -------
const impactSource = document.getElementById('impactSource');
const impactProjectSel = document.getElementById('impactProject');
const impactProjWrap = document.getElementById('impactProjWrap');
const impactMult = document.getElementById('impactMult');
const impactLead = document.getElementById('impactLead');
const impactOT = document.getElementById('impactOT');
const impactTarget = document.getElementById('impactTarget');
const impactInd = document.getElementById('impactInd');
const impactDel = document.getElementById('impactDel');
const impactRun = document.getElementById('impactRun');
const impactClear = document.getElementById('impactClear');
const impactResult = document.getElementById('impactResult');

const manualPanel = document.getElementById('manualPanel');
const manualHours = document.getElementById('manualHours');

function fmtDateInput(d){ const y=d.getFullYear(); const m=String(d.getMonth()+1).padStart(2,'0'); const da=String(d.getDate()).padStart(2,'0'); return `${y}-${m}-${da}`; }
function addWorkdays(d,n){ const t=new Date(d.getFullYear(), d.getMonth(), d.getDate()); let left=Math.max(0,Math.floor(n)); while(left>0){ t.setDate(t.getDate()+1); const dow=t.getDay(); if(dow>=1 && dow<=5) left--; } return t; }
function maxDate(a,b){ return (a>b)?a:b; }
function impactSourceProjects(){
  const src=impactSource.value;
  if(src==='manual'){
    const m={ number:document.getElementById('m_number').value||'P-Manual', customer:document.getElementById('m_customer').value||'Manual', aircraftModel:document.getElementById('m_aircraft').value||'', scope:document.getElementById('m_scope').value||'What-If', induction:document.getElementById('m_ind').value||fmtDateInput(new Date()), delivery:document.getElementById('m_del').value||fmtDateInput(addWorkdays(new Date(),10)) };
    departmentCapacities.forEach(d=>{ const v=parseFloat(document.getElementById('mh_'+d.key).value||'0')||0; m[d.key]=v; });
    return [m];
  }
  return (src==='potential')?potentialProjects:projects;
}
function setImpactProjects(){
  const src=impactSource.value;
  if(src==='manual'){
    impactProjWrap.style.display='none';
    manualPanel.style.display='block';
  } else {
    impactProjWrap.style.display='block';
    manualPanel.style.display='none';
    const arr=impactSourceProjects();
    impactProjectSel.innerHTML="";
    arr.forEach((p,i)=>{ const opt=document.createElement('option'); opt.value=String(i); opt.textContent=`${p.number||'—'} — ${p.customer||'Unknown'}`; impactProjectSel.appendChild(opt); });
    if(arr.length){ const p=arr[0]; if(p?.induction) impactInd.value=String(p.induction).slice(0,10); if(p?.delivery) impactDel.value=String(p.delivery).slice(0,10); } else { impactInd.value=""; impactDel.value=""; }
  }
}
impactSource.addEventListener('change', setImpactProjects);
(function initManualHours(){ let html=""; departmentCapacities.forEach(d=>{ html += `<label>${d.name} hours<input id="mh_${d.key}" type="number" step="1" value="0"></label>`; }); manualHours.innerHTML=html; document.getElementById('m_ind').value=fmtDateInput(new Date()); document.getElementById('m_del').value=fmtDateInput(addWorkdays(new Date(),10)); })();
impactProjectSel.addEventListener('change', ()=>{ const arr=impactSourceProjects(); const p=arr[Number(impactProjectSel.value)||0]; if(!p) return; if(p?.induction) impactInd.value=String(p.induction).slice(0,10); if(p?.delivery) impactDel.value=String(p.delivery).slice(0,10); });
setImpactProjects();

function capPerDay(key, otPct){ const dept=departmentCapacities.find(x=>x.key===key); const perWeek=(dept?.headcount||0)*HOURS_PER_FTE*PRODUCTIVITY_FACTOR; const uplift=1+Math.max(0,(parseFloat(otPct)||0))/100; return (perWeek*uplift)/5.0; }
function baselineSeries(period, key){ const mapC=(period==='weekly')?dataWConfirmed:dataMConfirmed; return (mapC[key]?.series||[]).slice(); }
function periodRange(period, labels, start, end){
  let s=-1, e=-1;
  for(let i=0;i<labels.length;i++){
    const L=parseDateLocalISO(labels[i]);
    const Pstart=(period==='weekly')?mondayOf(L):firstOfMonth(L);
    const Pend=(period==='weekly')?new Date(Pstart.getFullYear(), Pstart.getMonth(), Pstart.getDate()+6):lastOfMonth(L);
    if(s===-1 && Pend>=start) s=i;
    if(Pstart<=end) e=i;
  }
  if(s===-1 || e===-1 || e<s) return null; return {s,e};
}
function sumHeadroom(period, key, start, end, otPct){
  const labels=(period==='weekly')?weekLabels:monthLabels; const cap=capacityArray(key, labels, period); const base=baselineSeries(period, key); const uplift=1+Math.max(0,(parseFloat(otPct)||0))/100; const rng=periodRange(period, labels, start, end); if(!rng) return 0; let sum=0; for(let i=rng.s;i<=rng.e;i++){ const hr=Math.max(0, cap[i]*uplift - (base[i]||0)); sum += hr; } return sum;
}
function renderImpactResult(obj){
  const {earliestStart, targetStart, targetEnd, newEnd, slipDays, rows} = obj;
  const dfmt=d=>{ const y=d.getFullYear(), m=String(d.getMonth()+1).padStart(2,'0'), da=String(d.getDate()).padStart(2,'0'); return `${y}-${m}-${da}`; };
  let html = `
    <div><strong>Earliest allowable induction:</strong> ${dfmt(earliestStart)}</div>
    <div><strong>Requested induction:</strong> ${dfmt(targetStart)}</div>
    <div><strong>Requested delivery:</strong> ${dfmt(targetEnd)}</div>
    <div><strong>New delivery (what-if):</strong> ${dfmt(newEnd)} <em>${slipDays>0?`(+${slipDays} workdays)`:''}</em></div>
    <table class="impact-table">
      <thead><tr><th>Department</th><th>Proj Hours</th><th>Headroom</th><th>Shortfall</th><th>Slip (wd)</th></tr></thead>
      <tbody>
        ${rows.map(r=>`<tr>
          <td>${r.name}</td>
          <td>${r.h.toFixed(0)}</td>
          <td>${r.head.toFixed(0)}</td>
          <td style="color:${r.short>0?'#b91c1c':'#065f46'};">${r.short>0?(''+r.short.toFixed(0)):'0'}</td>
          <td><strong>${r.slip}</strong></td>
        </tr>`).join('')}
      </tbody>
    </table>
  `;
  impactResult.innerHTML = html;
  const monthly=(currentPeriod==='monthly');
  const startLbl = monthly ? ymd(firstOfMonth(earliestStart)) : ymd(mondayOf(earliestStart));
  const endLbl   = monthly ? ymd(firstOfMonth(newEnd))       : ymd(mondayOf(newEnd));
  chart.options.plugins.annotation.annotations.whatIfStart = { type:'line', xMin:startLbl, xMax:startLbl, borderColor:'#2563eb', borderWidth:2, label:{display:true, content:'What-If Start', position:'start', backgroundColor:'rgba(37,99,235,0.1)', color:'#2563eb'} };
  chart.options.plugins.annotation.annotations.whatIfEnd   = { type:'line', xMin:endLbl,   xMax:endLbl,   borderColor:'#7c3aed', borderWidth:2, label:{display:true, content:'What-If End',   position:'end',   backgroundColor:'rgba(124,58,237,0.1)', color:'#7c3aed'} };
  chart.update();
}
impactRun.addEventListener('click', ()=>{
  const arr = impactSourceProjects();
  const idx = (impactSource.value==='manual') ? 0 : (Number(impactProjectSel.value)||0);
  const proj = arr[idx]; if(!proj){ impactResult.textContent="No project selected."; return; }

  const mult = Math.max(0, parseFloat(impactMult.value||'1')||1);
  const minLead = Math.max(0, parseInt(impactLead.value||'0',10)||0);
  const otPct = Math.max(0, parseFloat(impactOT.value||'0')||0);

  const rawStart = parseDateLocalISO(impactInd.value?impactInd.value:proj.induction);
  const rawEnd   = parseDateLocalISO(impactDel.value?impactDel.value:proj.delivery);
  if(isNaN(rawStart) || isNaN(rawEnd) || rawEnd<rawStart){ impactResult.textContent="Invalid induction/delivery dates."; return; }

  const today = new Date(); const leadReady = (function addWD(d,n){ const t=new Date(d.getFullYear(), d.getMonth(), d.getDate()); let left=Math.max(0,Math.floor(n)); while(left>0){ t.setDate(t.getDate()+1); const dow=t.getDay(); if(dow>=1&&dow<=5) left--; } return t; })(today, minLead);
  const earliestStart = (rawStart>leadReady)?rawStart:leadReady;
  const targetStart = rawStart, targetEnd = rawEnd;

  const rows=[]; let overallSlip=0;
  departmentCapacities.forEach(d=>{
    const key=d.key, name=d.name; const capDay=capPerDay(key, otPct); const H=(proj[key]||0)*mult; const head=sumHeadroom(currentPeriod, key, earliestStart, targetEnd, otPct); const short=Math.max(0, H-head); const slip=(short>0 && capDay>0)?Math.ceil(short/capDay):0; overallSlip=Math.max(overallSlip, slip); rows.push({name,h:H,head,short,slip});
  });
  const newEnd = (function addWD(d,n){ const t=new Date(d.getFullYear(), d.getMonth(), d.getDate()); let left=Math.max(0,Math.floor(n)); while(left>0){ t.setDate(t.getDate()+1); const dow=t.getDay(); if(dow>=1&&dow<=5) left--; } return t; })(targetEnd, overallSlip);

  renderImpactResult({ earliestStart, targetStart, targetEnd, newEnd, slipDays: overallSlip, rows });
});
impactClear.addEventListener('click', ()=>{ impactResult.innerHTML=""; if(chart?.options?.plugins?.annotation?.annotations){ delete chart.options.plugins.annotation.annotations.whatIfStart; delete chart.options.plugins.annotation.annotations.whatIfEnd; chart.update(); } });

// -------------------- Snapshot (ECharts Sankey + Treemap, Chart.js Pareto) --------------------
let sankeyE=null, treemapE=null, paretoChart=null;
const snapConfirmed=document.getElementById('snapConfirmed');
const snapPotential=document.getElementById('snapPotential');
const snapTopN=document.getElementById('snapTopN');
const snapTopNVal=document.getElementById('snapTopNVal');
snapConfirmed.addEventListener('change', rebuildSnapshot);
snapPotential.addEventListener('change', rebuildSnapshot);
snapTopN.addEventListener('input', ()=>{ snapTopNVal.textContent=snapTopN.value; rebuildSnapshot(); });

const snapFrom = document.getElementById('snapFrom');
const snapTo   = document.getElementById('snapTo');
const snapReset = document.getElementById('snapReset');

const keyColor={ confirmed:getComputedStyle(document.documentElement).getPropertyValue('--confirmed').trim()||'#2563eb', potential:getComputedStyle(document.documentElement).getPropertyValue('--potential2').trim()||'#059669' };

// ---- snapshot date-range helpers ----
function labelsMinMaxDates(){
  const labels = currentLabels();
  if (!labels.length) return {min:null, max:null};
  const min = parseDateLocalISO(labels[0]);
  const max = parseDateLocalISO(labels[labels.length - 1]);
  return {min, max};
}
function clampDateToLabels(d){
  const {min, max} = labelsMinMaxDates();
  if (!min || !max || isNaN(d)) return d;
  if (d < min) return min;
  if (d > max) return max;
  return d;
}
function syncSnapshotRangeToLabels({force=false} = {}){
  const {min, max} = labelsMinMaxDates();
  if (!min || !max) return;
  if (force || !snapFrom.value) snapFrom.value = fmtDateInput(min);
  if (force || !snapTo.value)   snapTo.value   = fmtDateInput(max);
  snapFrom.value = fmtDateInput(clampDateToLabels(parseDateLocalISO(snapFrom.value)));
  snapTo.value   = fmtDateInput(clampDateToLabels(parseDateLocalISO(snapTo.value)));
}
snapFrom.addEventListener('change', ()=>{
  const f = parseDateLocalISO(snapFrom.value);
  const t = parseDateLocalISO(snapTo.value);
  if (t && f > t) snapTo.value = snapFrom.value;
  rebuildSnapshot();
});
snapTo.addEventListener('change', ()=>{
  const f = parseDateLocalISO(snapFrom.value);
  const t = parseDateLocalISO(snapTo.value);
  if (f && t < f) snapFrom.value = snapTo.value;
  rebuildSnapshot();
});
snapReset.addEventListener('click', ()=>{ syncSnapshotRangeToLabels({force:true}); rebuildSnapshot(); });
syncSnapshotRangeToLabels({force:true});

function gatherSnapshotBreakdown(){
  const includeC = snapConfirmed.checked, includeP = snapPotential.checked;
  const mapC = dataMap('c')[currentKey]?.breakdown || [];
  const mapP = dataMap('p')[currentKey]?.breakdown || [];

  const totalByProj = new Map();
  const byStatus = [];

  const from = parseDateLocalISO(snapFrom.value);
  const to   = parseDateLocalISO(snapTo.value);
  const labels = currentLabels();

  function periodBoundsForIndex(i){
    const L = parseDateLocalISO(labels[i]);
    const start = (currentPeriod==='weekly') ? mondayOf(L) : firstOfMonth(L);
    const end   = (currentPeriod==='weekly')
      ? new Date(start.getFullYear(), start.getMonth(), start.getDate()+6)
      : lastOfMonth(L);
    return { start, end };
  }
  function periodWorkdays(start, end){
    return workdaysInclusive(start, end); // uses your existing Mon–Fri counter
  }
  function fractionOfPeriodSelected(i){
    if (!from || !to || isNaN(from) || isNaN(to)) return 1;
    const { start, end } = periodBoundsForIndex(i);
    if (end < from || start > to) return 0;
    const ovStart = new Date(Math.max(start, from));
    const ovEnd   = new Date(Math.min(end, to));
    const denom = periodWorkdays(start, end);
    const numer = periodWorkdays(ovStart, ovEnd);
    return denom > 0 ? Math.min(1, Math.max(0, numer/denom)) : 0;
  }
  function addSet(breakArr, status){
    for (let i = 0; i < breakArr.length; i++){
      const frac = fractionOfPeriodSelected(i);
      if (frac <= 0) continue;
      const rows = breakArr[i] || [];
      rows.forEach(r=>{
        const key = r.label || r.customer;
        const v = (r.hours || 0) * frac;   // prorate by overlap fraction
        totalByProj.set(key, (totalByProj.get(key) || 0) + v);
        byStatus.push({ label:key, status, hours:v });
      });
    }
  }

  if (includeC) addSet(mapC, 'confirmed');
  if (includeP) addSet(mapP, 'potential');

  const aggStatus = new Map();
  byStatus.forEach(x=>{
    const k = x.label + '|' + x.status;
    aggStatus.set(k, (aggStatus.get(k)||0) + (x.hours||0));
  });
  const aggStatusRows = Array.from(aggStatus.entries()).map(([k,v])=>{
    const [label,status] = k.split('|');
    return { label, status, hours:v };
  });

  const total = Array.from(totalByProj.values()).reduce((a,b)=>a+b,0);
  return { totalByProj:Object.fromEntries(totalByProj), byStatus:aggStatusRows, total };
}

function rebuildSnapshot(){
  const sankeyDiv=document.getElementById('sankeyDiv');
  const treemapDiv=document.getElementById('treemapDiv');
  const paretoCtx=document.getElementById('paretoCanvas').getContext('2d');

  if(sankeyE){ sankeyE.dispose(); sankeyE=null; }
  if(treemapE){ treemapE.dispose(); treemapE=null; }
  if(paretoChart){ paretoChart.destroy(); paretoChart=null; }

  const deptName=(dataMap('c')[currentKey]?.name)||'Dept';
  const { totalByProj, byStatus, total } = gatherSnapshotBreakdown();

  // Top N grouping
  const N=parseInt(snapTopN.value||'8',10);
  const pairs=Object.entries(totalByProj).sort((a,b)=>b[1]-a[1]);
  const top=pairs.slice(0,N);
  const rest=pairs.slice(N);
  const topSet=new Set(top.map(p=>p[0]));
  const restSum=rest.reduce((a,[,v])=>a+v,0);

  // Sankey: split by status for color
  const nodesMap=new Map();
  function addNode(name, color){ if(!nodesMap.has(name)) nodesMap.set(name, {name, itemStyle:{color}}); }
  const targetNode = deptName;
  addNode(targetNode, '#6b7280');

  const links=[];
  byStatus.forEach(r=>{
    const proj = topSet.has(r.label) ? r.label : 'Other';
    if(proj==='Other' && restSum===0) return;
    const suffix = r.status==='confirmed' ? ' (C)' : ' (P)';
    const source = proj + suffix;
    addNode(source, r.status==='confirmed' ? keyColor.confirmed : keyColor.potential);
    links.push({ source, target: targetNode, value: +r.hours });
  });
  if(links.length===0){
    sankeyE = echarts.init(sankeyDiv);
    sankeyE.setOption({ title:{text:'No data', left:'center', top:'middle'} });
  } else {
    sankeyE = echarts.init(sankeyDiv);
    sankeyE.setOption({
      tooltip:{ formatter: (p)=> {
        if(p.dataType==='edge'){
          const v=p.data.value; const pct= total>0 ? (v/total*100).toFixed(1) : '0.0';
          return `${p.data.source} → ${p.data.target}<br/><b>${v.toFixed(1)} hrs</b> (${pct}%)`;
        }
        return p.name;
      }},
      series:[{
        type:'sankey',
        data:Array.from(nodesMap.values()),
        links:links,
        lineStyle:{ color:'source', curveness:0.5 },
        emphasis:{ focus:'adjacency' },
        nodeGap:14,
        nodeWidth:16,
        draggable:true,
        label:{ color:'#111827', fontSize:12 }
      }]
    });
  }

  // Treemap (group by Confirmed/Potential)
  const groups = [];
  const includeC = snapConfirmed.checked, includeP=snapPotential.checked;

  function buildChildren(status, color){
    const valByProj=new Map();
    byStatus.filter(x=>x.status===status).forEach(x=>{
      const proj = topSet.has(x.label) ? x.label : 'Other';
      valByProj.set(proj, (valByProj.get(proj)||0) + x.hours);
    });
    if(valByProj.size===0) return null;
    const children=[];
    Array.from(valByProj.entries()).forEach(([label, v])=>{
      if(label==='Other' && v<=0) return;
      children.push({ name: label, value: +v, itemStyle:{ color: color } });
    });
    return children;
  }

  const cChildren = includeC ? buildChildren('confirmed', keyColor.confirmed) : null;
  const pChildren = includeP ? buildChildren('potential', keyColor.potential) : null;

  if(cChildren && cChildren.length) groups.push({ name:'Confirmed', children:cChildren });
  if(pChildren && pChildren.length) groups.push({ name:'Potential', children:pChildren });

  if(groups.length===0){
    treemapE = echarts.init(treemapDiv);
    treemapE.setOption({ title:{text:'No data', left:'center', top:'middle'} });
  } else {
    treemapE = echarts.init(treemapDiv);
    treemapE.setOption({
      tooltip:{ formatter:(p)=> `${p.name}: <b>${(+p.value).toFixed(1)} hrs</b>${ total>0? ` (${(p.value/total*100).toFixed(1)}%)` : ''}` },
      series:[{
        type:'treemap',
        roam:false,
        nodeClick:'zoomToNode',
        breadcrumb:{ show:false },
        data: groups,
        label:{ show:true, formatter:(p)=>{
          const v=+p.value||0; const pct= total>0 ? Math.round(v/total*100) : 0;
          return `${p.name}\\n${v.toFixed(0)} hrs • ${pct}%`;
        }}
      }]
    });
  }

  // Pareto (Chart.js)
  const pairsAll=Object.entries(totalByProj).sort((a,b)=>b[1]-a[1]);
  const labels=pairsAll.map(p=>p[0]);
  const vals=pairsAll.map(p=>p[1]);
  const cum=[]; let running=0;
  vals.forEach(v=>{ running+=v; cum.push(total>0?(running/total*100):0); });
  paretoChart = new Chart(paretoCtx, {
    type:'bar',
    data:{ labels, datasets:[
      { label:'Hours', data:vals, borderWidth:0, backgroundColor:'#9CA3AF' },
      { label:'Cumulative %', data:cum, type:'line', yAxisID:'y2', borderColor:'#111827', backgroundColor:'rgba(17,24,39,0.1)', tension:0.2, pointRadius:2 }
    ]},
    options:{
      responsive:true, maintainAspectRatio:false,
      scales:{ y:{ title:{display:true, text:'Hours'}, beginAtZero:true },
               y2:{ position:'right', beginAtZero:true, suggestedMax:100, ticks:{callback:(v)=>`${v}%`}, grid:{drawOnChartArea:false} } },
      plugins:{ legend:{display:false} }
    }
  });

  // Resize handlers
  window.addEventListener('resize', ()=>{ if(sankeyE) sankeyE.resize(); if(treemapE) treemapE.resize(); });
}

// ---- Hangar Bay Planner ----

// User-pinned bay overrides (applies whenever the project overlaps that period)
// Slot options: H1, H2, D1, D2, D3
const bayOverrides = [
  { number: 'P7611', slot: 'H1' },
  { number: 'P7706', slot: 'D2' },
  { number: 'P7712', slot: 'D3' },
  // optional time-box fields: from:'YYYY-MM-DD', to:'YYYY-MM-DD'
];

function findProjectByNumber(num){
    return projects.find(p => p.number === num) ||
        potentialProjects.find(p => p.number === num) ||
        null;
}

function isOverrideActive(ovr, periodStart, periodEnd){
  if (ovr.from || ovr.to){
    const from = ovr.from ? parseDateLocalISO(ovr.from) : new Date(-8640000000000000);
    const to   = ovr.to   ? parseDateLocalISO(ovr.to)   : new Date( 8640000000000000);
    return !(to < periodStart || from > periodEnd);
  }
  const p = findProjectByNumber(ovr.number);
  if (p && p.induction && p.delivery){
    const a = parseDateLocalISO(p.induction);
    const b = parseDateLocalISO(p.delivery);
    if (!isNaN(a) && !isNaN(b)) return !(b < periodStart || a > periodEnd);
  }
  return true;
}

function slotToBay(slot, H, D){
  if (slot === 'H1') return H[0];
  if (slot === 'H2') return H[1];
  if (slot === 'D1') return D[0];
  if (slot === 'D2') return D[1];
  if (slot === 'D3') return D[2];
  return null;
}

const planIncPot = document.getElementById('planIncludePotential');
const planPeriods = document.getElementById('planPeriods');
const planFrom = document.getElementById('planFrom');
const hangarGrid = document.getElementById('hangarGrid');

function setPlannerDefaultDates(){
  const labels = currentLabels();
  if (!labels.length) return;
  const today = new Date();
  // start at the most recent label >= today, else the first label
  let start = parseDateLocalISO(labels[0]);
  for (const lbl of labels){
    const d = parseDateLocalISO(lbl);
    if (d >= today) { start = d; break; }
  }
  planFrom.value = fmtDateInput(start);
}
setPlannerDefaultDates();

function modelShort(m){
  if(!m) return '';
  const s = String(m).toUpperCase();
  if (s.startsWith('B777')) return '777';
  if (s.startsWith('B747')) return '747';
  if (s.startsWith('A340')) return 'A340';
  if (s.startsWith('A330')) return 'A330';
  if (s.startsWith('B757')) return '757';
  if (s.startsWith('B737')) return '737';
  if (s.startsWith('A319')) return 'A319';
  return s.replace('BOEING','B').replace('AIRBUS','A');
}
function classifyAircraft(model){
  const s = String(model||'').toUpperCase();
  if (!s) return null;
  if (s.startsWith('B777') || s.startsWith('B747') || s.startsWith('A340') || s.startsWith('A330')) return 'HEAVY';
  if (s.startsWith('B757')) return 'M757';
  if (s.startsWith('B737') || s.startsWith('A319')) return 'SMALL';
  return null;
}

function periodBoundsForIndex(i){
  const L = parseDateLocalISO(currentLabels()[i]);
  const start = (currentPeriod==='weekly') ? mondayOf(L) : firstOfMonth(L);
  const end   = (currentPeriod==='weekly') ? new Date(start.getFullYear(), start.getMonth(), start.getDate()+6) : lastOfMonth(L);
  return { start, end };
}
function overlaps(aStart,aEnd,bStart,bEnd){ return !(aEnd < bStart || aStart > bEnd); }

function activeProjectsForIdx(i, includePotential){
  const { start, end } = periodBoundsForIndex(i);
  const arr = [];
  function pull(list){
    for(const p of list){
      const a = parseDateLocalISO(p.induction);
      const b = parseDateLocalISO(p.delivery);
      if(isNaN(a)||isNaN(b)) continue;
      if(overlaps(a,b,start,end) && p.aircraftModel){
        const cls = classifyAircraft(p.aircraftModel);
        if(!cls) continue;
        arr.push({ number: p.number || '—', customer: p.customer || 'Unknown', model: p.aircraftModel, short: modelShort(p.aircraftModel), cls });
      }
    }
  }
  pull(projects);
  if (includePotential) pull(potentialProjects);
  return arr;
}

function assignForPeriod(aircraftList, periodIndex){
  const { start, end } = periodBoundsForIndex(periodIndex);

  const H = [{kind:'EMPTY', slots:[]}, {kind:'EMPTY', slots:[]}];
  const D = [{kind:'EMPTY', slots:[]}, {kind:'EMPTY', slots:[]}, {kind:'EMPTY', slots:[]}];
  const conflicts = [];

  const remaining = aircraftList.slice();
  for (const ovr of bayOverrides){
    if (!isOverrideActive(ovr, start, end)) continue;
    const idx = remaining.findIndex(a => a.number === ovr.number);
    if (idx === -1) continue;
    const p = remaining.splice(idx, 1)[0];
    const bay = slotToBay(ovr.slot, H, D);
    if (!bay || bay.kind !== 'EMPTY') { conflicts.push(p); continue; }
    if (p.cls === 'HEAVY' && !/^H[12]$/.test(ovr.slot)) { conflicts.push(p); continue; }
    if (p.cls === 'HEAVY') { bay.kind='HEAVY';  bay.slots=[p]; }
    else if (p.cls === 'M757'){ bay.kind='M757';  bay.slots=[p]; }
    else if (p.cls === 'SMALL'){ bay.kind='SMALL1'; bay.slots=[p]; }
    else { conflicts.push(p); }
  }

  const heavies = remaining.filter(x=>x.cls==='HEAVY');
  const m757s   = remaining.filter(x=>x.cls==='M757');
  const smalls  = remaining.filter(x=>x.cls==='SMALL');

  function takeFirstEmptyBay(cands){ for(const b of cands){ if(b.kind==='EMPTY') return b; } return null; }

  while (heavies.length){
    const p = heavies.shift();
    const bay = (H[0].kind==='EMPTY') ? H[0] : (H[1].kind==='EMPTY' ? H[1] : null);
    if(!bay){ conflicts.push(p); continue; }
    bay.kind='HEAVY'; bay.slots=[p];
  }

  while (m757s.length){
    const p = m757s.shift();
    const bay = takeFirstEmptyBay([D[1], D[0], H[0], H[1], D[2]]);
    if(!bay){ conflicts.push(p); continue; }
    bay.kind='M757'; bay.slots=[p];
  }

  let dSplitIdx = null;
  if (smalls.length >= 2){
    if (D[0].kind==='EMPTY' && D[1].kind!=='SPLIT' && D[1].kind!=='M757') dSplitIdx = 0;
    if (dSplitIdx===null && D[1].kind==='EMPTY' && D[0].kind!=='SPLIT' && D[0].kind!=='M757') dSplitIdx = 1;
    if (dSplitIdx===null && D[0].kind==='EMPTY' && D[1].kind==='EMPTY') dSplitIdx = 0;
  }

  function splitIfHelpful(bay){ if (smalls.length >= 2 && bay.kind==='EMPTY'){ bay.kind='SPLIT'; bay.slots=[]; return true; } return false; }
  splitIfHelpful(H[0]); splitIfHelpful(H[1]);
  if (dSplitIdx!==null && D[dSplitIdx].kind==='EMPTY'){ D[dSplitIdx].kind='SPLIT'; D[dSplitIdx].slots=[]; }

  for (const bay of [H[0],H[1],D[0],D[1]]){
    if (bay.kind==='SPLIT'){
      while (smalls.length && bay.slots.length<2){ bay.slots.push(smalls.shift()); }
    }
  }

  function pushSmallIntoAnySingle(p){
    for(const bay of [H[0],H[1],D[0],D[1]]){
      if (bay.kind==='SPLIT' && bay.slots.length<2){ bay.slots.push(p); return true; }
    }
    const cand = takeFirstEmptyBay([H[0],H[1],D[2],D[0],D[1]]);
    if (cand){ cand.kind='SMALL1'; cand.slots=[p]; return true; }
    return false;
  }
  while (smalls.length){ const p = smalls.shift(); if(!pushSmallIntoAnySingle(p)){ conflicts.push(p); } }

  function bayCell(bay){
    const label = (p)=> `${p.number || '—'} — ${p.customer || 'Unknown'}`;
    const tip   = (p)=> p.model || (p.short || '');
    if (bay.kind === 'EMPTY') { return { cls:'empty', text:'—', tips:[] }; }
    if (bay.kind === 'SPLIT') {
      const texts = bay.slots.map(label).join(' | ');
      const tips  = bay.slots.map(tip);
      return { cls:'occupied split', text: texts || '—', tips };
    }
    const s = bay.slots[0];
    return { cls:'occupied', text: label(s), tips:[tip(s)] };
  }

  return {
    H: [bayCell(H[0]), bayCell(H[1])],
    D: [bayCell(D[0]), bayCell(D[1]), bayCell(D[2])],
    conflicts
  };
}

function buildPlannerGrid(indices){
  const cols = indices.length + 1; // +1 for row header
  let html = `<div class="hgrid" style="grid-template-columns: 180px repeat(${indices.length}, minmax(110px,1fr));">`;

  html += `<div class="hcell header rowhdr"></div>`;
  for(const i of indices){ const lbl = currentLabels()[i]; html += `<div class="hcell header">${lbl}</div>`; }

  function row(title, getter){
    html += `<div class="hcell rowhdr">${title}</div>`;
    for(const i of indices){
      const cell = getter(i);
      const tip = (cell.tips && cell.tips.length) ? `title="${cell.tips.join(' \\n ')}"` : '';
      html += `<div class="hcell ${cell.cls}" ${tip}>${cell.text}</div>`;
    }
  }

  const includePot = planIncPot.checked;
  const assigned = indices.map(i=>{ const act = activeProjectsForIdx(i, includePot); return assignForPeriod(act, i); });

  row('Hangar H — Bay 1', (i)=> assigned[indices.indexOf(i)].H[0]);
  row('Hangar H — Bay 2', (i)=> assigned[indices.indexOf(i)].H[1]);
  row('Hangar D — Bay 1', (i)=> assigned[indices.indexOf(i)].D[0]);
  row('Hangar D — Bay 2', (i)=> assigned[indices.indexOf(i)].D[1]);
  row('Hangar D — Bay 3', (i)=> assigned[indices.indexOf(i)].D[2]);

  html += `<div class="hcell rowhdr">Conflicts</div>`;
  for (const pack of assigned){
    if (!pack.conflicts.length){ html += `<div class="hcell empty">0</div>`; }
    else { const txt = pack.conflicts.map(c=>`${c.short} (${c.number})`).join(', '); html += `<div class="hcell conflict" title="${txt}">${pack.conflicts.length}</div>`; }
  }

  html += `</div>`;
  hangarGrid.innerHTML = html;
}

function rebuildPlanner(){
  const labels = currentLabels();
  if (!labels.length){ hangarGrid.innerHTML = ''; return; }

  const startDate = parseDateLocalISO(planFrom.value) || parseDateLocalISO(labels[0]);
  const idxs = [];
  for(let i=0;i<labels.length;i++){
    const d = parseDateLocalISO(labels[i]);
    if (d>=startDate) idxs.push(i);
  }
  const N = Math.max(1, Math.min(parseInt(planPeriods.value||'12',10), idxs.length));
  buildPlannerGrid(idxs.slice(0,N));
}
planIncPot.addEventListener('change', rebuildPlanner);
planPeriods.addEventListener('change', rebuildPlanner);
planFrom.addEventListener('change', ()=>{ 
  const d = parseDateLocalISO(planFrom.value);
  const {min,max} = labelsMinMaxDates();
  let v = d;
  if (min && v<min) v=min;
  if (max && v>max) v=max;
  planFrom.value = fmtDateInput(v);
  rebuildPlanner();
});


// ---------- Event listeners ----------
sel.addEventListener('change', e=>{ currentKey=e.target.value; refreshDatasets(); });
chkPot.addEventListener('change', e=>{ showPotential=e.target.checked; refreshDatasets(); });
chkAct.addEventListener('change', e=>{ showActual=e.target.checked; refreshDatasets(); });
prodSlider.addEventListener('input', e=>{ PRODUCTIVITY_FACTOR=parseFloat(e.target.value||'0.85'); prodVal.textContent=PRODUCTIVITY_FACTOR.toFixed(2); refreshDatasets(); });
hoursInput.addEventListener('change', e=>{ const v=parseInt(e.target.value||'40',10); HOURS_PER_FTE=isNaN(v)?40:Math.min(60,Math.max(30,v)); e.target.value=HOURS_PER_FTE; refreshDatasets(); });
periodSel.addEventListener('change', e=>{ currentPeriod=e.target.value; refreshDatasets(); });
utilSepChk.addEventListener('change', e=>{ utilSeparate=e.target.checked; rebuildUtilChart(); });

// ---------- Initial render ----------
refreshDatasets();
rebuildUtilChart();
rebuildPlanner();
</script>
</body>
</html>
"""

# Inject live data and inline JS fallbacks
html_code = (
    html_template
      .replace("__PROJECTS__", json.dumps(st.session_state.projects))
      .replace("__POTENTIAL__", json.dumps(st.session_state.potential))
      .replace("__ACTUAL__", json.dumps(st.session_state.actual))
      .replace("__DEPTS__", json.dumps(st.session_state.depts))
      .replace("__CHART_JS_INLINE__", CHART_JS_INLINE)
      .replace("__CHART_ANNO_INLINE__", CHART_ANNO_INLINE)
      .replace("__ECHARTS_INLINE__", ECHARTS_INLINE)
)

components.html(html_code, height=2600, scrolling=False)
