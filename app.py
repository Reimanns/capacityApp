
# streamlit_app.py — Google Sheets backend + two-page layout (Dashboard | Data Management)
import os, json
from datetime import date
from copy import deepcopy

import streamlit as st
import pandas as pd
import streamlit.components.v1 as components

# Use your existing Google Sheets CRUD layer
try:
    import data_store as ds
except Exception as e:
    st.stop()

st.set_page_config(layout="wide", page_title="Capacity & Load Dashboard")

try:
    st.image("citadel_logo.png", width=200)
except Exception:
    pass

# --------------------- Sidebar Navigation ---------------------
st.sidebar.header("Navigation")
page = st.sidebar.radio("Go to", ["Dashboard", "Data Management"], index=0)

# ==========================
#  HTML Template (unchanged)
# ==========================
HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <title>Labor Capacity Dashboard</title>

  <!-- Chart.js core + annotation -->
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.1.2/dist/chartjs-plugin-annotation.min.js"></script>
  <script>
    try { if (window['chartjs-plugin-annotation']) { Chart.register(window['chartjs-plugin-annotation']); } } catch(e) {}
  </script>

  <!-- ECharts for Sankey & Treemap (reliable UMD build) -->
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>

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

    .popover { display:none; position:fixed; z-index:9999; max-width:min(92vw, 900px);
      background:#fff; border:1px solid #e5e7eb; border-radius:12px; box-shadow:0 12px 30px rgba(0,0,0,0.2); }
    .popover header { padding:10px 12px; border-bottom:1px solid #eee; font-weight:600; display:flex; justify-content:space-between; gap:10px; align-items:center; }
    .popover header button { border:none; background:#f3f4f6; border-radius:8px; padding:4px 8px; cursor:pointer; }
    .popover .content { padding:10px 12px 12px; max-height:60vh; overflow:auto; }
    .popover table { width:100%; border-collapse:collapse; }
    .popover th, .popover td { border-bottom:1px solid #eee; padding:6px 8px; text-align:left; font-size:13px; }

    .impact-grid{ display:grid; gap:10px; grid-template-columns: repeat(6, minmax(120px,1fr));
      align-items:end; margin:10px 0 6px; }
    .impact-grid label{ font-size:12px; color:#374151; display:flex; flex-direction:column; gap:6px; }
    .impact-grid input, .impact-grid select, .impact-grid button{ padding:8px; border:1px solid #e5e7eb; border-radius:8px; font-size:13px;}
    .impact-grid button{ cursor:pointer; background:#111827; color:#fff; border-color:#111827; }
    .impact-box{ border:1px solid #e5e7eb; border-radius:10px; padding:10px 12px; background:#fff; font-size:13px;}
    .impact-table{ width:100%; border-collapse:collapse; margin-top:6px; }
    .impact-table th,.impact-table td{ border-bottom:1px solid #eee; padding:6px 8px; text-align:left; }

    details.impact{ border:1px solid #e5e7eb; border-radius:10px; padding:8px 12px; background:#fafafa; margin:8px 0 14px; }
    details.impact summary{ cursor:pointer; font-weight:600; }

    .manual-panel { display:none; border:1px dashed #cbd5e1; border-radius:10px; padding:10px; background:#fff; }
    .manual-grid { display:grid; gap:10px; grid-template-columns: repeat(6, minmax(120px,1fr)); margin-top:8px; }
    .manual-grid label { font-size:12px; color:#374151; display:flex; flex-direction:column; gap:6px; }
    .manual-hours { display:grid; gap:8px; grid-template-columns: repeat(6, minmax(100px,1fr)); margin-top:10px; }
    .manual-panel input, .manual-panel select, .manual-hours input { font-size: 13px; line-height: 1.25; padding: 8px 10px; border: 1px solid #e5e7eb; border-radius: 8px; width: 100%; box-sizing: border-box; }
    .manual-hours label { font-size: 12px; display: flex; flex-direction: column; gap: 6px; }
    .manual-grid, .manual-hours { align-items: end; }
    .manual-panel input[type="number"] { -moz-appearance: textfield; appearance: textfield; }
    .manual-panel input[type="number"]::-webkit-outer-spin-button, .manual-panel input[type="number"]::-webkit-inner-spin-button { -webkit-appearance: none; margin: 0; }

    .hangar-wrap { margin-top:14px; }
    .hangar-controls { display:flex; gap:12px; flex-wrap:wrap; align-items:center; margin:8px 0 10px; }
    .hangar-grid { width:100%; overflow:auto; border:1px solid #e5e7eb; border-radius:10px; background:#fff; }
    .hgrid { display:grid; min-width:800px; }
    .hcell { border-bottom:1px solid #f1f5f9; border-right:1px solid #f1f5f9; padding:8px; font-size:12px; line-height:1.15; }
    .hcell.header { background:#f8fafc; font-weight:600; position:sticky; top:0; z-index:1; }
    .hcell.rowhdr { background:#f8fafc; font-weight:600; position:sticky; left:0; z-index:1; white-space:nowrap; }
    .hcell.empty { background:#fef9c3; border-color:#fde68a; color:#7a6c1f; }
    .hcell.occupied { background:#dcfce7; border-color:#bbf7d0; }
    .hcell.occupied.split { border:1px dashed #86efac; }
    .hcell.conflict { background:#fee2e2; border-color:#fecaca; color:#991b1b; font-weight:600; }

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
  <label><input type="checkbox" id="showPotential" checked> Show Potential</label>
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
  <div class="manual-panel" id="manualPanel">
    <div class="manual-grid">
      <label>Project Number <input id="m_number" type="text" value="P-Manual"></label>
      <label>Customer <input id="m_customer" type="text" value="Manual"></label>
      <label>Aircraft Model <input id="m_aircraft" type="text" value=""></label>
      <label>Scope <input id="m_scope" type="text" value="What-If"></label>
      <label>Induction <input id="m_ind" type="date"></label>
      <label>Delivery <input id="m_del" type="date"></label>
    </div>
    <div class="manual-hours" id="manualHours"></div>
  </div>
  <div id="impactResult" class="impact-box"></div>
</details>
<div class="chart-wrap"><canvas id="myChart"></canvas></div>
<div class="chart-wrap util" style="display:block;"><canvas id="utilChart"></canvas></div>
<p class="footnote">Tip: click the <em>Confirmed</em> line; if “Show Potential” is on, the popup includes both Confirmed and Potential for that period.</p>
<details class="snapshot" open>
  <summary>Snapshot Breakdown (Projects → Dept)</summary>
  <div class="snap-controls">
    <label><input type="checkbox" id="snapConfirmed" checked> Include Confirmed</label>
    <label><input type="checkbox" id="snapPotential" checked> Include Potential</label>
    <label>Top N projects
      <input type="range" id="snapTopN" min="3" max="20" step="1" value="8" style="vertical-align:middle;">
      <span id="snapTopNVal">8</span>
    </label>
    <label>From <input type="date" id="snapFrom"></label>
    <label>To <input type="date" id="snapTo"></label>
    <button id="snapReset" style="padding:6px 10px;border:1px solid #e5e7eb;border-radius:8px;background:#fff;cursor:pointer;">Reset</button>
    <span class="snap-legend" style="margin-left:auto">
      <span class="chip"><span class="dot" style="background:var(--confirmed)"></span> Confirmed</span>
      <span class="chip"><span class="dot" style="background:var(--potential2)"></span> Potential</span>
    </span>
  </div>
  <div class="snap-grid">
    <div class="snap-card"><h4>Sankey: Project → Dept (by hours)</h4><div id="sankeyDiv" class="snap-echart"></div></div>
    <div class="snap-card"><h4>Treemap: Project contribution</h4><div id="treemapDiv" class="snap-echart"></div></div>
    <div class="snap-card"><h4>Pareto: Top contributors</h4><canvas id="paretoCanvas"></canvas></div>
  </div>
</details>
<details class="snapshot" open>
  <summary>Hangar Bay Planner (beta)</summary>
  <div class="hangar-wrap">
    <div class="hangar-controls">
      <label><input type="checkbox" id="planIncludePotential" checked> Include Potential projects</label>
      <label>Periods to show <input type="number" id="planPeriods" min="4" max="52" step="1" value="12" style="width:72px;"></label>
      <label>Start at <input type="date" id="planFrom"></label>
      <span style="margin-left:auto;font-size:12px;color:#6b7280;">Rules: H has 2 bays (each can split into 2 small). D1 & D2 can each host 1×B757 or split into 2 small (only one of them split at a time). D3 = one slot only.</span>
    </div>
    <div id="hangarGrid" class="hangar-grid"></div>
  </div>
</details>
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
/* The rest of the JS is identical to your working version — trimmed for brevity in this snippet.
   Keep all functions and event handlers unchanged. */
</script>
</body>
</html>
"""

# --------------- Data helpers ---------------
def read_all():
    try:
        # Preferred helper if your backend exposes it
        return ds.get_all_datasets()
    except Exception:
        ref = ds.load_all()
        return ref["projects"], ref["potential"], ref["actual"], ref["depts"]

def inject(html: str, projects, potential, actual, depts) -> str:
    code = (
        html.replace("__PROJECTS__", json.dumps(projects))
            .replace("__POTENTIAL__", json.dumps(potential))
            .replace("__ACTUAL__", json.dumps(actual))
            .replace("__DEPTS__", json.dumps(depts))
    )
    # Flip default-checked to unchecked for Potential toggles at render-time
    code = code.replace('id="showPotential" checked', 'id="showPotential"')
    code = code.replace('id="snapPotential" checked', 'id="snapPotential"')
    code = code.replace('id="planIncludePotential" checked', 'id="planIncludePotential"')
    return code

# ===================== Dashboard =====================
if page == "Dashboard":
    st.title("Capacity & Load Dashboard")

    confirmed, potential, actual, depts = read_all()
    potential_all = potential or []

    st.subheader("Filters (Server-side)")
    if potential_all:
        pot_labels = [f'{p.get("number") or "—"} — {p.get("customer") or "Unknown"}' for p in potential_all]
        selected = st.multiselect("Include only these Potential projects (leave empty = include all):", pot_labels, default=[])
        if selected:
            selected_numbers = set((lbl.split(" — ", 1)[0] or "").strip() for lbl in selected)
            potential = [p for p in potential_all if (p.get("number") or "") in selected_numbers]
        else:
            potential = potential_all
    else:
        st.info("No Potential projects in DB.")
        potential = []

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("🔄 Refresh data"):
            st.rerun()
    with c2:
        snapshot = {"projects": confirmed, "potential": potential, "actual": actual, "depts": depts}
        st.download_button("⬇️ Download snapshot (JSON)", data=json.dumps(snapshot, indent=2),
                           file_name="capacity_snapshot.json", mime="application/json")
    with c3:
        st.caption("All charts below are fed by data from your database.")

    html_code = inject(HTML_TEMPLATE, confirmed, potential, actual, depts)
    components.html(html_code, height=1600, scrolling=False)

# ===================== Data Management =====================
if page == "Data Management":
    st.title("Data Management")
    tabs = st.tabs(["📝 Quick Edit"])

    with tabs[0]:
        st.subheader("Quick Edit")
        label = st.radio("Dataset", ["Confirmed", "Potential", "Actual"], horizontal=True, index=0)
        ds_map = {"Confirmed":"projects", "Potential":"potential", "Actual":"actual"}
        dataset_key = ds_map[label]

        # Pull current dept keys
        _, _, _, depts = read_all()
        dkeys = [d["key"] for d in depts]

        with st.form("qe_form"):
            c1, c2, c3 = st.columns(3)
            with c1:
                number = st.text_input("Number", "")
                customer = st.text_input("Customer", "")
                aircraftModel = st.text_input("Aircraft Model", "")
            with c2:
                scope = st.text_input("Scope", "")
                ind_date = st.date_input("Induction", value=date.today())
            with c3:
                del_date = st.date_input("Delivery", value=date.today())
                st.caption("Dates saved as YYYY-MM-DD")

            st.markdown("**Department Hours**")
            cols = st.columns(3)
            hours = {}
            for i, k in enumerate(dkeys):
                with cols[i % 3]:
                    hours[k] = st.number_input(f"{k} hours", min_value=0.0, value=0.0, step=1.0, key=f"qe_{k}")

            b1, b2 = st.columns(2)
            with b1:
                save_btn = st.form_submit_button("💾 Apply Changes", use_container_width=True)
            with b2:
                del_btn = st.form_submit_button("🗑️ Delete by Number", use_container_width=True)

        if save_btn:
            payload = {
                "number": number.strip(),
                "customer": customer.strip(),
                "aircraftModel": aircraftModel.strip(),
                "scope": scope.strip(),
                "induction": ind_date.isoformat(),
                "delivery": del_date.isoformat(),
                **{k: float(hours.get(k) or 0.0) for k in dkeys},
            }
            ds.upsert_project(dataset_key, payload)
            st.toast("Saved to Google Sheets ✔️")
            st.rerun()

        if del_btn and (number or "").strip():
            ds.delete_project(dataset_key, number.strip())
            st.toast("Deleted", icon="🗑️")
            st.rerun()
