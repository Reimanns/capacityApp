# data_store.py  — zero-dependency CRUD for your Streamlit app
import os, json, sqlite3

DB_PATH = os.getenv("DB_PATH", "capacity.db")

def init_db(path: str | None = None):
    global DB_PATH
    if path: DB_PATH = path
    with sqlite3.connect(DB_PATH) as con:
        c = con.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS projects(
            id INTEGER PRIMARY KEY,
            dataset TEXT,                -- 'projects' | 'potential' | 'actual'
            number TEXT,
            customer TEXT,
            aircraftModel TEXT,
            scope TEXT,
            induction TEXT,
            delivery TEXT,
            hours TEXT                   -- JSON: {dept_key: float}
        )""")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_proj ON projects(dataset, number)")
        c.execute("""CREATE TABLE IF NOT EXISTS depts(
            key TEXT PRIMARY KEY,
            name TEXT,
            headcount INTEGER
        )""")
        con.commit()

def seed_if_empty(projects, potential, actual, depts):
    with sqlite3.connect(DB_PATH) as con:
        c = con.cursor()
        total = c.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
        if total == 0:
            for dsname, arr in (("projects", projects), ("potential", potential), ("actual", actual)):
                for p in arr:
                    meta = {k: p.get(k) for k in ["number","customer","aircraftModel","scope","induction","delivery"]}
                    hours = {d["key"]: float(p.get(d["key"], 0) or 0) for d in depts}
                    c.execute("""INSERT INTO projects(dataset,number,customer,aircraftModel,scope,induction,delivery,hours)
                                 VALUES(?,?,?,?,?,?,?,?)""",
                              (dsname, meta["number"], meta["customer"], meta["aircraftModel"],
                               meta["scope"], meta["induction"], meta["delivery"], json.dumps(hours)))
            if c.execute("SELECT COUNT(*) FROM depts").fetchone()[0] == 0:
                for d in depts:
                    c.execute("INSERT OR REPLACE INTO depts(key,name,headcount) VALUES(?,?,?)",
                              (d["key"], d.get("name", d["key"]), int(d.get("headcount", 0))))
            con.commit()

def list_depts():
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        return [dict(r) for r in con.execute("SELECT key,name,headcount FROM depts ORDER BY rowid")]

def save_depts(depts):
    with sqlite3.connect(DB_PATH) as con:
        for d in depts:
            con.execute("INSERT OR REPLACE INTO depts(key,name,headcount) VALUES(?,?,?)",
                        (d["key"], d.get("name", d["key"]), int(d.get("headcount",0))))
        con.commit()

def _expand(row, depts):
    d = dict(row)
    hrs = json.loads(d.pop("hours") or "{}")
    for k in [x["key"] for x in depts]:
        d[k] = float(hrs.get(k, 0))
    return d

def load_dataset(dsname):
    depts = list_depts()
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        rows = con.execute("""SELECT dataset,number,customer,aircraftModel,scope,induction,delivery,hours
                              FROM projects WHERE dataset=?""", (dsname,)).fetchall()
        return [_expand(r, depts) for r in rows]

def upsert_project(dsname, entry: dict):
    depts = list_depts()
    hours = {x["key"]: float(entry.get(x["key"], 0) or 0) for x in depts}
    meta = {k: entry.get(k) for k in ["number","customer","aircraftModel","scope","induction","delivery"]}
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            INSERT INTO projects(dataset,number,customer,aircraftModel,scope,induction,delivery,hours)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(dataset,number) DO UPDATE SET
              customer=excluded.customer,
              aircraftModel=excluded.aircraftModel,
              scope=excluded.scope,
              induction=excluded.induction,
              delivery=excluded.delivery,
              hours=excluded.hours
        """, (dsname, meta["number"], meta["customer"], meta["aircraftModel"], meta["scope"],
              meta["induction"], meta["delivery"], json.dumps(hours)))
        con.commit()

def delete_project(dsname, number):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("DELETE FROM projects WHERE dataset=? AND number=?", (dsname, number))
        con.commit()

def replace_dataset(dsname, entries: list[dict]):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("DELETE FROM projects WHERE dataset=?", (dsname,))
        con.commit()
    for e in entries:
        upsert_project(dsname, e)

def load_all():
    return {
        "projects":  load_dataset("projects"),
        "potential": load_dataset("potential"),
        "actual":    load_dataset("actual"),
        "depts":     list_depts(),
    }
