import os, requests, pandas as pd, json
from io import StringIO
from datetime import datetime, timedelta

# ── Config ────────────────────────────────────────────────────
# Sheet: https://docs.google.com/spreadsheets/d/1lU18_6sXGMlQG4P-AZf-Qw-auu--4wac3IpcEZyoV5A
# GID 925773389 = Basedata tab
SHEET_ID  = "1lU18_6sXGMlQG4P-AZf-Qw-auu--4wac3IpcEZyoV5A"
SHEET_GID = "925773389"

# NOTE: Sheet ID is hardcoded here — no GitHub Secret needed anymore.
# Previously the secret was used so the ID could be changed without editing code.
# Now that we have one fixed sheet, hardcoding is simpler and more reliable.

URL      = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SHEET_GID}"
URL_GVIZ = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=Basedata"

# ── Fetch ─────────────────────────────────────────────────────
print(f"Fetching Basedata from Google Sheets...")
try:
    res = requests.get(URL, timeout=30)
    res.raise_for_status()
    print("Export URL worked")
except Exception as e1:
    print(f"Export URL failed ({e1}), trying gviz URL...")
    try:
        res = requests.get(URL_GVIZ, timeout=30)
        res.raise_for_status()
        print("Gviz URL worked")
    except Exception as e2:
        print(f"Both URLs failed. e1={e1}, e2={e2}")
        raise e2

df = pd.read_csv(StringIO(res.text))
print(f"Loaded {len(df)} rows, {len(df.columns)} columns")

# ── Helpers ───────────────────────────────────────────────────
EXCEL_EPOCH = datetime(1899, 12, 30)

def xl_to_datetime(series):
    """Convert Excel serial number column to datetime."""
    def conv(v):
        try:
            f = float(v)
            if f > 2:
                return EXCEL_EPOCH + timedelta(days=f)
        except (TypeError, ValueError):
            pass
        return pd.NaT
    return series.apply(conv)

def xl_to_hours(series):
    """Convert Excel fractional-day column to hours."""
    def conv(v):
        try:
            f = float(v)
            if f > 2:
                f = f - int(f)   # datetime serial → extract time portion only
            return round(f * 24, 4)
        except (TypeError, ValueError):
            return None
    return series.apply(conv)

# ── Parse date & time columns ─────────────────────────────────
if "Date of Connection" in df.columns:
    df["_date"] = xl_to_datetime(df["Date of Connection"])
else:
    df["_date"] = pd.NaT

# Departure ADH: ATD <= STD + 30 min
if "Vehicle STD" in df.columns and "Vehicle ATD" in df.columns:
    df["_std_dt"] = xl_to_datetime(df["Vehicle STD"])
    df["_atd_dt"] = xl_to_datetime(df["Vehicle ATD"])
    df["_dep_adh"] = (
        df["_atd_dt"].notna() & df["_std_dt"].notna() &
        (df["_atd_dt"] <= df["_std_dt"] + timedelta(minutes=30))
    )
else:
    df["_dep_adh"] = False

if "Delay Placement (Hr)" in df.columns:
    df["_pl_h"] = xl_to_hours(df["Delay Placement (Hr)"])
else:
    df["_pl_h"] = None

if "Designed Running TAT" in df.columns:
    df["_run_d"] = xl_to_hours(df["Designed Running TAT"])
if "Actual Running TAT" in df.columns:
    df["_run_a"] = xl_to_hours(df["Actual Running TAT"])

# ── Filter valid rows ─────────────────────────────────────────
VALID_PERF = ["On Time", "Breached", "Running On Time", "Running-Delay"]
valid = df[df["Overall Performance"].isin(VALID_PERF)].copy()
print(f"Valid rows: {len(valid)}")

# ── KPI metrics ───────────────────────────────────────────────
total    = len(valid)
ontime   = int((valid["Overall Performance"] == "On Time").sum())
breached = int((valid["Overall Performance"] == "Breached").sum())
running  = int(valid["Overall Performance"].isin(["Running On Time", "Running-Delay"]).sum())
intrans  = int((df["Trip Status"] == "In Transit").sum()) if "Trip Status" in df.columns else 0

# Departure ADH % (STD + 30 min rule) among closed trips
if "Trip Status" in df.columns:
    closed      = df["Trip Status"] == "Trip Closed"
    dep_adh_den = int(closed.sum())
    dep_adh_ok  = int(df.loc[closed, "_dep_adh"].sum())
else:
    dep_adh_den = len(df)
    dep_adh_ok  = int(df["_dep_adh"].sum())
dep_adh_pct = round(dep_adh_ok / dep_adh_den * 100, 1) if dep_adh_den else 0.0

# Placement ADH %
if "_pl_h" in valid.columns:
    pl_mask    = valid["_pl_h"].notna()
    pl_ok      = int((valid.loc[pl_mask, "_pl_h"] <= 0.0001).sum())
    pl_den     = int(pl_mask.sum())
    pl_adh_pct = round(pl_ok / pl_den * 100, 1) if pl_den else 0.0
else:
    pl_adh_pct = 0.0

# Running TAT ADH %
if "_run_d" in valid.columns and "_run_a" in valid.columns:
    run_mask = valid["_run_d"].notna() & valid["_run_a"].notna()
    run_ok   = int((valid.loc[run_mask, "_run_a"] <= valid.loc[run_mask, "_run_d"] + 0.0001).sum())
    run_den  = int(run_mask.sum())
    run_pct  = round(run_ok / run_den * 100, 1) if run_den else 0.0
else:
    run_pct = 0.0

# ── Weekly breakdown ──────────────────────────────────────────
weekly = []
if "Trip Starting Week" in valid.columns:
    for wk, grp in valid.groupby("Trip Starting Week"):
        weekly.append({
            "week":     str(wk),
            "total":    len(grp),
            "ontime":   int((grp["Overall Performance"] == "On Time").sum()),
            "breached": int((grp["Overall Performance"] == "Breached").sum()),
            "running":  int(grp["Overall Performance"].isin(["Running On Time","Running-Delay"]).sum())
        })

# ── Daily trend ───────────────────────────────────────────────
daily = []
valid_dated = valid[valid["_date"].notna()].copy()
if len(valid_dated):
    for dt, grp in valid_dated.groupby(valid_dated["_date"].dt.strftime("%b %d")):
        daily.append({
            "date":     dt,
            "ontime":   int((grp["Overall Performance"] == "On Time").sum()),
            "breached": int((grp["Overall Performance"] == "Breached").sum())
        })
    daily.sort(key=lambda x: datetime.strptime(x["date"], "%b %d").replace(year=2026))

# ── Origin DC breakdown ───────────────────────────────────────
origin = []
if "Origin_DC" in valid.columns:
    for dc, grp in valid.groupby("Origin_DC"):
        origin.append({
            "dc":       str(dc),
            "total":    len(grp),
            "ontime":   int((grp["Overall Performance"] == "On Time").sum()),
            "breached": int((grp["Overall Performance"] == "Breached").sum())
        })
    origin.sort(key=lambda x: -x["total"])

# ── Lane breach rates (top 10, min 5 trips) ───────────────────
lanes = []
if "Lane Code" in valid.columns:
    for lane, grp in valid.groupby("Lane Code"):
        if len(grp) >= 5:
            b = int((grp["Overall Performance"] == "Breached").sum())
            lanes.append({
                "lane":     str(lane),
                "total":    len(grp),
                "breached": b,
                "rate":     round(b / len(grp) * 100, 1)
            })
    lanes.sort(key=lambda x: -x["rate"])
    lanes = lanes[:10]

# ── Dependency type ───────────────────────────────────────────
dep = []
if "Dependency Type" in valid.columns:
    dep = [{"type": str(k), "count": int(v)}
           for k, v in valid["Dependency Type"].value_counts().items()
           if str(k) not in ["nan", "#REF!"]]

# ── Breach reasons ────────────────────────────────────────────
reasons = []
if "Standard Comment" in valid.columns:
    sc = valid[
        valid["Standard Comment"].notna() &
        ~valid["Standard Comment"].isin(["On Time", "#REF!"])
    ]["Standard Comment"].value_counts().head(8)
    reasons = [{"reason": str(k), "count": int(v)} for k, v in sc.items()]

# ── Assemble payload ──────────────────────────────────────────
data = {
    "kpi": {
        "total":       total,
        "ontime":      ontime,
        "breached":    breached,
        "running":     running,
        "intransit":   intrans,
        "dep_adh_pct": dep_adh_pct,
        "pl_adh_pct":  pl_adh_pct,
        "run_pct":     run_pct,
    },
    "weekly":     weekly,
    "daily":      daily,
    "origin":     origin,
    "lanes":      lanes,
    "dependency": dep,
    "reasons":    reasons,
    "updated":    datetime.utcnow().strftime("%d %b %Y %H:%M UTC")
}

# ── Inject into dashboard.html ────────────────────────────────
with open("dashboard.html", "r", encoding="utf-8") as f:
    html = f.read()

inject_line = "// @@DATA_INJECT@@"
if inject_line not in html:
    raise ValueError("Marker '// @@DATA_INJECT@@' not found in dashboard.html")

replacement = f"""{inject_line}
window.__LIVE_DATA__ = {json.dumps(data, ensure_ascii=False)};"""

html = html.replace(inject_line, replacement, 1)

with open("dashboard.html", "w", encoding="utf-8") as f:
    f.write(html)

pct_ot = round(ontime / total * 100, 1) if total else 0
print(f"\nDashboard rebuilt successfully!")
print(f"  Total trips      : {total}")
print(f"  On-time          : {ontime} ({pct_ot}%)")
print(f"  Breached         : {breached} ({round(breached/total*100,1) if total else 0}%)")
print(f"  In transit       : {intrans}")
print(f"  Dep ADH (STD+30m): {dep_adh_pct}%")
print(f"  Placement ADH    : {pl_adh_pct}%")
print(f"  Running TAT ADH  : {run_pct}%")
print(f"  Updated at       : {data['updated']}")
