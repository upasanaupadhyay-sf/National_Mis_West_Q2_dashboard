import os, requests, pandas as pd, json, re
from io import StringIO
from datetime import datetime

# ── Config ────────────────────────────────────────────────────
# Sheet ID is loaded from GitHub Secret (SHEET_ID)
# Your sheet: https://docs.google.com/spreadsheets/d/1ToWkm-UDTv6SK1I12rx-JV0vkNk_5nYa_h-jmbDii8k
SHEET_ID = os.environ.get("SHEET_ID", "1ToWkm-UDTv6SK1I12rx-JV0vkNk_5nYa_h-jmbDii8k")
SHEET_NAME = "Basedata"

URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}"

# ── Fetch ─────────────────────────────────────────────────────
print(f"Fetching: {SHEET_NAME} from Google Sheets...")
res = requests.get(URL, timeout=30)
res.raise_for_status()
df = pd.read_csv(StringIO(res.text))
print(f"Loaded {len(df)} rows, {len(df.columns)} columns")

# ── Clean ─────────────────────────────────────────────────────
VALID_PERF = ["On Time", "Breached", "Running On Time", "Running-Delay"]
valid = df[df["Overall Performance"].isin(VALID_PERF)].copy()

if "Date of Connection" in df.columns:
    valid["Date of Connection"] = pd.to_datetime(valid["Date of Connection"], errors="coerce")

# ── KPI metrics ───────────────────────────────────────────────
total    = len(valid)
ontime   = int((valid["Overall Performance"] == "On Time").sum())
breached = int((valid["Overall Performance"] == "Breached").sum())
running  = int((valid["Overall Performance"].isin(["Running On Time", "Running-Delay"])).sum())
intrans  = int((df["Trip Status"] == "In Transit").sum()) if "Trip Status" in df.columns else 0

# ── Weekly breakdown ──────────────────────────────────────────
weekly = []
if "Trip Starting Week" in valid.columns:
    for wk, grp in valid.groupby("Trip Starting Week"):
        weekly.append({
            "week":    str(wk),
            "total":   len(grp),
            "ontime":  int((grp["Overall Performance"] == "On Time").sum()),
            "breached":int((grp["Overall Performance"] == "Breached").sum()),
            "running": int(grp["Overall Performance"].isin(["Running On Time","Running-Delay"]).sum())
        })

# ── Daily trend ───────────────────────────────────────────────
daily = []
if "Date of Connection" in valid.columns:
    for dt, grp in valid.groupby(valid["Date of Connection"].dt.strftime("%b %d")):
        daily.append({
            "date":    dt,
            "ontime":  int((grp["Overall Performance"] == "On Time").sum()),
            "breached":int((grp["Overall Performance"] == "Breached").sum())
        })
    daily.sort(key=lambda x: x["date"])

# ── Origin DC ─────────────────────────────────────────────────
origin = []
if "Origin_DC" in valid.columns:
    for dc, grp in valid.groupby("Origin_DC"):
        origin.append({
            "dc":      str(dc),
            "total":   len(grp),
            "ontime":  int((grp["Overall Performance"] == "On Time").sum()),
            "breached":int((grp["Overall Performance"] == "Breached").sum())
        })
    origin.sort(key=lambda x: -x["total"])

# ── Lane breach rates (top 10, min 5 trips) ───────────────────
lanes = []
if "Lane Code" in valid.columns:
    for lane, grp in valid.groupby("Lane Code"):
        if len(grp) >= 5:
            b = int((grp["Overall Performance"] == "Breached").sum())
            lanes.append({
                "lane":    str(lane),
                "total":   len(grp),
                "breached":b,
                "rate":    round(b / len(grp) * 100, 1)
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
        (valid["Standard Comment"] != "On Time") &
        (valid["Standard Comment"] != "#REF!")
    ]["Standard Comment"].value_counts().head(8)
    reasons = [{"reason": str(k), "count": int(v)} for k, v in sc.items()]

# ── Assemble payload ──────────────────────────────────────────
data = {
    "kpi": {
        "total":    total,
        "ontime":   ontime,
        "breached": breached,
        "running":  running,
        "intransit":intrans
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

# Replace everything between @@DATA_INJECT@@ marker and next const
inject_line = "// @@DATA_INJECT@@"
if inject_line not in html:
    raise ValueError("Marker '// @@DATA_INJECT@@' not found in dashboard.html — please add it before the ALL_DATA block")

# Build replacement: marker + window.__LIVE_DATA__ assignment
replacement = f"""{inject_line}
window.__LIVE_DATA__ = {json.dumps(data, ensure_ascii=False)};"""

# Replace just the marker line (keep everything after it intact)
html = html.replace(inject_line, replacement, 1)

with open("dashboard.html", "w", encoding="utf-8") as f:
    f.write(html)

pct = round(ontime / total * 100, 1) if total else 0
print(f"Dashboard rebuilt successfully!")
print(f"  Total trips : {total}")
print(f"  On-time     : {ontime} ({pct}%)")
print(f"  Breached    : {breached} ({round(breached/total*100,1) if total else 0}%)")
print(f"  In transit  : {intrans}")
print(f"  Updated at  : {data['updated']}")
