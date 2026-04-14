import os, requests, pandas as pd, json, re
from io import StringIO
from datetime import datetime

# ── Config ────────────────────────────────────────────────────
SHEET_ID = "1lU18_6sXGMlQG4P-AZf-Qw-auu--4wac3IpcEZyoV5A"
SHEET_NAME = "Basedata"
SHEET_GID = "1839236023"
URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SHEET_GID}"
URL_GVIZ = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}"
DASHBOARD = os.path.join(os.path.dirname(__file__) or ".", "combined_dashboard.html")
MARKER = "// @@DATA_INJECT@@"

# ── Fetch ─────────────────────────────────────────────────────
print(f"Fetching: {SHEET_NAME} from Google Sheets...")
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
def parse_tat(val):
    """Parse H:MM:SS or HH:MM:SS to float hours."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s or s in ("#REF!", "-", "nan"):
        return None
    if not re.match(r"^-?\d", s):
        return None
    parts = s.split(":")
    if len(parts) >= 2:
        h = int(parts[0])
        m = int(parts[1])
        sec = int(parts[2]) if len(parts) > 2 else 0
        return round(h + m / 60 + sec / 3600, 6)
    return None


def parse_date(val):
    """Parse DD-MM-YYYY to YYYY-MM-DD."""
    if pd.isna(val) or not str(val).strip():
        return ""
    try:
        return pd.to_datetime(str(val).strip(), format="%d-%m-%Y").strftime("%Y-%m-%d")
    except Exception:
        return str(val).strip()


def parse_datetime_to_date(val):
    """Parse DD-MM-YY HH:MM AM/PM to YYYY-MM-DD."""
    if pd.isna(val) or not str(val).strip():
        return ""
    try:
        dt = pd.to_datetime(str(val).strip(), format="%d-%m-%y %I:%M %p")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


def safe_str(val, default=""):
    """Convert value to string, returning default for NaN/None."""
    if pd.isna(val):
        return default
    return str(val).strip()


# ── Filter valid rows ────────────────────────────────────────
VALID_PERF = ["On Time", "Breached", "Running On Time", "Running-Delay"]
mask = df["Overall Performance"].isin(VALID_PERF)
if "Trip Status" in df.columns:
    mask = mask | (df["Trip Status"] == "In Transit")
valid = df[mask].copy()
print(f"Valid rows: {len(valid)}")

# ── Build compact records ────────────────────────────────────
records = []
for _, r in valid.iterrows():
    d = parse_date(r["Date of Connection"])
    sta = parse_datetime_to_date(r.get("Destination-ATA")) or d
    rec = {
        "d": d,
        "w": safe_str(r.get("Trip Starting Week")),
        "l": safe_str(r.get("Lane Code")),
        "o": safe_str(r.get("Origin_DC")),
        "t": safe_str(r.get("Departure Type")),
        "v": safe_str(r.get("Vendor Name")),
        "pl": parse_tat(r.get("Delay Placement (Hr)")),
        "dp": parse_tat(r.get("Delay Departure (Hrs)")),
        "rT": parse_tat(r.get("Designed Running TAT")),
        "aR": parse_tat(r.get("Actual Running TAT")),
        "dT": parse_tat(r.get("Design + Holding = Total TAT")),
        "aT": parse_tat(r.get("Actual (Design + Holding) = Total TAT")),
        "st": safe_str(r.get("Overall Performance")),
        "rs": safe_str(r.get("LH Detailed Reason")),
        "rS": safe_str(r.get("Trip Status")),
        "sta": sta,
    }
    records.append(rec)

# ── Inject into dashboard ────────────────────────────────────
json_str = json.dumps(records, ensure_ascii=False)

with open(DASHBOARD, "r", encoding="utf-8") as f:
    html = f.read()

if MARKER not in html:
    raise ValueError(f"Marker '{MARKER}' not found in {DASHBOARD}")

# Replace const D=[...];// @@DATA_INJECT@@ with actual data
# Use a lambda to avoid re.sub interpreting backslash escapes in the replacement
replacement = f"const D={json_str}; {MARKER}"
html = re.sub(
    r"const D=\[.*?\];\s*" + re.escape(MARKER),
    lambda m: replacement,
    html,
    count=1,
    flags=re.DOTALL,
)

with open(DASHBOARD, "w", encoding="utf-8") as f:
    f.write(html)

# ── Summary ──────────────────────────────────────────────────
closed = [r for r in records if "closed" in (r["rS"] or "").lower()]
ot = sum(1 for r in closed if r["st"] == "On Time")
br = sum(1 for r in closed if r["st"] == "Breached")
print(f"\nDashboard rebuilt: {DASHBOARD}")
print(f"  Total records : {len(records)} ({len(closed)} closed)")
print(f"  On-time       : {ot} ({round(ot / len(closed) * 100, 1) if closed else 0}%)")
print(f"  Breached      : {br} ({round(br / len(closed) * 100, 1) if closed else 0}%)")
print(f"  Updated at    : {datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}")
