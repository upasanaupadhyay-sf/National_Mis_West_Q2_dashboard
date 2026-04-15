"""
Build National MIS dashboard by merging 6 regional sheets:
  North / South & East / West  ×  Q1 / Q2

Each record is tagged with `z` (zone) and `q` (quarter) in addition to the
normal trip fields. The injected JSON drives a client-side dashboard with
Region + Quarter filters on top of the standard lane/vendor/origin filters.
"""
import os, requests, pandas as pd, json, re
from io import StringIO
from datetime import datetime

# ── Sources ──────────────────────────────────────────────────
# (zone, quarter, sheet_id, gid)
SOURCES = [
    ("North",        "Q1", "1qhlDHp-OG6KdoFHLvz76YOBAUdAcUBSl_wWO9lVdkUc", "1740903760"),
    ("North",        "Q2", "1YrgdpepiUbH4xcU1gNc1L0H2nroyTlhud67d5xzVE1s", "1740903760"),
    ("South & East", "Q1", "1h71I-uudjZGJoamNxbhAySGOaSpxpnwOv9Cg6-bt2NI", "1740903760"),
    ("South & East", "Q2", "1oQf8AzfqJBhPUWpLla1-5-yBzXtdPA8XwuluJ9Ix3YU", "1740903760"),
    ("West",         "Q1", "1-bGk1C_BHRlsP1IgNh9NLkkDZLDy0Ezu2tlfcRkOk0k", "1839236023"),
    ("West",         "Q2", "1lU18_6sXGMlQG4P-AZf-Qw-auu--4wac3IpcEZyoV5A", "1839236023"),
]

DASHBOARD = os.path.join(os.path.dirname(__file__) or ".", "mis_national.html")
MARKER = "// @@DATA_INJECT@@"
VALID_PERF = ["On Time", "Breached", "Running On Time", "Running-Delay"]


# ── Helpers ──────────────────────────────────────────────────
def parse_tat(val):
    """Parse H:MM:SS to float hours; return None if not parseable."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s or s in ("#REF!", "-", "nan"):
        return None
    if not re.match(r"^-?\d", s):
        return None
    parts = s.split(":")
    if len(parts) >= 2:
        try:
            h = int(parts[0])
            m = int(parts[1])
            sec = int(parts[2]) if len(parts) > 2 else 0
            return round(h + m / 60 + sec / 3600, 6)
        except ValueError:
            return None
    return None


def parse_date(val):
    """Parse DD-MM-YYYY to YYYY-MM-DD."""
    if pd.isna(val) or not str(val).strip():
        return ""
    try:
        return pd.to_datetime(str(val).strip(), format="%d-%m-%Y").strftime("%Y-%m-%d")
    except Exception:
        try:
            return pd.to_datetime(str(val).strip(), errors="coerce").strftime("%Y-%m-%d")
        except Exception:
            return str(val).strip()


def parse_dt_to_date(val):
    """Parse DD-MM-YY HH:MM AM/PM to YYYY-MM-DD."""
    if pd.isna(val) or not str(val).strip():
        return ""
    try:
        dt = pd.to_datetime(str(val).strip(), format="%d-%m-%y %I:%M %p")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


def safe_str(val, default=""):
    if pd.isna(val):
        return default
    return str(val).strip()


def fetch_sheet(sheet_id, gid):
    """Fetch a sheet tab as a DataFrame, with gviz fallback."""
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    url_gviz = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid={gid}"
    try:
        res = requests.get(url, timeout=60)
        res.raise_for_status()
    except Exception as e1:
        print(f"   export URL failed ({e1}), trying gviz...")
        res = requests.get(url_gviz, timeout=60)
        res.raise_for_status()
    return pd.read_csv(StringIO(res.text))


def extract_records(df, zone, quarter):
    """Keep valid rows and in-transit; emit compact records tagged with zone/quarter."""
    mask = df["Overall Performance"].isin(VALID_PERF)
    if "Trip Status" in df.columns:
        mask = mask | (df["Trip Status"] == "In Transit")
    valid = df[mask].copy()

    recs = []
    for _, r in valid.iterrows():
        d = parse_date(r.get("Date of Connection"))
        sta = parse_dt_to_date(r.get("Destination-ATA")) or d
        recs.append({
            "z": zone,
            "q": quarter,
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
        })
    return recs


# ── Fetch all 6 sheets ──────────────────────────────────────
all_records = []
summary = []
for zone, quarter, sheet_id, gid in SOURCES:
    print(f"-> {zone} {quarter} ({sheet_id[:12]}...)")
    try:
        df = fetch_sheet(sheet_id, gid)
        print(f"   Loaded {len(df)} rows, {len(df.columns)} cols")
        recs = extract_records(df, zone, quarter)
        all_records.extend(recs)
        closed = [r for r in recs if "closed" in (r["rS"] or "").lower()]
        ot = sum(1 for r in closed if r["st"] == "On Time")
        br = sum(1 for r in closed if r["st"] == "Breached")
        summary.append((zone, quarter, len(recs), len(closed), ot, br))
        print(f"   Valid: {len(recs)} ({len(closed)} closed, {ot} on-time, {br} breached)")
    except Exception as e:
        print(f"   !! FAILED: {e}")
        summary.append((zone, quarter, 0, 0, 0, 0))

print(f"\nTotal merged records: {len(all_records)}")

# ── Inject into dashboard ───────────────────────────────────
json_str = json.dumps(all_records, ensure_ascii=False)

with open(DASHBOARD, "r", encoding="utf-8") as f:
    html = f.read()

if MARKER not in html:
    raise ValueError(f"Marker '{MARKER}' not found in {DASHBOARD}")

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
print("\n---- SUMMARY ---------------------------------")
print(f"{'Zone':<15}{'Qtr':<6}{'Valid':>8}{'Closed':>8}{'OnTime':>8}{'Breach':>8}")
for z, q, v, c, ot, br in summary:
    print(f"{z:<15}{q:<6}{v:>8}{c:>8}{ot:>8}{br:>8}")
print(f"\nDashboard rebuilt: {DASHBOARD}")
print(f"Updated at        : {datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}")
