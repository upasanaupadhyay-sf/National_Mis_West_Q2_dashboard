import os, requests, pandas as pd, json
from io import StringIO
from datetime import datetime, timedelta

# ── Config ────────────────────────────────────────────────────
# Sheet: https://docs.google.com/spreadsheets/d/1lU18_6sXGMlQG4P-AZf-Qw-auu--4wac3IpcEZyoV5A
# GID 925773389 = Basedata tab
SHEET_ID  = "1lU18_6sXGMlQG4P-AZf-Qw-auu--4wac3IpcEZyoV5A"
SHEET_GID = "925773389"

# Sheet ID is hardcoded — the DATA inside updates every run (every hour via GitHub Action).
# Hardcoding just means the sheet URL is fixed, NOT the data.

URL      = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SHEET_GID}"
URL_GVIZ = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=Basedata"

# ── Fetch ─────────────────────────────────────────────────────
print("Fetching Basedata from Google Sheets...")
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

def xl_to_date_str(val):
    """Excel serial date → YYYY-MM-DD string."""
    if pd.isna(val) or val is None or str(val).strip() == '':
        return None
    try:
        f = float(val)
        if f > 2:
            return (EXCEL_EPOCH + timedelta(days=f)).strftime('%Y-%m-%d')
    except (TypeError, ValueError):
        pass
    # Maybe already a date string
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y'):
        try:
            return datetime.strptime(str(val).strip(), fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass
    return None

def xl_to_datetime(val):
    """Excel serial datetime → datetime object."""
    try:
        f = float(val)
        if f > 2:
            return EXCEL_EPOCH + timedelta(days=f)
    except (TypeError, ValueError):
        pass
    return None

def xl_to_hours(val):
    """Excel fractional-day → hours (float)."""
    try:
        f = float(val)
        if f > 2:
            f = f - int(f)   # datetime serial: keep only time portion
        return round(f * 24, 4)
    except (TypeError, ValueError):
        return None

def safe_str(val):
    if pd.isna(val) or val is None:
        return ''
    s = str(val).strip()
    return '' if s in ('#REF!', 'nan', 'None') else s

# ── Required columns check ────────────────────────────────────
REQUIRED = ['Date of Connection', 'Trip Starting Week', 'Overall Performance']
for col in REQUIRED:
    if col not in df.columns:
        raise ValueError(f"Required column '{col}' not found. Available: {list(df.columns)}")

# ── Build records (one per row) ───────────────────────────────
# Each record matches the dashboard JS keys:
# d=date, w=week, l=lane, o=origin, t=dep_type, v=vendor
# pl=placement_delay_h, dp=departure_delay_h (vs STD+30min)
# rT=design_run_TAT_h, aR=actual_run_TAT_h
# dT=design_tot_TAT_h, aT=actual_tot_TAT_h
# st=On Time/Breached, rs=reason, rS=trip_status, sta=dest_STA_date

records = []
skipped = 0

for _, row in df.iterrows():
    date_str = xl_to_date_str(row.get('Date of Connection'))
    if not date_str:
        skipped += 1
        continue

    week = safe_str(row.get('Trip Starting Week'))
    if not week:
        skipped += 1
        continue

    # Departure ADH: ATD <= STD + 30 min → departure delay in hours
    std_dt = xl_to_datetime(row.get('Vehicle STD'))
    atd_dt = xl_to_datetime(row.get('Vehicle ATD'))
    if std_dt and atd_dt:
        dep_delay_h = round((atd_dt - std_dt).total_seconds() / 3600, 4)
    else:
        # Fall back to the pre-computed delay column
        dep_delay_h = xl_to_hours(row.get('Delay Departure (Hrs)'))

    # Placement delay
    pl_h = xl_to_hours(row.get('Delay Placement (Hr)'))

    # Running TAT
    rT = xl_to_hours(row.get('Designed Running TAT'))
    aR = xl_to_hours(row.get('Actual Running TAT'))

    # Total TAT
    dT = xl_to_hours(row.get('Design + Holding = Total TAT'))
    aT = xl_to_hours(row.get('Actual (Design + Holding) = Total TAT'))

    # Destination STA → trip closure date
    sta = xl_to_date_str(row.get('Destination-STA'))

    # Performance
    overall = safe_str(row.get('Overall Performance'))
    if overall == 'On Time':
        st = 'On Time'
    elif overall == 'Breached':
        st = 'Breached'
    elif overall in ('Running On Time', 'Running-Delay'):
        st = overall
    else:
        skipped += 1
        continue   # skip rows with no valid performance

    trip_status = safe_str(row.get('Trip Status'))
    reason      = safe_str(row.get('LH Detailed Reason')) or safe_str(row.get('Standard Comment'))

    rec = {
        'd':  date_str,
        'w':  week,
        'l':  safe_str(row.get('Lane Code')),
        'o':  safe_str(row.get('Origin_DC')),
        't':  safe_str(row.get('Departure Type')),
        'v':  safe_str(row.get('Vendor Name')),
        'pl': pl_h,
        'dp': dep_delay_h,
        'rT': rT,
        'aR': aR,
        'dT': dT,
        'aT': aT,
        'st': st,
        'rs': reason,
        'rS': trip_status,
        'sta': sta,
    }
    records.append(rec)

print(f"Records built: {len(records)}, skipped: {skipped}")

# ── Inject into dashboard.html ────────────────────────────────
with open('dashboard.html', 'r', encoding='utf-8') as f:
    html = f.read()

inject_line = '// @@DATA_INJECT@@'
if inject_line not in html:
    raise ValueError("Marker '// @@DATA_INJECT@@' not found in dashboard.html")

meta = {'updated': datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}

replacement = f"""{inject_line}
window.__LIVE_DATA__ = {json.dumps(records, ensure_ascii=False)};
window.__LIVE_META__ = {json.dumps(meta, ensure_ascii=False)};"""

html = html.replace(inject_line, replacement, 1)

with open('dashboard.html', 'w', encoding='utf-8') as f:
    f.write(html)

# ── Summary ───────────────────────────────────────────────────
closed   = [r for r in records if 'closed' in r['rS'].lower()]
ot       = sum(1 for r in closed if r['st'] == 'On Time')
br       = sum(1 for r in closed if r['st'] == 'Breached')
dep_ok   = sum(1 for r in closed if r['dp'] is not None and r['dp'] <= 0.5)
dep_den  = sum(1 for r in closed if r['dp'] is not None)
pct_ot   = round(ot / (ot+br) * 100, 1) if (ot+br) else 0
dep_pct  = round(dep_ok / dep_den * 100, 1) if dep_den else 0

print(f"\nDashboard rebuilt successfully!")
print(f"  Total records    : {len(records)}")
print(f"  Closed trips     : {len(closed)}")
print(f"  On-time          : {ot} ({pct_ot}%)")
print(f"  Breached         : {br}")
print(f"  Dep ADH (STD+30m): {dep_pct}%  ({dep_ok}/{dep_den})")
print(f"  Updated at       : {meta['updated']}")
