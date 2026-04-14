import requests, pandas as pd, json, re
from io import StringIO
from datetime import datetime, timedelta

# ── Config ────────────────────────────────────────────────────
SHEET_ID  = "1lU18_6sXGMlQG4P-AZf-Qw-auu--4wac3IpcEZyoV5A"
SHEET_GID = "1839236023"

URL      = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SHEET_GID}"
URL_GVIZ = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=Basedata"

# ── Fetch ─────────────────────────────────────────────────────
print("Fetching Basedata from Google Sheets...")
try:
    res = requests.get(URL, timeout=30)
    res.raise_for_status()
    print("Export URL worked")
except Exception as e1:
    print(f"Export URL failed ({e1}), trying gviz...")
    try:
        res = requests.get(URL_GVIZ, timeout=30)
        res.raise_for_status()
        print("Gviz URL worked")
    except Exception as e2:
        raise e2

# ── Auto-detect header row ────────────────────────────────────
ANCHORS = ['Date of Connection', 'Trip Starting Week', 'Overall Performance', 'Lane Code']
raw_text = res.text
lines    = raw_text.splitlines()

header_row_idx = 0
for i, line in enumerate(lines[:20]):
    if any(a in line for a in ANCHORS):
        header_row_idx = i
        print(f"Header found at line {i}")
        break

df = pd.read_csv(StringIO(raw_text), header=header_row_idx)
df.columns = [str(c).strip() for c in df.columns]
print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
print(f"Columns: {list(df.columns)}")

# ── CONFIRMED column mapping from actual sheet data ───────────
# Exact names from the Basedata sheet (verified from xlsx analysis):
#   [0]  Date of Connection         → Excel serial date (e.g. 46113.0)
#   [1]  Trip Starting Week         → "Week-14", "Week-15" etc
#   [4]  Departure Type             → "Regular", "Ad-hoc"
#   [5]  Vendor Name                → "Ruby Roadlines - FTL"
#   [10] Lane Code                  → "STV-AMD-1"
#   [11] Origin_DC                  → "Surat DC"
#   [15] Vehicle STD                → Excel datetime serial (e.g. 46113.333)
#   [16] Vehicle ATD                → Excel datetime serial (e.g. 46113.347)
#   [17] Delay Placement (Hr)       → fractional day (0.0 = no delay)
#   [18] Delay Departure (Hrs)      → fractional day (0.0138 = 20 min)  ← NAMED "Hrs" but is fractional day!
#   [50] Destination-STA            → Excel serial date
#   [59] Designed Running TAT       → fractional day (0.2916 = 7.0h)
#   [60] Actual Running TAT         → fractional day (0.2756 = 6.62h)
#   [63] Design + Holding = Total TAT             → fractional day
#   [64] Actual (Design + Holding) = Total TAT    → fractional day
#   [65] Trip Status                → "Trip Closed", "In Transit"
#   [67] Overall Performance        → "On Time", "Breached"
#   [74] Standard Comment
#   [75] LH Detailed Reason

COLS = {
    'date':      'Date of Connection',
    'week':      'Trip Starting Week',
    'dep_type':  'Departure Type',
    'vendor':    'Vendor Name',
    'lane':      'Lane Code',
    'origin':    'Origin_DC',
    'std':       'Vehicle STD',
    'atd':       'Vehicle ATD',
    'pl':        'Delay Placement (Hr)',
    'dep':       'Delay Departure (Hrs)',
    'rT':        'Designed Running TAT',
    'aR':        'Actual Running TAT',
    'dT':        'Design + Holding = Total TAT',
    'aT':        'Actual (Design + Holding) = Total TAT',
    'status':    'Trip Status',
    'perf':      'Overall Performance',
    'dest_sta':  'Destination-STA',
    'reason_std':'Standard Comment',
    'reason_lh': 'LH Detailed Reason',
}

# Verify columns exist, try fuzzy match if not
def find_col(name):
    if name in df.columns:
        return name
    # Case-insensitive contains
    name_l = name.lower()
    for c in df.columns:
        if name_l in c.lower() or c.lower() in name_l:
            return c
    return None

resolved = {}
print("\n=== Column resolution ===")
for key, name in COLS.items():
    col = find_col(name)
    resolved[key] = col
    print(f"  {'✓' if col else '✗'} {key:12s} '{name}' → {col or 'NOT FOUND'}")
print("===\n")

def gv(row, key):
    col = resolved.get(key)
    if not col: return None
    v = row.get(col)
    if isinstance(v, float) and pd.isna(v): return None
    return v

# ── Helpers ───────────────────────────────────────────────────
EXCEL_EPOCH = datetime(1899, 12, 30)

def safe_str(val):
    if val is None: return ''
    if isinstance(val, float) and pd.isna(val): return ''
    s = str(val).strip()
    return '' if s in ('#REF!', 'nan', 'None') else s

def frac_day_to_hours(val):
    """
    CONFIRMED: All TAT and delay columns are stored as Excel fractional days.
    Examples from actual data:
      0.2916 → 7.00h  (Designed Running TAT)
      0.2756 → 6.62h  (Actual Running TAT)
      0.0138 → 0.33h  (Delay Departure - ~20 min)
      0.375  → 9.00h  (Total TAT)
    Formula: hours = value × 24
    """
    s = safe_str(val)
    if not s: return None
    try:
        f = float(s)
        if f == 0.0: return 0.0
        if 0 < f < 10:
            return round(f * 24, 4)   # fractional day → hours
        if f > 40000:
            # datetime serial → extract time-of-day portion × 24
            return round((f - int(f)) * 24, 4)
        # f >= 10: treat as already in hours
        return round(f, 4)
    except (ValueError, TypeError):
        return None

def xl_serial_to_date(val):
    """Excel serial → YYYY-MM-DD. e.g. 46113.0 → 2026-04-01"""
    s = safe_str(val)
    if not s: return None
    try:
        f = float(s)
        if f > 2:
            return (EXCEL_EPOCH + timedelta(days=f)).strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        pass
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%m/%d/%Y'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass
    return None

def xl_serial_to_dt(val):
    """Excel serial → datetime object."""
    s = safe_str(val)
    if not s: return None
    try:
        f = float(s)
        if f > 2:
            return EXCEL_EPOCH + timedelta(days=f)
    except (ValueError, TypeError):
        pass
    return None

# ── Require key columns ───────────────────────────────────────
for req in ['date', 'week', 'perf']:
    if not resolved.get(req):
        raise ValueError(f"Column '{COLS[req]}' not found. Available: {list(df.columns)}")

# ── Build records ─────────────────────────────────────────────
records = []
skipped = 0

for _, row in df.iterrows():
    # Date
    date_str = xl_serial_to_date(gv(row, 'date'))
    if not date_str: skipped += 1; continue

    # Week
    week = safe_str(gv(row, 'week'))
    if not week: skipped += 1; continue

    # Performance
    perf = safe_str(gv(row, 'perf'))
    if perf == 'On Time':     st = 'On Time'
    elif perf == 'Breached':  st = 'Breached'
    elif perf in ('Running On Time', 'Running-Delay'): st = perf
    else: skipped += 1; continue

    # Departure delay in hours:
    # Method 1: ATD - STD (most accurate, confirmed both are datetime serials)
    std_dt = xl_serial_to_dt(gv(row, 'std'))
    atd_dt = xl_serial_to_dt(gv(row, 'atd'))
    if std_dt and atd_dt:
        dp = round((atd_dt - std_dt).total_seconds() / 3600, 4)
    else:
        # Method 2: pre-computed column (fractional day → hours)
        dp = frac_day_to_hours(gv(row, 'dep'))

    # All TAT / delay columns → fractional day × 24 = hours
    pl = frac_day_to_hours(gv(row, 'pl'))
    rT = frac_day_to_hours(gv(row, 'rT'))
    aR = frac_day_to_hours(gv(row, 'aR'))
    dT = frac_day_to_hours(gv(row, 'dT'))
    aT = frac_day_to_hours(gv(row, 'aT'))

    trip_st = safe_str(gv(row, 'status'))
    sta     = xl_serial_to_date(gv(row, 'dest_sta'))
    reason  = safe_str(gv(row, 'reason_lh')) or safe_str(gv(row, 'reason_std'))

    records.append({
        'd':   date_str,
        'w':   week,
        'l':   safe_str(gv(row, 'lane')),
        'o':   safe_str(gv(row, 'origin')),
        't':   safe_str(gv(row, 'dep_type')),
        'v':   safe_str(gv(row, 'vendor')),
        'pl':  pl,
        'dp':  dp,
        'rT':  rT,
        'aR':  aR,
        'dT':  dT,
        'aT':  aT,
        'st':  st,
        'rs':  reason,
        'rS':  trip_st,
        'sta': sta,
    })

print(f"Records built: {len(records)}, skipped: {skipped}")

# ── Sanity check ──────────────────────────────────────────────
closed  = [r for r in records if 'closed' in r['rS'].lower()]
ot      = sum(1 for r in closed if r['st'] == 'On Time')
br      = sum(1 for r in closed if r['st'] == 'Breached')
rT_ok   = sum(1 for r in records if r['rT'] is not None)
dp_ok   = sum(1 for r in records if r['dp'] is not None)
dep_adh = sum(1 for r in closed if r['dp'] is not None and r['dp'] <= 0.5)
dep_den = sum(1 for r in closed if r['dp'] is not None)

print(f"  Closed: {len(closed)} | On Time: {ot} | Breached: {br}")
print(f"  rT parsed OK: {rT_ok}/{len(records)}")
print(f"  dp parsed OK: {dp_ok}/{len(records)}")
print(f"  Dep ADH (<=30min): {dep_adh}/{dep_den} = {round(dep_adh/dep_den*100,1) if dep_den else 0}%")
if records:
    r0 = records[0]
    print(f"  Sample: rT={r0['rT']}h aR={r0['aR']}h dp={r0['dp']}h pl={r0['pl']}h st={r0['st']} rS='{r0['rS']}'")

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

print(f"\nDashboard rebuilt: {meta['updated']}")
