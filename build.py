import os, requests, pandas as pd, json
from io import StringIO
from datetime import datetime, timedelta

# ── Config ────────────────────────────────────────────────────
SHEET_ID  = "1lU18_6sXGMlQG4P-AZf-Qw-auu--4wac3IpcEZyoV5A"
SHEET_GID = "925773389"

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

# ── Auto-detect header row ────────────────────────────────────
# The sheet may have blank/title rows before the real header.
# We scan the first 20 rows to find the one containing 'Date of Connection'
# or a known column anchor like 'Trip Starting Week'.
ANCHOR_COLS = ['Date of Connection', 'Trip Starting Week', 'Overall Performance',
               'Lane Code', 'Origin_DC', 'Vendor Name']

raw_text = res.text
lines = raw_text.splitlines()

header_row_idx = None
for i, line in enumerate(lines[:20]):
    # Check if this line contains any of our anchor column names
    if any(anchor in line for anchor in ANCHOR_COLS):
        header_row_idx = i
        print(f"Header row found at line index: {i} → '{line[:120]}'")
        break

if header_row_idx is None:
    # Fallback: print first 10 lines to debug and try row 0
    print("WARNING: Could not find header row. First 10 lines:")
    for i, line in enumerate(lines[:10]):
        print(f"  [{i}]: {line[:150]}")
    header_row_idx = 0

df = pd.read_csv(StringIO(raw_text), header=header_row_idx)

# Strip whitespace from column names (Google Sheets sometimes adds spaces)
df.columns = [str(c).strip() for c in df.columns]

print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
print(f"Columns found: {list(df.columns[:20])}")  # show first 20 for debug

# ── Column name mapping ───────────────────────────────────────
# Handle slight naming differences across sheets
# (e.g. 'Vendor _ Name' vs 'Vendor Name', 'Location ' vs 'Origin_DC')
COL_MAP = {
    # Standard name : possible alternatives in the sheet
    'Date of Connection': ['Date of Connection', 'Date Of Connection'],
    'Trip Starting Week': ['Trip Starting Week', 'Week'],
    'Departure Type':     ['Departure Type', 'DepartureType'],
    'Vendor Name':        ['Vendor Name', 'Vendor _ Name', 'Vendor_Name', 'VendorName'],
    'Lane Code':          ['Lane Code', 'Lane', 'LaneCode'],
    'Origin_DC':          ['Origin_DC', 'Origin DC', 'Location ', 'Location', 'Origin'],
    'Vehicle STD':        ['Vehicle STD', 'STD'],
    'Vehicle ATD':        ['Vehicle ATD', 'ATD'],
    'Delay Placement (Hr)':  ['Delay Placement (Hr)', 'Delay Placement', 'Placement Delay'],
    'Delay Departure (Hrs)': ['Delay Departure (Hrs)', 'Delay Departure', 'Departure Delay'],
    'Designed Running TAT':  ['Designed Running TAT', 'Design Running TAT'],
    'Actual Running TAT':    ['Actual Running TAT'],
    'Design + Holding = Total TAT':             ['Design + Holding = Total TAT', 'Designed Total TAT', 'Design Total TAT'],
    'Actual (Design + Holding) = Total TAT':    ['Actual (Design + Holding) = Total TAT', 'Actual Total TAT'],
    'Trip Status':            ['Trip Status'],
    'Overall Performance':    ['Overall Performance'],
    'Destination-STA':        ['Destination-STA', 'Destination STA', 'Dest STA'],
    'LH Detailed Reason':     ['LH Detailed Reason', 'LH Reason'],
    'Standard Comment':       ['Standard Comment'],
}

def resolve_col(df, col_key):
    """Return the actual column name in df for a given standard key, or None."""
    for candidate in COL_MAP.get(col_key, [col_key]):
        if candidate in df.columns:
            return candidate
    return None

def get(row, col_key):
    """Safely get a value from a row using the standard column key."""
    col = resolve_col(df, col_key)
    if col is None:
        return None
    val = row.get(col)
    if pd.isna(val) if not isinstance(val, str) else False:
        return None
    return val

# Print resolved columns for debugging
print("\nColumn resolution:")
for key in COL_MAP:
    resolved = resolve_col(df, key)
    status = f"✓ → '{resolved}'" if resolved else "✗ NOT FOUND"
    print(f"  {key}: {status}")

# ── Helpers ───────────────────────────────────────────────────
EXCEL_EPOCH = datetime(1899, 12, 30)

def xl_to_date_str(val):
    if val is None: return None
    try:
        s = str(val).strip()
        if s in ('', 'nan', 'None', '#REF!'): return None
        f = float(s)
        if f > 2:
            return (EXCEL_EPOCH + timedelta(days=f)).strftime('%Y-%m-%d')
    except (TypeError, ValueError):
        pass
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%m/%d/%Y'):
        try:
            return datetime.strptime(str(val).strip(), fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass
    return None

def xl_to_datetime(val):
    if val is None: return None
    try:
        s = str(val).strip()
        if s in ('', 'nan', 'None', '#REF!'): return None
        f = float(s)
        if f > 2:
            return EXCEL_EPOCH + timedelta(days=f)
    except (TypeError, ValueError):
        pass
    return None

def xl_to_hours(val):
    if val is None: return None
    try:
        s = str(val).strip()
        if s in ('', 'nan', 'None', '#REF!'): return None
        f = float(s)
        if f > 2:
            f = f - int(f)  # datetime serial → keep only time portion
        return round(f * 24, 4)
    except (TypeError, ValueError):
        return None

def safe_str(val):
    if val is None: return ''
    s = str(val).strip()
    return '' if s in ('#REF!', 'nan', 'None', '') else s

# ── Verify required columns ───────────────────────────────────
missing = []
for req in ['Date of Connection', 'Trip Starting Week', 'Overall Performance']:
    if resolve_col(df, req) is None:
        missing.append(req)
if missing:
    print(f"\nERROR: Required columns not found: {missing}")
    print(f"All available columns:\n{list(df.columns)}")
    raise ValueError(f"Required columns missing: {missing}")

# ── Build records ─────────────────────────────────────────────
records = []
skipped = 0

for _, row in df.iterrows():
    date_str = xl_to_date_str(get(row, 'Date of Connection'))
    if not date_str:
        skipped += 1
        continue

    week = safe_str(get(row, 'Trip Starting Week'))
    if not week:
        skipped += 1
        continue

    # Departure ADH: actual departure vs STD + 30 min
    std_dt = xl_to_datetime(get(row, 'Vehicle STD'))
    atd_dt = xl_to_datetime(get(row, 'Vehicle ATD'))
    if std_dt and atd_dt:
        dep_delay_h = round((atd_dt - std_dt).total_seconds() / 3600, 4)
    else:
        dep_delay_h = xl_to_hours(get(row, 'Delay Departure (Hrs)'))

    pl_h = xl_to_hours(get(row, 'Delay Placement (Hr)'))
    rT   = xl_to_hours(get(row, 'Designed Running TAT'))
    aR   = xl_to_hours(get(row, 'Actual Running TAT'))
    dT   = xl_to_hours(get(row, 'Design + Holding = Total TAT'))
    aT   = xl_to_hours(get(row, 'Actual (Design + Holding) = Total TAT'))
    sta  = xl_to_date_str(get(row, 'Destination-STA'))

    overall = safe_str(get(row, 'Overall Performance'))
    if overall == 'On Time':       st = 'On Time'
    elif overall == 'Breached':    st = 'Breached'
    elif overall in ('Running On Time', 'Running-Delay'): st = overall
    else:
        skipped += 1
        continue

    trip_status = safe_str(get(row, 'Trip Status'))
    reason      = safe_str(get(row, 'LH Detailed Reason')) or safe_str(get(row, 'Standard Comment'))

    records.append({
        'd':   date_str,
        'w':   week,
        'l':   safe_str(get(row, 'Lane Code')),
        'o':   safe_str(get(row, 'Origin_DC')),
        't':   safe_str(get(row, 'Departure Type')),
        'v':   safe_str(get(row, 'Vendor Name')),
        'pl':  pl_h,
        'dp':  dep_delay_h,
        'rT':  rT,
        'aR':  aR,
        'dT':  dT,
        'aT':  aT,
        'st':  st,
        'rs':  reason,
        'rS':  trip_status,
        'sta': sta,
    })

print(f"\nRecords built: {len(records)}, skipped: {skipped}")

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
closed  = [r for r in records if 'closed' in r['rS'].lower()]
ot      = sum(1 for r in closed if r['st'] == 'On Time')
br      = sum(1 for r in closed if r['st'] == 'Breached')
dep_ok  = sum(1 for r in closed if r['dp'] is not None and r['dp'] <= 0.5)
dep_den = sum(1 for r in closed if r['dp'] is not None)
pct_ot  = round(ot / (ot+br) * 100, 1) if (ot+br) else 0
dep_pct = round(dep_ok / dep_den * 100, 1) if dep_den else 0

print(f"\nDashboard rebuilt successfully!")
print(f"  Total records    : {len(records)}")
print(f"  Closed trips     : {len(closed)}")
print(f"  On-time          : {ot} ({pct_ot}%)")
print(f"  Breached         : {br}")
print(f"  Dep ADH (STD+30m): {dep_pct}%  ({dep_ok}/{dep_den})")
print(f"  Updated at       : {meta['updated']}")
