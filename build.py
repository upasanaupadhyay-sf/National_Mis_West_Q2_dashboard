import os, requests, pandas as pd, json, re
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
    print(f"Export URL failed ({e1}), trying gviz URL...")
    try:
        res = requests.get(URL_GVIZ, timeout=30)
        res.raise_for_status()
        print("Gviz URL worked")
    except Exception as e2:
        print(f"Both URLs failed. e1={e1}, e2={e2}")
        raise e2

# ── Auto-detect header row ────────────────────────────────────
ANCHOR_COLS = ['Date of Connection', 'Trip Starting Week', 'Overall Performance',
               'Lane Code', 'Origin_DC', 'Vendor Name']
raw_text = res.text
lines = raw_text.splitlines()

header_row_idx = 0
for i, line in enumerate(lines[:20]):
    if any(anchor in line for anchor in ANCHOR_COLS):
        header_row_idx = i
        print(f"Header row found at line index: {i}")
        break

df = pd.read_csv(StringIO(raw_text), header=header_row_idx)
df.columns = [str(c).strip() for c in df.columns]
print(f"Loaded {len(df)} rows, {len(df.columns)} columns")

# ── DEBUG: print all column names & first 3 raw values ───────
print("\n=== ALL COLUMNS & SAMPLE VALUES (first 3 rows) ===")
for col in df.columns:
    vals = df[col].head(3).tolist()
    print(f"  '{col}': {vals}")
print("=== END COLUMNS ===\n")

# ── Column name mapping ───────────────────────────────────────
COL_MAP = {
    'Date of Connection':                    ['Date of Connection', 'Date Of Connection'],
    'Trip Starting Week':                    ['Trip Starting Week', 'Week'],
    'Departure Type':                        ['Departure Type', 'DepartureType'],
    'Vendor Name':                           ['Vendor Name', 'Vendor _ Name', 'Vendor_Name', 'VendorName'],
    'Lane Code':                             ['Lane Code', 'Lane', 'LaneCode'],
    'Origin_DC':                             ['Origin_DC', 'Origin DC', 'Location ', 'Location', 'Origin'],
    'Vehicle STD':                           ['Vehicle STD', 'STD', 'Scheduled Time of Departure'],
    'Vehicle ATD':                           ['Vehicle ATD', 'ATD', 'Actual Time of Departure'],
    'Delay Placement (Hr)':                  ['Delay Placement (Hr)', 'Delay Placement (Hrs)', 'Delay Placement', 'Placement Delay (Hr)', 'Placement Delay'],
    'Delay Departure (Hrs)':                 ['Delay Departure (Hrs)', 'Delay Departure (Hr)', 'Delay Departure', 'Departure Delay'],
    'Designed Running TAT':                  ['Designed Running TAT', 'Design Running TAT', 'Designed Running TAT (Hrs)', 'Running TAT (Design)'],
    'Actual Running TAT':                    ['Actual Running TAT', 'Actual Running TAT (Hrs)', 'Running TAT (Actual)'],
    'Design + Holding = Total TAT':          ['Design + Holding = Total TAT', 'Designed Total TAT', 'Design Total TAT', 'Total TAT (Design)'],
    'Actual (Design + Holding) = Total TAT': ['Actual (Design + Holding) = Total TAT', 'Actual Total TAT', 'Total TAT (Actual)'],
    'Trip Status':                           ['Trip Status'],
    'Overall Performance':                   ['Overall Performance'],
    'Destination-STA':                       ['Destination-STA', 'Destination STA', 'Dest STA', 'Destination STA Date'],
    'LH Detailed Reason':                    ['LH Detailed Reason', 'LH Reason', 'Detailed Reason'],
    'Standard Comment':                      ['Standard Comment'],
}

def resolve_col(col_key):
    for candidate in COL_MAP.get(col_key, [col_key]):
        if candidate in df.columns:
            return candidate
    return None

def get_val(row, col_key):
    col = resolve_col(col_key)
    if col is None:
        return None
    val = row.get(col)
    if isinstance(val, float) and pd.isna(val):
        return None
    return val

# Print resolved columns
print("=== COLUMN RESOLUTION ===")
for key in COL_MAP:
    r = resolve_col(key)
    print(f"  {'✓' if r else '✗'} {key} → {r or 'NOT FOUND'}")
print("=== END RESOLUTION ===\n")

# ── Helpers ───────────────────────────────────────────────────
EXCEL_EPOCH = datetime(1899, 12, 30)

def parse_time_str(s):
    """Parse HH:MM:SS or H:MM:SS string to hours (float)."""
    s = str(s).strip()
    m = re.match(r'^(\d+):(\d{2}):(\d{2})$', s)
    if m:
        return int(m.group(1)) + int(m.group(2))/60 + int(m.group(3))/3600
    return None

def xl_to_date_str(val):
    if val is None: return None
    s = str(val).strip()
    if s in ('', 'nan', 'None', '#REF!'): return None
    # Try Excel serial
    try:
        f = float(s)
        if f > 2:
            return (EXCEL_EPOCH + timedelta(days=f)).strftime('%Y-%m-%d')
    except (TypeError, ValueError):
        pass
    # Try date string formats
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%m/%d/%Y', '%d %b %Y'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass
    return None

def xl_to_datetime_obj(val):
    if val is None: return None
    s = str(val).strip()
    if s in ('', 'nan', 'None', '#REF!'): return None
    try:
        f = float(s)
        if f > 2:
            return EXCEL_EPOCH + timedelta(days=f)
    except (TypeError, ValueError):
        pass
    # Try datetime string formats
    for fmt in ('%Y-%m-%d %H:%M:%S', '%d-%m-%Y %H:%M:%S', '%d/%m/%Y %H:%M'):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None

def xl_to_hours(val):
    """Convert various formats to hours (float)."""
    if val is None: return None
    s = str(val).strip()
    if s in ('', 'nan', 'None', '#REF!'): return None

    # HH:MM:SS format (e.g. "7:00:00" = 7 hours)
    t = parse_time_str(s)
    if t is not None:
        return round(t, 4)

    try:
        f = float(s)
        # Excel fractional day (0 < f <= 2 means it's a time fraction)
        if 0 < f <= 2:
            return round(f * 24, 4)
        # Excel datetime serial (f > 2) — extract time portion only
        if f > 2:
            frac = f - int(f)
            return round(frac * 24, 4)
        # Could already be in hours (f > 0 but small means fraction of day)
        return round(f * 24, 4) if 0 < f < 1 else round(f, 4)
    except (TypeError, ValueError):
        pass
    return None

def dep_delay_hours(std_val, atd_val, fallback_val):
    """
    Compute departure delay in hours = ATD - STD.
    Falls back to pre-computed delay column if STD/ATD not available.
    """
    std_dt = xl_to_datetime_obj(std_val)
    atd_dt = xl_to_datetime_obj(atd_val)
    if std_dt and atd_dt:
        return round((atd_dt - std_dt).total_seconds() / 3600, 4)
    # fallback: pre-computed column
    return xl_to_hours(fallback_val)

def safe_str(val):
    if val is None: return ''
    s = str(val).strip()
    return '' if s in ('#REF!', 'nan', 'None', '') else s

# ── Verify required columns ───────────────────────────────────
missing = [r for r in ['Date of Connection', 'Trip Starting Week', 'Overall Performance']
           if resolve_col(r) is None]
if missing:
    print(f"ERROR: Required columns not found: {missing}")
    print(f"All columns: {list(df.columns)}")
    raise ValueError(f"Required columns missing: {missing}")

# ── Build records ─────────────────────────────────────────────
records = []
skipped = 0

for _, row in df.iterrows():
    date_str = xl_to_date_str(get_val(row, 'Date of Connection'))
    if not date_str:
        skipped += 1
        continue

    week = safe_str(get_val(row, 'Trip Starting Week'))
    if not week:
        skipped += 1
        continue

    # Departure ADH: ATD - STD in hours (positive = late, negative = early)
    dp = dep_delay_hours(
        get_val(row, 'Vehicle STD'),
        get_val(row, 'Vehicle ATD'),
        get_val(row, 'Delay Departure (Hrs)')
    )

    pl_h = xl_to_hours(get_val(row, 'Delay Placement (Hr)'))
    rT   = xl_to_hours(get_val(row, 'Designed Running TAT'))
    aR   = xl_to_hours(get_val(row, 'Actual Running TAT'))
    dT   = xl_to_hours(get_val(row, 'Design + Holding = Total TAT'))
    aT   = xl_to_hours(get_val(row, 'Actual (Design + Holding) = Total TAT'))
    sta  = xl_to_date_str(get_val(row, 'Destination-STA'))

    overall = safe_str(get_val(row, 'Overall Performance'))
    if overall == 'On Time':         st = 'On Time'
    elif overall == 'Breached':      st = 'Breached'
    elif overall in ('Running On Time', 'Running-Delay'): st = overall
    else:
        skipped += 1
        continue

    trip_status = safe_str(get_val(row, 'Trip Status'))
    reason = safe_str(get_val(row, 'LH Detailed Reason')) or safe_str(get_val(row, 'Standard Comment'))

    records.append({
        'd':   date_str,
        'w':   week,
        'l':   safe_str(get_val(row, 'Lane Code')),
        'o':   safe_str(get_val(row, 'Origin_DC')),
        't':   safe_str(get_val(row, 'Departure Type')),
        'v':   safe_str(get_val(row, 'Vendor Name')),
        'pl':  pl_h,
        'dp':  dp,
        'rT':  rT,
        'aR':  aR,
        'dT':  dT,
        'aT':  aT,
        'st':  st,
        'rs':  reason,
        'rS':  trip_status,
        'sta': sta,
    })

print(f"Records built: {len(records)}, skipped: {skipped}")

# Debug: show sample parsed values
if records:
    print("\n=== SAMPLE PARSED RECORD (first 3) ===")
    for r in records[:3]:
        print(f"  date={r['d']} week={r['w']} lane={r['l']}")
        print(f"  pl={r['pl']} dp={r['dp']} rT={r['rT']} aR={r['aR']} dT={r['dT']} aT={r['aT']}")
        print(f"  st={r['st']} tripStatus={r['rS']} sta={r['sta']}")
        print()
    print("=== END SAMPLE ===\n")

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
rT_ok   = sum(1 for r in closed if r['rT'] is not None)
dep_pct = round(dep_ok / dep_den * 100, 1) if dep_den else 0
pct_ot  = round(ot / (ot+br) * 100, 1) if (ot+br) else 0

print(f"Dashboard rebuilt successfully!")
print(f"  Total records    : {len(records)}")
print(f"  Closed trips     : {len(closed)}")
print(f"  On-time          : {ot} ({pct_ot}%)")
print(f"  Breached         : {br}")
print(f"  Records with rT  : {rT_ok}")
print(f"  Dep ADH (STD+30m): {dep_pct}%  ({dep_ok}/{dep_den})")
print(f"  Updated at       : {meta['updated']}")
