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

raw_text = res.text
lines    = raw_text.splitlines()

# ── Print first 5 lines for debugging ─────────────────────────
print("\n=== FIRST 5 RAW CSV LINES ===")
for i, line in enumerate(lines[:5]):
    print(f"  [{i}]: {line[:120]}")
print("===\n")

# ── Find the real header row ───────────────────────────────────
# The sheet has a formula in A1 which means the CSV could have:
# Case A: Row 0 = real headers (Date of Connection, Trip Starting Week...)
# Case B: Row 0 = formula/garbage, Row 1 = real headers
# Case C: Row 0 = formula, Row 1 = headers (with computed values mixed)
#
# Strategy: scan rows 0-10, find the row that contains the MOST
# known column names → that is the real header row.

KNOWN_HEADERS = [
    'Date of Connection', 'Trip Starting Week', 'Overall Performance',
    'Lane Code', 'Origin_DC', 'Vendor Name', 'Trip Status',
    'Designed Running TAT', 'Actual Running TAT', 'Vehicle STD', 'Vehicle ATD',
    'Delay Departure', 'Delay Placement', 'Destination-STA',
]

best_row = 0
best_score = 0
for i, line in enumerate(lines[:10]):
    score = sum(1 for h in KNOWN_HEADERS if h in line)
    print(f"Row {i}: score={score} | preview: {line[:80]}")
    if score > best_score:
        best_score = score
        best_row = i

print(f"\nBest header row: {best_row} (matched {best_score} known columns)\n")

# Load with detected header row
df = pd.read_csv(StringIO(raw_text), header=best_row)
df.columns = [str(c).strip() for c in df.columns]

# Drop rows where Date of Connection is clearly not a date
# (catches leftover formula rows that sneak into data)
date_col = 'Date of Connection'
if date_col in df.columns:
    # Keep only rows where date column looks like a number or date string
    def looks_like_date(v):
        s = str(v).strip()
        if not s or s in ('nan','None',''): return False
        try:
            f = float(s)
            return 30000 < f < 60000  # Valid Excel date range (1982-2064)
        except ValueError:
            pass
        # Try date string
        for fmt in ('%Y-%m-%d','%d-%m-%Y','%d/%m/%Y'):
            try:
                datetime.strptime(s, fmt)
                return True
            except ValueError:
                pass
        return False

    before = len(df)
    df = df[df[date_col].apply(looks_like_date)].copy()
    df = df.reset_index(drop=True)
    print(f"Filtered out {before - len(df)} non-data rows")

print(f"Final: {len(df)} rows, {len(df.columns)} columns")
print(f"Columns: {list(df.columns)}")

# ── Column resolution ─────────────────────────────────────────
COLS = {
    'date':       'Date of Connection',
    'week':       'Trip Starting Week',
    'dep_type':   'Departure Type',
    'vendor':     'Vendor Name',
    'lane':       'Lane Code',
    'origin':     'Origin_DC',
    'std':        'Vehicle STD',
    'atd':        'Vehicle ATD',
    'pl':         'Delay Placement (Hr)',
    'dep':        'Delay Departure (Hrs)',
    'rT':         'Designed Running TAT',
    'aR':         'Actual Running TAT',
    'dT':         'Design + Holding = Total TAT',
    'aT':         'Actual (Design + Holding) = Total TAT',
    'status':     'Trip Status',
    'perf':       'Overall Performance',
    'dest_sta':   'Destination-STA',
    'reason_lh':  'LH Detailed Reason',
    'reason_std': 'Standard Comment',
}

def find_col(name):
    if name in df.columns: return name
    name_l = name.lower().strip()
    for c in df.columns:
        if name_l in c.lower().strip(): return c
        if c.lower().strip() in name_l: return c
    return None

resolved = {}
print("\n=== COLUMN RESOLUTION ===")
for key, name in COLS.items():
    col = find_col(name)
    resolved[key] = col
    status = f"✓ → '{col}'" if col else "✗ NOT FOUND"
    print(f"  {key:12s}: {status}")
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
    return '' if s in ('#REF!', 'nan', 'None', '') else s

def frac_day_to_hours(val):
    """
    All TAT/delay columns in this sheet are Excel fractional days:
      0.0138  → × 24 = 0.33h  (20 min departure delay)
      0.2916  → × 24 = 7.0h   (Designed Running TAT)
      0.375   → × 24 = 9.0h   (Total TAT)
    """
    s = safe_str(val)
    if not s: return None
    # HH:MM:SS string
    m = re.match(r'^(\d+):(\d{2}):(\d{2})$', s)
    if m:
        return round(int(m.group(1)) + int(m.group(2))/60 + int(m.group(3))/3600, 4)
    try:
        f = float(s)
        if f == 0.0: return 0.0
        if 0 < f < 10:
            return round(f * 24, 4)  # fractional day → hours
        if f > 40000:
            return round((f - int(f)) * 24, 4)  # datetime serial → time portion
        return round(f, 4)  # already hours
    except (ValueError, TypeError):
        return None

def xl_to_date_str(val):
    s = safe_str(val)
    if not s: return None
    try:
        f = float(s)
        if 30000 < f < 60000:
            return (EXCEL_EPOCH + timedelta(days=f)).strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        pass
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%m/%d/%Y'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass
    return None

def xl_to_dt(val):
    s = safe_str(val)
    if not s: return None
    try:
        f = float(s)
        if f > 2:
            return EXCEL_EPOCH + timedelta(days=f)
    except (ValueError, TypeError):
        pass
    return None

# ── Verify required cols ──────────────────────────────────────
missing = [COLS[k] for k in ('date','week','perf') if not resolved.get(k)]
if missing:
    raise ValueError(f"Required columns not found: {missing}. Available: {list(df.columns)}")

# ── Build records ─────────────────────────────────────────────
records = []
skipped = 0

for _, row in df.iterrows():
    date_str = xl_to_date_str(gv(row, 'date'))
    if not date_str: skipped += 1; continue

    week = safe_str(gv(row, 'week'))
    if not week: skipped += 1; continue

    perf = safe_str(gv(row, 'perf'))
    if   perf == 'On Time':   st = 'On Time'
    elif perf == 'Breached':  st = 'Breached'
    elif perf in ('Running On Time','Running-Delay'): st = perf
    else: skipped += 1; continue

    # Departure delay: prefer ATD-STD, fallback to pre-computed column
    std_dt = xl_to_dt(gv(row, 'std'))
    atd_dt = xl_to_dt(gv(row, 'atd'))
    dp = round((atd_dt - std_dt).total_seconds() / 3600, 4) if (std_dt and atd_dt) \
         else frac_day_to_hours(gv(row, 'dep'))

    records.append({
        'd':   date_str,
        'w':   week,
        'l':   safe_str(gv(row, 'lane')),
        'o':   safe_str(gv(row, 'origin')),
        't':   safe_str(gv(row, 'dep_type')),
        'v':   safe_str(gv(row, 'vendor')),
        'pl':  frac_day_to_hours(gv(row, 'pl')),
        'dp':  dp,
        'rT':  frac_day_to_hours(gv(row, 'rT')),
        'aR':  frac_day_to_hours(gv(row, 'aR')),
        'dT':  frac_day_to_hours(gv(row, 'dT')),
        'aT':  frac_day_to_hours(gv(row, 'aT')),
        'st':  st,
        'rs':  safe_str(gv(row,'reason_lh')) or safe_str(gv(row,'reason_std')),
        'rS':  safe_str(gv(row, 'status')),
        'sta': xl_to_date_str(gv(row, 'dest_sta')),
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

print(f"  Trip Closed: {len(closed)} | On Time: {ot} | Breached: {br}")
print(f"  rT parsed:   {rT_ok}/{len(records)}")
print(f"  dp parsed:   {dp_ok}/{len(records)}")
print(f"  Dep ADH:     {dep_adh}/{dep_den} = {round(dep_adh/dep_den*100,1) if dep_den else 0}%")
if records:
    r0 = records[0]
    print(f"  Sample[0]:   rT={r0['rT']}h aR={r0['aR']}h dp={r0['dp']}h st={r0['st']} rS='{r0['rS']}'")

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
