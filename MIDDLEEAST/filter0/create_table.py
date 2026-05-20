"""
Flight CSV -> DuckDB Processor
------------------------------
Configure the parameters below, then run:
    python process_flights.py

Requirements:
    pip install duckdb
"""

import duckdb
import re
import csv
import time
from datetime import datetime
from pathlib import Path

# ===========================================================================
#  CONFIGURATION — edit these values before running
# ===========================================================================

CSV_FILE_PATH = r"C:\Users\cagri\Desktop\Agency\MiddleEast\Filter-0\merged_MiddleEast.csv"   # Path to your input CSV file
DATABASE_DIR = Path(r"C:\DuckDB")
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME
TABLE_NAME    = "MIDDLEEAST_RAW"                     # Table name inside DuckDB
CHUNK_SIZE    = 200_000                             # Rows per batch insert
DELIMITER     = None                               # None = auto-detect, or ',' or '\t'
MAX_ROW = 500_000

# ===========================================================================

MONTH_MAP = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
}

BASE_COLS = [
    'DAIS', 'TRNN', 'TDNR', 'TRNC', 'STAT', 'PNRR',
    'Class', 'FareBasis', 'FirstSectordate', 'LastSectordate',
    'PaxName', 'AirlineName', 'AirlineCode', '_SourceFile', '_SourceSheet'
]

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_flight_date(date_str, first_sector_date_str, dais_str):
    date_str = date_str.strip()

    # Full datetime format: M/D/YYYY H:MM
    full_match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
    if full_match:
        m, d, y = int(full_match.group(1)), int(full_match.group(2)), int(full_match.group(3))
        try:
            correct_year = datetime.strptime(first_sector_date_str.strip(), '%m/%d/%Y %H:%M').year
        except ValueError:
            correct_year = y
        if abs(y - correct_year) > 2:
            y = correct_year
        return f"{m:02d}/{d:02d}/{y}"

    # Short format: DDMMM (e.g. 07JAN, 26MAY)
    short_match = re.match(r'^(\d{1,2})([A-Z]{3})$', date_str)
    if short_match:
        day   = int(short_match.group(1))
        month = MONTH_MAP.get(short_match.group(2), 1)
        try:
            year = datetime.strptime(first_sector_date_str.strip(), '%m/%d/%Y %H:%M').year
        except ValueError:
            try:
                year = datetime.strptime(dais_str.strip(), '%m/%d/%Y %H:%M').year
            except ValueError:
                year = datetime.now().year
        return f"{month:02d}/{day:02d}/{year}"

    return date_str


def split_flight_nos(raw):
    tokens = re.findall(r'[A-Z]{2}-?\d+', raw)
    return [t.replace('-', '') for t in tokens]


def split_flight_dates(raw, first_sector_date_str, dais_str):
    raw = raw.strip()
    if re.match(r'^\d{1,2}/\d{1,2}/\d{4}', raw):
        return [parse_flight_date(raw, first_sector_date_str, dais_str)]
    tokens = re.findall(r'\d{1,2}[A-Z]{3}', raw)
    if tokens:
        return [parse_flight_date(t, first_sector_date_str, dais_str) for t in tokens]
    return [parse_flight_date(raw, first_sector_date_str, dais_str)]


def split_sectors(raw):
    """
    Extract the ordered list of airports from a sector string.

    Handles slash-separated pairs: 'BHH/JED JED/DAC DAC/DMM DMM/BHH'
    -> Build route by taking ORIG of first leg then DEST of every leg:
       [BHH, JED, DAC, DMM, BHH]  (always = num_flights + 1)

    Handles bare codes: 'BOM RUH'
    -> [BOM, RUH]
    """
    raw = raw.strip()
    tokens = raw.split()
    if not tokens:
        return []

    if '/' in tokens[0]:
        # ORIG/DEST pairs - build route
        airports = []
        for token in tokens:
            parts = token.split('/')
            if len(parts) == 2 and parts[0] and parts[1]:
                if not airports:
                    airports.append(parts[0])   # first origin
                airports.append(parts[1])       # each destination
        return airports
    else:
        # Bare airport codes
        return [t for t in tokens if t]

# ---------------------------------------------------------------------------
# Auto-detect delimiter
# ---------------------------------------------------------------------------

def detect_delimiter(csv_path):
    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        sample = f.read(4096)
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample, delimiters=',\t|;')
        return dialect.delimiter
    except csv.Error:
        return ','

# ---------------------------------------------------------------------------
# Pre-scan: find max column counts
# ---------------------------------------------------------------------------

def detect_max_counts(csv_path, delimiter):
    print("Scanning file to detect column counts...")
    max_flights = max_dates = max_airports = 0
    total_rows = 0

    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            row = {k: (v.strip() if v else v) for k, v in row.items()}
            fn = split_flight_nos(row.get('FlightNo', '') or '')
            fd = split_flight_dates(
                     row.get('FlightDate', '') or '',
                     row.get('FirstSectordate', '') or '',
                     row.get('DAIS', '') or '')
            ap = split_sectors(row.get('Sector', '') or '')
            max_flights  = max(max_flights,  len(fn))
            max_dates    = max(max_dates,    len(fd))
            max_airports = max(max_airports, len(ap))
            total_rows  += 1
            if total_rows % MAX_ROW == 0:
                print(f"   ... scanned {total_rows:,} rows")

    print(f"   Total rows      : {total_rows:,}")
    print(f"   Max FlightNos   : {max_flights}")
    print(f"   Max FlightDates : {max_dates}")
    print(f"   Max Airports    : {max_airports}")
    return max_flights, max_dates, max_airports, total_rows

# ---------------------------------------------------------------------------
# Schema & row builder
# ---------------------------------------------------------------------------

def build_all_cols(max_flights, max_dates, max_airports):
    return (
        BASE_COLS
        + [f'FlightNo{i+1}'   for i in range(max_flights)]
        + [f'FlightDate{i+1}' for i in range(max_dates)]
        + [f'Airport{i+1}'    for i in range(max_airports)]
    )


def process_row(row, all_cols):
    fn = split_flight_nos(row.get('FlightNo', '') or '')
    fd = split_flight_dates(
             row.get('FlightDate', '') or '',
             row.get('FirstSectordate', '') or '',
             row.get('DAIS', '') or '')
    ap = split_sectors(row.get('Sector', '') or '')

    values = {}
    for c in BASE_COLS:
        values[c] = row.get(c) or None
    for i, v in enumerate(fn):
        values[f'FlightNo{i+1}'] = v
    for i, v in enumerate(fd):
        values[f'FlightDate{i+1}'] = v
    for i, v in enumerate(ap):
        values[f'Airport{i+1}'] = v

    return [values.get(c) for c in all_cols]

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    csv_path = str(Path(CSV_FILE_PATH).resolve())
    db_path  = str(Path(DB_PATH).resolve())

    if not Path(csv_path).exists():
        print(f"ERROR: File not found -> {csv_path}")
        return

    # Resolve delimiter
    delimiter = DELIMITER if DELIMITER else detect_delimiter(csv_path)
    print(f"Delimiter : {repr(delimiter)}")

    print(f"\n{'='*60}")
    print(f"  Input  : {csv_path}")
    print(f"  Output : {db_path}")
    print(f"  Table  : {TABLE_NAME}")
    print(f"{'='*60}\n")

    t0 = time.time()

    # Step 1: Pre-scan
    max_flights, max_dates, max_airports, total_rows = detect_max_counts(csv_path, delimiter)
    all_cols = build_all_cols(max_flights, max_dates, max_airports)

    # Step 2: Create DuckDB table
    print(f"\nCreating table '{TABLE_NAME}' ({len(all_cols)} columns)...")
    con = duckdb.connect(db_path)
    con.execute(f'DROP TABLE IF EXISTS "{TABLE_NAME}"')
    col_defs = ', '.join([f'"{c}" VARCHAR' for c in all_cols])
    con.execute(f'CREATE TABLE "{TABLE_NAME}" ({col_defs})')

    col_list     = ', '.join([f'"{c}"' for c in all_cols])
    placeholders = ', '.join(['?' for _ in all_cols])
    insert_sql   = f'INSERT INTO "{TABLE_NAME}" ({col_list}) VALUES ({placeholders})'

    # Step 3: Stream and batch-insert
    print(f"\nInserting rows...")
    inserted = 0
    batch    = []
    t_start  = time.time()

    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            # Trim every value in the row before any processing
            row = {k: (v.strip() if v else v) for k, v in row.items()}
            batch.append(process_row(row, all_cols))
            if len(batch) >= CHUNK_SIZE:
                con.executemany(insert_sql, batch)
                inserted += len(batch)
                batch = []
                elapsed = time.time() - t_start
                rate    = inserted / elapsed if elapsed > 0 else 0
                pct     = (inserted / total_rows * 100) if total_rows > 0 else 0
                print(f"   {inserted:>9,} / {total_rows:,}  ({pct:5.1f}%)  {rate:,.0f} rows/sec")

        if batch:
            con.executemany(insert_sql, batch)
            inserted += len(batch)

    # Step 4: Verify
    count = con.execute(f'SELECT COUNT(*) FROM "{TABLE_NAME}"').fetchone()[0]
    con.close()

    elapsed_total = time.time() - t0
    print(f"\n{'='*60}")
    print(f"  Done!    {inserted:,} rows in {elapsed_total:.1f}s")
    print(f"  Verified : {count:,} rows in '{TABLE_NAME}'")
    print(f"  Columns  : {len(all_cols)}")
    print(f"             FlightNos   : FlightNo1 -> FlightNo{max_flights}")
    print(f"             FlightDates : FlightDate1 -> FlightDate{max_dates}")
    print(f"             Airports    : Airport1 -> Airport{max_airports}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()