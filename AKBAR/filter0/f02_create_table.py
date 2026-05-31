"""
Ultra-Fast Flight CSV -> DuckDB Loader
Optimized for 5M+ rows
"""

import duckdb
import pandas as pd
import re
import csv
import time
import os

from pathlib import Path

# ============================================================================
# CONFIG
# ============================================================================

CSV_FILE_PATH = r"C:\Users\cagri\Desktop\Agency\Akbar\filter-0\merged_Akbar.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "AKBAR_RAW_V2"
CHUNK_SIZE = 1_000_000
DELIMITER = None
MAX_FLIGHTS = 4
MAX_DATES = 4
MAX_AIRPORTS = 5

# ============================================================================

MONTH_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

BASE_COLS = [
    "DAIS",
    "TRNN",
    "TDNR",
    "TRNC",
    "STAT",
    "PNRR",
    "Class",
    "FareBasis",
    "FirstSectordate",
    "LastSectordate",
    "PaxName",
    "AirlineName",
    "AirlineCode",
    "_SourceFile",
    "_SourceSheet",
]

# ============================================================================
# PRECOMPILED REGEX
# ============================================================================

RE_FLIGHT_NO = re.compile(r"[A-Z]{2}-?\d+")
RE_FULL_DATE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})")  # MM/DD/YYYY or M/D/YYYY
RE_ISO_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")  # YYYY-MM-DD...
RE_SHORT_DATE = re.compile(r"^(\d{1,2})\s*([A-Za-z]{3})$")  # 12FEB or "12 FEB"
RE_DATE_TOKEN = re.compile(r"\d{1,2}\s*[A-Za-z]{3}")
RE_DMY_SHORT = re.compile(r"^(\d{1,2})-([A-Za-z]{3})-(\d{2})$")  # 27-Oct-25

# ============================================================================
# YEAR EXTRACTION  (THE KEY FIX)
# ============================================================================


def extract_year_from_datestr(s):
    """
    Robustly extract the year from any of these formats:
      - YYYY-MM-DD [HH:MM:SS]   e.g. "2020-01-07 00:00:00"
      - MM/DD/YYYY [H:MM]       e.g. "1/7/2020 0:00"
      - DD-MMM-YYYY             e.g. "07-JAN-2020"
    Returns int year or None.
    """
    if not s:
        return None

    s = s.strip()

    # ISO: YYYY-MM-DD
    m = RE_ISO_DATE.match(s)
    if m:
        return int(m.group(1))

    # MM/DD/YYYY
    m = RE_FULL_DATE.match(s)
    if m:
        return int(m.group(3))  # group(3) is the 4-digit year

    # DD-MMM-YYYY  (less common but handle it)
    m = re.match(r"(\d{1,2})-[A-Z]{3}-(\d{4})", s)
    if m:
        return int(m.group(2))

    return None


# ============================================================================
# DATE PARSING
# ============================================================================
def parse_flight_date(date_str, ref_year):
    date_str = date_str.strip()
    if not date_str:
        return None

    # Already ISO: 2026-02-09 00:00:00
    m = RE_ISO_DATE.match(date_str)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if ref_year:
            y = ref_year
        return f"{y}-{mo:02d}-{d:02d} 00:00:00"

    # MM/DD/YYYY
    m = RE_FULL_DATE.match(date_str)
    if m:
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if ref_year:
            y = ref_year
        return f"{y}-{mo:02d}-{d:02d} 00:00:00"

    # ── NEW: DD-MMM-YY  e.g. "27-Oct-25", "2-Aug-25" ─────────────────────
    m = RE_DMY_SHORT.match(date_str)
    if m:
        d = int(m.group(1))
        mon = m.group(2).upper()  # normalise to uppercase for MONTH_MAP
        mo = MONTH_MAP.get(mon, 1)
        yy = int(m.group(3))
        # 2-digit year → 4-digit: 00-49 → 2000s, 50-99 → 1900s
        y = (2000 + yy) if yy < 50 else (1900 + yy)
        if ref_year:
            y = ref_year  # still trust booking year if available
        return f"{y}-{mo:02d}-{d:02d} 00:00:00"

    # DD MMM or DDMMM  e.g. "28NOV", "19 SEP"
    m = RE_SHORT_DATE.match(date_str)
    if m:
        d = int(m.group(1))
        mon = m.group(2).upper()  # .upper() added here too
        mo = MONTH_MAP.get(mon, 1)
        y = ref_year or 2025
        return f"{y}-{mo:02d}-{d:02d} 00:00:00"

    return date_str  # return raw if nothing matched


def split_flight_dates(raw, ref_year):
    raw = raw.strip()
    if not raw:
        return []

    # Single full date
    if RE_FULL_DATE.match(raw) or RE_ISO_DATE.match(raw):
        return [parse_flight_date(raw, ref_year)]

    # Multiple short tokens: "07JAN 09JAN" or "12 FEB 04 APR"
    tokens = RE_DATE_TOKEN.findall(raw)
    if tokens:
        # Normalise "12 FEB" → "12FEB"
        tokens = [t.replace(" ", "") for t in tokens]
        return [parse_flight_date(t, ref_year) for t in tokens]

    return [parse_flight_date(raw, ref_year)]


# ============================================================================
# FLIGHT / SECTOR HELPERS  (unchanged)
# ============================================================================


def split_flight_nos(raw):
    return [x.replace("-", "") for x in RE_FLIGHT_NO.findall(raw)]


def split_sectors(raw):
    raw = raw.strip()
    if not raw:
        return []
    tokens = raw.split()
    if "/" in tokens[0]:
        airports = []
        for token in tokens:
            parts = token.split("/")
            if len(parts) == 2:
                if not airports:
                    airports.append(parts[0])
                airports.append(parts[1])
        return airports
    return tokens


def detect_delimiter(csv_path):
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        sample = f.read(4096)
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample, delimiters=",\t|;")
        return dialect.delimiter
    except:
        return ","


# ============================================================================
# SCHEMA
# ============================================================================


def build_all_cols():
    # id is added at the DB level via DEFAULT gen_random_uuid(), not in Python
    return (
        BASE_COLS
        + [f"FlightNo{i + 1}" for i in range(MAX_FLIGHTS)]
        + [f"FlightDate{i + 1}" for i in range(MAX_DATES)]
        + [f"Airport{i + 1}" for i in range(MAX_AIRPORTS)]
    )


def process_row(row, all_cols):
    get = row.get
    dais = (get("DAIS") or "").strip()
    ref_year = extract_year_from_datestr(dais)

    fn = split_flight_nos((get("FlightNo") or "").strip())
    fd = split_flight_dates((get("FlightDate") or "").strip(), ref_year)
    ap = split_sectors((get("Sector") or "").strip())

    values = {}
    for c in BASE_COLS:
        v = get(c)
        values[c] = v.strip() if isinstance(v, str) else v

    for i, v in enumerate(fn[:MAX_FLIGHTS]):
        values[f"FlightNo{i + 1}"] = v

    for i, v in enumerate(fd[:MAX_DATES]):
        values[f"FlightDate{i + 1}"] = v

    for i, v in enumerate(ap[:MAX_AIRPORTS]):
        values[f"Airport{i + 1}"] = v

    return [values.get(c) for c in all_cols]


# ============================================================================
# MAIN
# ============================================================================


def main():
    csv_path = str(Path(CSV_FILE_PATH).resolve())
    db_path = str(Path(DB_PATH).resolve())

    if not Path(csv_path).exists():
        print(f"ERROR: File not found -> {csv_path}")
        return

    delimiter = DELIMITER or detect_delimiter(csv_path)

    print(f"\n{'=' * 70}")
    print("ULTRA FAST CSV -> DUCKDB LOADER")
    print(f"{'=' * 70}")
    print(f"CSV       : {csv_path}")
    print(f"Database  : {db_path}")
    print(f"Table     : {TABLE_NAME}")
    print(f"Delimiter : {repr(delimiter)}")
    print(f"{'=' * 70}\n")

    start_total = time.time()

    con = duckdb.connect(db_path)
    con.execute(f"PRAGMA threads={os.cpu_count()}")
    try:
        con.execute("SET memory_limit='16GB'")
    except:
        pass

    # ── Create table with UUID primary key ───────────────────────────────────
    all_cols = build_all_cols()
    print(f"Creating table ({len(all_cols)} data columns + id UUID)...")

    con.execute(f'DROP TABLE IF EXISTS "{TABLE_NAME}"')

    col_defs = ", ".join([f'"{c}" VARCHAR' for c in all_cols])
    con.execute(f"""
        CREATE TABLE "{TABLE_NAME}" (
            id  UUID DEFAULT gen_random_uuid(),
            {col_defs}
        )
    """)

    # ── Load ─────────────────────────────────────────────────────────────────
    inserted = 0
    batch = []
    start_insert = time.time()
    print("Loading rows...\n")

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=delimiter)

        for row in reader:
            batch.append(process_row(row, all_cols))

            if len(batch) >= CHUNK_SIZE:
                batch_df = pd.DataFrame(batch, columns=all_cols)
                # Let DuckDB fill id automatically via DEFAULT
                col_list = ", ".join(f'"{c}"' for c in all_cols)
                con.execute(
                    f'INSERT INTO "{TABLE_NAME}" ({col_list}) SELECT * FROM batch_df'
                )
                inserted += len(batch)
                elapsed = time.time() - start_insert
                rate = inserted / elapsed if elapsed > 0 else 0
                print(f"{inserted:>12,} rows   {rate:>10,.0f} rows/sec")
                batch.clear()

        if batch:
            batch_df = pd.DataFrame(batch, columns=all_cols)
            col_list = ", ".join(f'"{c}"' for c in all_cols)
            con.execute(
                f'INSERT INTO "{TABLE_NAME}" ({col_list}) SELECT * FROM batch_df'
            )
            inserted += len(batch)

    # ── Verify ───────────────────────────────────────────────────────────────
    count = con.execute(f'SELECT COUNT(*) FROM "{TABLE_NAME}"').fetchone()[0]
    con.close()

    elapsed_total = time.time() - start_total

    print(f"\n{'=' * 70}")
    print("DONE")
    print(f"{'=' * 70}")
    print(f"Inserted Rows : {inserted:,}")
    print(f"Verified Rows : {count:,}")
    print(f"Elapsed Time  : {elapsed_total:.1f} sec")
    print(f"Rows / Second : {inserted / elapsed_total:,.0f}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
