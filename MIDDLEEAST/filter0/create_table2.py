"""
Ultra-Fast Flight CSV -> DuckDB Loader
Optimized for 5M+ rows
Compatible with older DuckDB versions

Requirements:
    pip install duckdb pandas
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

CSV_FILE_PATH = r"C:\Users\cagri\Desktop\Agency\MiddleEast\Filter-0\merged_MiddleEast.csv"

DATABASE_DIR = Path(r"C:\DuckDB")
DATABASE_NAME = "my_db.duckdb"

DB_PATH = DATABASE_DIR / DATABASE_NAME

TABLE_NAME = "MIDDLEEAST_RAW"

CHUNK_SIZE = 1_000_000

DELIMITER = None

# Avoid expensive pre-scan
MAX_FLIGHTS = 4
MAX_DATES = 4
MAX_AIRPORTS = 5

# ============================================================================

MONTH_MAP = {
    'JAN': 1,
    'FEB': 2,
    'MAR': 3,
    'APR': 4,
    'MAY': 5,
    'JUN': 6,
    'JUL': 7,
    'AUG': 8,
    'SEP': 9,
    'OCT': 10,
    'NOV': 11,
    'DEC': 12
}

BASE_COLS = [
    'DAIS',
    'TRNN',
    'TDNR',
    'TRNC',
    'STAT',
    'PNRR',
    'Class',
    'FareBasis',
    'FirstSectordate',
    'LastSectordate',
    'PaxName',
    'AirlineName',
    'AirlineCode',
    '_SourceFile',
    '_SourceSheet'
]

# ============================================================================
# PRECOMPILED REGEX
# ============================================================================

RE_FLIGHT_NO = re.compile(r'[A-Z]{2}-?\d+')
RE_FULL_DATE = re.compile(r'^(\d{1,2})/(\d{1,2})/(\d{4})')
RE_SHORT_DATE = re.compile(r'^(\d{1,2})([A-Z]{3})$')
RE_DATE_TOKEN = re.compile(r'\d{1,2}[A-Z]{3}')

# ============================================================================
# HELPERS
# ============================================================================

def extract_year(s):

    try:
        y = s[6:10]

        if y.isdigit():
            return int(y)

        return None

    except:
        return None

def parse_flight_date(date_str, first_sector_date_str, dais_str):

    date_str = date_str.strip()

    # Already correct format
    # 2026-02-09 00:00:00
    if (
        len(date_str) >= 19
        and date_str[4] == '-'
        and date_str[7] == '-'
    ):
        return date_str[:19]

    # MM/DD/YYYY
    m = RE_FULL_DATE.match(date_str)

    if m:

        month, day, year = m.groups()

        correct_year = extract_year(first_sector_date_str)

        if correct_year and abs(int(year) - correct_year) > 2:
            year = str(correct_year)

        return (
            f"{year}-"
            f"{int(month):02d}-"
            f"{int(day):02d} 00:00:00"
        )

    # DDMMM
    m = RE_SHORT_DATE.match(date_str)

    if m:

        day, mon = m.groups()

        year = (
            extract_year(first_sector_date_str)
            or extract_year(dais_str)
            or 2025
        )

        month = MONTH_MAP.get(mon, 1)

        return (
            f"{year}-"
            f"{month:02d}-"
            f"{int(day):02d} 00:00:00"
        )

    return date_str


def split_flight_nos(raw):

    return [
        x.replace('-', '')
        for x in RE_FLIGHT_NO.findall(raw)
    ]


def split_flight_dates(raw, first_sector_date_str, dais_str):

    raw = raw.strip()

    if RE_FULL_DATE.match(raw):

        return [
            parse_flight_date(
                raw,
                first_sector_date_str,
                dais_str
            )
        ]

    tokens = RE_DATE_TOKEN.findall(raw)

    if tokens:

        return [
            parse_flight_date(
                t,
                first_sector_date_str,
                dais_str
            )
            for t in tokens
        ]

    return [
        parse_flight_date(
            raw,
            first_sector_date_str,
            dais_str
        )
    ]


def split_sectors(raw):

    raw = raw.strip()

    if not raw:
        return []

    tokens = raw.split()

    # ORIG/DEST format
    if '/' in tokens[0]:

        airports = []

        for token in tokens:

            parts = token.split('/')

            if len(parts) == 2:

                if not airports:
                    airports.append(parts[0])

                airports.append(parts[1])

        return airports

    return tokens


def detect_delimiter(csv_path):

    with open(csv_path, newline='', encoding='utf-8-sig') as f:

        sample = f.read(4096)

    sniffer = csv.Sniffer()

    try:

        dialect = sniffer.sniff(
            sample,
            delimiters=',\t|;'
        )

        return dialect.delimiter

    except:

        return ','


# ============================================================================
# BUILD SCHEMA
# ============================================================================

def build_all_cols():

    return (
        BASE_COLS
        + [f'FlightNo{i+1}' for i in range(MAX_FLIGHTS)]
        + [f'FlightDate{i+1}' for i in range(MAX_DATES)]
        + [f'Airport{i+1}' for i in range(MAX_AIRPORTS)]
    )


def process_row(row, all_cols):

    get = row.get

    first_sector = (get('FirstSectordate') or '').strip()

    dais = (get('DAIS') or '').strip()

    fn = split_flight_nos(
        (get('FlightNo') or '').strip()
    )

    fd = split_flight_dates(
        (get('FlightDate') or '').strip(),
        first_sector,
        dais
    )

    ap = split_sectors(
        (get('Sector') or '').strip()
    )

    values = {}

    # Base columns
    for c in BASE_COLS:

        v = get(c)

        values[c] = (
            v.strip()
            if isinstance(v, str)
            else v
        )

    # FlightNos
    for i, v in enumerate(fn[:MAX_FLIGHTS]):

        values[f'FlightNo{i+1}'] = v

    # FlightDates
    for i, v in enumerate(fd[:MAX_DATES]):

        values[f'FlightDate{i+1}'] = v

    # Airports
    for i, v in enumerate(ap[:MAX_AIRPORTS]):

        values[f'Airport{i+1}'] = v

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

    print(f"\n{'='*70}")
    print("ULTRA FAST CSV -> DUCKDB LOADER")
    print(f"{'='*70}")
    print(f"CSV       : {csv_path}")
    print(f"Database  : {db_path}")
    print(f"Table     : {TABLE_NAME}")
    print(f"Delimiter : {repr(delimiter)}")
    print(f"{'='*70}\n")

    start_total = time.time()

    # =========================================================================
    # CONNECT
    # =========================================================================

    con = duckdb.connect(db_path)

    # Performance settings
    con.execute(f"PRAGMA threads={os.cpu_count()}")

    try:
        con.execute("SET memory_limit='16GB'")
    except:
        pass

    # =========================================================================
    # CREATE TABLE
    # =========================================================================

    all_cols = build_all_cols()

    print(f"Creating table ({len(all_cols)} columns)...")

    con.execute(f'DROP TABLE IF EXISTS "{TABLE_NAME}"')

    col_defs = ', '.join(
        [f'"{c}" VARCHAR' for c in all_cols]
    )

    con.execute(
        f'CREATE TABLE "{TABLE_NAME}" ({col_defs})'
    )

    # =========================================================================
    # LOAD CSV
    # =========================================================================

    inserted = 0

    batch = []

    start_insert = time.time()

    print("Loading rows...\n")

    with open(csv_path, newline='', encoding='utf-8-sig') as f:

        reader = csv.DictReader(
            f,
            delimiter=delimiter
        )

        for row in reader:

            batch.append(
                process_row(row, all_cols)
            )

            if len(batch) >= CHUNK_SIZE:

                batch_df = pd.DataFrame(
                    batch,
                    columns=all_cols
                )

                con.append(
                    TABLE_NAME,
                    batch_df
                )

                inserted += len(batch)

                elapsed = time.time() - start_insert

                rate = (
                    inserted / elapsed
                    if elapsed > 0
                    else 0
                )

                print(
                    f"{inserted:>12,} rows   "
                    f"{rate:>10,.0f} rows/sec"
                )

                batch.clear()

        # Final batch
        if batch:

            batch_df = pd.DataFrame(
                batch,
                columns=all_cols
            )

            con.append(
                TABLE_NAME,
                batch_df
            )

            inserted += len(batch)

    # =========================================================================
    # VERIFY
    # =========================================================================

    count = con.execute(
        f'SELECT COUNT(*) FROM "{TABLE_NAME}"'
    ).fetchone()[0]

    con.close()

    elapsed_total = time.time() - start_total

    print(f"\n{'='*70}")
    print("DONE")
    print(f"{'='*70}")
    print(f"Inserted Rows : {inserted:,}")
    print(f"Verified Rows : {count:,}")
    print(f"Elapsed Time  : {elapsed_total:.1f} sec")
    print(f"Rows / Second : {inserted / elapsed_total:,.0f}")
    print(f"{'='*70}")


if __name__ == '__main__':

    main()