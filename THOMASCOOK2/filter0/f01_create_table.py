"""
Ultra-Fast Flight CSV -> DuckDB Loader
Optimized for 5M+ rows
Auto-detects the max number of FlightNo / Airport / DepartureDate / ArrivalDate
columns needed, since the "/"-separated values can vary per row.
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

CSV_FILE_PATH = r"C:\Users\cagri\Desktop\Agency_Data\THOMASCOOK_2\filter-0\raw_data_csv.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "THOMASCOOK_RAW"
CHUNK_SIZE = 1_000_000
DELIMITER = None                  # None = auto-detect (tab/comma/etc.)

# The multi-value columns and the column-name prefix they expand into.
# Key = uppercase header name to match in the CSV (case-insensitive match).
SPECIAL_COLS = {
    "FLIGHTNUMBER": "FlightNo",
    "SECTOR": "Airport",
    "DEPARTUREDATE": "DepartureDate",
    "ARRIVALDATE": "ArrivalDate",
}

# ============================================================================
# DELIMITER DETECTION
# ============================================================================


def detect_encoding(csv_path):
    """
    Try encodings in order of strictness. utf-8-sig handles plain UTF-8 and
    UTF-8-with-BOM. If that fails (accented chars from Windows exports, etc.)
    fall back to cp1252 (standard Windows encoding), then latin-1 which never
    raises (every byte maps to a character) as a last resort.
    """
    candidates = ["utf-8-sig", "cp1252", "latin-1"]
    for enc in candidates:
        try:
            with open(csv_path, encoding=enc) as f:
                # read the whole file to make sure there's no bad byte further in,
                # not just in the first chunk
                f.read()
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"  # unreachable in practice, latin-1 always decodes


def detect_delimiter(csv_path, encoding):
    with open(csv_path, newline="", encoding=encoding) as f:
        sample = f.read(4096)
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample, delimiters=",\t|;")
        return dialect.delimiter
    except Exception:
        return ","


# ============================================================================
# "/" SPLIT HELPERS
# ============================================================================


def split_by_slash(raw):
    """Split a raw '/'-separated string into trimmed, non-empty tokens."""
    if not raw:
        return []
    return [t.strip() for t in str(raw).split("/") if t.strip()]


def split_flight_nos(raw):
    """
    'AK 585 / AK 11 / AK 12 / AK 582' -> ['AK585', 'AK11', 'AK12', 'AK582']
    """
    parts = split_by_slash(raw)
    return [re.sub(r"\s+", "", p) for p in parts]


def split_sectors(raw):
    """
    'MNL-KUL/KUL-MAA/MAA-KUL/KUL-MNL' -> ['MNL', 'KUL', 'MAA', 'KUL', 'MNL']
    Takes the origin of the first leg, then the destination of every leg.
    """
    tokens = split_by_slash(raw)
    airports = []
    for t in tokens:
        parts = [p.strip() for p in t.split("-")]
        if len(parts) < 2:
            continue
        if not airports:
            airports.append(parts[0])
        airports.append(parts[-1])
    return airports


# '2019-01-01 - 1240' -> '2019-01-01 - 12:40'
DATE_TOKEN_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\s*-\s*(\d{1,4})$")


def format_date_token(tok):
    tok = tok.strip()
    m = DATE_TOKEN_RE.match(tok)
    if m:
        date_part = m.group(1)
        digits = m.group(2).zfill(4)
        return f"{date_part} - {digits[:2]}:{digits[2:]}"
    return tok  # leave anything unexpected untouched rather than dropping it


def split_dates(raw):
    """
    '2019-01-01 - 1240 / 2019-01-01 - 0605' ->
    ['2019-01-01 - 12:40', '2019-01-01 - 06:05']
    """
    return [format_date_token(t) for t in split_by_slash(raw)]


# ============================================================================
# HEADER / SCHEMA DISCOVERY
# ============================================================================


def find_header_and_base_cols(csv_path, delimiter, encoding):
    with open(csv_path, newline="", encoding=encoding) as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader)
    header = [h.strip() for h in header]

    special_upper = set(SPECIAL_COLS.keys())
    base_cols = [h for h in header if h.upper() not in special_upper]

    # map SPECIAL_COLS key -> actual header text as it appears in the file
    special_actual = {}
    for h in header:
        if h.upper() in special_upper:
            special_actual[h.upper()] = h

    return header, base_cols, special_actual


def scan_max_counts(csv_path, delimiter, special_actual, encoding):
    """
    Pass 1: stream through the whole file just to find, per special column,
    the maximum number of '/'-separated values seen in any row. This tells us
    how many FlightNo1..N / Airport1..M / DepartureDate1..D / ArrivalDate1..A
    columns the table needs before we CREATE TABLE.
    """
    max_counts = {"FlightNo": 0, "Airport": 0, "DepartureDate": 0, "ArrivalDate": 0}

    fn_col = special_actual.get("FLIGHTNUMBER")
    sec_col = special_actual.get("SECTOR")
    dep_col = special_actual.get("DEPARTUREDATE")
    arr_col = special_actual.get("ARRIVALDATE")

    with open(csv_path, newline="", encoding=encoding) as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for i, row in enumerate(reader, 1):
            if fn_col:
                n = len(split_flight_nos(row.get(fn_col) or ""))
                if n > max_counts["FlightNo"]:
                    max_counts["FlightNo"] = n
            if sec_col:
                n = len(split_sectors(row.get(sec_col) or ""))
                if n > max_counts["Airport"]:
                    max_counts["Airport"] = n
            if dep_col:
                n = len(split_by_slash(row.get(dep_col) or ""))
                if n > max_counts["DepartureDate"]:
                    max_counts["DepartureDate"] = n
            if arr_col:
                n = len(split_by_slash(row.get(arr_col) or ""))
                if n > max_counts["ArrivalDate"]:
                    max_counts["ArrivalDate"] = n

            if i % 500_000 == 0:
                print(f"  scanned {i:,} rows...")

    return max_counts


def build_all_cols(base_cols, max_counts):
    cols = list(base_cols)
    cols += [f"FlightNo{i + 1}" for i in range(max_counts["FlightNo"])]
    cols += [f"Airport{i + 1}" for i in range(max_counts["Airport"])]
    cols += [f"DepartureDate{i + 1}" for i in range(max_counts["DepartureDate"])]
    cols += [f"ArrivalDate{i + 1}" for i in range(max_counts["ArrivalDate"])]
    return cols


# ============================================================================
# ROW PROCESSING
# ============================================================================


def process_row(row, all_cols, base_cols, special_actual):
    values = {}

    for c in base_cols:
        v = row.get(c)
        values[c] = v.strip() if isinstance(v, str) else v

    fn_col = special_actual.get("FLIGHTNUMBER")
    sec_col = special_actual.get("SECTOR")
    dep_col = special_actual.get("DEPARTUREDATE")
    arr_col = special_actual.get("ARRIVALDATE")

    if fn_col:
        for i, v in enumerate(split_flight_nos(row.get(fn_col) or "")):
            values[f"FlightNo{i + 1}"] = v
    if sec_col:
        for i, v in enumerate(split_sectors(row.get(sec_col) or "")):
            values[f"Airport{i + 1}"] = v
    if dep_col:
        for i, v in enumerate(split_dates(row.get(dep_col) or "")):
            values[f"DepartureDate{i + 1}"] = v
    if arr_col:
        for i, v in enumerate(split_dates(row.get(arr_col) or "")):
            values[f"ArrivalDate{i + 1}"] = v

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

    encoding = detect_encoding(csv_path)
    delimiter = DELIMITER or detect_delimiter(csv_path, encoding)

    print(f"\n{'=' * 70}")
    print("ULTRA FAST CSV -> DUCKDB LOADER (auto-detected split counts)")
    print(f"{'=' * 70}")
    print(f"CSV       : {csv_path}")
    print(f"Database  : {db_path}")
    print(f"Table     : {TABLE_NAME}")
    print(f"Encoding  : {encoding}")
    print(f"Delimiter : {repr(delimiter)}")
    print(f"{'=' * 70}\n")

    start_total = time.time()

    header, base_cols, special_actual = find_header_and_base_cols(csv_path, delimiter, encoding)

    print("Pass 1/2: scanning file to find max split counts...")
    max_counts = scan_max_counts(csv_path, delimiter, special_actual, encoding)
    print(f"  Max FlightNo columns      : {max_counts['FlightNo']}")
    print(f"  Max Airport columns       : {max_counts['Airport']}")
    print(f"  Max DepartureDate columns : {max_counts['DepartureDate']}")
    print(f"  Max ArrivalDate columns   : {max_counts['ArrivalDate']}\n")

    all_cols = build_all_cols(base_cols, max_counts)

    con = duckdb.connect(db_path)
    con.execute(f"PRAGMA threads={os.cpu_count()}")
    try:
        con.execute("SET memory_limit='16GB'")
    except Exception:
        pass

    print(f"Creating table ({len(all_cols)} data columns + id UUID)...")
    con.execute(f'DROP TABLE IF EXISTS "{TABLE_NAME}"')
    col_defs = ", ".join([f'"{c}" VARCHAR' for c in all_cols])
    con.execute(f"""
        CREATE TABLE "{TABLE_NAME}" (
            id UUID DEFAULT gen_random_uuid(),
            {col_defs}
        )
    """)

    print("Pass 2/2: loading rows...\n")
    inserted = 0
    batch = []
    start_insert = time.time()

    with open(csv_path, newline="", encoding=encoding) as f:
        reader = csv.DictReader(f, delimiter=delimiter)

        for row in reader:
            batch.append(process_row(row, all_cols, base_cols, special_actual))

            if len(batch) >= CHUNK_SIZE:
                batch_df = pd.DataFrame(batch, columns=all_cols)
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

    count = con.execute(f'SELECT COUNT(*) FROM "{TABLE_NAME}"').fetchone()[0]
    con.close()

    elapsed_total = time.time() - start_total

    print(f"\n{'=' * 70}")
    print("DONE")
    print(f"{'=' * 70}")
    print(f"Inserted Rows : {inserted:,}")
    print(f"Verified Rows : {count:,}")
    print(f"Elapsed Time  : {elapsed_total:.1f} sec")
    if elapsed_total > 0:
        print(f"Rows / Second : {inserted / elapsed_total:,.0f}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()