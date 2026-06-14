"""
Flight Row Splitter  —  optimized for 5M+ rows
================================================
Rules
-----
1. count(FLIGHTNO) > count(DEPARTURE_DATE)  → insert into THOMASCOOK_REJECTION
2. count(FLIGHTNO) <= count(DEPARTURE_DATE) → trim to count(FlightNo) flights/dates,
                                              keep other columns unchanged
3. count(FLIGHTNO) <= count(SECTOR)         → trim SECTOR accordingly
4. Any consecutive date gap > 1 day         → split at that boundary
   All consecutive date gaps <= 1 day       → do NOT split
"""

import duckdb
import uuid
import os
import time
import pandas as pd
import re
import math
from datetime import datetime

# ============================================================================
# CONFIG
# ============================================================================

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "THOMASCOOK_RAW"
TARGET_TABLE = "THOMASCOOK_SPLIT"
REJECT_TABLE = "THOMASCOOK_REJECTION"

MAX_FLIGHTS = 9
MAX_DEPARTURE_DATES = 12  # FIX: table has 12 departure date slots
MAX_ARRIVAL_DATES = 12  # FIX: table has 12 arrival date slots
MAX_SECTORS = 12  # FIX: table has 12 sector slots

BATCH_SIZE = 200_000

# ============================================================================
# COLUMN LISTS
# ============================================================================

FLIGHT_COLS = [f"FLIGHTNO{i + 1}" for i in range(MAX_FLIGHTS)]
DEPARTURE_DATE_COLS = [f"DEPARTURE_DATE{i + 1}" for i in range(MAX_DEPARTURE_DATES)]
ARRIVAL_DATE_COLS = [f"ARRIVAL_DATE{i + 1}" for i in range(MAX_ARRIVAL_DATES)]
SECTOR_COLS = [f"SECTOR{i + 1}" for i in range(MAX_SECTORS)]
DYNAMIC_COLS = FLIGHT_COLS + DEPARTURE_DATE_COLS + ARRIVAL_DATE_COLS + SECTOR_COLS

_RE_FLTNO = re.compile(r"^[A-Z0-9]{2,3}\d+$")

STATIC_COLS = [
    "COMPANY",
    "AIRLINE_PNR",
    "GDS_PNR",
    "TICKET_NO",
    "INVOICE_AND_REFUNDID",
    "GROUP_NAME",
    "AIRLINE_CARRIER_CODE",
    "AIRLINE_CARRIER_NAME",
    "STATUS",
]

COL_IDX: dict = {}

# ============================================================================
# HELPERS
# ============================================================================


def _isna(val) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    if isinstance(val, str) and val.strip() == "":
        return True
    return False


def normalize_flight_numbers(row: dict) -> dict:
    row = dict(row)
    for i in range(1, MAX_FLIGHTS + 1):
        fn = row.get(f"FLIGHTNO{i}")
        if not _isna(fn):
            fn_str = str(fn).strip().upper()
            if _RE_FLTNO.fullmatch(fn_str):
                row[f"FLIGHTNO{i}"] = _normalize_flightno(fn_str)
    return row


def _normalize_flightno(fn: str) -> str:
    m = re.fullmatch(r"([A-Z0-9]{2,3}?)(\d+)", fn)
    if not m:
        return fn
    prefix, digits = m.group(1), m.group(2)
    return prefix + (digits.lstrip("0") or "0")


def parse_dt(val):
    if val is None:
        return None
    # Catches pandas NaT, float NaN, and anything pd.isnull recognises
    try:
        if pd.isnull(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, datetime):
        return val
    s = str(val).strip()[:19]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def day_gap(d1, d2):
    a, b = parse_dt(d1), parse_dt(d2)
    if a is None or b is None:
        return None
    return abs((b.date() - a.date()).days)


def is_valid(val):
    if val is None:
        return False
    if isinstance(val, float):
        return not math.isnan(val)
    if isinstance(val, str):
        return val.strip() != ""
    return True


# ============================================================================
# RULE 1 & 2: count flights vs dates, then trim
# ============================================================================


def extract_and_validate(row_list):
    """
    Returns:
        ("reject", None, None)  — count(FLIGHTNO) > count(DEPARTURE_DATE)  [Rule 1]
        (flights, sectors, cnt_no)  — trimmed lists  [Rules 2 & 3]

    flights  : list of (FLIGHTNO, DEPARTURE_DATE, ARRIVAL_DATE) trimmed to cnt_no
    sectors  : list of SECTOR strings trimmed to cnt_no
    """
    flight_nos = []
    for i in range(MAX_FLIGHTS):
        v = row_list[COL_IDX[f"FLIGHTNO{i + 1}"]]
        if is_valid(v):
            flight_nos.append(v.strip() if isinstance(v, str) else str(v))

    departure_dates = []
    for i in range(MAX_DEPARTURE_DATES):
        v = row_list[COL_IDX[f"DEPARTURE_DATE{i + 1}"]]
        if is_valid(v):
            departure_dates.append(v)

    arrival_dates = []
    for i in range(MAX_ARRIVAL_DATES):
        v = row_list[COL_IDX[f"ARRIVAL_DATE{i + 1}"]]
        if is_valid(v):
            arrival_dates.append(v)

    cnt_no = len(flight_nos)
    cnt_dep = len(departure_dates)

    # Rule 1: more flight numbers than departure dates → reject
    if cnt_no > cnt_dep:
        return "reject", None, None

    # Rule 2 & 3: trim to cnt_no
    trimmed_dep = departure_dates[:cnt_no]
    trimmed_arr = arrival_dates[:cnt_no]  # may be shorter than cnt_no; that's fine
    flights = list(zip(flight_nos, trimmed_dep, trimmed_arr))

    all_sectors = []
    for i in range(MAX_SECTORS):
        v = row_list[COL_IDX[f"SECTOR{i + 1}"]]
        if is_valid(v):
            all_sectors.append(v.strip() if isinstance(v, str) else str(v))
    sectors = all_sectors[:cnt_no]

    return flights, sectors, cnt_no


# ============================================================================
# RULE 4: split-point detection (date gap only)
# ============================================================================


def find_split_points(flights):
    """
    Split wherever consecutive DEPARTURE_DATE gap > 1 day.
    Returns sorted list of indices where a new segment begins.
    """
    split_points = []
    for i in range(len(flights) - 1):
        gap = day_gap(flights[i][1], flights[i + 1][1])
        if gap is not None and gap > 1:
            split_points.append(i + 1)
    return split_points


# ============================================================================
# BUILD CHILD ROW
# ============================================================================


def build_child_row(parent_list, flights_slice, sectors_slice, parent_id):
    """Clone parent, clear all dynamic cols, fill in segment data."""
    child = list(parent_list)

    # Clear all dynamic columns
    for c in DYNAMIC_COLS:
        child[COL_IDX[c]] = None

    # FIX: write to correct column names (FLIGHTNO, DEPARTURE_DATE, ARRIVAL_DATE, SECTOR)
    for i, (fn, dep_dt, arr_dt) in enumerate(flights_slice):
        child[COL_IDX[f"FLIGHTNO{i + 1}"]] = fn
        child[COL_IDX[f"DEPARTURE_DATE{i + 1}"]] = dep_dt
        child[COL_IDX[f"ARRIVAL_DATE{i + 1}"]] = arr_dt

    for i, sec in enumerate(sectors_slice):
        child[COL_IDX[f"SECTOR{i + 1}"]] = sec

    # New identity
    # FIX: column is "Id" (capital I) — match exact casing from information_schema
    id_col = next(c for c in COL_IDX if c.lower() == "id")
    child[COL_IDX[id_col]] = str(uuid.uuid4())
    child[COL_IDX["ParentId"]] = str(parent_id)

    return child


# ============================================================================
# BATCH PROCESSOR
# ============================================================================


def process_batch(rows_df, all_cols):
    unsplit_rows = []
    child_rows = []
    rejection_rows = []

    records = rows_df.values.tolist()

    for row_list in records:
        # Rule 7: normalize flight numbers
        row_dict = dict(zip(all_cols, row_list))
        row_dict = normalize_flight_numbers(row_dict)
        row_list = [row_dict[c] for c in all_cols]

        result, sectors, cnt_no = extract_and_validate(row_list)

        # Rule 1: reject
        if result == "reject":
            rejection_rows.append(list(row_list))
            continue

        flights = result  # list of (fn, dep_dt, arr_dt)

        # Apply trim back onto row (Rules 2 & 3)
        trimmed_row = list(row_list)
        for c in DYNAMIC_COLS:
            trimmed_row[COL_IDX[c]] = None
        for i, (fn, dep_dt, arr_dt) in enumerate(flights):
            trimmed_row[COL_IDX[f"FLIGHTNO{i + 1}"]] = fn
            trimmed_row[COL_IDX[f"DEPARTURE_DATE{i + 1}"]] = dep_dt
            trimmed_row[COL_IDX[f"ARRIVAL_DATE{i + 1}"]] = arr_dt
        for i, sec in enumerate(sectors):
            trimmed_row[COL_IDX[f"SECTOR{i + 1}"]] = sec

        # Rule 4: check for date gaps
        split_points = find_split_points(flights)

        if not split_points:
            unsplit_rows.append(trimmed_row)
            continue

        # Split into children
        # FIX: use the correct column to get parent id
        id_col = next(c for c in COL_IDX if c.lower() == "id")
        parent_id = row_list[COL_IDX[id_col]]
        boundaries = [0] + split_points + [len(flights)]

        for k in range(len(boundaries) - 1):
            f_start = boundaries[k]
            f_end = boundaries[k + 1]

            seg_flights = flights[f_start:f_end]
            # Sectors align 1-to-1 with flights (Rule 3)
            seg_sectors = sectors[f_start:f_end]

            if not seg_flights:
                continue

            child_rows.append(
                build_child_row(trimmed_row, seg_flights, seg_sectors, parent_id)
            )

    empty = pd.DataFrame(columns=all_cols)
    unsplit_df = pd.DataFrame(unsplit_rows, columns=all_cols) if unsplit_rows else empty
    children_df = pd.DataFrame(child_rows, columns=all_cols) if child_rows else empty
    rejection_df = (
        pd.DataFrame(rejection_rows, columns=all_cols) if rejection_rows else empty
    )

    return unsplit_df, children_df, rejection_df


# ============================================================================
# DB HELPERS
# ============================================================================


def col_names(con, table):
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE LOWER(table_name) = LOWER('{table}') ORDER BY ordinal_position"
    ).fetchall()
    return [r[0] for r in rows]


def ensure_parent_id_column(con, table):
    cols = col_names(con, table)
    if "ParentId" not in cols:
        con.execute(f'ALTER TABLE "{table}" ADD COLUMN "ParentId" UUID')
        cols.append("ParentId")
        print("  Added ParentId column.")
    return cols  # FIX: return updated list (original omitted this)


def ensure_target_table(con, source_table, target_table):
    con.execute(f'DROP TABLE IF EXISTS "{target_table}"')
    con.execute(
        f'CREATE TABLE "{target_table}" AS SELECT * FROM "{source_table}" WHERE 1=0'
    )
    print(f"  Recreated table '{target_table}'.")


def ensure_rejection_table(con, source_table, reject_table):
    con.execute(f'DROP TABLE IF EXISTS "{reject_table}"')
    con.execute(
        f'CREATE TABLE "{reject_table}" AS SELECT * FROM "{source_table}" WHERE 1=0'
    )
    print(f"  Recreated table '{reject_table}'.")


# ============================================================================
# MAIN
# ============================================================================


def process_table(db_path=DB_PATH, table=SOURCE_TABLE, batch_size=BATCH_SIZE):
    con = duckdb.connect(db_path)
    con.execute(f"PRAGMA threads={os.cpu_count()}")
    try:
        con.execute("SET memory_limit='16GB'")
    except Exception:
        pass

    # FIX: capture returned cols after ParentId may have been added
    all_cols = ensure_parent_id_column(con, table)
    ensure_target_table(con, table, TARGET_TABLE)
    ensure_rejection_table(con, table, REJECT_TABLE)

    # Re-read cols from DB to be authoritative (ensure_target adds ParentId to source)
    all_cols = col_names(con, table)

    global COL_IDX
    COL_IDX = {c: i for i, c in enumerate(all_cols)}

    col_list = ", ".join(f'"{c}"' for c in all_cols)
    total = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

    unsplit_total = 0
    split_count = 0  # number of parent rows that were split
    child_count = 0
    reject_count = 0
    t0 = time.time()

    cursor = con.cursor()
    cursor.execute(f'SELECT {col_list} FROM "{table}" WHERE "ParentId" IS NULL')

    while True:
        raw = cursor.fetchmany(batch_size)
        if not raw:
            break

        batch_df = pd.DataFrame(raw, columns=all_cols)
        unsplit_df, children_df, rejection_df = process_batch(batch_df, all_cols)

        if not unsplit_df.empty:
            con.execute(
                f'INSERT INTO "{TARGET_TABLE}" ({col_list}) SELECT * FROM unsplit_df'
            )
            unsplit_total += len(unsplit_df)

        if not children_df.empty:
            con.execute(
                f'INSERT INTO "{TARGET_TABLE}" ({col_list}) SELECT * FROM children_df'
            )
            child_count += len(children_df)
            split_count += children_df["ParentId"].nunique()

        if not rejection_df.empty:
            con.execute(
                f'INSERT INTO "{REJECT_TABLE}" ({col_list}) SELECT * FROM rejection_df'
            )
            reject_count += len(rejection_df)

        elapsed = time.time() - t0
        # FIX: scanned = rows consumed from source, not child count
        scanned = unsplit_total + split_count + reject_count
        rate = scanned / elapsed if elapsed > 0 else 0
        print(
            f"  {scanned:>10,} / {total:,} scanned  |"
            f"  {split_count:>6,} splits  |"
            f"  {child_count:>8,} children  |"
            f"  {reject_count:>6,} rejected  |  {rate:>8,.0f} rows/sec"
        )

    cursor.close()

    final_split = con.execute(f'SELECT COUNT(*) FROM "{TARGET_TABLE}"').fetchone()[0]
    final_reject = con.execute(f'SELECT COUNT(*) FROM "{REJECT_TABLE}"').fetchone()[0]
    con.close()

    elapsed = time.time() - t0
    print(f"\n{'=' * 70}")
    print(f"DONE  ({elapsed:.1f}s)")
    print(f"  Source rows scanned  : {total:,}")
    print(f"  Rows NOT split       : {unsplit_total:,}")
    print(f"  Rows split           : {split_count:,}")
    print(f"  Children added       : {child_count:,}")
    print(f"  Rows rejected        : {reject_count:,}")
    print(f"  Expected in SPLIT    : {unsplit_total + child_count:,}")
    print(f"  Actual   in SPLIT    : {final_split:,}")
    print(f"  Actual   in REJECT   : {final_reject:,}")
    print(f"{'=' * 70}\n")


if __name__ == "__main__":
    process_table()
