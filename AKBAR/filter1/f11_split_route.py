"""
Flight Row Splitter  —  optimized for 5M+ rows
================================================
Rules
-----
1. count(FlightNo) > count(FlightDate)  → insert into MIDDLEEAST_REJECTION
2. count(FlightNo) <= count(FlightDate) → trim to count(FlightNo) flights/dates,
                                          keep other columns (DAIS, TRNN, etc.) unchanged
3. count(Airport)  = count(FlightNo) + 1 (trim airports accordingly)
4. Any consecutive date gap > 1 day     → split at that boundary
   All consecutive date gaps <= 1 day   → do NOT split
"""

import duckdb
import uuid
import os
import time
import math
import re
import pandas as pd
from datetime import datetime

# ============================================================================
# CONFIG
# ============================================================================

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "AKBAR_RAW_V2"
TARGET_TABLE = "AKBAR_SPLIT1_V2"
REJECT_TABLE = "AKBAR_REJECTION_V2"

MAX_FLIGHTS = 4
MAX_DATES = 4
MAX_AIRPORTS = 5

BATCH_SIZE = 200_000

# ============================================================================
# COLUMN LISTS
# ============================================================================

FLIGHT_COLS = [f"FlightNo{i + 1}" for i in range(MAX_FLIGHTS)]
DATE_COLS = [f"FlightDate{i + 1}" for i in range(MAX_DATES)]
AIRPORT_COLS = [f"Airport{i + 1}" for i in range(MAX_AIRPORTS)]
DYNAMIC_COLS = FLIGHT_COLS + DATE_COLS + AIRPORT_COLS

_RE_FLTNO = re.compile(r"^[A-Z0-9]{2,3}\d+$")

STATIC_COLS = [
    "DAIS",
    "TRNN",
    "TDNR",
    "TRNC",
    "STAT",
    "PNRR",
    "FirstSectordate",
    "LastSectordate",
    "PaxName",
    "AirlineCodes",
    "AirlineName",
    "AirlineCode",
    "_SourceFile",
    "_SourceSheet",
]

COL_IDX: dict = {}


# ============================================================================
# HELPERS
# ============================================================================


def _isna(val) -> bool:
    """True if val is None, NaN, or blank-after-strip."""
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    if isinstance(val, str) and val.strip() == "":
        return True
    return False


def normalize_flight_numbers(row: dict) -> dict:
    """
    Rule 7 — strip leading zeros between prefix letters and digit suffix.
    Returns a mutated copy of the row dict.
    """
    row = dict(row)
    for i in range(1, MAX_FLIGHTS + 1):
        fn = row.get(f"FlightNo{i}")
        if not _isna(fn):
            fn_str = str(fn).strip().upper()
            if _RE_FLTNO.fullmatch(fn_str):
                row[f"FlightNo{i}"] = _normalize_flightno(fn_str)
    return row


def _normalize_flightno(fn: str) -> str:
    """
    Remove leading zeros between the alphabetic prefix and the numeric suffix.
    E.g.  SV0020 → SV20,  AF0459 → AF459,  EK001 → EK1
    Assumes fn is already stripped and uppercased.
    """
    m = re.fullmatch(r"([A-Z]{1,3})(\d+)", fn)
    if not m:
        return fn
    prefix, digits = m.group(1), m.group(2)
    normalized_digits = digits.lstrip("0") or "0"
    return prefix + normalized_digits


def parse_dt(val):
    if val is None:
        return None
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
    return abs((b - a).days)


# ============================================================================
# RULE 1 & 2: count flights vs dates, then trim
# ============================================================================


def extract_and_validate(row_list):
    """
    Returns:
        ("reject", None, None)  — count(FlightNo) > count(FlightDate)  [Rule 1]
        (flights, airports, cnt_no) — trimmed data                      [Rules 2 & 3]

    Rule 2: take only the first count(FlightNo) dates.
    Rule 3: airports trimmed to count(FlightNo) + 1.
    """
    flight_nos = []
    for i in range(MAX_FLIGHTS):
        v = row_list[COL_IDX[f"FlightNo{i + 1}"]]
        if not _isna(v):
            flight_nos.append(v.strip() if isinstance(v, str) else str(v))

    flight_dates = []
    for i in range(MAX_DATES):
        v = row_list[COL_IDX[f"FlightDate{i + 1}"]]
        if not _isna(v):
            flight_dates.append(v.strip() if isinstance(v, str) else str(v))

    cnt_no = len(flight_nos)
    cnt_date = len(flight_dates)

    # Rule 1
    if cnt_no > cnt_date:
        return "reject", None, None

    # Rule 2: trim dates to match flight number count
    trimmed_dates = flight_dates[:cnt_no]
    flights = list(zip(flight_nos, trimmed_dates))

    # Rule 3: airports trimmed to cnt_no + 1
    all_airports = []
    for i in range(MAX_AIRPORTS):
        v = row_list[COL_IDX[f"Airport{i + 1}"]]
        if not _isna(v):
            all_airports.append(v.strip() if isinstance(v, str) else str(v))

    airports = all_airports[: cnt_no + 1]

    return flights, airports, cnt_no


# ============================================================================
# RULE 4: split-point detection (date gap only)
# ============================================================================


def find_split_points(flights):
    """
    Rule 4: split wherever consecutive FlightDate gap > 1 day.
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


def build_child_row(trimmed_row, flights_slice, airports_slice, parent_id):
    """Clone trimmed_row, clear dynamic cols, fill in segment data."""
    child = list(trimmed_row)

    for c in DYNAMIC_COLS:
        child[COL_IDX[c]] = None

    for i, (fn, fd) in enumerate(flights_slice):
        child[COL_IDX[f"FlightNo{i + 1}"]] = fn
        child[COL_IDX[f"FlightDate{i + 1}"]] = fd

    for i, ap in enumerate(airports_slice):
        child[COL_IDX[f"Airport{i + 1}"]] = ap

    child[COL_IDX["id"]] = str(uuid.uuid4())
    child[COL_IDX["ParentId"]] = str(parent_id)

    return child


# ============================================================================
# BATCH PROCESSOR
# ============================================================================


def process_batch(rows_df, all_cols):
    """
    For each row:
      - Reject  → rejection_rows        (Rule 1)
      - No split → unsplit_rows         (Rules 2+3, no gap)
      - Split   → child_rows            (Rules 2+3+4)

    Returns:
        unsplit_df    — pass-through rows (trimmed per rules 2 & 3)
        children_df   — split child rows
        rejection_df  — rows violating rule 1
    """
    unsplit_rows = []
    child_rows = []
    rejection_rows = []

    records = rows_df.values.tolist()

    for row_list in records:
        # Rule 7: normalize FlightNo values
        row_dict = dict(zip(all_cols, row_list))
        row_dict = normalize_flight_numbers(row_dict)
        row_list = [row_dict[c] for c in all_cols]

        result, airports, cnt_no = extract_and_validate(row_list)

        # Rule 1: reject
        if result == "reject":
            rejection_rows.append(list(row_list))
            continue

        flights = result  # list of (fn, fd)

        # Apply trim back onto row (Rules 2 & 3)
        trimmed_row = list(row_list)
        for c in DYNAMIC_COLS:
            trimmed_row[COL_IDX[c]] = None
        for i, (fn, fd) in enumerate(flights):
            trimmed_row[COL_IDX[f"FlightNo{i + 1}"]] = fn
            trimmed_row[COL_IDX[f"FlightDate{i + 1}"]] = fd
        for i, ap in enumerate(airports):
            trimmed_row[COL_IDX[f"Airport{i + 1}"]] = ap

        # Rule 4: check for date gaps
        split_points = find_split_points(flights)

        if not split_points:
            unsplit_rows.append(trimmed_row)
            continue

        # Has gap → produce children
        # FIX: pull parent_id from trimmed_row (id is not a dynamic col, so
        # it's unchanged — but being explicit about the source is clearer)
        parent_id = trimmed_row[COL_IDX["id"]]
        boundaries = [0] + split_points + [len(flights)]

        for k in range(len(boundaries) - 1):
            f_start = boundaries[k]
            f_end = boundaries[k + 1]
            a_end = f_end + 1  # include arrival airport for this segment

            seg_flights = flights[f_start:f_end]
            seg_airports = airports[f_start : min(a_end, len(airports))]

            if not seg_flights:
                continue

            child_rows.append(
                build_child_row(trimmed_row, seg_flights, seg_airports, parent_id)
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
        f"WHERE table_name = '{table}' ORDER BY ordinal_position"
    ).fetchall()
    return [r[0] for r in rows]


def ensure_parent_id_column(con, table):
    """Add ParentId to the source table if missing. Returns updated col list."""
    cols = col_names(con, table)
    if "ParentId" not in cols:
        con.execute(f'ALTER TABLE "{table}" ADD COLUMN "ParentId" VARCHAR')
        print("  Added ParentId column.")
        cols = col_names(con, table)  # re-fetch after ALTER
    return cols


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

    # FIX: ensure_parent_id_column now returns the updated col list so that
    # all_cols and COL_IDX reflect the newly added column immediately.
    all_cols = ensure_parent_id_column(con, table)

    # Validate required identity columns exist before going any further
    for required in ("id", "ParentId"):
        if required not in all_cols:
            raise RuntimeError(
                f"Column '{required}' not found in '{table}'. "
                "Add it to the source table before running."
            )

    ensure_target_table(con, table, TARGET_TABLE)
    ensure_rejection_table(con, table, REJECT_TABLE)

    global COL_IDX
    COL_IDX = {c: i for i, c in enumerate(all_cols)}

    col_list = ", ".join(f'"{c}"' for c in all_cols)
    total = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

    unsplit_total = 0
    split_count = 0
    child_count = 0
    reject_count = 0
    rows_scanned = 0  # FIX: dedicated counter so progress is accurate
    t0 = time.time()

    cursor = con.cursor()
    # FIX: removed WHERE "ParentId" IS NULL — target is always dropped/recreated
    # so there's no resume scenario, and the filter would silently skip rows
    # if the source has non-null ParentId values from a previous run.
    cursor.execute(f'SELECT {col_list} FROM "{table}"')

    while True:
        raw = cursor.fetchmany(batch_size)
        if not raw:
            break

        batch_df = pd.DataFrame(raw, columns=all_cols)
        unsplit_df, children_df, rejection_df = process_batch(batch_df, all_cols)

        # FIX: use a transaction per batch so a mid-run crash leaves the
        # target tables in a consistent state (all-or-nothing per batch).
        con.execute("BEGIN")
        try:
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

            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        rows_scanned += len(batch_df)  # FIX: accurate scanned count
        elapsed = time.time() - t0
        rate = rows_scanned / elapsed if elapsed > 0 else 0
        print(
            f"  {rows_scanned:>10,} / {total:,} scanned  |"
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
