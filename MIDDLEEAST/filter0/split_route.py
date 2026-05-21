"""
Flight Row Splitter  —  optimized for 5M+ rows
================================================
Reads MIDDLEEAST_RAW in chunks via fetchdf() (no OFFSET scan),
processes splits in Python, bulk-inserts children via pandas→DuckDB.

Rules
-----
1. Round-trip  : Airport1 == AirportLast  → split at every turnaround point
2. One-way / connecting (ALL consecutive date gaps ≤ 1 day) → do NOT split
3. One-way / with gap   (ANY consecutive date gap  > 1 day) → split at gaps
"""

import duckdb
import uuid
import os
import time
import pandas as pd
from datetime import datetime

# ============================================================================
# CONFIG
# ============================================================================

DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "MIDDLEEAST_RAW"

MAX_FLIGHTS = 4
MAX_DATES = 4
MAX_AIRPORTS = 5

# Tune to taste: larger = fewer round-trips, more RAM
BATCH_SIZE = 200_000

# ============================================================================
# COLUMN LISTS  (built once at module level)
# ============================================================================

FLIGHT_COLS = [f"FlightNo{i + 1}" for i in range(MAX_FLIGHTS)]
DATE_COLS = [f"FlightDate{i + 1}" for i in range(MAX_DATES)]
AIRPORT_COLS = [f"Airport{i + 1}" for i in range(MAX_AIRPORTS)]

DYNAMIC_COLS = FLIGHT_COLS + DATE_COLS + AIRPORT_COLS  # columns zeroed in children

# Pre-build column index maps (used instead of dict lookups in hot loop)
# Populated after we know all_cols from the DB.
COL_IDX: dict = {}

# ============================================================================
# HELPERS
# ============================================================================


def parse_dt(val):
    """Return datetime or None — fast path for already-parsed datetime objects."""
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


def is_valid(val):
    if val is None:
        return False
    if isinstance(val, float):  # catches NaN / inf from pandas
        import math

        return not math.isnan(val)
    if isinstance(val, str):
        return val.strip() != ""
    return True  # datetime, int, etc.


# ============================================================================
# ROW-LEVEL SPLIT LOGIC  (operates on plain lists for speed)
# ============================================================================


def get_flights_airports(row_list):
    """
    Extract (flights, airports) from a row represented as a plain list.
    Uses pre-built COL_IDX for O(1) access instead of dict lookups.
    Returns:
        flights  : list of (FlightNo str, FlightDate str)  — both valid
        airports : list of airport code strings
    """
    flights = []
    for i in range(MAX_FLIGHTS):
        fn = row_list[COL_IDX[f"FlightNo{i + 1}"]]
        fd = row_list[COL_IDX[f"FlightDate{i + 1}"]]
        if is_valid(fn) and is_valid(fd):
            fn = fn if isinstance(fn, str) else str(fn)
            fd = fd if isinstance(fd, str) else str(fd)
            flights.append((fn.strip(), fd.strip()))

    airports = []
    for c in AIRPORT_COLS:
        v = row_list[COL_IDX[c]]
        if is_valid(v):
            airports.append(v.strip())

    return flights, airports


def find_all_split_points(flights, airports):
    """
    Return sorted list of split indices in the flights array.
    Empty → no split.
    """
    n_f = len(flights)
    n_a = len(airports)

    if n_f < 2:
        return []

    split_points = set()

    # Rule 1: round-trip — interior airport matches origin
    if n_a >= 3 and airports[-1] == airports[0]:
        origin = airports[0]
        for j in range(1, n_a - 1):
            if airports[j] == origin:
                split_points.add(j)

    # Rule 3: date gap > 1 day
    for i in range(n_f - 1):
        gap = day_gap(flights[i][1], flights[i + 1][1])
        if gap is not None and gap > 1:
            split_points.add(i + 1)

    return sorted(split_points)


def build_child_list(parent_list, all_cols, flights_slice, airports_slice, parent_id):
    """
    Clone parent_list, overwrite dynamic columns with the segment slice,
    assign new id and ParentId.  Returns a plain list (same order as all_cols).
    """
    child = list(parent_list)  # shallow copy — fast

    # Zero out all dynamic columns
    for c in DYNAMIC_COLS:
        child[COL_IDX[c]] = None

    # Fill flight slice
    for i, (fn, fd) in enumerate(flights_slice):
        child[COL_IDX[f"FlightNo{i + 1}"]] = fn
        child[COL_IDX[f"FlightDate{i + 1}"]] = fd

    # Fill airport slice
    for i, ap in enumerate(airports_slice):
        child[COL_IDX[f"Airport{i + 1}"]] = ap

    child[COL_IDX["id"]] = str(uuid.uuid4())
    child[COL_IDX["ParentId"]] = str(parent_id)

    return child


def process_batch(rows_df, all_cols):
    """
    Takes a pandas DataFrame batch.
    Returns a DataFrame of NEW child rows to insert (may be empty).
    """
    children = []

    records = rows_df.values.tolist()  # convert once to list-of-lists

    for row_list in records:
        flights, airports = get_flights_airports(row_list)
        split_points = find_all_split_points(flights, airports)

        if not split_points:
            continue

        parent_id = row_list[COL_IDX["id"]]
        boundaries = [0] + split_points + [len(flights)]

        for k in range(len(boundaries) - 1):
            f_start = boundaries[k]
            f_end = boundaries[k + 1]
            a_start = boundaries[k]
            a_end = (
                (boundaries[k + 1] + 1) if k < len(boundaries) - 2 else len(airports)
            )

            seg_flights = flights[f_start:f_end]
            seg_airports = airports[a_start:a_end]

            if not seg_flights:
                continue

            children.append(
                build_child_list(
                    row_list, all_cols, seg_flights, seg_airports, parent_id
                )
            )

    if not children:
        return pd.DataFrame(columns=all_cols)

    return pd.DataFrame(children, columns=all_cols)


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
    cols = col_names(con, table)
    if "ParentId" not in cols:
        con.execute(f'ALTER TABLE "{table}" ADD COLUMN "ParentId" UUID')
        print("  Added ParentId column.")


# ============================================================================
# MAIN
# ============================================================================


def process_table(db_path=DB_PATH, table=TABLE_NAME, batch_size=BATCH_SIZE):
    con = duckdb.connect(db_path)

    # Performance knobs
    con.execute(f"PRAGMA threads={os.cpu_count()}")
    try:
        con.execute("SET memory_limit='16GB'")
    except Exception:
        pass

    ensure_parent_id_column(con, table)

    all_cols = col_names(con, table)

    # Build global index map once
    global COL_IDX
    COL_IDX = {c: i for i, c in enumerate(all_cols)}

    col_list = ", ".join(f'"{c}"' for c in all_cols)
    total = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

    print(f"\n{'=' * 65}")
    print(f"FLIGHT ROW SPLITTER  —  {total:,} rows")
    print(f"{'=' * 65}")

    processed = 0
    split_count = 0
    child_count = 0
    t0 = time.time()

    # ── Stream via fetchmany on a single cursor (no OFFSET scan) ─────────────
    cursor = con.cursor()
    cursor.execute(f'SELECT {col_list} FROM "{table}" WHERE "ParentId" IS NULL')

    while True:
        # fetchmany returns list-of-tuples; wrap in DataFrame for bulk insert
        raw = cursor.fetchmany(batch_size)
        if not raw:
            break

        batch_df = pd.DataFrame(raw, columns=all_cols)
        children_df = process_batch(batch_df, all_cols)

        n_splits = children_df["ParentId"].notna().any() and len(
            children_df["ParentId"].unique()
        )

        if not children_df.empty:
            # Bulk insert via DuckDB's zero-copy DataFrame reader
            con.execute(f'INSERT INTO "{table}" ({col_list}) SELECT * FROM children_df')
            child_count += len(children_df)
            split_count += children_df["ParentId"].nunique()

        processed += len(raw)
        elapsed = time.time() - t0
        rate = processed / elapsed if elapsed > 0 else 0
        print(
            f"  {processed:>10,} / {total:,} scanned  |"
            f"  {child_count:>8,} children  |  {rate:>8,.0f} rows/sec"
        )

    cursor.close()

    final_count = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    con.close()

    elapsed = time.time() - t0
    print(f"\n{'=' * 65}")
    print(f"DONE  ({elapsed:.1f}s)")
    print(f"  Original rows  : {total:,}")
    print(f"  Rows split     : {split_count:,}")
    print(f"  Children added : {child_count:,}")
    print(f"  Total rows now : {final_count:,}")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    process_table()
