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
SOURCE_TABLE = "AKBAR_SPLIT4_V2"
TARGET_TABLE = "AKBAR_SPLIT5_V2"

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

COL_IDX: dict = {}

# ============================================================================
# HELPERS
# ============================================================================


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


def is_valid(val):
    if val is None:
        return False
    if isinstance(val, float):
        import math

        return not math.isnan(val)
    if isinstance(val, str):
        return val.strip() != ""
    return True


# ============================================================================
# ROW-LEVEL SPLIT LOGIC
# ============================================================================


def get_flights_airports(row_list):
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
    Rule 1 — Exactly 4 flights, 5 airports, Airport3 == Airport5, Airport1<>Airport5:
        e.g. CPT → JNB → DOH → EBL → DOH
             [0]    [1]   [2]   [3]     [4]
        Split at flight boundary 1:
          Segment 1: Flight1,Flight2,Flight3         | Airport1 → Airport2 → Airport3 -> Airport4  (CPT → JNB → DOH → EBL)
          Segment 2: Flight4         | Airport4 -> Airport5  (EBL → DOH)

    Rule 2 — Date gap > 1 day between consecutive flights:
        Split at that flight boundary.
    """
    n_f = len(flights)
    n_a = len(airports)

    if n_f < 4:
        return []

    split_points = set()

    # ── Rule 1: exactly 4 flights, 5 airports, Airport3 == Airport5, Airport1<>Airport5 ──────────
    if (
        n_f == 4
        and n_a == 5
        and airports[2] == airports[4]
        and airports[0] != airports[4]
    ):
        split_points.add(3)

    # ── Rule 2: date gap > 1 day ─────────────────────────────────────────────
    for i in range(n_f - 1):
        gap = day_gap(flights[i][1], flights[i + 1][1])
        if gap is not None and gap > 1:
            split_points.add(i + 1)

    return sorted(split_points)


def build_child_list(parent_list, all_cols, flights_slice, airports_slice, parent_id):
    child = list(parent_list)

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


def process_batch(rows_df, all_cols):
    """
    - Rows with NO split  → counted but NOT inserted (already in TARGET from Step 1)
    - Rows WITH a split   → only their children inserted; original deleted in Step 3
    """
    unsplit_rows = []
    child_rows = []

    records = rows_df.values.tolist()

    for row_list in records:
        flights, airports = get_flights_airports(row_list)
        split_points = find_all_split_points(flights, airports)

        if not split_points:
            unsplit_rows.append(list(row_list))
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

            child_rows.append(
                build_child_list(
                    row_list, all_cols, seg_flights, seg_airports, parent_id
                )
            )

    empty = pd.DataFrame(columns=all_cols)
    unsplit_df = pd.DataFrame(unsplit_rows, columns=all_cols) if unsplit_rows else empty
    children_df = pd.DataFrame(child_rows, columns=all_cols) if child_rows else empty

    return unsplit_df, children_df


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


def ensure_target_table(con, source_table, target_table):
    con.execute(f'DROP TABLE IF EXISTS "{target_table}"')
    con.execute(
        f'CREATE TABLE "{target_table}" AS SELECT * FROM "{source_table}" WHERE 1=0'
    )
    print(f"  Recreated target table '{target_table}'.")


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

    ensure_parent_id_column(con, table)
    ensure_target_table(con, table, TARGET_TABLE)

    all_cols = col_names(con, table)

    global COL_IDX
    COL_IDX = {c: i for i, c in enumerate(all_cols)}

    col_list = ", ".join(f'"{c}"' for c in all_cols)

    t0 = time.time()

    # ── Step 1: Copy ALL rows from SOURCE into TARGET as baseline ─────────────
    print(f"  Step 1: Copying all rows from '{table}' into '{TARGET_TABLE}'...")
    con.execute(f"""
        INSERT INTO "{TARGET_TABLE}" ({col_list})
        SELECT {col_list} FROM "{table}"
    """)
    total_source = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    print(f"  Copied {total_source:,} rows into '{TARGET_TABLE}'.")

    # ── Step 2: Fetch and process matching 4-flight round-trip rows ───────────
    filtered_total = con.execute(f"""
        SELECT COUNT(*)
        FROM "{table}"
        WHERE 
            (FlightNo1 IS NOT NULL AND FlightNo1 <> '')
            AND (FlightDate1 IS NOT NULL AND FlightDate1 <> '')
            AND (FlightNo2 IS NOT NULL AND FlightNo2 <> '')
            AND (FlightDate2 IS NOT NULL AND FlightDate2 <> '')
            AND (FlightNo3 IS NOT NULL AND FlightNo3 <> '')
            AND (FlightDate3 IS NOT NULL AND FlightDate3 <> '')
            AND (FlightNo4 IS NOT NULL AND FlightNo4 <> '')
            AND (FlightDate4 IS NOT NULL AND FlightDate4 <> '')
            AND Airport1 IS NOT NULL
            AND Airport2 IS NOT NULL
            AND Airport3 IS NOT NULL
            AND Airport4 IS NOT NULL
            AND Airport5 IS NOT NULL
            AND Airport1 <> Airport5
            AND Airport3 = Airport5
    """).fetchone()[0]
    print(
        f"\n  Step 2: Processing {filtered_total:,} 4-flight round-trip rows for split..."
    )

    unsplit_total = 0
    split_count = 0
    child_count = 0
    split_parent_ids = []

    cursor = con.cursor()
    cursor.execute(f"""
        SELECT {col_list}
        FROM "{table}"
        WHERE 
            (FlightNo1 IS NOT NULL AND FlightNo1 <> '')
            AND (FlightDate1 IS NOT NULL AND FlightDate1 <> '')
            AND (FlightNo2 IS NOT NULL AND FlightNo2 <> '')
            AND (FlightDate2 IS NOT NULL AND FlightDate2 <> '')
            AND (FlightNo3 IS NOT NULL AND FlightNo3 <> '')
            AND (FlightDate3 IS NOT NULL AND FlightDate3 <> '')
            AND (FlightNo4 IS NOT NULL AND FlightNo4 <> '')
            AND (FlightDate4 IS NOT NULL AND FlightDate4 <> '')
            AND Airport1 IS NOT NULL
            AND Airport2 IS NOT NULL
            AND Airport3 IS NOT NULL
            AND Airport4 IS NOT NULL
            AND Airport5 IS NOT NULL
            AND Airport1 <> Airport5
            AND Airport3 = Airport5
    """)

    while True:
        raw = cursor.fetchmany(batch_size)
        if not raw:
            break

        batch_df = pd.DataFrame(raw, columns=all_cols)
        unsplit_df, children_df = process_batch(batch_df, all_cols)

        # Unsplit rows already exist in TARGET from Step 1 — do not insert again
        unsplit_total += len(unsplit_df)

        if not children_df.empty:
            con.execute(
                f'INSERT INTO "{TARGET_TABLE}" ({col_list}) SELECT * FROM children_df'
            )
            child_count += len(children_df)
            split_count += children_df["ParentId"].nunique()

            split_parent_ids.extend(children_df["ParentId"].dropna().unique().tolist())

        elapsed = time.time() - t0
        rate = (unsplit_total + split_count) / elapsed if elapsed > 0 else 0
        print(
            f"  {unsplit_total + split_count:>10,} / {filtered_total:,} scanned  |"
            f"  {split_count:>6,} splits  |"
            f"  {child_count:>8,} children  |  {rate:>8,.0f} rows/sec"
        )

    cursor.close()

    # ── Step 3: Delete originals from TARGET that were split ──────────────────
    if split_parent_ids:
        print(
            f"\n  Step 3: Deleting {len(split_parent_ids):,} original split rows from '{TARGET_TABLE}'..."
        )
        id_df = pd.DataFrame({"pid": split_parent_ids})
        con.execute(f"""
            DELETE FROM "{TARGET_TABLE}"
            WHERE CAST(id AS VARCHAR) IN (SELECT pid FROM id_df)
        """)
        print(
            f"  Deleted {len(split_parent_ids):,} original rows from '{TARGET_TABLE}'."
        )

    final_count = con.execute(f'SELECT COUNT(*) FROM "{TARGET_TABLE}"').fetchone()[0]
    con.close()

    elapsed = time.time() - t0
    print(f"\n{'=' * 65}")
    print(f"DONE  ({elapsed:.1f}s)")
    print(f"  Source rows (total)  : {total_source:,}")
    print(f"  Rows NOT split       : {unsplit_total:,}")
    print(f"  Rows split           : {split_count:,}")
    print(f"  Children added       : {child_count:,}")
    print(f"  Expected in TARGET   : {total_source + child_count - split_count:,}")
    print(f"  Actual   in TARGET   : {final_count:,}")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    process_table()
