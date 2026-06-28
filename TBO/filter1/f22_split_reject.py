import duckdb
import uuid
import os
import time
import math
import pandas as pd

# ============================================================================
# CONFIG
# ============================================================================

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "TBO_REJECTION"
TARGET_TABLE = "TBO_REJECTION2"

MAX_FLIGHTS = 7
MAX_DATES = 7
MAX_AIRPORTS = 8

BATCH_SIZE = 200_000

RNK_CODE = "RNK"

# ============================================================================
# COLUMN LISTS
# ============================================================================

FLIGHT_COLS = [f"FlightNumber{i + 1}" for i in range(MAX_FLIGHTS)]
DATE_COLS = [f"DepartureDateLocal{i + 1}" for i in range(MAX_DATES)]
AIRPORT_COLS = [f"Airport{i + 1}" for i in range(MAX_AIRPORTS)]

DYNAMIC_COLS = FLIGHT_COLS + DATE_COLS + AIRPORT_COLS

COL_IDX: dict = {}

# Any of Airport1..Airport8 == 'RNK' (trimmed) selects the row for processing
RNK_WHERE_CLAUSE = "WHERE " + " OR ".join(
    f"TRIM(\"{c}\") = '{RNK_CODE}'" for c in AIRPORT_COLS
)

# ============================================================================
# HELPERS
# ============================================================================


def is_valid(val):
    if val is None:
        return False
    if isinstance(val, float):
        return not math.isnan(val)
    if isinstance(val, str):
        return val.strip() != ""
    return True


def norm_text(val):
    """Strip strings, leave everything else (dates, None, etc.) untouched."""
    return val.strip() if isinstance(val, str) else val


# ============================================================================
# ROW-LEVEL RNK SPLIT LOGIC
# ============================================================================


def get_legs(row_list):
    """
    Build a positionally-aligned list of legs from the wide row.

    Leg i (0-indexed) uses FlightNumber{i+1} / DepartureDateLocal{i+1} and
    runs Airport{i+1} -> Airport{i+2}.

    A leg is marked keep=False (i.e. dropped) if EITHER endpoint airport
    is 'RNK'. Slots with no flight number/date at all are skipped entirely
    (they were never a real leg).
    """
    legs = []
    for i in range(MAX_FLIGHTS):
        fn = row_list[COL_IDX[f"FlightNumber{i + 1}"]]
        fd = row_list[COL_IDX[f"DepartureDateLocal{i + 1}"]]

        if not (is_valid(fn) and is_valid(fd)):
            continue

        ap_from = (
            row_list[COL_IDX[f"Airport{i + 1}"]] if i + 1 <= MAX_AIRPORTS else None
        )
        ap_to = row_list[COL_IDX[f"Airport{i + 2}"]] if i + 2 <= MAX_AIRPORTS else None

        ap_from_c = norm_text(ap_from)
        ap_to_c = norm_text(ap_to)

        is_rnk = (ap_from_c == RNK_CODE) or (ap_to_c == RNK_CODE)

        legs.append(
            {
                "pos": i,
                "fn": norm_text(fn),
                "fd": fd,
                "ap_from": ap_from_c,
                "ap_to": ap_to_c,
                "keep": not is_rnk,
            }
        )

    return legs


def build_segments(legs):
    """
    Group consecutive-by-original-position kept legs into segments.
    A dropped (RNK) leg breaks contiguity and is discarded entirely.
    """
    segments = []
    current = []

    for leg in legs:
        if not leg["keep"]:
            if current:
                segments.append(current)
                current = []
            continue

        if current and leg["pos"] != current[-1]["pos"] + 1:
            segments.append(current)
            current = []

        current.append(leg)

    if current:
        segments.append(current)

    return segments


def build_row_from_segment(parent_list, segment, new_id, parent_id):
    # Start from a full copy of the original row, so every non-leg column
    # (PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
    # and anything else not in DYNAMIC_COLS) is carried over unchanged.
    row = list(parent_list)

    for c in DYNAMIC_COLS:
        row[COL_IDX[c]] = None

    for i, leg in enumerate(segment):
        row[COL_IDX[f"FlightNumber{i + 1}"]] = leg["fn"]
        row[COL_IDX[f"DepartureDateLocal{i + 1}"]] = leg["fd"]

    airports = [segment[0]["ap_from"]] + [leg["ap_to"] for leg in segment]
    for i, ap in enumerate(airports):
        row[COL_IDX[f"Airport{i + 1}"]] = ap

    row[COL_IDX["id"]] = new_id
    row[COL_IDX["ParentId"]] = parent_id

    return row


def process_batch(rows_df, all_cols):
    """
    Every row in this batch is guaranteed to contain 'RNK' in at least one
    airport column.

      - 0 segments left -> nothing usable once RNK legs are stripped;
                           the row is dropped entirely.
      - 1 segment       -> in-place update: SAME id, ParentId = None.
      - >1 segments      -> split: each segment gets a fresh id with
                           ParentId pointing at the original row's id.

    Returns (result_rows_list, processed_original_ids, dropped_ids)
    """
    result_rows = []
    processed_ids = []
    dropped_ids = []

    records = rows_df.values.tolist()

    for row_list in records:
        original_id = row_list[COL_IDX["id"]]
        processed_ids.append(original_id)

        legs = get_legs(row_list)
        segments = build_segments(legs)

        if not segments:
            dropped_ids.append(original_id)
            continue

        if len(segments) == 1:
            result_rows.append(
                build_row_from_segment(row_list, segments[0], original_id, None)
            )
        else:
            for segment in segments:
                result_rows.append(
                    build_row_from_segment(
                        row_list, segment, str(uuid.uuid4()), original_id
                    )
                )

    return result_rows, processed_ids, dropped_ids


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

    # ── Step 1: Copy ALL rows from SOURCE into TARGET as baseline ────────────
    print(f"  Step 1: Copying all rows from '{table}' into '{TARGET_TABLE}'...")
    con.execute(f"""
        INSERT INTO "{TARGET_TABLE}" ({col_list})
        SELECT {col_list} FROM "{table}"
    """)
    total_source = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    print(f"  Copied {total_source:,} rows into '{TARGET_TABLE}'.")

    # ── Step 2: Fetch and process rows containing an RNK airport ─────────────
    filtered_total = con.execute(f"""
        SELECT COUNT(*)
        FROM "{table}"
        {RNK_WHERE_CLAUSE}
    """).fetchone()[0]
    print(f"\n  Step 2: Processing {filtered_total:,} rows containing '{RNK_CODE}'...")

    scanned = 0
    updated_count = 0
    split_parent_ids = set()
    child_count = 0
    dropped_count = 0

    all_processed_ids = []  # original ids that matched the RNK filter (to delete)
    all_result_rows = []  # accumulated cleaned/updated/split rows (to insert)

    cursor = con.cursor()
    cursor.execute(f"""
        SELECT {col_list}
        FROM "{table}"
        {RNK_WHERE_CLAUSE}
    """)

    while True:
        raw = cursor.fetchmany(batch_size)
        if not raw:
            break

        batch_df = pd.DataFrame(raw, columns=all_cols)
        result_rows, processed_ids, dropped_ids = process_batch(batch_df, all_cols)

        all_processed_ids.extend(processed_ids)
        all_result_rows.extend(result_rows)
        scanned += len(batch_df)
        dropped_count += len(dropped_ids)

        for row in result_rows:
            parent_id = row[COL_IDX["ParentId"]]
            if parent_id is None:
                updated_count += 1
            else:
                split_parent_ids.add(parent_id)
                child_count += 1

        elapsed = time.time() - t0
        rate = scanned / elapsed if elapsed > 0 else 0
        print(
            f"  {scanned:>10,} / {filtered_total:,} scanned  |"
            f"  {updated_count:>6,} updated  |"
            f"  {len(split_parent_ids):>6,} split  |"
            f"  {child_count:>8,} children  |"
            f"  {dropped_count:>6,} dropped  |  {rate:>8,.0f} rows/sec"
        )

    cursor.close()

    # ── Step 3: Delete ALL original RNK rows, then insert the results ────────
    # IMPORTANT: this must happen in this order. An "updated" row reuses the
    # SAME id as its original row, so if we inserted before deleting, the
    # delete-by-original-id would wipe out the row we just inserted.
    if all_processed_ids:
        print(
            f"\n  Step 3: Deleting {len(all_processed_ids):,} original RNK rows from '{TARGET_TABLE}'..."
        )
        id_df = pd.DataFrame({"pid": [str(x) for x in all_processed_ids]})
        con.execute(f"""
            DELETE FROM "{TARGET_TABLE}"
            WHERE CAST(id AS VARCHAR) IN (SELECT pid FROM id_df)
        """)
        print(
            f"  Deleted {len(all_processed_ids):,} original rows from '{TARGET_TABLE}'."
        )

    if all_result_rows:
        print(f"\n  Step 4: Inserting {len(all_result_rows):,} cleaned/split rows...")
        result_df = pd.DataFrame(all_result_rows, columns=all_cols)
        con.execute(
            f'INSERT INTO "{TARGET_TABLE}" ({col_list}) SELECT * FROM result_df'
        )
        print(f"  Inserted {len(all_result_rows):,} rows.")

    final_count = con.execute(f'SELECT COUNT(*) FROM "{TARGET_TABLE}"').fetchone()[0]
    con.close()

    elapsed = time.time() - t0
    print(f"\n{'=' * 65}")
    print(f"DONE  ({elapsed:.1f}s)")
    print(f"  Source rows (total)   : {total_source:,}")
    print(f"  Rows matching '{RNK_CODE}'      : {filtered_total:,}")
    print(f"  Updated in place      : {updated_count:,}")
    print(
        f"  Split into children   : {len(split_parent_ids):,} parents -> {child_count:,} children"
    )
    print(f"  Dropped (no legs left): {dropped_count:,}")
    expected_total = total_source - filtered_total + (updated_count + child_count)
    print(f"  Expected in TARGET    : {expected_total:,}")
    print(f"  Actual   in TARGET    : {final_count:,}")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    process_table()
