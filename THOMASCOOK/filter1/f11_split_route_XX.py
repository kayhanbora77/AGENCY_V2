import duckdb
import uuid
import os
import time
import pandas as pd
import re
import math
from datetime import datetime
from itertools import zip_longest

# ============================================================================
# CONFIG
# ============================================================================

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "THOMASCOOK_RAW"
TARGET_TABLE = "THOMASCOOK_SPLIT"

MAX_FLIGHTS = 9
MAX_DATES = 12
MAX_SECTORS = 12

BATCH_SIZE = 200_000

# ============================================================================
# COLUMN LISTS
# ============================================================================

FLIGHT_COLS = [f"FLIGHTNO{i + 1}" for i in range(MAX_FLIGHTS)]
DATE_COLS = [f"DEPARTURE_DATE{i + 1}" for i in range(MAX_DATES)]
SECTOR_COLS = [f"SECTOR{i + 1}" for i in range(MAX_SECTORS)]
DYNAMIC_COLS = FLIGHT_COLS + DATE_COLS + SECTOR_COLS

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


def _normalize_flightno(fn: str) -> str:
    m = re.fullmatch(r"([A-Z0-9]{2,3}?)(\d+)", fn.upper().strip())
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
    """Calculates absolute precise day difference in decimal format."""
    a, b = parse_dt(d1), parse_dt(d2)
    if a is None or b is None:
        return None
    # 24 saati aşan kesin süreyi hesaplamak için total_seconds kullanıyoruz
    return abs((b - a).total_seconds()) / 86400.0


def is_valid(val):
    if val is None:
        return False
    if isinstance(val, float):
        return not math.isnan(val)
    if isinstance(val, str):
        return val.strip() != ""
    return True


# ============================================================================
# EXTRACTION & CLEANING LOGIC
# ============================================================================


def extract_and_clean(row_list):
    flight_nos = []
    for i in range(MAX_FLIGHTS):
        v = row_list[COL_IDX[f"FLIGHTNO{i + 1}"]]
        if is_valid(v):
            fn_str = (v.strip() if isinstance(v, str) else str(v)).upper()
            if _RE_FLTNO.fullmatch(fn_str):
                fn_str = _normalize_flightno(fn_str)
            flight_nos.append(fn_str)

    flight_dates = []
    for i in range(MAX_DATES):
        v = row_list[COL_IDX[f"DEPARTURE_DATE{i + 1}"]]
        if is_valid(v):
            flight_dates.append(v.strip() if isinstance(v, str) else str(v))

    cnt_no = len(flight_nos)
    paired_flights = list(
        zip_longest(flight_nos, flight_dates[:cnt_no], fillvalue=None)
    )

    sectors = []
    for i in range(MAX_SECTORS):
        v = row_list[COL_IDX[f"SECTOR{i + 1}"]]
        if is_valid(v):
            sectors.append(v.strip() if isinstance(v, str) else str(v))

    sectors = sectors[:cnt_no]
    return paired_flights, sectors, cnt_no


def find_split_points(flights):
    """Splits wherever consecutive Departure Date gap > 1.0 day."""
    split_points = []
    for i in range(len(flights) - 1):
        if flights[i] is None or flights[i + 1] is None:
            continue
        # flights[i][1] -> Tuple içindeki gerçek DEPARTURE_DATE değerine erişir
        d1 = flights[i][1]
        d2 = flights[i + 1][1]

        if d1 is None or d2 is None:
            continue

        gap = day_gap(d1, d2)
        if gap is not None and gap > 1.0:
            split_points.append(i + 1)
    return split_points


def build_child_row(
    parent_list, flights_slice, sectors_slice, parent_id, assign_parent
):
    child = list(parent_list)

    for c in DYNAMIC_COLS:
        child[COL_IDX[c]] = None

    for i, (fn, fd) in enumerate(flights_slice):
        child[COL_IDX[f"FLIGHTNO{i + 1}"]] = fn
        child[COL_IDX[f"DEPARTURE_DATE{i + 1}"]] = fd

    for i, sec in enumerate(sectors_slice):
        child[COL_IDX[f"SECTOR{i + 1}"]] = sec

    if assign_parent:
        child[COL_IDX["id"]] = str(uuid.uuid4())
        child[COL_IDX["ParentId"]] = str(parent_id)
    else:
        child[COL_IDX["id"]] = str(parent_id)
        child[COL_IDX["ParentId"]] = None

    return child


# ============================================================================
# BATCH PROCESSOR
# ============================================================================


def process_batch(rows_df, all_cols):
    unsplit_rows = []
    child_rows = []

    records = rows_df.values.tolist()

    for row_list in records:
        parent_id = row_list[COL_IDX["id"]]
        flights, sectors, cnt_no = extract_and_clean(row_list)

        if cnt_no == 0:
            unsplit_rows.append(row_list)
            continue

        split_points = find_split_points(flights)

        if not split_points:
            cleaned_row = build_child_row(
                row_list, flights, sectors, parent_id, assign_parent=False
            )
            unsplit_rows.append(cleaned_row)
        else:
            start_f_idx = 0
            for split_idx in split_points + [len(flights)]:
                flights_slice = flights[start_f_idx:split_idx]
                sectors_slice = sectors[start_f_idx:split_idx]

                child = build_child_row(
                    row_list,
                    flights_slice,
                    sectors_slice,
                    parent_id,
                    assign_parent=True,
                )
                child_rows.append(child)

                start_f_idx = split_idx

    unsplit_df = (
        pd.DataFrame(unsplit_rows, columns=all_cols)
        if unsplit_rows
        else pd.DataFrame(columns=all_cols)
    )
    children_df = (
        pd.DataFrame(child_rows, columns=all_cols)
        if child_rows
        else pd.DataFrame(columns=all_cols)
    )

    return unsplit_df, children_df


# ============================================================================
# MAIN ORCHESTRATION PIPELINE
# ============================================================================


def main():
    global COL_IDX
    con = duckdb.connect(DB_PATH)

    print(f"Reading columns from {SOURCE_TABLE}...")

    pragma_rows = con.execute(f"PRAGMA table_info('{SOURCE_TABLE}')").fetchall()
    pragma_cols = [row[1] for row in pragma_rows]
    pragma_lower = [c.lower() for c in pragma_cols]

    all_cols = []
    id_source_expression = "uuid() AS id"

    if "id" in pragma_lower:
        existing_id_col = pragma_cols[pragma_lower.index("id")]
        id_source_expression = f'"{existing_id_col}" AS id'
        for col in pragma_cols:
            if col.lower() == "id":
                all_cols.append("id")
            else:
                all_cols.append(col)
    else:
        all_cols = list(pragma_cols) + ["id"]

    if "parentid" not in pragma_lower:
        all_cols.append("ParentId")

    COL_IDX = {col: idx for idx, col in enumerate(all_cols)}

    con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

    col_defs = [f'"{c}" VARCHAR' for c in all_cols]
    con.execute(f"CREATE TABLE {TARGET_TABLE} ({', '.join(col_defs)})")

    parent_init_expr = (
        "CAST(NULL AS VARCHAR) AS ParentId" if "parentid" not in pragma_lower else ""
    )

    select_items = []
    for col in pragma_cols:
        if col.lower() == "id":
            select_items.append(id_source_expression)
        else:
            select_items.append(f'"{col}"')

    if "id" not in pragma_lower:
        select_items.append(id_source_expression)
    if "parentid" not in pragma_lower:
        select_items.append(parent_init_expr)

    total_rows = con.execute(f"SELECT count(*) FROM {SOURCE_TABLE}").fetchone()[0]
    print(f"Total Rows to process: {total_rows}")

    offset = 0
    start_time = time.time()

    while offset < total_rows:
        batch_query = f"""
            SELECT {", ".join(select_items)}
            FROM {SOURCE_TABLE}
            LIMIT {BATCH_SIZE} OFFSET {offset}
        """
        df_chunk = con.execute(batch_query).df()
        df_chunk = df_chunk[all_cols]

        unsplit_df, children_df = process_batch(df_chunk, all_cols)

        if not unsplit_df.empty:
            con.append(TARGET_TABLE, unsplit_df)
        if not children_df.empty:
            con.append(TARGET_TABLE, children_df)

        offset += BATCH_SIZE
        print(f"Processed {min(offset, total_rows)}/{total_rows} rows...")

    final_count = con.execute(f"SELECT count(*) FROM {TARGET_TABLE}").fetchone()[0]
    print(f"\nExecution finished cleanly in {time.time() - start_time:.2f}s!")
    print(f"Total entries loaded into final table {TARGET_TABLE}: {final_count}")
    con.close()


if __name__ == "__main__":
    main()
