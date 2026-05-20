from datetime import timedelta
import duckdb
import pandas as pd
import time
from pathlib import Path

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path(r"C:\DuckDB")
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

SOURCE_TABLE = "AKBAR"
TARGET_TABLE = "AKBAR_TARGET"

FLTNO_REGEX = r"^([A-Z]{2,3})0+([1-9][0-9]*)$"

BATCH_SIZE = 100_000

VALID_YEAR_MIN = 2010
VALID_YEAR_MAX = 2030

THREADS = 8
MEMORY_LIMIT = "8GB"
TEMP_DIR = "/tmp/duckdb_temp"


# ==================================================
# UTILS
# ==================================================
def log(msg: str):
    print(msg, flush=True)


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


# ==================================================
# DATABASE
# ==================================================
def connect_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads={THREADS}")
    con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    return con


def create_target_table(con):
    log("♻️ Creating target table")
    con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    con.execute(f"""
        CREATE TABLE {TARGET_TABLE} (
            DAIS TIMESTAMP,
            TRNN VARCHAR,
            TDNR VARCHAR,
            AIRCODE VARCHAR,
            AIRNAME VARCHAR,
            TRNC VARCHAR,
            STAT VARCHAR,
            PNRR VARCHAR,

            FlightNumber1 VARCHAR,
            FlightNumber2 VARCHAR,
            FlightNumber3 VARCHAR,
            FlightNumber4 VARCHAR,
            
            FlightDate1 TIMESTAMP,
            FlightDate2 TIMESTAMP,
            FlightDate3 TIMESTAMP,
            FlightDate4 TIMESTAMP,

            FirstSectordate TIMESTAMP,
            LastSectordate TIMESTAMP,

            Airport1 VARCHAR,
            Airport2 VARCHAR,
            Airport3 VARCHAR,
            Airport4 VARCHAR,
            Airport5 VARCHAR,
            
            PXNM VARCHAR,
            ORIT VARCHAR,
            OriginalTktNo VARCHAR,

            CONSTRAINT uq_akbar UNIQUE (
                PNRR, AIRCODE, OriginalTktNo,
                FlightNumber1, FlightNumber2, FlightNumber3, FlightNumber4,
                FlightDate1, FlightDate2, FlightDate3, FlightDate4,
                Airport1, Airport2, Airport3, Airport4, Airport5
            )
        )
    """)


def get_total_rows(con) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").fetchone()[0]


def create_clean_view(con):
    log("🧹 Creating cleaned source view")

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW cleaned_source AS
        SELECT
            DAIS,
            TRNN,
            TDNR,
            AIRCODE,
            AIRNAME,
            TRNC,
            STAT,
            PNRR,

            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber1)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN1,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber2)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN2,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber3)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN3,
            NULLIF(
                regexp_replace(
                    replace(trim(upper(FlightNumber4)), ' ', ''),
                    '{FLTNO_REGEX}',
                    '\\1\\2'),'') AS FN4,

            TRY_CAST(FlightDate1 AS TIMESTAMP) AS DT1,
            TRY_CAST(FlightDate2 AS TIMESTAMP) AS DT2,
            TRY_CAST(FlightDate3 AS TIMESTAMP) AS DT3,
            TRY_CAST(FlightDate4 AS TIMESTAMP) AS DT4,

            TRY_CAST(FirstSectordate AS TIMESTAMP) AS FSD,
            TRY_CAST(LastSectordate AS TIMESTAMP) AS LSD,

            Airport1 AS AP1,
            Airport2 AS AP2,
            Airport3 AS AP3,
            Airport4 AS AP4,
            Airport5 AS AP5,

            PXNM,
            ORIT,
            OriginalTktNo
        FROM {SOURCE_TABLE}
        WHERE
              (TRY_CAST(FlightDate1 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(FlightDate2 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(FlightDate3 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
           OR (TRY_CAST(FlightDate4 AS TIMESTAMP) BETWEEN '{VALID_YEAR_MIN}-01-01' AND '{VALID_YEAR_MAX}-12-31')
    """)


# ==================================================
# ROUTE LOGIC
# ==================================================
def same_route(d1, d2):
    # Check for NaT (Not a Time) before calculation
    if pd.isna(d1) or pd.isna(d2):
        return False
    return abs(d2 - d1) <= timedelta(days=1)


def process_batch(con, offset):
    df = con.execute(f"""
        SELECT *
        FROM cleaned_source
        LIMIT {BATCH_SIZE} OFFSET {offset}
    """).df()

    if df.empty:
        return 0

    out_rows = []
    base_data = df[
        ["DAIS", "TRNN", "TDNR", "AIRCODE", "AIRNAME", "TRNC", "STAT", "PNRR"]
    ].values

    for idx, row in enumerate(df.itertuples(index=False)):
        flights = []

        for i in range(1, 5):
            fn = getattr(row, f"FN{i}")
            dt = getattr(row, f"DT{i}")

            # FIX: Use pd.isna to catch None, NaN, and NaT (Not a Time)
            if pd.isna(fn) or pd.isna(dt):
                continue

            fn = str(fn).strip()

            # ❌ Ignore empty strings
            if not fn:
                continue

            # ❌ Remove TK000, TK0000, 0000, etc. (Numeric part all zeros)
            stripped = fn.rstrip("0")

            # If string is empty (e.g., "0000") -> Drop
            # If string is all alpha (e.g., "TK") -> Drop
            if not stripped or stripped.isalpha():
                continue

            depAp = getattr(row, f"AP{i}")
            arrAp = getattr(row, f"AP{i + 1}")

            flights.append((fn, dt, depAp, arrAp))

        # ❌ No valid (FltNo + FltDate) pairs → skip row
        if not flights:
            continue

        # ==================================================
        # SORTING LOGIC
        # ==================================================
        # Sort flights by Date ONLY (x[1]).
        flights.sort(key=lambda x: x[1])

        # ==================================================
        # DEDUPLICATE EXACT FLIGHT SEGMENTS (CORRECT)
        # ==================================================
        seen_segments = set()
        unique_flights = []

        for fn, dt, dep_ap, arr_ap in flights:
            # Deduplicate by FlightNo + FlightDate (date-level)
            key = (fn, dt.date())

            if key not in seen_segments:
                seen_segments.add(key)
                unique_flights.append((fn, dt, dep_ap, arr_ap))

        flights = unique_flights

        # ==================================================
        # GROUP FLIGHTS INTO ROUTES (MAX 1-DAY SPAN)
        # ==================================================
        routes = []

        current = [flights[0]]
        route_start_date = flights[0][1]

        for f in flights[1:]:
            if abs(f[1] - route_start_date) <= timedelta(days=1):
                current.append(f)
            else:
                routes.append(current)
                current = [f]
                route_start_date = f[1]

        # IMPORTANT: append the last route
        routes.append(current)

        # Build output rows
        for route in routes:
            row_out = list(base_data[idx])
            # row_out = list(base_data[idx]) + [row.FSD, row.LSD]
            fn_out = [None] * 4
            dt_out = [None] * 4
            ap_out = [None] * 5

            for i, (fn, dt, dep_ap, arr_ap) in enumerate(route[:4]):
                fn_out[i] = fn
                dt_out[i] = dt
                ap_out[i] = dep_ap
                if i + 1 < len(ap_out):
                    ap_out[i + 1] = arr_ap

            out_rows.append(
                row_out
                + fn_out
                + dt_out
                + [row.FSD, row.LSD]
                + ap_out
                + [row.PXNM, row.ORIT, row.OriginalTktNo]
            )

    if not out_rows:
        return 0

    df_out = pd.DataFrame(out_rows, dtype="object")
    con.execute(f"INSERT OR IGNORE INTO {TARGET_TABLE} SELECT * FROM df_out")

    return len(out_rows)


# ==================================================
# MAIN
# ==================================================
def main():
    start = time.time()
    log(f"🚀 Start {now_str()}")

    con = connect_db()
    create_target_table(con)
    create_clean_view(con)

    result = con.execute("SELECT COUNT(*) FROM cleaned_source").fetchone()
    total = result[0] if result else 0
    log(f"📊 Cleaned rows: {total:,}")

    offset = 0
    batch = 0
    processed = 0

    while offset < total:
        batch += 1
        batch_start = time.time()
        log(f"🔄 Batch {batch} | {offset:,} → {min(offset + BATCH_SIZE, total):,}")

        rows_processed = process_batch(con, offset)
        processed += rows_processed
        offset += BATCH_SIZE

        batch_time = time.time() - batch_start
        progress = (offset / total) * 100
        eta = (batch_time * (total - offset) / BATCH_SIZE) / 3600

        log(f"✅ Processed {rows_processed:,} rows | {progress:.1f}% | ETA: {eta:.2f}h")

    elapsed = time.time() - start
    log(f"📊 Total processed: {processed:,} rows")
    log(f"⏱️ Execution Time: {elapsed / 3600:.2f} hours")
    log("🎉 ETL COMPLETED")

    con.close()


if __name__ == "__main__":
    main()
