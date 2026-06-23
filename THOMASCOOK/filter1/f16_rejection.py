import duckdb
import math
import os
import re
import time
from datetime import datetime
from enum import Enum

import pandas as pd

# ============================================================================
# CONFIG
# ============================================================================

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "THOMASCOOK_SPLIT5"
TARGET_TABLE = "THOMASCOOK_CLEANED"
REJECTION_TABLE = "THOMASCOOK_REJECTION"

# Perfectly aligned with your DESC THOMASCOOK_SPLIT3 schema
MAX_FLIGHTS = 9  # You have FLIGHTNO1 to FLIGHTNO9
MAX_DEPARTURE_DATES = 12  # You have DEPARTURE_DATE1 to DEPARTURE_DATE12
MAX_AIRPORTS = 13  # You have AIRPORT1 to AIRPORT13

MAX_FLTNO_LEN = 8  # STRICT RULE: Max 8 characters for flight numbers
BATCH_SIZE = 200_000

DATE_YEAR_MIN = 2015
DATE_YEAR_MAX = 2030

AIRLINE_ALPHA_COL = "AIRLINE_CARRIER_CODE"

# Dynamically generate duplicate key columns matching your exact schema limits
DUP_KEY_COLS = (
    ["AIRLINE_PNR", "TICKET_NO"]
    + [f"FLIGHTNO{i}" for i in range(1, MAX_FLIGHTS + 1)]
    + [f"DEPARTURE_DATE{i}" for i in range(1, MAX_DEPARTURE_DATES + 1)]
    + [f"AIRPORT{i}" for i in range(1, MAX_AIRPORTS + 1)]
)

# ============================================================================
# REJECTION REASONS
# ============================================================================


class Reason(str, Enum):
    FN_NULL = "FlightNumber NULL"
    FN_EMPTY = "FlightNumber EMPTY"
    FN_PURELY_NUMERIC = "FlightNumber PURELY_NUMERIC"
    FN_TOO_LONG = "FlightNumber TOO_LONG"
    FN_ALL_ZEROS = "FlightNumber ALL_ZEROS"
    FN_ALL_ALPHA_AFTER_STRIP = "FlightNumber ALL_ALPHA_AFTER_STRIP"
    FN_BAD_FORMAT = "FlightNumber BAD_FORMAT"
    FD_NULL = "FlightDate NULL"
    FD_EMPTY = "FlightDate EMPTY"
    FD_BAD_FORMAT = "FlightDate BAD_FORMAT"
    FD_OUT_OF_RANGE = "FlightDate OUT_OF_RANGE"
    ROUTE_OVERFLOW = "ROUTE OVERFLOW"
    DUPLICATE_SEGMENT = "DUPLICATE SEGMENT"
    BATCH_DUPLICATE = "BATCH DUPLICATE"
    MISSING_REQUIRED_SLOT = "MISSING REQUIRED SLOT (FlightNumber1/DepartureDate1)"
    AC_BAD_FORMAT = "AirlineCode BAD_FORMAT"
    AP_BAD_FORMAT = "Airport BAD_FORMAT"


# ============================================================================
# HELPERS
# ============================================================================

# Regex checks prefix (2 or 3 letters) + digits. Total length is strictly enforced by MAX_FLTNO_LEN = 8.
_RE_FLTNO = re.compile(r"^(?:[A-Z]{3}|[A-Z0-9]{2})\d+$")
_RE_AIRLINECODE_23 = re.compile(r"^[A-Z0-9]{2,3}$", re.IGNORECASE)
_RE_AIRPORT_3 = re.compile(r"^[A-Za-z]{3}$")

_DATE_FMTS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y%m%d",
    "%d%m%Y",
    "%d.%m.%Y",
    "%Y.%m.%d",
    "%b %d %Y",
    "%d %b %Y",
    "%B %d %Y",
    "%d %B %Y",
]


def _isna(val) -> bool:
    # pd.isna robustly catches None, np.nan, pd.NaT (from empty DuckDB timestamps), and pd.NA
    if pd.isna(val):
        return True
    if isinstance(val, str) and val.strip() == "":
        return True
    return False


def _parse_date(dt_str):
    from datetime import date as _date

    if isinstance(dt_str, datetime):
        return dt_str
    if isinstance(dt_str, _date):
        return datetime(dt_str.year, dt_str.month, dt_str.day)
    s = str(dt_str).strip()
    if not s:
        return None
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    try:
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return pd.to_datetime(s, dayfirst=False).to_pydatetime()
    except Exception:
        return None


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================


def check_flightno_validity(row) -> tuple[bool, str | None]:
    reasons = []
    # Loop exactly 9 times (MAX_FLIGHTS)
    for i in range(1, MAX_FLIGHTS + 1):
        fn = row.get(f"FLIGHTNO{i}")
        dt = row.get(f"DEPARTURE_DATE{i}")

        if _isna(fn) and _isna(dt):
            continue
        if _isna(fn):
            reasons.append(f"Slot{i}: {Reason.FN_NULL} (fn={fn!r})")
            continue
        if _isna(dt):
            reasons.append(f"Slot{i}: {Reason.FD_NULL} (dt={dt!r})")
            continue

        fn_str = str(fn).strip()
        if not fn_str:
            reasons.append(f"Slot{i}: {Reason.FN_EMPTY} (fn={fn!r})")
            continue

        # 1. STRICT FORMAT CHECK: Rejects 'AI2704-225', 'QR8618,740', 'DL2971`', etc.
        if not fn_str.isalnum():
            reasons.append(
                f"Slot{i}: {Reason.FN_BAD_FORMAT} (contains invalid characters: {fn_str!r})"
            )
            continue

        fn_upper = fn_str.upper()

        # 2. Fix duplicated prefixes (e.g., "6E6E124" -> "6E124")
        if len(fn_upper) >= 4 and fn_upper[:2] == fn_upper[2:4]:
            fn_upper = fn_upper[2:]
        elif len(fn_upper) >= 6 and fn_upper[:3] == fn_upper[3:6]:
            fn_upper = fn_upper[3:]

        # 3. STRICT LENGTH CHECK: Enforces MAX_FLTNO_LEN = 8
        if len(fn_upper) > MAX_FLTNO_LEN:
            reasons.append(
                f"Slot{i}: {Reason.FN_TOO_LONG} (original={fn_str!r}, cleaned={fn_upper!r}, len={len(fn_upper)})"
            )
            continue

        # 4. Validate format strictly: 2-3 letters + digits
        if not _RE_FLTNO.fullmatch(fn_upper):
            reasons.append(
                f"Slot{i}: {Reason.FN_BAD_FORMAT} (original={fn_str!r}, cleaned={fn_upper!r})"
            )
            continue

        # 5. SUCCESS: Update the row with the cleaned flight number
        row[f"FLIGHTNO{i}"] = fn_upper

    return (True, "; ".join(reasons)) if reasons else (False, None)


def check_route_overflow(row) -> tuple[bool, str | None]:
    # Dynamic counting: safely counts all populated FLIGHTNO (up to 9) and DEPARTURE_DATE (up to 12)
    fn_count = sum(
        1 for key, val in row.items() if key.startswith("FLIGHTNO") and not _isna(val)
    )
    dt_count = sum(
        1
        for key, val in row.items()
        if key.startswith("DEPARTURE_DATE") and not _isna(val)
    )

    if fn_count != dt_count:
        return (
            True,
            f"{Reason.ROUTE_OVERFLOW} (FLIGHTNO={fn_count}, DEPARTURE_DATE={dt_count})",
        )
    return False, None


def check_missing_required_slot(row) -> tuple[bool, str | None]:
    if _isna(row.get("FLIGHTNO1")) and _isna(row.get("DEPARTURE_DATE1")):
        return (
            True,
            f"{Reason.MISSING_REQUIRED_SLOT} (both FLIGHTNO1 and DEPARTURE_DATE1 are empty)",
        )
    return False, None


def check_flightdate_format_and_range(row) -> tuple[bool, str | None]:
    from datetime import date as _date

    reasons = []
    # Loop exactly 12 times (MAX_DEPARTURE_DATES) to check all dates in your schema
    for i in range(1, MAX_DEPARTURE_DATES + 1):
        dt = row.get(f"DEPARTURE_DATE{i}")
        if _isna(dt):
            continue
        if isinstance(dt, (datetime, _date)):
            if not (DATE_YEAR_MIN <= dt.year <= DATE_YEAR_MAX):
                reasons.append(
                    f"Slot{i}: {Reason.FD_OUT_OF_RANGE} (year={dt.year}, allowed {DATE_YEAR_MIN}-{DATE_YEAR_MAX})"
                )
            continue
        parsed = _parse_date(dt)
        if parsed is None:
            reasons.append(f"Slot{i}: {Reason.FD_BAD_FORMAT} ({dt!r})")
        elif not (DATE_YEAR_MIN <= parsed.year <= DATE_YEAR_MAX):
            reasons.append(
                f"Slot{i}: {Reason.FD_OUT_OF_RANGE} (year={parsed.year}, allowed {DATE_YEAR_MIN}-{DATE_YEAR_MAX})"
            )
    return (True, "; ".join(reasons)) if reasons else (False, None)


def check_airline_code(row) -> tuple[bool, str | None]:
    if AIRLINE_ALPHA_COL is None:
        return False, None
    ac = row.get(AIRLINE_ALPHA_COL)
    if _isna(ac):
        return False, None
    ac_str = str(ac).strip()
    if not _RE_AIRLINECODE_23.fullmatch(ac_str):
        return True, f"{Reason.AC_BAD_FORMAT} col={AIRLINE_ALPHA_COL!r} val={ac_str!r}"
    return False, None


def check_airport(row) -> tuple[bool, str | None]:
    reasons = []
    for col, val in row.items():
        if "airport" in col.lower() and not _isna(val):
            v = str(val).strip()
            if v and not _RE_AIRPORT_3.fullmatch(v):
                reasons.append(f"{col}: {Reason.AP_BAD_FORMAT} ({v!r})")
    return (True, "; ".join(reasons)) if reasons else (False, None)


# ============================================================================
# BATCH-LEVEL DUPLICATE DETECTION
# ============================================================================


def _dup_key(row) -> tuple:
    return tuple(
        None if _isna(row.get(c)) else str(row.get(c)).strip() for c in DUP_KEY_COLS
    )


def find_batch_duplicates(batch: list[dict], seen_keys: set) -> tuple[list[int], set]:
    local_seen, dup_indices = set(), []
    for idx, row in enumerate(batch):
        key = _dup_key(row)
        if key in seen_keys or key in local_seen:
            dup_indices.append(idx)
        else:
            local_seen.add(key)
    seen_keys.update(local_seen)
    return dup_indices, seen_keys


_CHECKS = [
    check_missing_required_slot,
    check_flightno_validity,
    check_route_overflow,
    check_flightdate_format_and_range,
    check_airline_code,
    check_airport,
]

# ============================================================================
# PROCESS ONE BATCH
# ============================================================================


def process_batch(
    batch_df: pd.DataFrame, seen_keys: set
) -> tuple[pd.DataFrame, pd.DataFrame, set]:
    records = batch_df.to_dict("records")
    reject_rows, reject_reasons, reject_indices = [], [], set()

    for idx, row in enumerate(records):
        for check_fn in _CHECKS:
            rejected, reason = check_fn(row)
            if rejected:
                reject_indices.add(idx)
                reject_rows.append(row)
                reject_reasons.append(reason)
                break

    clean_records = [r for i, r in enumerate(records) if i not in reject_indices]
    dup_indices_local, seen_keys = find_batch_duplicates(clean_records, seen_keys)

    clean_idx_map = [i for i in range(len(records)) if i not in reject_indices]
    for local_idx in dup_indices_local:
        orig_idx = clean_idx_map[local_idx]
        reject_indices.add(orig_idx)
        reject_rows.append(records[orig_idx])
        reject_reasons.append(Reason.DUPLICATE_SEGMENT)

    reject_df = pd.DataFrame(reject_rows)
    if not reject_df.empty:
        reject_df["REJECTION_REASON"] = reject_reasons

    clean_indices = [i for i in range(len(records)) if i not in reject_indices]
    clean_df = (
        pd.DataFrame([records[i] for i in clean_indices])
        if clean_indices
        else pd.DataFrame(columns=batch_df.columns)
    )

    return clean_df, reject_df, seen_keys


# ============================================================================
# DB HELPERS
# ============================================================================


def col_names(con, table: str) -> list[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
        [table],
    ).fetchall()
    return [r[0] for r in rows]


def _sanitize_col(col: str) -> str:
    return col.strip().replace('"', '""')


def create_target_table(con, source_cols: list[str], target_table: str):
    con.execute(f'DROP TABLE IF EXISTS "{target_table}"')
    if not source_cols:
        raise ValueError("source_cols is empty")
    col_defs = ", ".join(f'"{_sanitize_col(c)}" VARCHAR' for c in source_cols)
    con.execute(f'CREATE TABLE "{target_table}" ({col_defs})')
    print(f"  Created {target_table}.")


# ============================================================================
# MAIN
# ============================================================================


def process_table(
    db_path=DB_PATH,
    source=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    rejection_table=REJECTION_TABLE,
    batch_size=BATCH_SIZE,
):
    con = duckdb.connect(db_path)
    con.execute(f"PRAGMA threads={os.cpu_count()}")
    try:
        con.execute("SET memory_limit='16GB'")
    except Exception:
        pass

    src_cols = col_names(con, source)
    col_list = ", ".join(f'"{c}"' for c in src_cols)
    print(f"  Detected {len(src_cols)} columns.")

    create_target_table(con, src_cols, target_table)
    src_col_list = ", ".join(f'"{c}"' for c in src_cols)

    rej_cols = src_cols + ["REJECTION_REASON"]
    rej_col_list = ", ".join(f'"{c}"' for c in rej_cols)

    total = con.execute(f'SELECT COUNT(*) FROM "{source}"').fetchone()[0]

    print(f"\n{'=' * 65}")
    print(f"FLIGHT ROW REJECTION  —  {total:,} rows")
    print(f"  Source     : {source}")
    print(f"  Target     : {target_table}")
    print(f"  Rejection  : {rejection_table}")
    print(f"{'=' * 65}")

    processed = rejected_tot = clean_tot = 0
    seen_keys: set = set()
    t0 = time.time()

    cursor = con.cursor()
    cursor.execute(f'SELECT {col_list} FROM "{source}"')

    while True:
        raw = cursor.fetchmany(batch_size)
        if not raw:
            break

        batch_df = pd.DataFrame(raw, columns=src_cols)
        clean_df, reject_df, seen_keys = process_batch(batch_df, seen_keys)

        if not reject_df.empty:
            for c in rej_cols:
                if c not in reject_df.columns:
                    reject_df[c] = None
            reject_df = reject_df[rej_cols]
            con.execute(
                f'INSERT INTO "{rejection_table}" ({rej_col_list}) SELECT * FROM reject_df'
            )
            rejected_tot += len(reject_df)

        if not clean_df.empty:
            for c in src_cols:
                if c not in clean_df.columns:
                    clean_df[c] = None
            clean_df = clean_df[src_cols]
            con.execute(
                f'INSERT INTO "{target_table}" ({src_col_list}) SELECT * FROM clean_df'
            )
            clean_tot += len(clean_df)

        processed += len(raw)
        elapsed = time.time() - t0
        rate = processed / elapsed if elapsed > 0 else 0
        print(
            f"  {processed:>10,} / {total:,} scanned  |  {clean_tot:>8,} clean  |  {rejected_tot:>8,} rejected  |  {rate:>8,.0f} rows/sec"
        )

    cursor.close()

    rej_count = con.execute(f'SELECT COUNT(*) FROM "{rejection_table}"').fetchone()[0]
    target_count = con.execute(f'SELECT COUNT(*) FROM "{target_table}"').fetchone()[0]

    print(f"\n{'=' * 65}\nREJECTION SUMMARY\n{'=' * 65}")
    for reason, cnt in con.execute(
        f'SELECT "REJECTION_REASON", COUNT(*) AS cnt FROM "{rejection_table}" GROUP BY "REJECTION_REASON" ORDER BY cnt DESC'
    ).fetchall():
        print(f"  {cnt:>10,}  {reason}")

    con.close()

    elapsed = time.time() - t0
    print(f"\n{'=' * 65}\nDONE  ({elapsed:.1f}s)")
    print(f"  Source rows    : {total:,}")
    print(f"  Clean rows     : {target_count:,}  → {target_table}")
    print(f"  Rejected rows  : {rej_count:,}  → {rejection_table}")
    print(f"  Pass rate      : {target_count / total * 100:.1f}%")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    process_table()
