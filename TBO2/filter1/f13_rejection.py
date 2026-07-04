"""
Flight Row Rejection  —  optimized for 5M+ rows
=================================================
Reads SOURCE_TABLE in batches, applies rejection rules, and routes rows:
  • Rejected rows  → REJECTION_TABLE  (with a RejectionReason column)
  • Clean rows     → TARGET_TABLE     (with normalized FlightNo values)

Rejection Rules
---------------
1.  FlightNo validation      (FN_NULL, FN_EMPTY, DT_NULL, DT_EMPTY,
                              FN_PURELY_NUMERIC, FN_TOO_LONG, FN_ALL_ZEROS,
                              FN_ALL_ALPHA_AFTER_STRIP, FN_BAD_FORMAT)
2.  FlightNo count != FlightDate count          → ROUTE_OVERFLOW
3.  Duplicate segment (cross-batch, keep first) → DUPLICATE_SEGMENT
4.  Batch-level duplicate                       → BATCH_DUPLICATE
5.  FlightDate format / range [2015-2030]       → DT_BAD_FORMAT / DT_OUT_OF_RANGE
6.  Missing FlightNo1 + FlightDate1             → MISSING_REQUIRED_SLOT
7.  FlightNo bad format (1-3 alpha + digits)    → FN_BAD_FORMAT
8.  AirlineCode not 2-3 letters                 → AC_BAD_FORMAT
9.  Airport not exactly 3 letters               → AP_BAD_FORMAT
"""

import duckdb
import math
import os
import re
import time
from datetime import datetime
from enum import Enum

import pandas as pd

# ============================================================================
# CONFIG  — edit these values to point at your environment
# ============================================================================

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "TBO_SPLIT2"  # input  table (read-only)
TARGET_TABLE = "TBO_SPLIT3"  # output table for clean / passing rows
REJECTION_TABLE = "TBO_REJECTION"  # output table for rejected rows

MAX_FLIGHTS = 7
MAX_FLTNO_LEN = 8  # max characters a flight number may have
BATCH_SIZE = 200_000

DATE_YEAR_MIN = 2015
DATE_YEAR_MAX = 2030

AIRLINE_ALPHA_COL = "Airline"  # set to None to skip airline validation

_RE_SCI_NOTATION = re.compile(r"^(\d+)(?:\.0+)?E\+?(\d+)$", re.IGNORECASE)

# Columns that define a duplicate segment
DUP_KEY_COLS = [
    "BookingRef",
    "PaxName",
    "FlightNumber1",
    "FlightNumber2",
    "FlightNumber3",
    "FlightNumber4",
    "FlightNumber5",
    "FlightNumber6",
    "FlightNumber7",
    "DepartureDateLocal1",
    "DepartureDateLocal2",
    "DepartureDateLocal3",
    "DepartureDateLocal4",
    "DepartureDateLocal5",
    "DepartureDateLocal6",
    "DepartureDateLocal7",
    "Airport1",
    "Airport2",
    "Airport3",
    "Airport4",
    "Airport5",
    "Airport6",
    "Airport7",
    "Airport8",
]

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
    MISSING_REQUIRED_SLOT = "MISSING REQUIRED SLOT (FlightNo1/FlightDate1)"
    AC_BAD_FORMAT = "AirlineCode BAD_FORMAT"
    AP_BAD_FORMAT = "Airport BAD_FORMAT"
    FN_CONSECUTIVE_DUPLICATE = "FlightNumber CONSECUTIVE_DUPLICATE"


# ============================================================================
# HELPERS
# ============================================================================

_RE_FLTNO = re.compile(r"^[A-Z0-9]{2,3}\d+$")
_RE_AIRLINECODE_23 = re.compile(r"^[A-Za-z0-9]{2,3}$")
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


def check_consecutive_duplicate_flightno(row):
    """Reject if any two consecutive FlightNumber slots are identical
    (e.g. FlightNumber1 == FlightNumber2, or FlightNumber3 == FlightNumber4)."""
    reasons = []
    for i in range(1, MAX_FLIGHTS):
        fn_a = row.get(f"FlightNumber{i}")
        fn_b = row.get(f"FlightNumber{i + 1}")

        if _isna(fn_a) or _isna(fn_b):
            continue

        fn_a_str = str(fn_a).strip().upper()
        fn_b_str = str(fn_b).strip().upper()

        if fn_a_str and fn_a_str == fn_b_str:
            reasons.append(
                f"Slot{i}/Slot{i + 1}: {Reason.FN_CONSECUTIVE_DUPLICATE} "
                f"(FlightNumber{i}={fn_a_str!r} == FlightNumber{i + 1}={fn_b_str!r})"
            )

    if reasons:
        return True, "; ".join(reasons)
    return False, None


def _fix_row_flightnos(row: dict) -> dict:
    """Apply scientific-notation fix to all FlightNumber slots, in place semantics."""
    row = dict(row)
    for i in range(1, MAX_FLIGHTS + 1):
        fn = row.get(f"FlightNumber{i}")
        if not _isna(fn):
            fixed = _fix_scientific_notation(str(fn).strip())
            row[f"FlightNumber{i}"] = fixed
    return row


def _fix_scientific_notation(fn: str) -> str:
    """Collapse Excel-mangled scientific notation, e.g. '6.00E+78' -> '6E78'."""
    m = _RE_SCI_NOTATION.fullmatch(fn.strip())
    if not m:
        return fn
    mantissa, exponent = m.group(1), m.group(2)
    return f"{mantissa}E{exponent}"


def _isna(val) -> bool:
    """True if val is None, NaN, NaT, or blank-after-strip."""
    if val is None:
        return True
    if isinstance(val, str):
        return val.strip() == ""
    try:
        result = pd.isna(val)
    except (TypeError, ValueError):
        return False
    # pd.isna can return an array for list-like input; only trust scalar bools
    if isinstance(result, bool):
        return result
    return False


def _normalize_flightno(fn: str) -> str:
    m = re.fullmatch(r"([A-Z0-9]{2,3}?)(\d+)", fn.upper().strip())
    if not m:
        return fn
    prefix, digits = m.group(1), m.group(2)
    normalized_digits = digits.lstrip("0") or "0"
    return prefix + normalized_digits


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


def is_valid_flightno(fn, dt):
    fn_na = _isna(fn)
    dt_na = _isna(dt)

    if fn_na and dt_na:
        return False, Reason.FN_NULL, f"fn={fn!r}, dt={dt!r}"
    if fn_na:
        return False, Reason.FN_NULL, f"fn={fn!r}"
    if dt_na:
        return False, Reason.FD_NULL, f"dt={dt!r}"

    dt = str(dt).strip()
    if not dt:
        return False, Reason.FD_EMPTY, f"dt={dt!r}"

    fn = str(fn).strip()
    if not fn:
        return False, Reason.FN_EMPTY, f"fn={fn!r}"

    if fn.isdigit():
        return False, Reason.FN_PURELY_NUMERIC, f"fn={fn!r}"

    if len(fn) > MAX_FLTNO_LEN:
        return False, Reason.FN_TOO_LONG, f"fn={fn!r} len={len(fn)}"

    stripped = fn.rstrip("0")
    if not stripped:
        return False, Reason.FN_ALL_ZEROS, f"fn={fn!r}"
    if stripped.isalpha():
        return False, Reason.FN_ALL_ALPHA_AFTER_STRIP, f"fn={fn!r}"

    fn_upper = fn.upper()
    if not _RE_FLTNO.fullmatch(fn_upper):
        return False, Reason.FN_BAD_FORMAT, f"fn={fn_upper!r}"

    return True, None, None


def check_flightno_validity(row):
    reasons = []
    for i in range(1, MAX_FLIGHTS + 1):
        fn = row.get(f"FlightNumber{i}")
        dt = row.get(f"DepartureDateLocal{i}")

        fn_empty = _isna(fn)
        dt_empty = _isna(dt)

        if fn_empty and dt_empty:
            continue
        if fn_empty or dt_empty:
            continue

        ok, reason, detail = is_valid_flightno(fn, dt)
        if not ok:
            reasons.append(f"Slot{i}: {reason} ({detail})")

    if reasons:
        return True, "; ".join(reasons)
    return False, None


def check_route_overflow(row):
    """Rule 2 — FlightNo count must equal FlightDate count."""
    fn_count = sum(
        1 for i in range(1, MAX_FLIGHTS + 1) if not _isna(row.get(f"FlightNumber{i}"))
    )
    dt_count = sum(
        1
        for i in range(1, MAX_FLIGHTS + 1)
        if not _isna(row.get(f"DepartureDateLocal{i}"))
    )
    if fn_count != dt_count:
        return (
            True,
            f"{Reason.ROUTE_OVERFLOW} (FlightNumber={fn_count}, DepartureDateLocal={dt_count})",
        )
    return False, None


def check_missing_required_slot(row):
    fn1_missing = _isna(row.get("FlightNumber1"))
    dt1_missing = _isna(row.get("DepartureDateLocal1"))

    if fn1_missing and dt1_missing:
        return (
            True,
            f"{Reason.MISSING_REQUIRED_SLOT} (both FlightNumber1 and DepartureDateLocal1 are empty)",
        )
    return False, None


def check_flightdate_format_and_range(row):
    from datetime import date as _date

    reasons = []
    for i in range(1, MAX_FLIGHTS + 1):
        dt = row.get(f"DepartureDateLocal{i}")
        if _isna(dt):
            continue

        if isinstance(dt, (datetime, _date)):
            year = dt.year
            if not (DATE_YEAR_MIN <= year <= DATE_YEAR_MAX):
                reasons.append(
                    f"Slot{i}: {Reason.FD_OUT_OF_RANGE} "
                    f"(year={year}, allowed {DATE_YEAR_MIN}-{DATE_YEAR_MAX})"
                )
            continue

        dt_str = str(dt).strip()
        if not dt_str:
            continue

        parsed = _parse_date(dt_str)
        if parsed is None:
            reasons.append(f"Slot{i}: {Reason.FD_BAD_FORMAT} ({dt_str!r})")
            continue

        if not (DATE_YEAR_MIN <= parsed.year <= DATE_YEAR_MAX):
            reasons.append(
                f"Slot{i}: {Reason.FD_OUT_OF_RANGE} "
                f"(year={parsed.year}, allowed {DATE_YEAR_MIN}-{DATE_YEAR_MAX})"
            )

    if reasons:
        return True, "; ".join(reasons)
    return False, None


def check_airline_code(row):
    if AIRLINE_ALPHA_COL is None:
        return False, None

    ac = row.get(AIRLINE_ALPHA_COL)
    if _isna(ac):
        return False, None
    ac_str = str(ac).strip()
    if not _RE_AIRLINECODE_23.fullmatch(ac_str):
        return True, f"{Reason.AC_BAD_FORMAT} col={AIRLINE_ALPHA_COL!r} val={ac_str!r}"
    return False, None


def check_airport(row):
    reasons = []
    for col, val in row.items():
        if "airport" in col.lower() and not _isna(val):
            v = str(val).strip()
            if v and not _RE_AIRPORT_3.fullmatch(v):
                reasons.append(f"{col}: {Reason.AP_BAD_FORMAT} ({v!r})")
    if reasons:
        return True, "; ".join(reasons)
    return False, None


# ============================================================================
# NORMALIZATION
# ============================================================================


def normalize_flight_numbers(row: dict) -> dict:
    row = dict(row)
    for i in range(1, MAX_FLIGHTS + 1):
        fn = row.get(f"FlightNumber{i}")
        if not _isna(fn):
            fn_str = str(fn).strip()
            fn_upper = fn_str.upper()
            if _RE_FLTNO.fullmatch(fn_upper):
                row[f"FlightNumber{i}"] = _normalize_flightno(fn_upper)
    return row


# ============================================================================
# TABLE CREATION / ENSURE HELPERS
# ============================================================================


def _sanitize_col(col: str) -> str:
    return col.strip().replace('"', '""')


def ensure_rejection_table(con, source_cols: list[str], rejection_table: str):
    """Create rejection table only if it doesn't exist."""
    exists = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_name = '{rejection_table}'
    """).fetchone()[0]

    if exists:
        cols = {
            r[0]
            for r in con.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = '{rejection_table}'
        """).fetchall()
        }

        if "RejectionReason" not in cols:
            con.execute(
                f'ALTER TABLE "{rejection_table}" ADD COLUMN "RejectionReason" VARCHAR'
            )
            print(f"  Added RejectionReason column to {rejection_table}.")
        else:
            print(f"  Rejection table '{rejection_table}' already exists.")
        return

    col_defs = ", ".join(f'"{_sanitize_col(c)}" VARCHAR' for c in source_cols)
    con.execute(f"""
        CREATE TABLE "{rejection_table}" (
            {col_defs},
            "RejectionReason" VARCHAR
        )
    """)
    print(f"  Created {rejection_table}.")


def ensure_target_table(con, source_cols: list[str], target_table: str):
    """Drop and recreate target table fresh on every run."""
    con.execute(f'DROP TABLE IF EXISTS "{target_table}"')
    col_defs = ", ".join(f'"{_sanitize_col(c)}" VARCHAR' for c in source_cols)
    con.execute(f'CREATE TABLE "{target_table}" ({col_defs})')
    print(f"  Dropped and recreated {target_table}.")


# ============================================================================
# BATCH-LEVEL DUPLICATE DETECTION
# ============================================================================


def _dup_key(row) -> tuple:
    return tuple(
        None if _isna(row.get(c)) else str(row.get(c)).strip() for c in DUP_KEY_COLS
    )


def find_batch_duplicates(batch: list[dict], seen_keys: set):
    local_seen: set = set()
    dup_indices: list[int] = []

    for idx, row in enumerate(batch):
        key = _dup_key(row)
        if key in seen_keys or key in local_seen:
            dup_indices.append(idx)
        else:
            local_seen.add(key)

    seen_keys.update(local_seen)
    return dup_indices, seen_keys


# ============================================================================
# ALL VALIDATION CHECKS IN ORDER
# ============================================================================

_CHECKS = [
    check_missing_required_slot,
    check_flightno_validity,
    check_consecutive_duplicate_flightno,
    check_route_overflow,
    check_flightdate_format_and_range,
    check_airline_code,
    check_airport,
]


# ============================================================================
# PROCESS ONE BATCH
# ============================================================================


def process_batch(batch_df: pd.DataFrame, seen_keys: set):
    records = batch_df.to_dict("records")

    reject_rows: list[dict] = []
    reject_reasons: list[str] = []
    reject_indices: set[int] = set()

    for idx, row in enumerate(records):
        row = _fix_row_flightnos(row)
        records[idx] = row
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

    if reject_rows:
        reject_df = pd.DataFrame(reject_rows)
        reject_df["RejectionReason"] = reject_reasons
    else:
        reject_df = pd.DataFrame()

    clean_indices = [i for i in range(len(records)) if i not in reject_indices]
    if clean_indices:
        clean_records_normalized = [
            normalize_flight_numbers(records[i]) for i in clean_indices
        ]
        clean_df = pd.DataFrame(clean_records_normalized)
    else:
        clean_df = pd.DataFrame(columns=batch_df.columns)

    return clean_df, reject_df, seen_keys


# ============================================================================
# DB HELPERS
# ============================================================================


def col_names(con, table: str) -> list[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{table}' ORDER BY ordinal_position"
    ).fetchall()
    return [r[0] for r in rows]


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

    # Ensure tables exist without dropping them
    ensure_rejection_table(con, src_cols, rejection_table)
    ensure_target_table(con, src_cols, target_table)

    src_col_list = ", ".join(f'"{c}"' for c in src_cols)
    rej_cols = src_cols + ["RejectionReason"]
    rej_col_list = ", ".join(f'"{c}"' for c in rej_cols)

    total = con.execute(f'SELECT COUNT(*) FROM "{source}"').fetchone()[0]

    print(f"\n{'=' * 65}")
    print(f"FLIGHT ROW REJECTION  —  {total:,} rows")
    print(f"  Source     : {source}")
    print(f"  Target     : {target_table}")
    print(f"  Rejection  : {rejection_table}")
    print(f"{'=' * 65}")

    processed = 0
    rejected_tot = 0
    clean_tot = 0
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

        # ── Write rejected rows → REJECTION_TABLE ────────────────────────────
        if not reject_df.empty:
            for c in rej_cols:
                if c not in reject_df.columns:
                    reject_df[c] = None
            reject_df = reject_df[rej_cols]
            con.execute(
                f'INSERT INTO "{rejection_table}" ({rej_col_list}) SELECT * FROM reject_df'
            )
            rejected_tot += len(reject_df)

        # ── Write clean rows → TARGET_TABLE ──────────────────────────────────
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
            f"  {processed:>10,} / {total:,} scanned  |"
            f"  {clean_tot:>8,} clean  |"
            f"  {rejected_tot:>8,} rejected  |  {rate:>8,.0f} rows/sec"
        )

    cursor.close()

    rej_count = con.execute(f'SELECT COUNT(*) FROM "{rejection_table}"').fetchone()[0]
    target_count = con.execute(f'SELECT COUNT(*) FROM "{target_table}"').fetchone()[0]

    print(f"\n{'=' * 65}")
    print("REJECTION SUMMARY")
    print(f"{'=' * 65}")
    summary = con.execute(f"""
        SELECT "RejectionReason", COUNT(*) AS cnt
        FROM "{rejection_table}"
        GROUP BY "RejectionReason"
        ORDER BY cnt DESC
    """).fetchall()
    for reason, cnt in summary:
        print(f"  {cnt:>10,}  {reason}")

    con.close()

    elapsed = time.time() - t0
    print(f"\n{'=' * 65}")
    print(f"DONE  ({elapsed:.1f}s)")
    print(f"  Source rows    : {total:,}")
    print(f"  Clean rows     : {target_count:,}  → {target_table}")
    print(f"  Rejected rows  : {rej_count:,}  → {rejection_table}")
    print(f"  Pass rate      : {target_count / total * 100:.1f}%")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    process_table()
