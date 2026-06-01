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
import uuid
from datetime import datetime
from enum import Enum

import pandas as pd

# ============================================================================
# CONFIG  — edit these values to point at your environment
# ============================================================================

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "MIDDLEEAST_SPLIT7"  # input  table (read-only)
TARGET_TABLE = "MIDDLEEAST_CLEANED"  # output table for clean / passing rows
REJECTION_TABLE = "MIDDLEEAST_REJECTION"  # output table for rejected rows

MAX_FLIGHTS = 4
MAX_FLTNO_LEN = 8  # max characters a flight number may have
BATCH_SIZE = 200_000

DATE_YEAR_MIN = 2015
DATE_YEAR_MAX = 2030

# Column that holds the 2-3 letter IATA airline code (e.g. 'SV', 'EK').
# NOTE: 'AirlineCode' in this table is a NUMERIC code (e.g. '65', '176') —
#       do NOT validate that column for letter format.
#       'AirlineCodes' is the correct alpha column to validate.
AIRLINE_ALPHA_COL = "AirlineCodes"  # set to None to skip airline validation

# Columns that define a duplicate segment
DUP_KEY_COLS = [
    "PNRR",
    "TDNR",
    "PaxName",
    "FlightNo1",
    "FlightNo2",
    "FlightNo3",
    "FlightNo4",
    "FlightDate1",
    "FlightDate2",
    "FlightDate3",
    "FlightDate4",
    "Airport1",
    "Airport2",
    "Airport3",
    "Airport4",
    "Airport5",
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


# ============================================================================
# HELPERS
# ============================================================================

_RE_FLTNO = re.compile(r"^[A-Z0-9]{2,3}\d+$")
_RE_AIRLINECODE_23 = re.compile(r"^[A-Za-z]{2,3}$")
_RE_AIRPORT_3 = re.compile(r"^[A-Za-z]{3}$")

# Common date formats to try when parsing FlightDate values.
# Timestamp variants (with time component) are listed first because they are
# the most specific — a value like "2023-05-14 00:00:00" would be caught here
# before falling through to the bare-date patterns.
_DATE_FMTS = [
    # ── with time component ──────────────────────────────────────────────────
    "%Y-%m-%d %H:%M:%S",  # 2023-05-14 00:00:00   ← DuckDB / SQL default
    "%Y-%m-%d %H:%M",  # 2023-05-14 00:00
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    # ── date only ───────────────────────────────────────────────────────────
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
    """True if val is None, NaN, or blank-after-strip."""
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    if isinstance(val, str) and val.strip() == "":
        return True
    return False


def _normalize_flightno(fn: str) -> str:
    """
    Remove leading zeros between the alphabetic prefix and the numeric suffix.
    E.g.  SV0020 → SV20,  AF0459 → AF459,  EK001 → EK1
    Assumes fn is already stripped and uppercased.
    """
    m = re.fullmatch(r"([A-Z0-9]{2,3}?)(\d+)", fn.upper().strip())
    if not m:
        return fn
    prefix, digits = m.group(1), m.group(2)
    # Strip leading zeros from the digit part, but keep at least '0' if all zeros
    normalized_digits = digits.lstrip("0") or "0"
    return prefix + normalized_digits


def _parse_date(dt_str):
    """
    Accept a string, date, or datetime and return a datetime (or None on failure).

    Handles:
      • Python date / datetime objects returned by DuckDB for DATE/TIMESTAMP cols
      • Strings with or without a time component
    """
    from datetime import date as _date

    # DuckDB may return native date or datetime objects — handle them directly
    if isinstance(dt_str, datetime):
        return dt_str
    if isinstance(dt_str, _date):
        return datetime(dt_str.year, dt_str.month, dt_str.day)

    s = str(dt_str).strip()
    if not s:
        return None

    # Try each explicit format first (fast, no warnings)
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue

    # Last-resort: pandas with explicit format=None and dayfirst=False to
    # avoid the %Y-%m-%d / dayfirst ambiguity warning
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


def is_valid_flightno(fn, dt) -> tuple[bool, str | None, str | None]:
    """
    Validate a single (FlightNo, FlightDate) pair.
    Returns (ok, reason_or_None, detail_or_None).
    """
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


def check_flightno_validity(row) -> tuple[bool, str | None]:
    """
    Rule 1 — validate the FlightNo value in every slot where BOTH FlightNo
    AND FlightDate are present.

    Slots where only one of the two is present are a count mismatch and are
    handled by check_route_overflow — do NOT validate them here to avoid
    generating misleading FN_NULL/DT_NULL reasons for those rows.
    """
    reasons = []
    for i in range(1, MAX_FLIGHTS + 1):
        fn = row.get(f"FlightNo{i}")
        dt = row.get(f"FlightDate{i}")

        fn_empty = _isna(fn)
        dt_empty = _isna(dt)

        # Both empty → unused slot, skip
        if fn_empty and dt_empty:
            continue

        # Only one present → count mismatch, let route_overflow handle it
        if fn_empty or dt_empty:
            continue

        # Both present → validate the flight number format
        ok, reason, detail = is_valid_flightno(fn, dt)
        if not ok:
            reasons.append(f"Slot{i}: {reason} ({detail})")

    if reasons:
        return True, "; ".join(reasons)
    return False, None


def check_route_overflow(row) -> tuple[bool, str | None]:
    """Rule 2 — FlightNo count must equal FlightDate count."""
    fn_count = sum(
        1 for i in range(1, MAX_FLIGHTS + 1) if not _isna(row.get(f"FlightNo{i}"))
    )
    dt_count = sum(
        1 for i in range(1, MAX_FLIGHTS + 1) if not _isna(row.get(f"FlightDate{i}"))
    )
    if fn_count != dt_count:
        return (
            True,
            f"{Reason.ROUTE_OVERFLOW} (FlightNo={fn_count}, FlightDate={dt_count})",
        )
    return False, None


def check_missing_required_slot(row) -> tuple[bool, str | None]:
    """
    Rule 4 — reject rows that have NO flight data at all (both FlightNo1
    and FlightDate1 are null/empty).

    NOTE: if only one of the two is present, that is a count mismatch and
    will be caught by check_route_overflow — do NOT double-reject here.
    """
    fn1_missing = _isna(row.get("FlightNo1"))
    dt1_missing = _isna(row.get("FlightDate1"))

    # Both missing → truly empty row, reject
    if fn1_missing and dt1_missing:
        return (
            True,
            f"{Reason.MISSING_REQUIRED_SLOT} (both FlightNo1 and FlightDate1 are empty)",
        )

    # Only one missing → route_overflow will catch it
    return False, None


def check_flightdate_format_and_range(row) -> tuple[bool, str | None]:
    """
    Rule 5 — FlightDate values that are present must be parseable dates
    and fall within [DATE_YEAR_MIN, DATE_YEAR_MAX].

    DuckDB may return native date/datetime objects for DATE/TIMESTAMP columns;
    these are accepted directly without string-parsing.
    """
    from datetime import date as _date

    reasons = []
    for i in range(1, MAX_FLIGHTS + 1):
        dt = row.get(f"FlightDate{i}")
        if _isna(dt):
            continue

        # Native date/datetime from DuckDB — parse directly, no string round-trip
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


def check_airline_code(row) -> tuple[bool, str | None]:
    """
    Rule 8 — The designated alpha airline-code column (AIRLINE_ALPHA_COL)
    must contain 2 or 3 letters.

    IMPORTANT: 'AirlineCode' in this dataset is a NUMERIC code ('65', '176').
    We only validate AIRLINE_ALPHA_COL ('AirlineCodes'), which holds 'SV', 'EK'
    etc.  Set AIRLINE_ALPHA_COL = None in CONFIG to skip this check entirely.
    """
    if AIRLINE_ALPHA_COL is None:
        return False, None

    ac = row.get(AIRLINE_ALPHA_COL)
    if _isna(ac):
        return False, None  # absent → not our rule to reject
    ac_str = str(ac).strip()
    if not _RE_AIRLINECODE_23.fullmatch(ac_str):
        return True, f"{Reason.AC_BAD_FORMAT} col={AIRLINE_ALPHA_COL!r} val={ac_str!r}"
    return False, None


def check_airport(row) -> tuple[bool, str | None]:
    """Rule 9 — Airport must be exactly 3 alphabetic characters."""
    # Check any column whose name contains 'Airport' (Origin, Destination, etc.)
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
    """
    Rule 7 — strip leading zeros between prefix letters and digit suffix.
    Mutates a copy of the row dict.
    """
    row = dict(row)
    for i in range(1, MAX_FLIGHTS + 1):
        fn = row.get(f"FlightNo{i}")
        if not _isna(fn):
            fn_str = str(fn).strip()
            fn_upper = fn_str.upper()
            if _RE_FLTNO.fullmatch(fn_upper):
                row[f"FlightNo{i}"] = _normalize_flightno(fn_upper)
    return row


# ============================================================================
# BATCH-LEVEL DUPLICATE DETECTION
# ============================================================================


def _dup_key(row) -> tuple:
    """
    Hashable key from DUP_KEY_COLS.
    None for null/empty values so they do not affect equality.
    """
    return tuple(
        None if _isna(row.get(c)) else str(row.get(c)).strip() for c in DUP_KEY_COLS
    )


def find_batch_duplicates(
    batch: list[dict],
    seen_keys: set,
) -> tuple[list[int], set]:
    """
    Within a batch, mark rows as BATCH_DUPLICATE if their dup_key was seen
    before (either in a previous batch via seen_keys, or earlier in this batch).
    Returns indices of duplicate rows and the updated seen_keys set.
    """
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
    check_missing_required_slot,  # Rule 4: FlightNo1 + FlightDate1 required
    check_flightno_validity,  # Rule 1: FN format
    check_route_overflow,  # Rule 2: count parity
    check_flightdate_format_and_range,  # Rules 4+5: date format & year range
    check_airline_code,  # Rule 8
    check_airport,  # Rule 9
]


# ============================================================================
# PROCESS ONE BATCH
# ============================================================================


def process_batch(
    batch_df: pd.DataFrame,
    seen_keys: set,
) -> tuple[pd.DataFrame, pd.DataFrame, set]:
    """
    Returns:
        clean_df   — rows that passed all checks (normalized flight numbers)
        reject_df  — rows to write to MIDDLEEAST_REJECTION
        seen_keys  — updated duplicate-key set
    """
    records = batch_df.to_dict("records")

    reject_rows: list[dict] = []
    reject_reasons: list[str] = []
    reject_indices: set[int] = set()

    # ── Per-row validation checks ────────────────────────────────────────────
    for idx, row in enumerate(records):
        for check_fn in _CHECKS:
            rejected, reason = check_fn(row)
            if rejected:
                reject_indices.add(idx)
                reject_rows.append(row)
                reject_reasons.append(reason)
                break  # first failing rule wins; no need to check further

    # ── Cross-batch duplicate detection ──────────────────────────────────────
    # Only run on rows that passed the per-row checks
    clean_records = [r for i, r in enumerate(records) if i not in reject_indices]
    dup_indices_local, seen_keys = find_batch_duplicates(clean_records, seen_keys)

    # Map local clean indices back to original batch indices
    clean_idx_map = [i for i in range(len(records)) if i not in reject_indices]
    for local_idx in dup_indices_local:
        orig_idx = clean_idx_map[local_idx]
        reject_indices.add(orig_idx)
        reject_rows.append(records[orig_idx])
        reject_reasons.append(Reason.DUPLICATE_SEGMENT)

    # ── Build rejection DataFrame ─────────────────────────────────────────────
    if reject_rows:
        reject_df = pd.DataFrame(reject_rows)
        reject_df["RejectionReason"] = reject_reasons
        reject_df["_id"] = [str(uuid.uuid4()) for _ in range(len(reject_df))]
    else:
        reject_df = pd.DataFrame()

    # ── Normalize flight numbers in clean rows ────────────────────────────────
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


def _sanitize_col(col: str) -> str:
    """Strip whitespace/control characters from a column name."""
    return col.strip().replace('"', '""')  # escape any embedded quotes


# def create_rejection_table(con, source_cols: list[str], rejection_table: str):
#    con.execute(f'DROP TABLE IF EXISTS "{rejection_table}"')
#    if not source_cols:
#        raise ValueError("source_cols is empty — cannot create rejection table")
#    col_defs = ", ".join(f'"{_sanitize_col(c)}" VARCHAR' for c in source_cols)
#    con.execute(f"""
#        CREATE TABLE "{rejection_table}" (
#            {col_defs},
#            "RejectionReason" VARCHAR
#        )
#    """)
#    print(f"  Created {rejection_table}.")


def create_target_table(con, source_cols: list[str], target_table: str):
    con.execute(f'DROP TABLE IF EXISTS "{target_table}"')
    if not source_cols:
        raise ValueError("source_cols is empty — cannot create target table")
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
    """
    Main entry point.

    Parameters
    ----------
    db_path         : path to the DuckDB database file
    source          : name of the raw source table to read from
    target_table    : name of the table where clean rows are written
    rejection_table : name of the table where rejected rows are written
    batch_size      : number of rows to process per batch
    """
    con = duckdb.connect(db_path)
    con.execute(f"PRAGMA threads={os.cpu_count()}")
    try:
        con.execute("SET memory_limit='16GB'")
    except Exception:
        pass

    # Paste this temporarily at the top of process_table(), right after con = duckdb.connect(...)
    all_tables = con.execute("SHOW TABLES").fetchall()
    print("Tables in DB:", all_tables)

    # Also try case-insensitive lookup
    rows = con.execute(
        "SELECT table_name, column_name FROM information_schema.columns "
        f"WHERE LOWER(table_name) = LOWER('{source}') ORDER BY ordinal_position"
    ).fetchall()
    print("Columns found (case-insensitive):", rows)

    src_cols = col_names(con, source)
    col_list = ", ".join(f'"{c}"' for c in src_cols)

    print(f"  Detected {len(src_cols)} columns.")
    for i, c in enumerate(src_cols):
        if c != c.strip() or any(ord(ch) < 32 for ch in c):
            print(f"  ⚠ Column {i} has bad chars: {c!r}")
    # Create both output tables fresh each run
    # create_rejection_table(con, src_cols, rejection_table)
    create_target_table(con, src_cols, target_table)

    # Column lists for INSERT statements
    src_col_list = ", ".join(f'"{c}"' for c in src_cols)  # target insert
    rej_cols = src_cols + ["RejectionReason"]
    rej_col_list = ", ".join(f'"{c}"' for c in rej_cols)  # rejection insert

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

    # Rejection summary by reason
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
