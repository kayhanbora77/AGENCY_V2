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
SOURCE_TABLE = "AKBAR_SPLIT5_V2"
TARGET_TABLE = "AKBAR_CLEANED_V2"
REJECTION_TABLE = "AKBAR_REJECTION_V2"

MAX_FLIGHTS = 4
MAX_FLTNO_LEN = 8
BATCH_SIZE = 200_000

DATE_YEAR_MIN = 2015
DATE_YEAR_MAX = 2030

AIRLINE_ALPHA_COL = "AirlineCodes"

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

_RE_FLTNO = re.compile(r"^[A-Z]{1,3}\d+$")
_RE_AIRLINECODE_23 = re.compile(r"^[A-Za-z]{2,3}$")
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
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
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


def is_valid_flightno(fn, dt) -> tuple[bool, str | None, str | None]:
    fn_na, dt_na = _isna(fn), _isna(dt)
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
    reasons = []
    for i in range(1, MAX_FLIGHTS + 1):
        fn, dt = row.get(f"FlightNo{i}"), row.get(f"FlightDate{i}")
        if _isna(fn) or _isna(dt):
            continue
        ok, reason, detail = is_valid_flightno(fn, dt)
        if not ok:
            reasons.append(f"Slot{i}: {reason} ({detail})")
    return (True, "; ".join(reasons)) if reasons else (False, None)


def check_route_overflow(row) -> tuple[bool, str | None]:
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
    if _isna(row.get("FlightNo1")) and _isna(row.get("FlightDate1")):
        return (
            True,
            f"{Reason.MISSING_REQUIRED_SLOT} (both FlightNo1 and FlightDate1 are empty)",
        )
    return False, None


def check_flightdate_format_and_range(row) -> tuple[bool, str | None]:
    from datetime import date as _date

    reasons = []
    for i in range(1, MAX_FLIGHTS + 1):
        dt = row.get(f"FlightDate{i}")
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
        reject_df["RejectionReason"] = reject_reasons

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

    # THE FIX: Only map the exact source columns + RejectionReason.
    # DuckDB will safely leave extra table columns (like 'id', 'DAIS', 'ParentId') as NULL.
    rej_cols = src_cols + ["RejectionReason"]
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
            f"  {processed:>10,} / {total:,} scanned  |  {clean_tot:>8,} clean  |  {rejected_tot:>8,} rejected  |  {rate:>8,.0f} rows/sec"
        )

    cursor.close()

    rej_count = con.execute(f'SELECT COUNT(*) FROM "{rejection_table}"').fetchone()[0]
    target_count = con.execute(f'SELECT COUNT(*) FROM "{target_table}"').fetchone()[0]

    print(f"\n{'=' * 65}\nREJECTION SUMMARY\n{'=' * 65}")
    for reason, cnt in con.execute(
        f'SELECT "RejectionReason", COUNT(*) AS cnt FROM "{rejection_table}" GROUP BY "RejectionReason" ORDER BY cnt DESC'
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
