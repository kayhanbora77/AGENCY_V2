"""
diagnose_raw.py
===============
Run this FIRST before flight_rejection.py.
It connects to your DuckDB, samples 20 rows, and prints the exact raw values
for every FlightNo, FlightDate, Airport, AirlineCode column — so you can see
what the data actually looks like and why rules are firing.

Usage:
    python diagnose_raw.py
"""

import duckdb
import math

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "MIDDLEEAST_RAW"
SAMPLE_ROWS = 20  # rows to inspect visually
# ─────────────────────────────────────────────────────────────────────────────


def _isna(val) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    if isinstance(val, str) and val.strip() == "":
        return True
    return False


def main():
    con = duckdb.connect(DB_PATH, read_only=True)

    # ── 1. List all columns and their DuckDB types ────────────────────────────
    print("\n" + "=" * 70)
    print(f"TABLE: {SOURCE_TABLE}")
    print("=" * 70)
    type_rows = con.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{SOURCE_TABLE}'
        ORDER BY ordinal_position
    """).fetchall()

    if not type_rows:
        # DuckDB stores names case-insensitively; try uppercase
        type_rows = con.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE UPPER(table_name) = UPPER('{SOURCE_TABLE}')
            ORDER BY ordinal_position
        """).fetchall()

    print(f"{'Column':<35} {'DuckDB Type'}")
    print("-" * 55)
    col_types = {}
    for col, dtype in type_rows:
        print(f"  {col:<33} {dtype}")
        col_types[col] = dtype
    print()

    all_cols = [r[0] for r in type_rows]
    focus_cols = [
        c
        for c in all_cols
        if any(
            tag in c
            for tag in ("FlightNo", "FlightDate", "Airport", "Airline", "PNRR", "Pax")
        )
    ]

    col_list = ", ".join(f'"{c}"' for c in all_cols)
    total = con.execute(f'SELECT COUNT(*) FROM "{SOURCE_TABLE}"').fetchone()[0]
    print(f"Total rows: {total:,}\n")

    # ── 2. Sample rows — show all focus columns raw ───────────────────────────
    rows = con.execute(
        f'SELECT {col_list} FROM "{SOURCE_TABLE}" LIMIT {SAMPLE_ROWS}'
    ).fetchall()
    sample = [dict(zip(all_cols, r)) for r in rows]

    print("=" * 70)
    print(f"RAW SAMPLE  ({SAMPLE_ROWS} rows) — focus columns")
    print("=" * 70)
    for i, row in enumerate(sample, 1):
        print(f"\n  ── Row {i} ──")
        for col in focus_cols:
            val = row.get(col)
            print(f"    {col:<30} = {val!r}  (type={type(val).__name__})")

    # ── 3. Distinct value snapshots for each FlightDate slot ──────────────────
    print("\n" + "=" * 70)
    print("DISTINCT FlightDate values  (up to 10 each)")
    print("=" * 70)
    for i in range(1, 5):
        col = f"FlightDate{i}"
        if col not in all_cols:
            continue
        vals = con.execute(f"""
            SELECT "{col}", COUNT(*) AS cnt
            FROM "{SOURCE_TABLE}"
            WHERE "{col}" IS NOT NULL
            GROUP BY "{col}"
            ORDER BY cnt DESC
            LIMIT 10
        """).fetchall()
        print(f"\n  {col}:")
        for v, cnt in vals:
            print(f"    {v!r:<40}  ({type(v).__name__})  ×{cnt:,}")

    # ── 4. Distinct value snapshots for each FlightNo slot ───────────────────
    print("\n" + "=" * 70)
    print("DISTINCT FlightNo values  (top 10 each)")
    print("=" * 70)
    for i in range(1, 5):
        col = f"FlightNo{i}"
        if col not in all_cols:
            continue
        vals = con.execute(f"""
            SELECT "{col}", COUNT(*) AS cnt
            FROM "{SOURCE_TABLE}"
            WHERE "{col}" IS NOT NULL AND "{col}" <> ''
            GROUP BY "{col}"
            ORDER BY cnt DESC
            LIMIT 10
        """).fetchall()
        print(f"\n  {col}:")
        for v, cnt in vals:
            print(f"    {v!r:<25}  ×{cnt:,}")

    # ── 5. Airport distinct values ────────────────────────────────────────────
    airport_cols = [c for c in all_cols if "airport" in c.lower()]
    if airport_cols:
        print("\n" + "=" * 70)
        print("DISTINCT Airport values  (top 10 each)")
        print("=" * 70)
        for col in airport_cols:
            vals = con.execute(f"""
                SELECT "{col}", COUNT(*) AS cnt
                FROM "{SOURCE_TABLE}"
                WHERE "{col}" IS NOT NULL AND "{col}" <> ''
                GROUP BY "{col}"
                ORDER BY cnt DESC
                LIMIT 10
            """).fetchall()
            print(f"\n  {col}:")
            for v, cnt in vals:
                print(f"    {v!r:<20}  ×{cnt:,}")

    # ── 6. AirlineCode distinct values ───────────────────────────────────────
    airline_cols = [c for c in all_cols if "airline" in c.lower()]
    if airline_cols:
        print("\n" + "=" * 70)
        print("DISTINCT AirlineCode values  (top 10 each)")
        print("=" * 70)
        for col in airline_cols:
            vals = con.execute(f"""
                SELECT "{col}", COUNT(*) AS cnt
                FROM "{SOURCE_TABLE}"
                WHERE "{col}" IS NOT NULL AND "{col}" <> ''
                GROUP BY "{col}"
                ORDER BY cnt DESC
                LIMIT 10
            """).fetchall()
            print(f"\n  {col}:")
            for v, cnt in vals:
                print(f"    {v!r:<20}  ×{cnt:,}")

    # ── 7. NULL / empty counts for key columns ────────────────────────────────
    print("\n" + "=" * 70)
    print("NULL / EMPTY counts for focus columns")
    print("=" * 70)
    print(f"  {'Column':<35} {'NULL':>10}  {'EMPTY':>10}  {'FILLED':>10}")
    print("  " + "-" * 68)
    for col in focus_cols:
        null_cnt = con.execute(
            f'SELECT COUNT(*) FROM "{SOURCE_TABLE}" WHERE "{col}" IS NULL'
        ).fetchone()[0]
        empty_cnt = con.execute(
            f'SELECT COUNT(*) FROM "{SOURCE_TABLE}" WHERE "{col}" IS NOT NULL AND TRIM(CAST("{col}" AS VARCHAR)) = \'\''
        ).fetchone()[0]
        filled = total - null_cnt - empty_cnt
        print(f"  {col:<35} {null_cnt:>10,}  {empty_cnt:>10,}  {filled:>10,}")

    con.close()
    print("\n" + "=" * 70)
    print("DONE — paste this output into your chat so the rules can be tuned.")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
