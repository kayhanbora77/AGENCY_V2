import duckdb
import os
import time
import pandas as pd

# ============================================================================
# CONFIG
# ============================================================================

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "TBO_SPLIT9"
TARGET_TABLE = "TBO_SPLIT10"
REJECTION_TABLE = "TBO_REJECTION"

MAX_AIRPORTS = 8
BATCH_SIZE = 200_000

AP4_AP5_CONDITIONS = "AIRPORT4 IS NOT NULL AND AIRPORT5 IS NULL"

# ============================================================================
# DB HELPERS
# ============================================================================

def col_names(con, table):
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{table}' ORDER BY ordinal_position"
    ).fetchall()
    return [r[0] for r in rows]


def ensure_table_like(con, source_table, target_table):
    con.execute(f'DROP TABLE IF EXISTS "{target_table}"')
    con.execute(
        f'CREATE TABLE "{target_table}" AS SELECT * FROM "{source_table}" WHERE 1=0'
    )
    print(f"  Recreated table '{target_table}'.")


# ============================================================================
# DUPLICATE AIRPORT CHECK (Python logic)
# ============================================================================

def has_duplicate_airports(row_dict):
    """
    Check if any two non-null airport columns in the same row have the same value.
    """
    airports = []
    for i in range(1, MAX_AIRPORTS + 1):
        col = f"Airport{i}"
        val = row_dict.get(col)
        if val is not None and str(val).strip() != "":
            airports.append(str(val).strip().upper())

    return len(airports) != len(set(airports))


# ============================================================================
# MAIN
# ============================================================================

def process_table(db_path=DB_PATH, table=SOURCE_TABLE, batch_size=BATCH_SIZE):
    con = duckdb.connect(db_path)
    con.execute(f"PRAGMA threads={os.cpu_count()}")

    ensure_table_like(con, table, TARGET_TABLE)

    all_cols = col_names(con, table)
    col_list = ", ".join(f'"{c}"' for c in all_cols)

    t0 = time.time()
    total_source = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

    # Track rejection count BEFORE insertion
    rej_before = con.execute(f'SELECT COUNT(*) FROM "{REJECTION_TABLE}"').fetchone()[0]

    # =====================================================================
    # STEP 1: Rows that do NOT match AP4_AP5 → straight to TARGET (SQL)
    # =====================================================================
    print(f"  Step 1: Copying rows NOT matching ({AP4_AP5_CONDITIONS}) to TARGET...")
    con.execute(f"""
        INSERT INTO "{TARGET_TABLE}" ({col_list})
        SELECT {col_list} FROM "{table}"
        WHERE NOT ({AP4_AP5_CONDITIONS})
    """)
    non_match_count = con.execute(f'SELECT COUNT(*) FROM "{TARGET_TABLE}"').fetchone()[0]
    print(f"  Copied {non_match_count:,} rows to TARGET (no duplicate check).")

    # =====================================================================
    # STEP 2: Rows matching AP4_AP5 → check duplicates in Python
    # =====================================================================
    match_total = con.execute(f"""
        SELECT COUNT(*) FROM "{table}" WHERE {AP4_AP5_CONDITIONS}
    """).fetchone()[0]
    print(f"\n  Step 2: Processing {match_total:,} rows matching ({AP4_AP5_CONDITIONS})...")

    keep_total = non_match_count  # already in TARGET from Step 1
    reject_total = 0

    # Stream only matching rows in batches
    cursor = con.cursor()
    cursor.execute(f'SELECT {col_list} FROM "{table}" WHERE {AP4_AP5_CONDITIONS}')

    while True:
        raw = cursor.fetchmany(batch_size)
        if not raw:
            break

        batch_df = pd.DataFrame(raw, columns=all_cols)

        keep_rows = []
        reject_rows = []

        for _, row in batch_df.iterrows():
            row_dict = row.to_dict()

            if has_duplicate_airports(row_dict):
                row_dict["RejectionReason"] = "DUPLICATE_AIRPORT_IN_ROW"
                reject_rows.append(row_dict)
            else:
                keep_rows.append(row_dict)

        # Insert kept rows into TARGET
        if keep_rows:
            keep_df = pd.DataFrame(keep_rows, columns=all_cols)
            con.execute(f'INSERT INTO "{TARGET_TABLE}" ({col_list}) SELECT * FROM keep_df')
            keep_total += len(keep_rows)

        # Insert rejected rows into REJECTION
        if reject_rows:
            reject_df = pd.DataFrame(reject_rows, columns=all_cols + ["RejectionReason"])
            reject_cols = all_cols + ["RejectionReason"]
            reject_col_list = ", ".join(f'"{c}"' for c in reject_cols)
            con.execute(f'INSERT INTO "{REJECTION_TABLE}" ({reject_col_list}) SELECT * FROM reject_df')
            reject_total += len(reject_rows)

        processed = non_match_count + keep_total - non_match_count + reject_total
        print(
            f"    Processed matching rows: {keep_total - non_match_count + reject_total:,} / {match_total:,} | "
            f"Keep: {keep_total - non_match_count:,} | Reject: {reject_total:,}"
        )

    cursor.close()

    # Validate
    assert keep_total + reject_total == total_source, \
        f"Count mismatch: {keep_total + reject_total} != {total_source}"

    rej_after = con.execute(f'SELECT COUNT(*) FROM "{REJECTION_TABLE}"').fetchone()[0]

    con.close()

    elapsed = time.time() - t0
    print(f"\n{'=' * 65}")
    print(f"DONE  ({elapsed:.1f}s)")
    print(f"  Source rows (total)     : {total_source:,}")
    print(f"  Rows -> TARGET          : {keep_total:,}")
    print(f"  Rows -> REJECTION       : {reject_total:,}  (total in table: {rej_after:,})")
    print(f"  Sum check               : {keep_total + reject_total:,} == {total_source:,} ✓")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    process_table()