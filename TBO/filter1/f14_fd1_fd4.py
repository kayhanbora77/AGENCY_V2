import duckdb
import os
import time

# ============================================================================
# CONFIG
# ============================================================================

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "TBO_SPLIT3"
TARGET_TABLE = "TBO_SPLIT4"
REJECTION_TABLE = "TBO_REJECTION"

FD1_FD4_WHERE_CLAUSE = """
WHERE DepartureDateLocal1 = DepartureDateLocal2
  AND DepartureDateLocal2 = DepartureDateLocal3
  AND DepartureDateLocal3 = DepartureDateLocal4
"""

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


def ensure_table_like(con, source_table, target_table):
    con.execute(f'DROP TABLE IF EXISTS "{target_table}"')
    con.execute(
        f'CREATE TABLE "{target_table}" AS SELECT * FROM "{source_table}" WHERE 1=0'
    )
    print(f"  Recreated table '{target_table}'.")


# ============================================================================
# MAIN
# ============================================================================


def process_table(db_path=DB_PATH, table=SOURCE_TABLE):
    con = duckdb.connect(db_path)

    con.execute(f"PRAGMA threads={os.cpu_count()}")
    try:
        con.execute("SET memory_limit='16GB'")
    except Exception:
        pass

    ensure_parent_id_column(con, table)
    ensure_table_like(con, table, TARGET_TABLE)
    # ensure_table_like(con, table, REJECTION_TABLE)

    all_cols = col_names(con, table)
    col_list = ", ".join(f'"{c}"' for c in all_cols)

    t0 = time.time()

    total_source = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

    rejected_total = con.execute(f"""
        SELECT COUNT(*) FROM "{table}"
        {FD1_FD4_WHERE_CLAUSE}
    """).fetchone()[0]

    # ── Step 1: Insert rows that do NOT match FD1=FD2=FD3=FD4 into TARGET ────
    print(f"  Step 1: Copying non-matching rows from '{table}' into '{TARGET_TABLE}'...")
    con.execute(f"""
        INSERT INTO "{TARGET_TABLE}" ({col_list})
        SELECT {col_list} FROM "{table}"
        WHERE NOT (
            DepartureDateLocal1 = DepartureDateLocal2
            AND DepartureDateLocal2 = DepartureDateLocal3
            AND DepartureDateLocal3 = DepartureDateLocal4
        )
        OR DepartureDateLocal1 IS NULL
        OR DepartureDateLocal2 IS NULL
        OR DepartureDateLocal3 IS NULL
        OR DepartureDateLocal4 IS NULL
    """)
    target_count = con.execute(f'SELECT COUNT(*) FROM "{TARGET_TABLE}"').fetchone()[0]
    print(f"  Copied {target_count:,} rows into '{TARGET_TABLE}'.")

    # ── Step 2: Insert matching rows into REJECTION table ─────────────────────
    print(f"\n  Step 2: Copying {rejected_total:,} matching rows into '{REJECTION_TABLE}'...")
    con.execute(f"""
        INSERT INTO "{REJECTION_TABLE}" ({col_list}, "RejectionReason")
        SELECT {col_list}, 'FD1EqFD4' AS "RejectionReason" FROM "{table}" 
        {FD1_FD4_WHERE_CLAUSE}
    """)
    rejection_count = con.execute(f'SELECT COUNT(*) FROM "{REJECTION_TABLE}"').fetchone()[0]
    print(f"  Copied {rejection_count:,} rows into '{REJECTION_TABLE}'.")

    con.close()

    elapsed = time.time() - t0
    print(f"\n{'=' * 65}")
    print(f"DONE  ({elapsed:.1f}s)")
    print(f"  Source rows (total)     : {total_source:,}")
    print(f"  Rows -> TARGET          : {target_count:,}")
    print(f"  Rows -> REJECTION       : {rejection_count:,}")
    print(f"  Sum (should = source)   : {target_count + rejection_count:,}")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    process_table()