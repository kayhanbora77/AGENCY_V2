import duckdb
import os
import time

# ============================================================================
# CONFIG
# ============================================================================

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "TBO_SPLIT8"
TARGET_TABLE = "TBO_SPLIT9"
REJECTION_TABLE = "TBO_REJECTION"

FD1_FD2_FD3_CONDITIONS = """
DEPARTUREDATELOCAL1=DEPARTUREDATELOCAL2 AND DEPARTUREDATELOCAL2=DEPARTUREDATELOCAL3
AND AIRPORT4 IS NOT NULL AND AIRPORT5 IS NULL
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

    ensure_table_like(con, table, TARGET_TABLE)

    all_cols = col_names(con, table)
    col_list = ", ".join(f'"{c}"' for c in all_cols)

    t0 = time.time()
    total_source = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

    rejected_total = con.execute(f"""
        SELECT COUNT(*) FROM "{table}"
        WHERE {FD1_FD2_FD3_CONDITIONS}
    """).fetchone()[0]

    # Track rejection count BEFORE insertion
    rej_before = con.execute(f'SELECT COUNT(*) FROM "{REJECTION_TABLE}"').fetchone()[0]

    # Step 1: Keep rows
    con.execute(f"""
        INSERT INTO "{TARGET_TABLE}" ({col_list})
        SELECT {col_list} FROM "{table}"
        WHERE NOT ({FD1_FD2_FD3_CONDITIONS})
    """)
    target_count = con.execute(f'SELECT COUNT(*) FROM "{TARGET_TABLE}"').fetchone()[0]

    # Step 2: Reject rows
    con.execute(f"""
        INSERT INTO "{REJECTION_TABLE}" ({col_list}, "RejectionReason")
        SELECT {col_list}, 'FD1=FD2=FD3' FROM "{table}" 
        WHERE {FD1_FD2_FD3_CONDITIONS}
    """)
    rej_after = con.execute(f'SELECT COUNT(*) FROM "{REJECTION_TABLE}"').fetchone()[0]
    rejection_inserted = rej_after - rej_before

    # Validate
    assert target_count + rejection_inserted == total_source, \
        f"Count mismatch: {target_count + rejection_inserted} != {total_source}"

    con.close()

    elapsed = time.time() - t0
    print(f"\n{'=' * 65}")
    print(f"DONE  ({elapsed:.1f}s)")
    print(f"  Source rows (total)     : {total_source:,}")
    print(f"  Rows -> TARGET          : {target_count:,}")
    print(f"  Rows -> REJECTION       : {rejection_inserted:,}  (total in table: {rej_after:,})")
    print(f"  Sum check               : {target_count + rejection_inserted:,} == {total_source:,} ✓")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    process_table()