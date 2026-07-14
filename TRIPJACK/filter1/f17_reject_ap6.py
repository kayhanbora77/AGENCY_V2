"""
TRIPJACK - Split TRIPJACK_SPLIT6 into TRIPJACK_SPLIT7 (clean) and TRIPJACK_REJECT
(rows where AirportIATACode6 IS NOT NULL), WITHOUT modifying TRIPJACK_SPLIT6.

Rule:
  If AirportIATACode6 IS NOT NULL:
    - Insert the row into TRIPJACK_REJECT with RejectionReason='Airport6 Is Not NULL'
  Otherwise:
    - Insert the row into TRIPJACK_SPLIT7 (clean rows)

  TRIPJACK_SPLIT6 is only read from - never deleted or updated.

Adjust:
  - DB_PATH
  - SOURCE_TABLE / TARGET_TABLE / REJECT_TABLE names
"""

import duckdb

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "TRIPJACK_SPLIT6"
TARGET_TABLE = "TRIPJACK_SPLIT7"
REJECT_TABLE = "TRIPJACK_REJECT"

REJECT_CONDITION = "AirportIATACode6 IS NOT NULL"
REJECT_REASON = "Airport6 Is Not NULL"


def split_by_airport6(db_path: str = DB_PATH) -> None:
    con = duckdb.connect(db_path)

    # Make sure REJECT table has a RejectionReason column.
    reject_cols = {row[1] for row in con.execute(f"PRAGMA table_info('{REJECT_TABLE}')").fetchall()}
    if "RejectionReason" not in reject_cols:
        raise RuntimeError(
            f"{REJECT_TABLE} has no RejectionReason column - check schema before running."
        )

    source_cols = [row[1] for row in con.execute(f"PRAGMA table_info('{SOURCE_TABLE}')").fetchall()]
    if "AirportIATACode6" not in source_cols:
        raise RuntimeError(f"AirportIATACode6 column is missing from {SOURCE_TABLE}")

    source_col_list = ", ".join(source_cols)

    # Preview counts
    reject_count = con.execute(
        f"SELECT COUNT(*) FROM {SOURCE_TABLE} WHERE {REJECT_CONDITION}"
    ).fetchone()[0]
    clean_count = con.execute(
        f"SELECT COUNT(*) FROM {SOURCE_TABLE} WHERE NOT ({REJECT_CONDITION})"
    ).fetchone()[0]
    print(f"Rows where {REJECT_CONDITION} (-> {REJECT_TABLE}): {reject_count}")
    print(f"Clean rows (-> {TARGET_TABLE}): {clean_count}")

    # Create TARGET_TABLE with the clean rows (same schema as SOURCE_TABLE).
    # CREATE OR REPLACE so re-running the script is safe/idempotent.
    con.execute(f"""
        CREATE OR REPLACE TABLE {TARGET_TABLE} AS
        SELECT {source_col_list}
        FROM {SOURCE_TABLE}
        WHERE NOT ({REJECT_CONDITION})
    """)

    # Insert the rejected rows into TRIPJACK_REJECT with RejectionReason.
    if reject_count > 0:
        con.execute(f"""
            INSERT INTO {REJECT_TABLE} ({source_col_list}, RejectionReason)
            SELECT {source_col_list}, '{REJECT_REASON}'
            FROM {SOURCE_TABLE}
            WHERE {REJECT_CONDITION}
        """)

    print(f"Created {TARGET_TABLE} with {clean_count} row(s).")
    print(f"Inserted {reject_count} row(s) into {REJECT_TABLE}.")
    print(f"{SOURCE_TABLE} was not modified.")
    con.close()


if __name__ == "__main__":
    split_by_airport6()