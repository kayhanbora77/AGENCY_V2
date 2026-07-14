"""
TRIPJACK - Split TRIPJACK_SPLIT8 into TRIPJACK_SPLIT9 (clean) and TRIPJACK_REJECT
(rows where AirportIATACode1 = AirportIATACode5), WITHOUT modifying TRIPJACK_SPLIT8.

Rule:
  If AirportIATACode1 = AirportIATACode5 (and both are non-null):
    - Insert the row into TRIPJACK_REJECT with RejectionReason='Airport1 == Airport5'
  Otherwise:
    - Insert the row into TRIPJACK_SPLIT9 (clean rows)

  TRIPJACK_SPLIT8 is only read from - never deleted or updated.

NOTE - bug fixed here:
  Plain `AirportIATACode1 = AirportIATACode5` evaluates to SQL NULL (neither true
  nor false) whenever either column is NULL. Since `WHERE NOT (NULL)` is also NULL
  (not TRUE), rows with a NULL in either column were being excluded from BOTH the
  "clean" query and the "reject" query - silently disappearing from every table.
  The condition below explicitly requires both columns to be non-null before
  comparing, so every source row lands in exactly one of TARGET_TABLE / REJECT_TABLE.

Adjust:
  - DB_PATH
  - SOURCE_TABLE / TARGET_TABLE / REJECT_TABLE names
"""

import duckdb

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "TRIPJACK_SPLIT8"
TARGET_TABLE = "TRIPJACK_SPLIT9"
REJECT_TABLE = "TRIPJACK_REJECT"

REJECT_CONDITION = (
    "AirportIATACode1 IS NOT NULL "
    "AND AirportIATACode5 IS NOT NULL "
    "AND AirportIATACode1 = AirportIATACode5"
)
REJECT_REASON = "Airport1 == Airport5"


def split_by_airport1_equals_airport5(db_path: str = DB_PATH) -> None:
    con = duckdb.connect(db_path)

    # Make sure REJECT table has a RejectionReason column.
    reject_cols = {row[1] for row in con.execute(f"PRAGMA table_info('{REJECT_TABLE}')").fetchall()}
    if "RejectionReason" not in reject_cols:
        raise RuntimeError(
            f"{REJECT_TABLE} has no RejectionReason column - check schema before running."
        )

    source_cols = [row[1] for row in con.execute(f"PRAGMA table_info('{SOURCE_TABLE}')").fetchall()]

    # Validate that the required columns exist in the source table
    if "AirportIATACode1" not in source_cols or "AirportIATACode4" not in source_cols:
        raise RuntimeError(f"AirportIATACode1 or AirportIATACode4 column is missing from {SOURCE_TABLE}")

    source_col_list = ", ".join(source_cols)

    # Preview counts
    total_count = con.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").fetchone()[0]
    reject_count = con.execute(
        f"SELECT COUNT(*) FROM {SOURCE_TABLE} WHERE {REJECT_CONDITION}"
    ).fetchone()[0]
    clean_count = con.execute(
        f"SELECT COUNT(*) FROM {SOURCE_TABLE} WHERE NOT ({REJECT_CONDITION})"
    ).fetchone()[0]

    print(f"Total rows in {SOURCE_TABLE}: {total_count}")
    print(f"Rows where {REJECT_CONDITION} (-> {REJECT_TABLE}): {reject_count}")
    print(f"Clean rows (-> {TARGET_TABLE}): {clean_count}")

    # Sanity check: clean_count + reject_count should equal total_count.
    # If this ever fails again, it's the same NULL-comparison trap - go fix the
    # condition rather than the counts.
    if clean_count + reject_count != total_count:
        raise RuntimeError(
            f"Row counts don't add up: clean({clean_count}) + reject({reject_count}) "
            f"!= total({total_count}). Check REJECT_CONDITION for NULL-handling issues."
        )

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
    split_by_airport1_equals_airport5()