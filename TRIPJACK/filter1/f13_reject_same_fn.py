"""
TRIPJACK - Split TRIPJACK_SPLIT2 into TRIPJACK_SPLIT3 (clean) and TRIPJACK_REJECT
(rows with a repeated FlightNumber), WITHOUT modifying TRIPJACK_SPLIT2.

Rule:
  For a given row, look at FlightNumber1, FlightNumber2, FlightNumber3, FlightNumber4
  (adjust the list below if there are more/fewer flight-number columns in your schema).
  If ANY two of these are equal (non-null, non-empty):
    - Insert the row into TRIPJACK_REJECT with RejectionReason='Same FlightNumber'
  Otherwise:
    - Insert the row into TRIPJACK_SPLIT3 (clean rows)

  TRIPJACK_SPLIT2 is only read from - never deleted or updated.

Examples covered:
  FlightNumber1 == FlightNumber3  -> reject
  FlightNumber1 == FlightNumber4  -> reject

Adjust:
  - DB_PATH
  - FLIGHT_NUMBER_COLS (must match your actual column names, in order)
  - SPLIT_TABLE / SPLIT3_TABLE / REJECT_TABLE names
"""

import duckdb

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SPLIT_TABLE = "TRIPJACK_SPLIT2"            # source, read-only
SPLIT3_TABLE = "TRIPJACK_SPLIT3"           # destination for clean rows (created here)
REJECT_TABLE = "TRIPJACK_REJECT"           # destination for rejected rows

# Column names for the flight numbers on a single row, in order.
# Update this list to match the real schema (e.g. add FlightNumber5 if present).
FLIGHT_NUMBER_COLS = ["FlightNumber1", "FlightNumber2", "FlightNumber3", "FlightNumber4", "FlightNumber5"]


def split_flightnumber_duplicates(db_path: str = DB_PATH) -> None:
    con = duckdb.connect(db_path)

    # Make sure REJECT table has a RejectionReason column; if TRIPJACK_REJECT already
    # mirrors TRIPJACK_SPLIT2's schema plus RejectionReason, this is a no-op check.
    reject_cols = {row[1] for row in con.execute(f"PRAGMA table_info('{REJECT_TABLE}')").fetchall()}
    if "RejectionReason" not in reject_cols:
        raise RuntimeError(
            f"{REJECT_TABLE} has no RejectionReason column - check schema before running."
        )

    split_cols = [row[1] for row in con.execute(f"PRAGMA table_info('{SPLIT_TABLE}')").fetchall()]
    missing = [c for c in FLIGHT_NUMBER_COLS if c not in split_cols]
    if missing:
        raise RuntimeError(f"These flight number columns are missing from {SPLIT_TABLE}: {missing}")

    # Build a SQL condition: any pair of the flight number columns match (and are not blank/null)
    pair_conditions = []
    for i in range(len(FLIGHT_NUMBER_COLS)):
        for j in range(i + 1, len(FLIGHT_NUMBER_COLS)):
            col_a, col_b = FLIGHT_NUMBER_COLS[i], FLIGHT_NUMBER_COLS[j]
            pair_conditions.append(
                f"(TRIM({col_a}) <> '' AND {col_a} IS NOT NULL "
                f"AND TRIM({col_b}) <> '' AND {col_b} IS NOT NULL "
                f"AND TRIM({col_a}) = TRIM({col_b}))"
            )
    duplicate_condition = " OR ".join(pair_conditions)
    split_col_list = ", ".join(split_cols)

    # Preview counts
    dup_count = con.execute(
        f"SELECT COUNT(*) FROM {SPLIT_TABLE} WHERE {duplicate_condition}"
    ).fetchone()[0]
    clean_count = con.execute(
        f"SELECT COUNT(*) FROM {SPLIT_TABLE} WHERE NOT ({duplicate_condition})"
    ).fetchone()[0]
    print(f"Rows with a duplicate FlightNumber (-> {REJECT_TABLE}): {dup_count}")
    print(f"Clean rows (-> {SPLIT3_TABLE}): {clean_count}")

    # Create TRIPJACK_SPLIT3 with the clean rows (same schema as TRIPJACK_SPLIT2).
    # CREATE OR REPLACE so re-running the script is safe/idempotent.
    con.execute(f"""
        CREATE OR REPLACE TABLE {SPLIT3_TABLE} AS
        SELECT {split_col_list}
        FROM {SPLIT_TABLE}
        WHERE NOT ({duplicate_condition})
    """)

    # Insert the duplicate rows into TRIPJACK_REJECT with RejectionReason.
    if dup_count > 0:
        con.execute(f"""
            INSERT INTO {REJECT_TABLE} ({split_col_list}, RejectionReason)
            SELECT {split_col_list}, 'Same FlightNumber'
            FROM {SPLIT_TABLE}
            WHERE {duplicate_condition}
        """)

    print(f"Created {SPLIT3_TABLE} with {clean_count} row(s).")
    print(f"Inserted {dup_count} row(s) into {REJECT_TABLE}.")
    print(f"{SPLIT_TABLE} was not modified.")
    con.close()


if __name__ == "__main__":
    split_flightnumber_duplicates()