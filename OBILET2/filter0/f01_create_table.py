"""
Load all 70K rows from the OBILET CSV into DuckDB.

Fixes applied (confirmed from the actual sample data):
  - 23 columns declared, matching the real file exactly (no phantom
    _SourceFile / _SourceSheet columns that don't exist in this CSV).
  - Header name is "PaxName" (no space).
  - Comma-delimited.
  - DepartureDateLocalN loaded as VARCHAR (values are date-only, e.g.
    "2/18/2025", no time component) then parsed into a real TIMESTAMP
    column afterward with TRY_STRPTIME so a format mismatch can't drop
    or block rows.

Usage:
    python load_obilet_simple.py
"""

import duckdb

SOURCE_CSV = r"C:\Users\cagri\Desktop\Agency_Data\OBilet\filter-0\Obilet_Sheet1.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TARGET_TABLE = "OBILET_RAW2"

COLUMNS = {
    "PaxName": "VARCHAR",
    "BookingRef": "VARCHAR",
    "Airline": "VARCHAR",
    "ETicketNo": "VARCHAR",
    "FlightNumber1": "VARCHAR",
    "FlightNumber2": "VARCHAR",
    "FlightNumber3": "VARCHAR",
    "FlightNumber4": "VARCHAR",
    "FlightNumber5": "VARCHAR",
    "FlightNumber6": "VARCHAR",
    "DepartureDateLocal1": "VARCHAR",
    "DepartureDateLocal2": "VARCHAR",
    "DepartureDateLocal3": "VARCHAR",
    "DepartureDateLocal4": "VARCHAR",
    "DepartureDateLocal5": "VARCHAR",
    "DepartureDateLocal6": "VARCHAR",
    "AirportIataCode1": "VARCHAR",
    "AirportIataCode2": "VARCHAR",
    "AirportIataCode3": "VARCHAR",
    "AirportIataCode4": "VARCHAR",
    "AirportIataCode5": "VARCHAR",
    "AirportIataCode6": "VARCHAR",
    "AirportIataCode7": "VARCHAR",
}

DATE_COLS = [f"DepartureDateLocal{i}" for i in range(1, 7)]


def main():
    con = duckdb.connect(DB_PATH)
    con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

    con.execute(f"""
        CREATE TABLE {TARGET_TABLE} AS
        SELECT *
        FROM read_csv(
            '{SOURCE_CSV}',
            header = true,
            delim = ',',
            auto_detect = false,
            columns = {COLUMNS!r},
            nullstr = ''
        )
    """)

    # Parse the date columns now that they're loaded as text.
    for col in DATE_COLS:
        con.execute(f"ALTER TABLE {TARGET_TABLE} ADD COLUMN {col}_PARSED TIMESTAMP")
        con.execute(f"""
            UPDATE {TARGET_TABLE}
            SET {col}_PARSED = COALESCE(
                TRY_STRPTIME({col}, '%-m/%-d/%Y %H:%M'),
                TRY_STRPTIME({col}, '%-m/%-d/%Y')
            )
        """)

    count = con.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}").fetchone()[0]
    print(f"Loaded {count} rows into {TARGET_TABLE}")

    con.close()


if __name__ == "__main__":
    main()