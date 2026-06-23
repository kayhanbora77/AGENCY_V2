import duckdb
import uuid
import pandas as pd


DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "MMT_RAW"


# ============================================================================
# Connection Detection Logic
# ============================================================================


def is_connection(prev_row, curr_row):
    """Check if two rows represent a valid connecting flight pair."""

    if prev_row.get("BookingId") != curr_row.get("BookingId"):
        return False

    if prev_row.get("NewArrAirport") != curr_row.get("NewDepAirport"):
        return False

    prev_depdate = prev_row.get("DepartureDate")
    curr_depdate = curr_row.get("DepartureDate")

    if prev_depdate is None or curr_depdate is None:
        return False

    try:
        return (curr_depdate - prev_depdate).days <= 1
    except Exception:
        return False


def tag_connection(prev_row, curr_row):
    """Assign ConnectionId and LegNo to a connected flight pair."""
    conn_id = uuid.uuid4()
    prev_row["ConnectionId"] = conn_id
    prev_row["LegNo"] = 1
    curr_row["ConnectionId"] = conn_id
    curr_row["LegNo"] = 2


def detect_connections(df):
    """Walk through ordered rows and tag connecting flight pairs."""

    df_list = df.to_dict(orient="records")
    connected_pairs = []

    for i in range(1, len(df_list)):
        prev_row = df_list[i - 1]
        curr_row = df_list[i]

        if is_connection(prev_row, curr_row):
            tag_connection(prev_row, curr_row)
            connected_pairs.append((prev_row, curr_row))

    # All rows without a LegNo are single (non-connected) flights → LegNo = 1
    for row in df_list:
        if "LegNo" not in row:
            row["LegNo"] = 1

    return df_list, connected_pairs


# ============================================================================
# Database Operations
# ============================================================================


def fetch_data(conn, table, limit=None):
    """Pull rows from DuckDB, ordered for sequential comparison."""
    query = f"""
        SELECT * FROM {table}
        WHERE NEWDEPAIRPORT IS NOT NULL
        ORDER BY BOOKINGID, DEPARTUREDATE
    """
    if limit:
        query += f" LIMIT {limit}"

    return conn.execute(query).fetchdf()


def persist_connections(conn, df_list):
    """Write ConnectionId and LegNo back to MMT_RAW."""

    conn.execute("ALTER TABLE MMT_RAW ADD COLUMN IF NOT EXISTS ConnectionId UUID;")
    conn.execute("ALTER TABLE MMT_RAW ADD COLUMN IF NOT EXISTS LegNo INTEGER;")

    updates = [
        (r["ConnectionId"], r["LegNo"], r["Id"])
        for r in df_list
        if r.get("ConnectionId") is not None and r.get("LegNo") is not None
    ]

    if not updates:
        print("No connections to persist.")
        return

    conn.executemany(
        "UPDATE MMT_RAW SET ConnectionId = ?, LegNo = ? WHERE Id = ?", updates
    )
    print(f"Persisted {len(updates)} connection records.")


# ============================================================================
# Main
# ============================================================================


def main():
    conn = duckdb.connect(DB_PATH)

    df = fetch_data(conn, SOURCE_TABLE, limit=50)

    result_list, connected_pairs = detect_connections(df)

    if connected_pairs:
        print(f"Found {len(connected_pairs)} connected flight pairs:\n")
        for prev, curr in connected_pairs:
            print(f"  Booking: {prev['BookingId']}")
            print(f"  {prev['FlightNumber']} → {curr['FlightNumber']}")
            print(f"  ConnectionId: {prev['ConnectionId']}")
            print()
    else:
        print("No connections found.")

    persist_connections(conn, result_list)


if __name__ == "__main__":
    main()
