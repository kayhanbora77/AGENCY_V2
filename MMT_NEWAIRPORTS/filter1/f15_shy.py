import duckdb
import pandas as pd
from typing import Set, Dict, List

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "TA_STANDARD_MMT"
INDIGO_CARRIER = "6E"

def is_tr_airport(airport, tr_airports: Set[str]) -> bool:
    """Check if an airport code is an EU airport."""
    if airport is None:
        return False
    try:
        if pd.isna(airport):
            return False
    except (TypeError, ValueError):
        pass
    return str(airport).strip().upper() in tr_airports


def load_tr_airports(con) -> frozenset:
    """Load TR airport codes."""
    rows = con.execute("""
        SELECT CodeIataAirport 
        FROM AIRPORTS 
        WHERE CodeIso2Country  IN ('TR')
    """).fetchall()
    return frozenset(r[0].strip().upper() for r in rows if r and r[0])


def determine_shy_eligibility(
    connection_legs: List[Dict],
    tr_airports: Set[str],
    INDIGO_CARRIER: str,
) -> Dict[int, bool]:
    """
    Rule-1: If First Leg Departure Airport=TR and Last Leg Arrival Airport=TR, set IsSHY=true for all legs in the connection.
    Rule-2: If Departure Airport=TR and Carrier=Indigo (6E), set IsSHY=true for all legs in the connection.
    """
    results = {}
    
    first_departure = connection_legs[0].get("FromAirport")
    last_arrival = connection_legs[-1].get("ToAirport")

    is_tr_departure = is_tr_airport(first_departure, tr_airports)
    is_tr_arrival = is_tr_airport(last_arrival, tr_airports)
    is_indigo_carrier = connection_legs[0].get("AirlineCode") == INDIGO_CARRIER

    if is_tr_departure and is_tr_arrival:
        for leg in connection_legs:
            results[leg["RowId"]] = True
    elif is_tr_departure and is_indigo_carrier:
        for leg in connection_legs:
            results[leg["RowId"]] = True
    else:
        for leg in connection_legs:
            results[leg["RowId"]] = False

    return results

def main():
    with duckdb.connect(DB_PATH) as conn:
        # Setup: Ensure column exists and reset all values
        conn.execute(
            f"ALTER TABLE {SOURCE_TABLE} ADD COLUMN IF NOT EXISTS IsSHY BOOLEAN DEFAULT NULL"
        )
        conn.execute(f"UPDATE {SOURCE_TABLE} SET IsSHY = NULL")

        # Load reference data sets
        tr_airports = load_tr_airports(conn)

        print(f"Loaded {len(tr_airports)} TR airports")

        if not tr_airports:
            print("WARNING: No TR airports found in AIRPORTS_ALL table!")
            return

        # Fetch all connected rows (FIXED: Added AirlineCode to SELECT statement)
        df = conn.execute(f"""
            SELECT RowId, ConnectionID, LegNo, FromAirport, ToAirport, AirlineCode
            FROM {SOURCE_TABLE}
            WHERE EUEligible IS FALSE AND IsCanEligible IS FALSE AND ConnectionID IS NOT NULL
            ORDER BY ConnectionID, LegNo
        """).fetchdf()

        if df.empty:
            print("No connections found. Run connection detection first.")
            return

        print(
            f"Processing {len(df)} legs across {df['ConnectionID'].nunique()} connections..."
        )

        # Group by ConnectionID and determine eligibility
        updates = []
        for connection_id, group in df.groupby("ConnectionID"):
            legs = group.sort_values("LegNo").to_dict("records")            
            eligibility = determine_shy_eligibility(
                legs, tr_airports, INDIGO_CARRIER
            )
            updates.extend(eligibility.items())

        if not updates:
            print("No rows to process")
            return

        # Batch update using temp table
        updates_df = pd.DataFrame(updates, columns=["RowId", "IsSHY"])
        conn.register("shy_updates", updates_df)

        conn.execute(f"""
            UPDATE {SOURCE_TABLE} AS t
            SET IsSHY = u.IsSHY
            FROM shy_updates AS u
            WHERE t.RowId = u.RowId
        """)

        conn.unregister("shy_updates")

        # Summary
        stats = conn.execute(f"""
            SELECT 
                COUNT(*) AS total_rows,
                SUM(CASE WHEN IsSHY = true THEN 1 ELSE 0 END) AS eligible_rows
            FROM {SOURCE_TABLE}
            WHERE ConnectionID IS NOT NULL AND IsSHY IS NOT NULL
        """).fetchone()

        print("\nResults:")
        print(f"  Total processed legs: {stats[0] if stats[0] else 0}")
        print(f"  Marked IsSHY=True: {stats[1] if stats[1] else 0}")


if __name__ == "__main__":
    main()
