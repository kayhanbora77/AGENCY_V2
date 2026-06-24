import duckdb
import pandas as pd
from typing import Set, Dict, List


DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "TA_STANDARD_MMT"


def is_canada_airport(airport, canada_airports: Set[str]) -> bool:
    """Check if an airport code is a Canadian airport."""
    if airport is None:
        return False
    try:
        if pd.isna(airport):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(airport, str) and airport.strip() == "":
        return False
    return str(airport).strip().upper() in canada_airports


def load_canada_airports(conn) -> Set[str]:
    """Load Canadian airport codes into a frozenset for O(1) lookup."""
    rows = conn.execute(
        "SELECT iata FROM AIRPORTS_ALL WHERE upper(COUNTRY) = upper('Canada')"
    ).fetchall()
    return frozenset(str(r[0]).strip().upper() for r in rows if r and r[0] is not None)


def determine_can_eligibility(
    connection_legs: List[Dict], canada_airports: Set[str]
) -> Dict[int, bool]:
    """
    Determine IsCanEligible for each leg in a connection.

    Rule: If ANY leg touches a Canada airport (From OR To),
          then ALL legs in the connection get IsCanEligible=true
    """
    results = {}

    # Check if ANY leg in this connection touches Canada
    any_leg_touches_canada = False
    for leg in connection_legs:
        from_can = is_canada_airport(leg.get("FromAirport"), canada_airports)
        to_can = is_canada_airport(leg.get("ToAirport"), canada_airports)
        if from_can or to_can:
            any_leg_touches_canada = True
            break

    # Apply to ALL legs
    for leg in connection_legs:
        results[leg["RowId"]] = any_leg_touches_canada

    return results


def main():
    with duckdb.connect(DB_PATH) as conn:
        # Setup: Ensure column exists and reset all values
        conn.execute(
            f"ALTER TABLE {SOURCE_TABLE} ADD COLUMN IF NOT EXISTS IsCanEligible BOOLEAN DEFAULT NULL"
        )
        conn.execute(f"UPDATE {SOURCE_TABLE} SET IsCanEligible = NULL")

        # Load Canada airports once
        canada_airports = load_canada_airports(conn)
        print(f"Loaded {len(canada_airports)} Canadian airports")

        if not canada_airports:
            print("WARNING: No Canadian airports found in AIRPORTS_ALL table!")
            return

        # Fetch all connected rows
        df = conn.execute(f"""
            SELECT RowId, ConnectionID, LegNo, FromAirport, ToAirport
            FROM {SOURCE_TABLE}
            WHERE EUEligible IS FALSE AND ConnectionID IS NOT NULL
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
            eligibility = determine_can_eligibility(legs, canada_airports)
            updates.extend(eligibility.items())

        if not updates:
            print("No rows to process")
            return

        # Batch update using temp table
        updates_df = pd.DataFrame(updates, columns=["RowId", "IsCanEligible"])
        conn.register("can_updates", updates_df)

        conn.execute(f"""
            UPDATE {SOURCE_TABLE} AS t
            SET IsCanEligible = u.IsCanEligible
            FROM can_updates AS u
            WHERE t.RowId = u.RowId
        """)

        conn.unregister("can_updates")

        # Summary
        stats = conn.execute(f"""
            SELECT 
                COUNT(*) AS total_rows,
                SUM(CASE WHEN IsCanEligible = 1 THEN 1 ELSE 0 END) AS eligible_rows
            FROM {SOURCE_TABLE}
            WHERE ConnectionID IS NOT NULL
        """).fetchone()

        print("\nResults:")
        print(f"  Total connected legs: {stats[0]}")
        print(f"  Marked IsCanEligible=True: {stats[1]}")


if __name__ == "__main__":
    main()
