import duckdb
import pandas as pd
from typing import Set, Dict, List

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "TA_STANDARD_MMT"
SPECIAL_NON_EU_CARRIERS = frozenset({"BA", "TK", "PC", "JU", "FH", "VF", "VS", "XQ"})


def is_eu_airport(airport, eu_airports: Set[str]) -> bool:
    """Check if an airport code is an EU airport."""
    if airport is None:
        return False
    try:
        if pd.isna(airport):
            return False
    except (TypeError, ValueError):
        pass
    return str(airport).strip().upper() in eu_airports


def is_non_eu_carrier(carrier, eu_carriers: Set[str]) -> bool:
    """Check if a carrier code is a NON-EU carrier."""
    if carrier is None:
        return False
    try:
        if pd.isna(carrier):
            return False
    except (TypeError, ValueError):
        pass
    # If it is NOT in the EU carriers set, it is a non-EU carrier
    return (
        str(carrier).strip().upper() not in eu_carriers
        and str(carrier).strip().upper() not in SPECIAL_NON_EU_CARRIERS
    )


def is_canada_airport(airport, canada_airports: Set[str]) -> bool:
    """Check if an airport code is a Canadian airport."""
    if airport is None:
        return False
    try:
        if pd.isna(airport):
            return False
    except (TypeError, ValueError):
        pass
    return str(airport).strip().upper() in canada_airports


def load_canada_airports(conn) -> Set[str]:
    """Load Canadian airport codes."""
    rows = conn.execute(
        "SELECT iata FROM AIRPORTS_ALL WHERE upper(COUNTRY) = upper('Canada')"
    ).fetchall()
    return frozenset(str(r[0]).strip().upper() for r in rows if r and r[0] is not None)


def load_eu_airports(con) -> frozenset:
    """Load EU airport codes."""
    rows = con.execute("""
        SELECT CodeIataAirport 
        FROM AIRPORTS 
        WHERE CodeIso2Country NOT IN ('TR','MA')
    """).fetchall()
    return frozenset(r[0].strip().upper() for r in rows if r and r[0])


def load_eu_carriers(con) -> frozenset:
    """Load EU carriers (IsInUnion = 1) to make 'is_non_eu_carrier' logic work correctly."""
    rows = con.execute("""
        SELECT IataCode 
        FROM AIRLINES 
        WHERE IsInUnion = 1
    """).fetchall()
    return frozenset(r[0].strip().upper() for r in rows if r and r[0])


def determine_canada_eligibility(
    connection_legs: List[Dict],
    canada_airports: Set[str],
    eu_airports: Set[str],
    eu_carriers: Set[str],
) -> Dict[int, bool]:
    """
    Rule: If ANY leg touches Canada (From OR To), 
          ALL legs in the connection get IsCanEligible=True
    """
    results = {}
    any_leg_touches_canada = False

    for leg in connection_legs:
        from_can = is_canada_airport(leg.get("FromAirport"), canada_airports)
        to_can = is_canada_airport(leg.get("ToAirport"), canada_airports)

        if from_can or to_can:
            any_leg_touches_canada = True
            break

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

        # Load reference data sets
        canada_airports = load_canada_airports(conn)
        eu_airports = load_eu_airports(conn)
        eu_carriers = load_eu_carriers(conn)  # Fixed: Loading EU carriers now

        print(f"Loaded {len(canada_airports)} Canadian airports")
        print(f"Loaded {len(eu_airports)} EU airports")
        print(f"Loaded {len(eu_carriers)} EU carriers")

        if not canada_airports:
            print("WARNING: No Canadian airports found in AIRPORTS_ALL table!")
            return

        # Fetch all connected rows (FIXED: Added AirlineCode to SELECT statement)
        df = conn.execute(f"""
            SELECT RowId, ConnectionID, LegNo, FromAirport, ToAirport, AirlineCode
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
            # FIXED: Passed all 4 required positional arguments
            eligibility = determine_canada_eligibility(
                legs, canada_airports, eu_airports, eu_carriers
            )
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
                SUM(CASE WHEN IsCanEligible = true THEN 1 ELSE 0 END) AS eligible_rows
            FROM {SOURCE_TABLE}
            WHERE ConnectionID IS NOT NULL AND IsCanEligible IS NOT NULL
        """).fetchone()

        print("\nResults:")
        print(f"  Total processed legs: {stats[0] if stats[0] else 0}")
        print(f"  Marked IsCanEligible=True: {stats[1] if stats[1] else 0}")


if __name__ == "__main__":
    main()
