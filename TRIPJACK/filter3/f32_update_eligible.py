import pandas as pd
import duckdb
import logging
from typing import FrozenSet

# Configure logging
logger = logging.getLogger(__name__)

# Constants
SOURCE_TABLE = "TRIPJACK_OPERATING_FLTNO"
TARGET_TABLE = "TRIPJACK_OPERATING_FLTNO_RESULT"

DB_PATH = r"C:\DuckDB\my_db.duckdb"

# Carrier and Airport Sets
SPECIAL_NON_EU_CARRIERS: FrozenSet[str] = frozenset({"BA", "TK", "PC", "JU", "FH", "VF", "VS", "XQ"})
TR_CARRIERS: FrozenSet[str] = frozenset({"TK", "PC", "FH", "XQ", "VF"})
UK_CARRIERS: FrozenSet[str] = frozenset({"BA", "VS"})
SRB_CARRIERS: FrozenSet[str] = frozenset({"JU"})
SRB_AIRPORTS: FrozenSet[str] = frozenset({"BEG", "INI", "KVO"})
SPECIAL_CARRIERS: FrozenSet[str] = frozenset({"LH", "XQ", "QR"})

class ReferenceData:
    __slots__ = (
        "eu_airports",
        "eu_carriers",
        "tr_airports",
        "uk_airports",
        "airport_tz",
    )

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.eu_airports: frozenset = self._load_airports(con)
        self.eu_carriers: frozenset = self._load_carriers(con)
        self.tr_airports: frozenset = self._load_tr_airports(con)
        self.uk_airports: frozenset = self._load_uk_airports(con)
        self.airport_tz: dict = self._load_airport_tz(con)
        logger.info(
            f"Loaded {len(self.eu_airports):,} EU airports, {len(self.eu_carriers):,} EU carriers | "
            f"Loaded {len(self.tr_airports):,} TR airports, {len(self.uk_airports):,} UK airports"
        )

    @staticmethod
    def _load_airports(con) -> frozenset:
        rows = con.execute("""
            SELECT CodeIataAirport, timezone
            FROM AIRPORTS
            WHERE CodeIso2Country NOT IN ('TR','MA')
        """).fetchall()
        return frozenset(r[0].strip().upper() for r in rows if r and r[0])

    @staticmethod
    def _load_carriers(con) -> frozenset:
        rows = con.execute("""
            SELECT IataCode
            FROM AIRLINES
            WHERE IsInUnion = 1
        """).fetchall()
        return frozenset(r[0].strip().upper() for r in rows if r and r[0])

    @staticmethod
    def _load_uk_airports(con) -> frozenset:
        rows = con.execute("""
            SELECT CodeIataAirport
            FROM AIRPORTS
            WHERE CodeIso2Country = 'GB'
        """).fetchall()
        return frozenset(r[0].strip().upper() for r in rows if r and r[0])

    @staticmethod
    def _load_tr_airports(con) -> frozenset:
        rows = con.execute("""
            SELECT CodeIataAirport
            FROM AIRPORTS
            WHERE CodeIso2Country = 'TR'
        """).fetchall()
        return frozenset(r[0].strip().upper() for r in rows if r and r[0])

    @staticmethod
    def _load_airport_tz(con) -> dict:
        rows = con.execute(
            "SELECT iata, timezone FROM AIRPORTS_ALL WHERE iata IS NOT NULL AND timezone IS NOT NULL"
        ).fetchall()
        return {code.strip().upper(): tz for code, tz in rows if code and tz}

def _vectorized_eligibility(df: pd.DataFrame, ref_data: ReferenceData) -> pd.Series:
    """
    Determines journey eligibility using a fully vectorized, priority-based rule engine.

    Processing Logic
    ----------------
    Eligibility is evaluated at the journey (ConnectionID) level, meaning
    every flight leg belonging to the same journey receives the same final
    eligibility result.

    Journey Definitions
    -------------------
    FirstLegAirport: Departure airport of the first flight leg (lowest LegNo).
    LastLegAirport: Arrival airport of the final flight leg (highest LegNo).
    Airline: LEFT(TRIM(OperatingFlightNo), 2)
    """
    if df.empty:
        return pd.Series(dtype=bool)

    uid_col = "ConnectionID"
    grp_uid = df[uid_col]

    # Derive AirlineCode from OperatingFlightNo
    df["AirlineCode"] = df["OperatingFlightNo"].str.strip().str[:2]

    from_ap = df["FromAirport"]
    last_leg_airport = df["LastLegAirport"]

    # ==================================================
    # FirstLegAirport = departure airport of the first leg (lowest LegNo)
    # Airline1 = airline operating that first leg
    # ==================================================
    first_idx = df.groupby(uid_col, sort=False)["LegNo"].idxmin()

    first_leg_airport_by_uid = from_ap.loc[first_idx]
    first_leg_airport_by_uid.index = df.loc[first_idx, uid_col].values

    airline1_by_uid = df.loc[first_idx, "AirlineCode"]
    airline1_by_uid.index = df.loc[first_idx, uid_col].values

    first_leg_airport = grp_uid.map(first_leg_airport_by_uid)
    airline1 = grp_uid.map(airline1_by_uid)

    # Use ReferenceData for EU and TR airports
    first_is_tr = first_leg_airport.isin(ref_data.tr_airports)
    last_is_tr = last_leg_airport.isin(ref_data.tr_airports)
    first_is_eu = first_leg_airport.isin(ref_data.eu_airports)
    last_is_eu = last_leg_airport.isin(ref_data.eu_airports)
    first_is_srb = first_leg_airport.isin(SRB_AIRPORTS)
    last_is_srb = last_leg_airport.isin(SRB_AIRPORTS)

    airline1_is_special = airline1.isin(SPECIAL_CARRIERS)  # LH/XQ/QR on leg1
    is_special_non_eu_carrier = df["AirlineCode"].isin(SPECIAL_NON_EU_CARRIERS)

    # ---- Rule conditions (journey-level; uniform across every leg row sharing a ConnectionID) ----
    # RULE 5: FirstLegAirport is EU -> Eligible True
    rule5_true = first_is_eu

    # RULE 4: bookend Non-EU -> Non-EU (first-leg dep + last-leg arr) -> Eligible False
    rule4_false = (~first_is_eu) & (~last_is_eu)

    # RULE 3: any leg's airline is a SPECIAL_NON_EU_CARRIER -> True
    rule3_true = is_special_non_eu_carrier.groupby(grp_uid, sort=False).transform("any")

    # RULE 2: FirstLegAirport is TR and Airline1 is LH/XQ/QR -> Eligible True
    rule2_true = first_is_tr & airline1_is_special

    # RULE 1: bookend TR -> TR (first-leg dep + last-leg arr) -> Eligible False
    rule1_false = first_is_tr & last_is_tr

    # ==================================================
    # Combine in priority order: apply lowest priority first, then let
    # each subsequent (higher-priority) rule overwrite where it matches.
    # Rule 1 is applied last, so it wins over everything.
    # ==================================================
    eligible = pd.Series(False, index=df.index)  # default when no rule matches
    eligible = eligible.mask(rule5_true, True)  # Rule 5
    eligible = eligible.mask(rule4_false, False)  # Rule 4
    eligible = eligible.mask(rule3_true, True)  # Rule 3
    eligible = eligible.mask(rule2_true, True)  # Rule 2
    eligible = eligible.mask(rule1_false, False)  # Rule 1 (highest priority, applied last)

    # RULE 6: if any leg in a journey ends up eligible, mark every leg in that connection eligible.
    eligible = eligible.groupby(grp_uid, sort=False).transform("any")

    return eligible.astype(bool)

def process_table():
    # Connect to DuckDB
    con = duckdb.connect(DB_PATH)

    # Load reference data
    ref_data = ReferenceData(con)

    # Load data from SOURCE_TABLE
    query = f"SELECT * FROM {SOURCE_TABLE}"
    df = con.execute(query).fetchdf()

    # Apply eligibility rules
    df["EUEligible"] = _vectorized_eligibility(df, ref_data)

    # Update IsTimeLimitL1 and IsTimeLimitL2 based on EUEligible
    # Convert boolean False to integer 0
    df.loc[~df["EUEligible"], "IsTimeLimitL1"] = 0
    df.loc[~df["EUEligible"], "IsTimeLimitL2"] = 0

    # Save results to TARGET_TABLE
    con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    con.execute(f"CREATE TABLE {TARGET_TABLE} AS SELECT * FROM df")

    # Close connection
    con.close()
    
if __name__ == "__main__":
    process_table()