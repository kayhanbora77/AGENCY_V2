import time
import tempfile
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

# ────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────
DB_PATH = r"C:\DuckDB\my_db.duckdb"
THREADS = 8
MEMORY_LIMIT = "12GB"
TEMP_DIR = Path(tempfile.gettempdir()) / "duckdb_temp"
SOURCE_TABLE = "TA_STANDARD_MIDDLEEAST"


# ────────────────────────────────────────────────
# CONSTANTS
# ────────────────────────────────────────────────
SPECIAL_NON_EU_TIME_LIMITS = {
    "BA": (6, 6),
    "TK": (2, 2),
    "PC": (2, 2),
    "JU": (2, 2),
    "FH": (2, 2),
    "VF": (2, 2),
    "VS": (6, 6),
}


# ────────────────────────────────────────────────
# UTILITIES
# ────────────────────────────────────────────────
def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def get_connection() -> duckdb.DuckDBPyConnection:
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads = {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{TEMP_DIR}'")
    return con


# ────────────────────────────────────────────────
# DATA LOADING
# ────────────────────────────────────────────────
def get_eu_eligible_data(con: duckdb.DuckDBPyConnection) -> Optional[pd.DataFrame]:
    log("Loading EU eligible data...")

    query = f"""
        SELECT            
            t.ConnectionID,
            t.AirlineCode,
            strftime('%Y-%m-%d', t.DepartureDate)     AS DepartureDate,
            depAirport.NameCountry                    AS depCountry,
            arrAirport.NameCountry                    AS arrCountry,
            COALESCE(depLimits.LimitL1, 0)            AS depL1,
            COALESCE(depLimits.LimitL2, 0)            AS depL2,
            COALESCE(arrLimits.LimitL1, 0)            AS arrL1,
            COALESCE(arrLimits.LimitL2, 0)            AS arrL2,
            t.LegNo
        FROM {SOURCE_TABLE} t
        LEFT JOIN AIRPORTS depAirport
            ON t.FromAirport = depAirport.CodeIataAirport
        LEFT JOIN AIRPORTS arrAirport
            ON t.ToAirport = arrAirport.CodeIataAirport
        LEFT JOIN TIME_LIMITS depLimits
            ON depAirport.NameCountry = depLimits.Country
        LEFT JOIN TIME_LIMITS arrLimits
            ON arrAirport.NameCountry = arrLimits.Country
        WHERE t.EUEligible IS TRUE
        ORDER BY t.ConnectionID NULLS LAST, t.LegNo
    """

    try:
        df = con.execute(query).df()
    except Exception as e:
        log(f"Error fetching data: {e}")
        return None

    if df.empty:
        log("No EU eligible records found.")
        return None

    log(f"Loaded {len(df):,} legs")
    return df


# ────────────────────────────────────────────────
# BUSINESS LOGIC
# ────────────────────────────────────────────────
def calculate_timelimits_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ConnectionID", "IsTimeLimitL1", "IsTimeLimitL2"])

    df = df.copy()

    # Numeric conversion
    num_cols = ["depL1", "arrL1", "depL2", "arrL2"]
    df[num_cols] = (
        df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0).astype(float)
    )

    df["group_key"] = df["ConnectionID"]

    # Aggregate per group
    agg = (
        df.groupby("group_key", sort=False)
        .agg(
            ConnectionID=("ConnectionID", "first"),
            AirlineCode=("AirlineCode", "first"),
            DepartureDate=("DepartureDate", "min"),
            depL1=("depL1", "max"),
            arrL1=("arrL1", "max"),
            depL2=("depL2", "max"),
            arrL2=("arrL2", "max"),
        )
        .reset_index(drop=True)
    )

    # Max limit across dep/arr airports
    agg["limitL1"] = agg[["depL1", "arrL1"]].max(axis=1)
    agg["limitL2"] = agg[["depL2", "arrL2"]].max(axis=1)

    # Special non-EU carrier fallback when both limits are zero
    needs_special = (agg["limitL1"] == 0) & (agg["limitL2"] == 0)
    if needs_special.any():
        special_map = agg.loc[needs_special, "AirlineCode"].map(
            SPECIAL_NON_EU_TIME_LIMITS
        )
        agg.loc[needs_special, "limitL1"] = special_map.apply(
            lambda x: float(x[0]) if isinstance(x, tuple) else 0.0
        )
        agg.loc[needs_special, "limitL2"] = special_map.apply(
            lambda x: float(x[1]) if isinstance(x, tuple) else 0.0
        )

    # Time limit check against June 2026
    june_target = pd.to_datetime("2026-06-20")
    agg["DepartureDate"] = pd.to_datetime(agg["DepartureDate"])
    diff_years = (june_target - agg["DepartureDate"]).dt.days / 365.25

    agg["IsTimeLimitL1"] = agg["limitL1"] >= diff_years
    agg["IsTimeLimitL2"] = agg["limitL2"] >= diff_years

    return agg[["ConnectionID", "IsTimeLimitL1", "IsTimeLimitL2"]]


# ────────────────────────────────────────────────
# DATABASE UPDATE
# ────────────────────────────────────────────────
def set_time_limits(con, df_updates):
    if df_updates.empty:
        log("No updates to apply")
        return

    log(f"Updating {len(df_updates):,} connection groups...")
    con.register("temp_updates", df_updates)
    try:
        con.execute("BEGIN")
        con.execute(f"""
            UPDATE {SOURCE_TABLE} t
               SET IsTimeLimitL1 = u.IsTimeLimitL1,
                   IsTimeLimitL2 = u.IsTimeLimitL2
              FROM temp_updates u
             WHERE t.ConnectionID = u.ConnectionID
               AND t.EUEligible IS TRUE
        """)
        con.execute("COMMIT")
        log("Committed successfully")
    except Exception:
        con.execute("ROLLBACK")
        log("UPDATE failed — rolled back")
        raise
    finally:
        con.unregister("temp_updates")


# ────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────
def main():
    start_time = time.time()
    log("Starting process")

    con = get_connection()

    try:
        df = get_eu_eligible_data(con)

        if df is not None:
            df_updates = calculate_timelimits_vectorized(df)
            set_time_limits(con, df_updates)

            log("──────────── DONE ────────────")
            log(f"Updated {len(df_updates):,} rows")
            log(f"Processed {len(df):,} legs")
            log(f"Finished in {time.time() - start_time:.2f} seconds")
        else:
            log("No data to process")

    finally:
        con.close()


if __name__ == "__main__":
    main()
