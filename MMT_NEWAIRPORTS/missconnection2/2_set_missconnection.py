import duckdb
import pandas as pd
from datetime import timedelta
import logging

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "MMT_MISSCONNECTION_2"
MIN_LAYOVER_MINUTES = 60

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATETIME_COLS = [
    "DepartureDate", "FlightDate", "ScheduledDepartureTime",
    "ScheduledArrivalTime", "ActualDepartureTime", "ActualArrivalTime",
]


def load_missconnection_data() -> pd.DataFrame:
    """Load data from DuckDB and parse datetime columns."""
    with duckdb.connect(DB_PATH) as con:
        df = con.execute(f"SELECT * FROM {SOURCE_TABLE}").df()

    if df.empty:
        logger.info("No data found in %s", SOURCE_TABLE)
        return pd.DataFrame()

    for col in DATETIME_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col].replace("NULL", pd.NaT), errors="coerce")

    return df


def set_missconnection(group: pd.DataFrame) -> pd.DataFrame:
    """Process a single connection group to detect missed connections."""
    group = group.copy().reset_index(drop=True)
    group["DelayMissConnection"] = pd.NA
    group["DelayMissConnectionId"] = pd.NA
    group["IsMissConnection"] = False

    # Check if ANY leg in THIS group is cancelled, diverted, or delayed
    if "Status" in group.columns:
        statuses = group["Status"].astype(str).str.lower()        
        if statuses.str.startswith(("cancel", "divert", "delay")).any():            
            logger.debug("Skipping connection group due to bad status")
            return group

    # Process consecutive legs
    for i in range(len(group) - 1):
        row = group.iloc[i]
        next_row = group.iloc[i + 1]

        actual_arrival = row["ActualArrivalTime"]
        scheduled_departure = next_row["ScheduledDepartureTime"]

        if pd.isna(actual_arrival) or pd.isna(scheduled_departure):
            continue

        # Calculate layover: Next Scheduled Departure - Current Actual Arrival
        layover = scheduled_departure - actual_arrival
        layover_seconds = int(layover.total_seconds())

        # Store delay on the CURRENT leg (represents delay before next connection)
        group.loc[i, "DelayMissConnection"] = layover_seconds
        group.loc[i, "DelayMissConnectionId"] = str(row["Id"])

        # RULE: <= 60 minutes is a missed connection
        if layover <= timedelta(minutes=MIN_LAYOVER_MINUTES):
            group.loc[i, "IsMissConnection"] = True

    return group


def update_missconnection(processed_df: pd.DataFrame) -> None:
    """Batch update using a temp table."""
    # Only update rows that have a calculated delay
    updates = processed_df[processed_df["DelayMissConnectionId"].notna()][
        ["DelayMissConnectionId", "DelayMissConnection", "IsMissConnection"]
    ].copy()

    if updates.empty:
        logger.info("No rows to update")
        return

    updates.columns = ["Id", "DelayMissConnection", "IsMissConnection"]
    logger.info("Updating %d rows in database...", len(updates))

    with duckdb.connect(DB_PATH) as con:
        con.execute("CREATE OR REPLACE TEMP TABLE _miss_updates AS SELECT * FROM updates")
        con.execute(f"""
            UPDATE {SOURCE_TABLE} AS t
            SET 
                DelayMissConnection = u.DelayMissConnection::BIGINT,
                IsMissConnection = u.IsMissConnection::BOOLEAN
            FROM _miss_updates u
            WHERE t.Id = u.Id
        """)
        con.execute("DROP TABLE _miss_updates")


def main():
    df = load_missconnection_data()

    if df.empty:
        logger.info("No data to process")
        return

    # CRITICAL: Sort by ConnectionID and LegNo to ensure proper ordering
    if "LegNo" in df.columns:
        df = df.sort_values(["ConnectionID", "LegNo"]).reset_index(drop=True)
    else:
        logger.warning("LegNo column not found, sorting by ConnectionID only")
        df = df.sort_values(["ConnectionID"]).reset_index(drop=True)

    # Process each connection group separately
    processed_groups = []
    for conn_id, group in df.groupby("ConnectionID"):
        processed_group = set_missconnection(group)
        processed_groups.append(processed_group)

    # Combine all processed groups
    processed_df = pd.concat(processed_groups, ignore_index=True)

    missed_count = processed_df["IsMissConnection"].sum()
    logger.info("Missed connections detected: %d", missed_count)

    update_missconnection(processed_df)
    logger.info("Processing complete")


if __name__ == "__main__":
    main()