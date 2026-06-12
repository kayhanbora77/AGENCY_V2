import duckdb
import pandas as pd
import uuid
import logging
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Config:
    db_path: str = r"C:\DuckDB\my_db.duckdb"
    threads: int = 8
    memory_limit: str = "8GB"
    temp_dir: str = r"C:\DuckDB\temp"
    source_table: str = "MMT_RAW"
    target_table: str = "TA_STANDARD_MMT"


def assign_ids_and_connection_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Id          : unique UUID per row (every row gets its own)
    - ConnectionID: shared UUID for consecutive rows within the same BookingId
                    where DepartureDate gap <= 1 day (Rule 1),
                    new UUID when gap > 1 day (Rule 2).
    Rows from different BookingIds are NEVER compared against each other.
    Each BookingId always starts with a brand-new ConnectionID UUID.
    """
    df = df.copy()
    df["DepartureDate"] = pd.to_datetime(df["DepartureDate"])
    df = df.sort_values(["BookingId", "DepartureDate"]).reset_index(drop=True)

    # Every row gets its own unique Id
    df["Id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    connection_ids = [""] * len(df)

    for booking_id, group in df.groupby("BookingId", sort=False):
        indices = group.index.tolist()

        # Each BookingId always starts with a fresh ConnectionID UUID
        current_uuid = str(uuid.uuid4())

        for pos, idx in enumerate(indices):
            if pos == 0:
                # First row of this BookingId: assign fresh UUID, no comparison
                connection_ids[idx] = current_uuid
            else:
                prev_idx = indices[pos - 1]
                gap_days = (
                    df.at[idx, "DepartureDate"] - df.at[prev_idx, "DepartureDate"]
                ).days

                if gap_days <= 1:
                    # Rule 1: gap <= 1 day → same ConnectionID as previous row
                    connection_ids[idx] = current_uuid
                else:
                    # Rule 2: gap > 1 day → new ConnectionID for this row
                    current_uuid = str(uuid.uuid4())
                    connection_ids[idx] = current_uuid

    df["ConnectionID"] = connection_ids
    return df


def main():
    log.info("Connecting to DuckDB: %s", Config.db_path)
    con = duckdb.connect(Config.db_path)
    con.execute(f"SET threads={Config.threads}")
    con.execute(f"SET memory_limit='{Config.memory_limit}'")
    con.execute(f"SET temp_directory='{Config.temp_dir}'")

    log.info("Reading source table: %s", Config.source_table)
    result_df = con.execute(
        f"SELECT * FROM {Config.source_table} ORDER BY BookingId, DepartureDate"
    ).df()

    log.info("Assigning IDs to %d rows", len(result_df))
    result_df = assign_ids_and_connection_ids(result_df)

    result_df["IsSingleFlight"] = None

    log.info("Writing target table: %s", Config.target_table)
    con.execute(f"DROP TABLE IF EXISTS {Config.target_table}")
    con.execute(f"""
        CREATE TABLE {Config.target_table} AS
        SELECT
            Id::UUID          AS Id,
            ConnectionID::UUID AS ConnectionID,
            BookingId,
            FlightNumber,
            DepartureDate,
            ArrivalDate,
            DepartureAirport,
            ArrivalAirport,
            Airline,
            FlightNo,
            Identifier,
            PAX,
            IsSingleFlight
        FROM result_df
    """)

    row_count = con.execute(f"SELECT COUNT(*) FROM {Config.target_table}").fetchone()[0]
    log.info("Done. Rows written: %d", row_count)
    con.close()


if __name__ == "__main__":
    main()
