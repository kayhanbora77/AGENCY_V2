import duckdb
import pandas as pd
import logging
from pathlib import Path
from dataclasses import dataclass


# ==================================================
# CONFIG
# ==================================================
@dataclass(frozen=True)
class Config:
    db_path: str = r"C:\DuckDB\my_db.duckdb"
    threads: int = 8
    memory_limit: str = "8GB"
    temp_dir: str = r"C:\DuckDB\temp"
    source_table: str = "AGENCY_TEST_DATA"
    target_table: str = "TA_STANDARD_AGENCY_TEST"


CONFIG = Config()

SPECIAL_NON_EU_CARRIERS = frozenset({"BA", "TK", "PC", "JU", "FH", "VF", "VS"})


TARGET_COLUMNS = [
    "PaxName",
    "AgencyRefNumber",
    "ETicketNo",
    "FlightNumber",
    "DepartureDate",
    "FileName",
    "BookingRef",
    "AirlineCode",
    "FromAirport",
    "ToAirport",
    "LastLegAirport",
    "EUEligible",
    "EUEligibleDuration",
    "ExtraNote",
    "FlightFound",
    "LegNo",
    "IsTimeLimitL1",
    "IsTimeLimitL2",
    "EUFlights_Id",
    "Link_Id",
    "DelayInSecond",
    "Status",
    "IsSingleFlight",
]


# ==================================================
# LOGGING
# ==================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


Path(CONFIG.db_path).parent.mkdir(parents=True, exist_ok=True)
Path(CONFIG.temp_dir).mkdir(parents=True, exist_ok=True)


# ==================================================
# REFERENCE DATA
# ==================================================
class ReferenceData:
    __slots__ = ("eu_airports", "eu_carriers")

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.eu_airports: frozenset = self._load_airports(con)
        self.eu_carriers: frozenset = self._load_carriers(con)
        logger.info(
            f"Loaded {len(self.eu_airports):,} EU airports, {len(self.eu_carriers):,} EU carriers"
        )

    @staticmethod
    def _load_airports(con) -> frozenset:
        rows = con.execute("""
            SELECT CodeIataAirport 
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


# ==================================================
# SIMPLE PROCESSOR (no legs)
# ==================================================
class SimpleProcessor:
    __slots__ = ("eu_airports", "eu_carriers", "_target_cols")

    def __init__(self, eu_airports: frozenset, eu_carriers: frozenset):
        self.eu_airports = eu_airports
        self.eu_carriers = eu_carriers
        self._target_cols = TARGET_COLUMNS

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=self._target_cols)

        df = df.copy()
        # Clean columns
        from_airport = (
            df["DepartureAirport"].fillna("").astype(str).str.strip().str.upper()
        )
        to_airport = df["ArrivalAirport"].fillna("").astype(str).str.strip().str.upper()
        carrier = df["Airline"].fillna("").astype(str).str.strip().str.upper()
        flight_number = (
            df["FlightNumber"].fillna("").astype(str).str.strip().str.upper()
        )
        # EU eligibility — single flight, no connection logic needed
        eu_dep = from_airport.isin(self.eu_airports)
        eu_arr = to_airport.isin(self.eu_airports)
        eu_carrier = carrier.isin(self.eu_carriers)
        is_special = carrier.isin(SPECIAL_NON_EU_CARRIERS)

        eligible = eu_dep | eu_arr | eu_carrier | is_special

        # Build output
        result = pd.DataFrame(
            {
                "PaxName": None,
                "AgencyRefNumber": None,
                "ETicketNo": None,
                "FlightNumber": flight_number.values,
                "DepartureDate": pd.to_datetime(
                    df["FlightDate"], errors="coerce"
                ).values,
                "FileName": None,
                "BookingRef": None,
                "AirlineCode": carrier.values,
                "FromAirport": from_airport.values,
                "ToAirport": to_airport.values,
                "LastLegAirport": to_airport.values,
                "EUEligible": eligible.values,
                "EUEligibleDuration": 0,
                "ExtraNote": None,
                "FlightFound": False,
                "LegNo": 1,
                "IsTimeLimitL1": False,
                "IsTimeLimitL2": False,
                "EUFlights_Id": None,
                "Link_Id": None,
                "DelayInSecond": None,
                "Status": None,
                "IsSingleFlight": True,
            }
        )

        return result[self._target_cols].reset_index(drop=True)


# ==================================================
# IMPORTER
# ==================================================
class CreateTAStandardTable:
    def __init__(self, config: Config = CONFIG):
        self.config = config
        self.read_con = duckdb.connect(config.db_path)
        self.write_con = duckdb.connect(config.db_path)

        self._configure_connection(self.read_con)
        self._configure_connection(self.write_con)

        ref = ReferenceData(self.read_con)
        self.processor = SimpleProcessor(ref.eu_airports, ref.eu_carriers)
        self._create_target_table()

    @staticmethod
    def _configure_connection(con: duckdb.DuckDBPyConnection):
        con.execute(f"SET threads={CONFIG.threads}")
        con.execute(f"SET memory_limit='{CONFIG.memory_limit}'")
        con.execute("SET preserve_insertion_order=false")
        con.execute(f"SET temp_directory='{CONFIG.temp_dir}'")

    def _create_target_table(self):
        self.write_con.execute(f"DROP TABLE IF EXISTS {self.config.target_table}")
        self.write_con.execute(f"""
            CREATE TABLE {self.config.target_table} (
                Id VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
                PaxName VARCHAR, AgencyRefNumber VARCHAR, 
                ETicketNo VARCHAR, FlightNumber VARCHAR, DepartureDate TIMESTAMP, 
                FileName VARCHAR, BookingRef VARCHAR, AirlineCode VARCHAR, 
                FromAirport VARCHAR, ToAirport VARCHAR, LastLegAirport VARCHAR, 
                EUEligible BOOLEAN, EUEligibleDuration INTEGER, ExtraNote VARCHAR, 
                FlightFound BOOLEAN, LegNo INTEGER, IsTimeLimitL1 BOOLEAN, 
                IsTimeLimitL2 BOOLEAN, EUFlights_Id VARCHAR, Link_Id VARCHAR, 
                DelayInSecond INTEGER, Status VARCHAR, IsSingleFlight BOOLEAN
            )
        """)
        logger.info(f"Created target table: {self.config.target_table}")

    def run(self):
        total_rows = self.read_con.execute(
            f"SELECT COUNT(*) FROM {self.config.source_table}"
        ).fetchone()[0]
        logger.info(
            f"Source table '{self.config.source_table}': {total_rows:,} records"
        )

        # Load all at once (simple table, no need for chunking)
        df = self.read_con.execute(
            f"SELECT * FROM {self.config.source_table}"
        ).fetch_df()
        logger.info(f"Loaded {len(df):,} rows")

        result = self.processor.process(df)
        logger.info(f"Processed to {len(result):,} leg records")

        total_inserted = self._execute_db_insert(result)
        logger.info(
            f"Inserted {total_inserted:,} records into {self.config.target_table}"
        )

    def _execute_db_insert(self, df: pd.DataFrame) -> int:
        self.write_con.register("__tmp_insert__", df)
        cols_str = ", ".join(TARGET_COLUMNS)
        self.write_con.execute(f"""
            INSERT INTO {self.config.target_table} ({cols_str}) 
            SELECT {cols_str} FROM __tmp_insert__
        """)
        self.write_con.unregister("__tmp_insert__")
        return len(df)

    def close(self):
        if hasattr(self, "read_con"):
            self.read_con.close()
        if hasattr(self, "write_con"):
            self.write_con.close()
        logger.info("Connections closed")


def main():
    pipeline = CreateTAStandardTable()
    try:
        pipeline.run()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
