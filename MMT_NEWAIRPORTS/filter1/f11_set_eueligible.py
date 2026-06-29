import duckdb
import pandas as pd
import uuid
import logging
import concurrent.futures
from typing import Optional, Iterator
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from zoneinfo import ZoneInfo
from datetime import datetime, time
import re


# ==================================================
# CONFIG
# ==================================================
@dataclass(frozen=True)
class Config:
    db_path: str = r"C:\DuckDB\my_db.duckdb"
    threads: int = 8
    memory_limit: str = "8GB"
    temp_dir: str = r"C:\DuckDB\temp"
    source_table: str = "MMT_RAW"
    target_table: str = "TA_STANDARD_MMT"
    read_chunk: int = 200_000
    parse_workers: int = 4
    max_legs: int = 4


CONFIG = Config()
SPECIAL_NON_EU_CARRIERS = frozenset({"BA", "TK", "PC", "JU", "FH", "VF", "VS", "XQ"})
TR_CARRIERS = frozenset({"TK", "PC", "FH", "XQ", "VF"})
UK_CARRIERS = frozenset({"BA", "VS"})
SRB_CARRIERS = frozenset({"JU"})
SRB_AIRPORTS = frozenset({"BEG", "INI", "KVO"})
_RE_SCI_NOTATION = re.compile(r"^(\d+)(?:\.0+)?E\+?(\d+)$", re.IGNORECASE)

TARGET_COLUMNS = [
    "ConnectionID",
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
    "GMTDeparture",
    "GMTArrival",
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
    "IsAirportProblem",
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


def _fix_scientific_notation(fn: str) -> str:
    """Collapse Excel-mangled scientific notation, e.g. '6.00E+78' -> '6E78'."""
    m = _RE_SCI_NOTATION.fullmatch(fn.strip())
    if not m:
        return fn
    mantissa, exponent = m.group(1), m.group(2)
    return f"{mantissa}E{exponent}"


# ==================================================
# REFERENCE DATA
# ==================================================
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
            SELECT CodeIataAirport,timezone 
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
        rows = con.execute("""
            SELECT iata, timezone
            FROM AIRPORTS_ALL
            WHERE iata IS NOT NULL AND timezone IS NOT NULL
            """).fetchall()
        return {code.strip().upper(): tz for code, tz in rows if code and tz}

    @staticmethod
    def calc_gmt_offset(tz_name: str, date_val) -> float | None:
        if not tz_name or pd.isna(date_val):
            return None
        try:
            if isinstance(date_val, (pd.Timestamp, datetime)):
                date_only = date_val.date()
            else:
                date_only = pd.Timestamp(date_val).date()
            dt = datetime.combine(date_only, time(12, 0)).replace(
                tzinfo=ZoneInfo(tz_name)
            )
            return dt.utcoffset().total_seconds() / 3600
        except Exception:
            return None


# ==================================================
# VECTORIZED CHUNK PROCESSOR
# ==================================================
class ChunkProcessor:
    __slots__ = (
        "eu_airports",
        "eu_carriers",
        "tr_airports",
        "uk_airports",
        "airport_tz",
        "_target_cols",
    )

    def __init__(
        self,
        eu_airports: frozenset,
        eu_carriers: frozenset,
        tr_airports: frozenset,
        uk_airports: frozenset,
        airport_tz: dict,
    ):
        self.eu_airports = eu_airports
        self.eu_carriers = eu_carriers
        self.tr_airports = tr_airports
        self.uk_airports = uk_airports
        self.airport_tz = airport_tz
        self._target_cols = TARGET_COLUMNS

    def _gmt_offsets_vectorized(
        self, airport_codes: pd.Series, dates: pd.Series
    ) -> pd.Series:
        tz_names = airport_codes.map(self.airport_tz)
        result = pd.Series(
            [None] * len(airport_codes), index=airport_codes.index, dtype=object
        )
        for tz_name in tz_names.dropna().unique():
            mask = tz_names == tz_name
            result.loc[mask] = [
                ReferenceData.calc_gmt_offset(tz_name, d) for d in dates.loc[mask]
            ]
        return result

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=self._target_cols)

        df = df.copy()

        # Clean flight number
        clean_flight = df["FlightNumber"].apply(_fix_scientific_notation)
        clean_flight = clean_flight.astype(str).str.strip().str.upper()

        # Validate required fields
        valid_mask = (
            df["FlightNumber"].notna()
            & df["NewDepAirport"].notna()
            & df["NewArrAirport"].notna()
            & df["DepartureDate"].notna()
            & (clean_flight != "")
            & (clean_flight != "NAN")
            & (clean_flight != "NONE")
            & (clean_flight != "N/A")
        )
        df = df.loc[valid_mask].copy()
        if df.empty:
            return pd.DataFrame(columns=self._target_cols)

        # Parse dates
        df["DepartureDate_parsed"] = pd.to_datetime(
            df["DepartureDate"], errors="coerce"
        )
        df = df.loc[df["DepartureDate_parsed"].notna()].copy()
        if df.empty:
            return pd.DataFrame(columns=self._target_cols)

        # Map columns to target format
        df["FlightNumber"] = clean_flight.loc[valid_mask].values
        df["FromAirport"] = df["NewDepAirport"].astype(str).str.strip().str.upper()
        df["ToAirport"] = df["NewArrAirport"].astype(str).str.strip().str.upper()
        df["AirlineCode"] = df["Airline"].astype(str).str.strip().str.upper()
        df["DepartureDate"] = df["DepartureDate_parsed"]

        # GMT offsets
        df["GMTDeparture"] = self._gmt_offsets_vectorized(
            df["FromAirport"], df["DepartureDate"]
        ).values
        df["GMTArrival"] = self._gmt_offsets_vectorized(
            df["ToAirport"], df["DepartureDate"]
        ).values

        # ConnectionID: use existing or generate new
        df["ConnectionID"] = df["ConnectionId"].copy()
        single_mask = df["ConnectionID"].isna()
        df.loc[single_mask, "ConnectionID"] = [
            str(uuid.uuid4()) for _ in range(single_mask.sum())
        ]

        # Last leg airport per journey
        last_airports = (
            df.sort_values("LegNo", kind="mergesort")
            .groupby("ConnectionID", sort=False)["ToAirport"]
            .last()
            .rename("LastLegAirport")
        )
        df = df.join(last_airports, on="ConnectionID")

        # EU Eligibility
        df["EUEligible"] = self._vectorized_eligibility(df)

        # Fill remaining columns
        df["ETicketNo"] = ""
        df["BookingRef"] = df["BookingId"].fillna("").astype(str).str.strip()
        df["PaxName"] = ""
        df["FileName"] = (
            df.get("_SourceFile", pd.Series([""] * len(df)))
            .fillna("")
            .astype(str)
            .str.strip()
        )

        # IsSingleFlight: journeys with only 1 leg
        journey_leg_count = df.groupby("ConnectionID")["ConnectionID"].transform("size")
        df["IsSingleFlight"] = journey_leg_count == 1

        # IsAirportProblem
        if "IsAirportProblem" in df.columns:
            df["IsAirportProblem"] = (
                df["IsAirportProblem"].fillna(False).astype(str).str.strip().str.title()
                == "True"
            )
        else:
            df["IsAirportProblem"] = False

        # Placeholders
        for col, val in {
            "AgencyRefNumber": None,
            "EUEligibleDuration": 0,
            "ExtraNote": None,
            "FlightFound": False,
            "IsTimeLimitL1": False,
            "IsTimeLimitL2": False,
            "EUFlights_Id": None,
            "Link_Id": None,
            "DelayInSecond": None,
            "Status": None,
        }.items():
            df[col] = val

        return df[self._target_cols].reset_index(drop=True)

    """
            Determines EU 261/2004 eligibility for each flight leg in the DataFrame.

            A journey is eligible under any of the following rule groups:
            - UK Rule   : operated by a UK carrier (BA, VS) AND touches a UK airport
            - TR Rule   : operated by a TR carrier (TK, PC, FH, XQ, VF) AND touches a TR airport
                            AND the journey is NOT purely domestic within Turkey
            - SRB Rule  : operated by a SRB carrier (JU) AND touches a Serbian airport (BEG/INI/KVO)
            - EU Rule   : departs from an EU airport (outbound)  OR
                            arrives at an EU airport on an inbound leg operated by an EU carrier
                            or a special non-EU carrier (BA, TK, PC, JU, FH, VF, VS, XQ)

            Final result applies to every leg of an eligible journey EXCEPT legs that are
            part of a purely domestic Turkish itinerary (TR→TR), which are always excluded.
    """
    def _vectorized_eligibility(self, df: pd.DataFrame) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=bool)

        # Use ConnectionID for grouping (not _uid)
        uid_col = "ConnectionID"

        sorted_legs = df.sort_values("LegNo", kind="mergesort")
        first_airports = sorted_legs.groupby(uid_col, sort=False)["FromAirport"].first()
        last_airports = sorted_legs.groupby(uid_col, sort=False)["ToAirport"].last()

        domestic_tr_journeys = first_airports.isin(
            self.tr_airports
        ) & last_airports.isin(self.tr_airports)
        is_domestic_tr = (
            df[uid_col].map(domestic_tr_journeys).fillna(False).astype(bool)
        )

        is_uk_carrier = df["AirlineCode"].isin(UK_CARRIERS)
        touches_uk = df["FromAirport"].isin(self.uk_airports) | df["ToAirport"].isin(
            self.uk_airports
        )
        uk_eligible_legs = is_uk_carrier & touches_uk
        uk_journey_eligible = uk_eligible_legs.groupby(df[uid_col], sort=False).any()

        is_tr_carrier = df["AirlineCode"].isin(TR_CARRIERS)
        touches_tr = df["FromAirport"].isin(self.tr_airports) | df["ToAirport"].isin(
            self.tr_airports
        )
        tr_eligible_legs = is_tr_carrier & touches_tr & ~is_domestic_tr
        tr_journey_eligible = tr_eligible_legs.groupby(df[uid_col], sort=False).any()

        is_srb_carrier = df["AirlineCode"].isin(SRB_CARRIERS)
        touches_srb = df["FromAirport"].isin(SRB_AIRPORTS) | df["ToAirport"].isin(
            SRB_AIRPORTS
        )
        srb_eligible_legs = is_srb_carrier & touches_srb
        srb_journey_eligible = srb_eligible_legs.groupby(df[uid_col], sort=False).any()

        eu_dep = df["FromAirport"].isin(self.eu_airports)
        eu_arr = df["ToAirport"].isin(self.eu_airports)
        eu_carrier = df["AirlineCode"].isin(self.eu_carriers)
        is_special = df["AirlineCode"].isin(SPECIAL_NON_EU_CARRIERS)
        inbound_ok = (~eu_dep) & eu_arr & (eu_carrier | is_special)

        journey_df = pd.DataFrame(
            {
                uid_col: df[uid_col],
                "eu_dep": eu_dep,
                "eu_arr": eu_arr,
                "inbound_ok": inbound_ok,
            }
        )

        journey_agg = journey_df.groupby(uid_col, sort=False).agg(
            first_eu_dep=("eu_dep", "first"),
            last_eu_arr=("eu_arr", "last"),
            any_inbound_ok=("inbound_ok", "any"),
        )

        eu_journey_eligible = journey_agg["first_eu_dep"] | (
            ~journey_agg["first_eu_dep"]
            & journey_agg["last_eu_arr"]
            & journey_agg["any_inbound_ok"]
        )

        all_eligible = (
            uk_journey_eligible
            | tr_journey_eligible
            | srb_journey_eligible
            | eu_journey_eligible
        )

        eligible_mapped = df[uid_col].map(all_eligible).fillna(False).astype(bool)
        return eligible_mapped & (~is_domestic_tr)


# ==================================================
# IMPORTER / ORCHESTRATOR
# ==================================================
class CreateTAStandardTable:
    def __init__(self, config: Config = CONFIG):
        self.config = config
        self.read_con = duckdb.connect(config.db_path)
        self.write_con = duckdb.connect(config.db_path)

        self._configure_connection(self.read_con)
        self._configure_connection(self.write_con)

        ref = ReferenceData(self.read_con)
        self.processor = ChunkProcessor(
            ref.eu_airports,
            ref.eu_carriers,
            ref.tr_airports,
            ref.uk_airports,
            ref.airport_tz,
        )
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
                ConnectionID VARCHAR, PaxName VARCHAR, AgencyRefNumber VARCHAR, 
                ETicketNo VARCHAR, FlightNumber VARCHAR, DepartureDate TIMESTAMP, 
                FileName VARCHAR, BookingRef VARCHAR, AirlineCode VARCHAR, 
                FromAirport VARCHAR, ToAirport VARCHAR, LastLegAirport VARCHAR, 
                GMTDeparture DECIMAL(3,1), GMTArrival DECIMAL(3,1),
                EUEligible BOOLEAN, EUEligibleDuration INTEGER, ExtraNote VARCHAR, 
                FlightFound BOOLEAN, LegNo INTEGER, IsTimeLimitL1 BOOLEAN, 
                IsTimeLimitL2 BOOLEAN, EUFlights_Id VARCHAR, Link_Id VARCHAR, 
                DelayInSecond INTEGER, Status VARCHAR, IsSingleFlight BOOLEAN,
                IsAirportProblem BOOLEAN,
            )
        """)
        logger.info(f"Created target table: {self.config.target_table}")

    def _stream_chunks(self) -> Iterator[pd.DataFrame]:
        self.read_con.execute(
            f"SELECT * FROM {self.config.source_table} WHERE NEWDEPAIRPORT IS NOT NULL ORDER BY BOOKINGID, DEPARTUREDATE"
        )
        while True:
            chunk = self.read_con.fetch_df_chunk(self.config.read_chunk)
            if chunk.empty:
                break
            yield chunk

    def run(self):
        total_rows = self.read_con.execute(
            f"SELECT COUNT(*) FROM {self.config.source_table} "
            "WHERE NEWDEPAIRPORT IS NOT NULL"
        ).fetchone()[0]

        logger.info(
            f"Source table '{self.config.source_table}': {total_rows:,} records"
        )

        total_inserted = 0
        chunks_processed = 0
        max_queued = self.config.parse_workers * 2

        with ThreadPoolExecutor(max_workers=self.config.parse_workers) as executor:
            futures = {}

            for chunk_df in self._stream_chunks():
                chunks_processed += 1
                logger.info(f"Queued chunk {chunks_processed} ({len(chunk_df):,} rows)")

                future = executor.submit(self.processor.process, chunk_df)
                futures[future] = len(chunk_df)

                # CRITICAL: Block the reader if the queue is full until a worker finishes
                if len(futures) >= max_queued:
                    done, _ = concurrent.futures.wait(
                        futures.keys(), return_when=concurrent.futures.FIRST_COMPLETED
                    )
                    for f in done:
                        res_df = f.result()
                        if not res_df.empty:
                            total_inserted += self._execute_db_insert(res_df)
                        del futures[f]

            # Final sequential drain for remaining tasks
            if futures:
                for future in concurrent.futures.as_completed(futures):
                    res_df = future.result()
                    if not res_df.empty:
                        total_inserted += self._execute_db_insert(res_df)

        logger.info(
            f"Complete: {chunks_processed} chunks processed, {total_inserted:,} leg records inserted"
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
