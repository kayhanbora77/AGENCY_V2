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


# ==================================================
# CONFIG
# ==================================================
@dataclass(frozen=True)
class Config:
    db_path: str = r"C:\DuckDB\my_db.duckdb"
    threads: int = 8
    memory_limit: str = "8GB"
    temp_dir: str = r"C:\DuckDB\temp"
    source_table: str = "TBO_CLEANED6"
    target_table: str = "TA_STANDARD_TBO"
    read_chunk: int = 200_000
    parse_workers: int = 4
    max_legs: int = 4


CONFIG = Config()
SPECIAL_NON_EU_CARRIERS = frozenset({"BA", "TK", "PC", "JU", "FH", "VF", "VS", "XQ"})
TR_CARRIERS = frozenset({"TK", "PC", "FH", "XQ", "VF"})
UK_CARRIERS = frozenset({"BA", "VS"})
SRB_CARRIERS = frozenset({"JU"})
SRB_AIRPORTS = frozenset({"BEG", "INI", "KVO"})

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
        rows = con.execute(
            "SELECT iata, timezone FROM AIRPORTS_ALL WHERE iata IS NOT NULL AND timezone IS NOT NULL"
        ).fetchall()
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
        "_target_cols",
        "airport_tz",
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
        df["_uid"] = range(len(df))

        leg_frames = [
            leg_df
            for i in range(1, CONFIG.max_legs + 1)
            if (leg_df := self._extract_leg(df, i)) is not None
        ]

        if not leg_frames:
            return pd.DataFrame(columns=self._target_cols)

        all_legs = pd.concat(leg_frames, ignore_index=True)
        del leg_frames

        # Last leg airport per journey
        last_airports = (
            all_legs.sort_values("LegNo", kind="mergesort")
            .groupby("_uid", sort=False)["ToAirport"]
            .last()
            .rename("LastLegAirport")
        )
        all_legs = all_legs.join(last_airports, on="_uid")
        # Vectorized Connection IDs mapping
        uid_list = all_legs["_uid"].unique()
        conn_map = {uid: str(uuid.uuid4()) for uid in uid_list}
        all_legs["ConnectionID"] = all_legs["_uid"].map(conn_map)
        # EU Eligibility (vectorized)
        all_legs["EUEligible"] = self._vectorized_eligibility(all_legs)

        # High-speed static configuration initialization
        placeholders = {
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
            "IsSingleFlight": None,
        }
        for col, val in placeholders.items():
            all_legs[col] = val
        journey_leg_count = all_legs.groupby("ConnectionID")["ConnectionID"].transform(
            "size"
        )
        all_legs["IsSingleFlight"] = journey_leg_count == 1
        return all_legs[self._target_cols].reset_index(drop=True)

    def _extract_leg(self, df: pd.DataFrame, leg_num: int) -> Optional[pd.DataFrame]:
        # Map your actual column names
        fn_col = f"FlightNumber{leg_num}"
        fd_col = f"DepartureDateLocal{leg_num}"
        ap_from = f"Airport{leg_num}"
        ap_to = f"Airport{leg_num + 1}"

        # Check if columns exist (you have up to 6 flights)
        if fn_col not in df.columns or fd_col not in df.columns:
            return None

        # Clean flight numbers (handle nulls, empty strings, etc.)
        clean_flight = df[fn_col].astype(str).str.strip().str.upper()

        # Create valid mask - exclude null/empty/NA values
        valid_mask = (
            df[fn_col].notna()
            & df[ap_from].notna()
            & df[ap_to].notna()
            & (clean_flight != "")
            & (clean_flight != "NAN")
            & (clean_flight != "NONE")
            & (clean_flight != "N/A")
        )

        sub = df.loc[valid_mask].copy()
        if sub.empty:
            return None

        # Parse dates - your dates might already be in proper format
        # Try to convert to datetime, if it fails, keep as string
        try:
            dates = pd.to_datetime(sub[fd_col], errors="coerce")
            valid_date_mask = dates.notna()
            sub = sub.loc[valid_date_mask]
            dates = dates[valid_date_mask]
            if sub.empty:
                return None
        except:
            # If date conversion fails, use as-is (might already be datetime)
            dates = sub[fd_col]

        # Create the leg dataframe
        return pd.DataFrame(
            {
                "_uid": sub["_uid"].values,
                "LegNo": leg_num,
                "FlightNumber": clean_flight.loc[sub.index].values,
                "DepartureDate": dates.values
                if isinstance(dates, pd.Series)
                else dates,
                "FromAirport": sub[ap_from].astype(str).str.strip().str.upper().values,
                "ToAirport": sub[ap_to].astype(str).str.strip().str.upper().values,
                "GMTDeparture": self._gmt_offsets_vectorized(
                    sub[ap_from].astype(str).str.strip().str.upper(), dates
                ).values,
                "GMTArrival": self._gmt_offsets_vectorized(
                    sub[ap_to].astype(str).str.strip().str.upper(), dates
                ).values,
                "AirlineCode": sub["Airline"]
                .astype(str)
                .str.strip()
                .str.upper()
                .values,
                "PaxName": sub["PaxName"].fillna("").astype(str).str.strip().values,
                "ETicketNo": sub["ETicketNo"]
                .fillna("")
                .astype(str)
                .str.strip()
                .values,  # Changed from TDNR to ETicketNo
                "BookingRef": sub["BookingRef"]
                .fillna("")
                .astype(str)
                .str.strip()
                .values,  # Changed from PNRR to BookingRef
                "FileName": sub.get("_SourceFile", pd.Series([""] * len(sub)))
                .fillna("")
                .astype(str)
                .str.strip()
                .values,
            }
        )

    """
    Determines eligibility for EU 261/2004, UK 261/2004, and Turkish SHY regulations 
    for each flight leg in the DataFrame.

    A journey is eligible under any of the following rule groups:

    1. EU Rule (EU261/2004):
    - Outbound: Departs from an EU/EEA airport (any airline)
    - Inbound: Arrives at an EU/EEA airport on an EU carrier OR a special non-EU carrier
        (BA, TK, PC, JU, FH, VF, VS, XQ)

    2. UK Rule (UK261/2004):
    - Outbound: Departs from a UK airport (any airline)
    - Inbound: Arrives at a UK airport on a UK carrier (BA, VS) OR an EU carrier

    3. TR Rule (Turkish SHY):
    - Operated by a Turkish carrier (TK, PC, FH, XQ, VF) AND touches a Turkish airport
    - EXCLUSION: Purely domestic Turkish itineraries (all legs TR→TR) are NOT eligible

    4. SRB Rule (Serbian Regulation):
    - Operated by a Serbian carrier (JU) AND touches a Serbian airport (BEG, INI, KVO)

    Eligibility Logic:
    - If ANY leg in a multi-leg journey is eligible, the ENTIRE journey is marked eligible
    - Exception: Purely domestic Turkish journeys (TR→TR→TR...) are ALWAYS excluded,
    even if they contain eligible legs

    Returns:
        pd.Series: Boolean Series where True indicates the leg is part of an eligible journey
    """
    def _vectorized_eligibility(self, df: pd.DataFrame) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=bool)

        uid_col = "ConnectionID"
        sorted_legs = df.sort_values("LegNo", kind="mergesort")

        leg_is_tr_domestic = df["FromAirport"].isin(self.tr_airports) & df["ToAirport"].isin(self.tr_airports)
        all_legs_tr_domestic = leg_is_tr_domestic.groupby(df[uid_col], sort=False).all()
        is_domestic_tr = df[uid_col].map(all_legs_tr_domestic).fillna(False).astype(bool)
        
        # --- Per-leg eligibility flags (order doesn't matter here) ---
        is_uk_or_eu_carrier = df["AirlineCode"].isin(UK_CARRIERS | self.eu_carriers)
        touches_uk = df["FromAirport"].isin(self.uk_airports) | df["ToAirport"].isin(self.uk_airports)
        uk_leg_eligible = is_uk_or_eu_carrier & touches_uk

        is_tr_carrier = df["AirlineCode"].isin(TR_CARRIERS)
        touches_tr = df["FromAirport"].isin(self.tr_airports) | df["ToAirport"].isin(self.tr_airports)
        tr_leg_eligible = is_tr_carrier & touches_tr  # domestic-TR exclusion applied at journey level below

        is_srb_carrier = df["AirlineCode"].isin(SRB_CARRIERS)
        touches_srb = df["FromAirport"].isin(SRB_AIRPORTS) | df["ToAirport"].isin(SRB_AIRPORTS)
        srb_leg_eligible = is_srb_carrier & touches_srb

        eu_dep = df["FromAirport"].isin(self.eu_airports)
        eu_arr = df["ToAirport"].isin(self.eu_airports)
        eu_carrier = df["AirlineCode"].isin(self.eu_carriers)
        is_special = df["AirlineCode"].isin(SPECIAL_NON_EU_CARRIERS)
        inbound_ok = (~eu_dep) & eu_arr & (eu_carrier | is_special)
        eu_leg_eligible = eu_dep | inbound_ok

        leg_eligible = uk_leg_eligible | tr_leg_eligible | srb_leg_eligible | eu_leg_eligible

        # --- If ANY leg in the journey is eligible, mark the whole journey eligible ---
        journey_eligible = leg_eligible.groupby(df[uid_col], sort=False).any()
        journey_eligible_mapped = df[uid_col].map(journey_eligible).fillna(False).astype(bool)

        return journey_eligible_mapped & (~is_domestic_tr)


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
                GMTDeparture DECIMAL(4,1), GMTArrival DECIMAL(4,1),
                EUEligible BOOLEAN, EUEligibleDuration INTEGER, ExtraNote VARCHAR, 
                FlightFound BOOLEAN, LegNo INTEGER, IsTimeLimitL1 BOOLEAN, 
                IsTimeLimitL2 BOOLEAN, EUFlights_Id VARCHAR, Link_Id VARCHAR, 
                DelayInSecond INTEGER, Status VARCHAR, IsSingleFlight BOOLEAN
            )
        """)
        logger.info(f"Created target table: {self.config.target_table}")

    def _stream_chunks(self) -> Iterator[pd.DataFrame]:
        self.read_con.execute(f"SELECT * FROM {self.config.source_table}")
        while True:
            chunk = self.read_con.fetch_df_chunk(self.config.read_chunk)
            if chunk.empty:
                break
            yield chunk

    def run(self):
        total_rows = self.read_con.execute(
            f"SELECT COUNT(*) FROM {self.config.source_table}"
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
