import duckdb
import pandas as pd
import uuid
import logging
import concurrent.futures
from typing import Optional, Iterator
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
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
    source_table: str = "OBILET_CLEANED"
    target_table: str = "TA_STANDARD_OBILET"
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
    )

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.eu_airports: frozenset = self._load_airports(con)
        self.eu_carriers: frozenset = self._load_carriers(con)
        self.tr_airports: frozenset = self._load_tr_airports(con)
        self.uk_airports: frozenset = self._load_uk_airports(con)
        logger.info(
            f"Loaded {len(self.eu_airports):,} EU airports, {len(self.eu_carriers):,} EU carriers | "
            f"Loaded {len(self.tr_airports):,} TR airports, {len(self.uk_airports):,} UK airports"
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
    )

    def __init__(
        self,
        eu_airports: frozenset,
        eu_carriers: frozenset,
        tr_airports: frozenset,
        uk_airports: frozenset,
    ):
        self.eu_airports = eu_airports
        self.eu_carriers = eu_carriers
        self.tr_airports = tr_airports
        self.uk_airports = uk_airports
        self._target_cols = TARGET_COLUMNS

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

        # EU Eligibility (vectorized)
        all_legs["EUEligible"] = self._vectorized_eligibility(all_legs)

        # Vectorized Connection IDs mapping
        uid_list = all_legs["_uid"].unique()
        conn_map = {uid: str(uuid.uuid4()) for uid in uid_list}
        all_legs["ConnectionID"] = all_legs["_uid"].map(conn_map)

        if "AirlineCodes" in all_legs.columns:
            all_legs = all_legs.rename(columns={"AirlineCodes": "AirlineCode"})

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

        return all_legs[self._target_cols].reset_index(drop=True)

    def _extract_leg(self, df: pd.DataFrame, leg_num: int) -> Optional[pd.DataFrame]:
        fn_col = f"FlightNo{leg_num}"
        fd_col = f"FlightDate{leg_num}"
        ap_from = f"Airport{leg_num}"
        ap_to = f"Airport{leg_num + 1}"

        required = [fn_col, fd_col, ap_from, ap_to]
        if not all(c in df.columns for c in required):
            return None

        # Pre-clean strings (handles "nan", "NaN", spaces, etc. in one pass)
        clean_flight = df[fn_col].astype(str).str.strip().str.upper()

        valid_mask = (
            df[fn_col].notna()
            & df[ap_from].notna()
            & df[ap_to].notna()
            & (clean_flight != "")
            & (clean_flight != "NAN")
            & (clean_flight != "NONE")
        )

        sub = df.loc[valid_mask].copy()
        if sub.empty:
            return None

        dates = pd.to_datetime(sub[fd_col], errors="coerce")
        valid_date_mask = dates.notna()
        sub = sub.loc[valid_date_mask]
        dates = dates[valid_date_mask]
        clean_flight = clean_flight[valid_mask]

        if sub.empty:
            return None

        return pd.DataFrame(
            {
                "_uid": sub["_uid"].values,
                "LegNo": leg_num,
                "FlightNumber": clean_flight.values,
                "DepartureDate": dates.values,
                "FromAirport": sub[ap_from].astype(str).str.strip().str.upper().values,
                "ToAirport": sub[ap_to].astype(str).str.strip().str.upper().values,
                "AirlineCodes": sub["AirlineCodes"]
                .astype(str)
                .str.strip()
                .str.upper()
                .values,
                "PaxName": sub["PaxName"].fillna("").astype(str).str.strip().values,
                "ETicketNo": sub["TDNR"].fillna("").astype(str).str.strip().values,
                "BookingRef": sub["PNRR"].fillna("").astype(str).str.strip().values,
                "FileName": sub["_SourceFile"]
                .fillna("")
                .astype(str)
                .str.strip()
                .values,
            }
        )

    def _vectorized_eligibility(self, df: pd.DataFrame) -> pd.Series:
        """
        Check every leg's airline carrier:
        1. If carrier is UK and FromAirport or ToAirport is UK → ALL legs with same _uid are Eligible
        2. If carrier is TR and FromAirport or ToAirport is TR → ALL legs with same _uid are Eligible
        3. If carrier is SRB and FromAirport or ToAirport is SRB → ALL legs with same _uid are Eligible
        4. Otherwise apply EU261 rules
        """
        if df.empty:
            return pd.Series(dtype=bool)

        # --- RULE 1: UK Carrier + UK Airport on ANY leg → entire journey eligible ---
        is_uk_carrier = df["AirlineCodes"].isin(UK_CARRIERS)
        touches_uk = df["FromAirport"].isin(self.uk_airports) | df["ToAirport"].isin(
            self.uk_airports
        )
        uk_eligible_legs = is_uk_carrier & touches_uk
        uk_journey_eligible = uk_eligible_legs.groupby(df["_uid"], sort=False).any()

        # --- RULE 2: TR Carrier + TR Airport on ANY leg → entire journey eligible ---
        is_tr_carrier = df["AirlineCodes"].isin(TR_CARRIERS)
        touches_tr = df["FromAirport"].isin(self.tr_airports) | df["ToAirport"].isin(
            self.tr_airports
        )
        tr_eligible_legs = is_tr_carrier & touches_tr
        tr_journey_eligible = tr_eligible_legs.groupby(df["_uid"], sort=False).any()

        # --- RULE 3: SRB Carrier + SRB Airport on ANY leg → entire journey eligible ---
        is_srb_carrier = df["AirlineCodes"].isin(SRB_CARRIERS)
        touches_srb = df["FromAirport"].isin(SRB_AIRPORTS) | df["ToAirport"].isin(
            SRB_AIRPORTS
        )
        srb_eligible_legs = is_srb_carrier & touches_srb
        srb_journey_eligible = srb_eligible_legs.groupby(df["_uid"], sort=False).any()

        # EU261 logic
        eu_dep = df["FromAirport"].isin(self.eu_airports)
        eu_arr = df["ToAirport"].isin(self.eu_airports)
        eu_carrier = df["AirlineCodes"].isin(self.eu_carriers)
        is_special = df["AirlineCodes"].isin(SPECIAL_NON_EU_CARRIERS)

        # Inbound: non-EU departure, EU arrival, with EU or special carrier
        inbound_ok = (~eu_dep) & eu_arr & (eu_carrier | is_special)

        # Journey-level aggregation for EU261
        journey_df = pd.DataFrame(
            {
                "_uid": df["_uid"],
                "eu_dep": eu_dep,
                "eu_arr": eu_arr,
                "inbound_ok": inbound_ok,
            }
        )

        journey_agg = journey_df.groupby("_uid", sort=False).agg(
            first_eu_dep=("eu_dep", "first"),
            last_eu_arr=("eu_arr", "last"),
            any_inbound_ok=("inbound_ok", "any"),
        )

        # EU261: departure from EU, OR (non-EU departure + EU arrival + inbound_ok)
        eu_journey_eligible = journey_agg["first_eu_dep"] | (
            ~journey_agg["first_eu_dep"]
            & journey_agg["last_eu_arr"]
            & journey_agg["any_inbound_ok"]
        )

        # Combine all rules — FIX: include srb_journey_eligible!
        all_eligible = (
            uk_journey_eligible
            | tr_journey_eligible
            | srb_journey_eligible
            | eu_journey_eligible
        )

        # Map back to each row
        return df["_uid"].map(all_eligible).fillna(False).astype(bool)


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
