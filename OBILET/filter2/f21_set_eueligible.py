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
import numpy as np  


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
SPECIAL_CARRIERS = frozenset({"LH","XQ","QR"})

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
        ap_from = f"AirportIataCode{leg_num}"
        ap_to = f"AirportIataCode{leg_num + 1}"

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
                "AirlineCode": sub["AirlineCode"]
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
    
    # ============================================
    """
    Determines EU261-style flight delay/compensation eligibility for each leg,
    evaluated at the journey (ConnectionID) level.

    ORDER OF OPERATIONS
    --------------------
    1. RULE 1 (multi-leg "bookend" check):
       For journeys with more than one leg, if the FIRST leg departs from a
       non-EU airport AND the LAST leg arrives at a non-EU airport, the whole
       journey is treated as a single "non-EU to non-EU" trip that just happens
       to connect through Europe.
           - Eligible = True  ONLY if a special carrier (SPECIAL_NON_EU_CARRIERS)
             operates somewhere in the journey.
           - Eligible = False otherwise, even if individual legs would have
             qualified under Rule 2 (Rule 1 wins over Rule 2 when it applies).

    2. RULE 2 (per-leg check, used when Rule 1 does NOT apply):
       Each leg is evaluated independently based on its own airports/carrier:
           2.1 NonEU -> NonEU  : Eligible only if carrier is "special".
           2.2 NonEU -> EU     : Eligible if carrier is "special" OR an EU carrier.
           2.3 EU -> (EU/NonEU): Always eligible (EU departure rule).
       If ANY leg in the journey is eligible, the ENTIRE journey is marked
       eligible (an eligible leg elsewhere in the trip is enough).

       Additional carve-outs are OR'd into the per-leg result:
           - Turkish SHY rule: eligible if a Turkish carrier operates a leg
             that touches Turkey (regardless of EU/non-EU classification).
           - Serbian rule: eligible if a Serbian carrier operates a leg that
             touches Serbia.
           2.4 / 2.5 LH/XQ/QR-from-Turkey rule: eligible if an LH/XQ/QR-coded
             carrier (SPECIAL_CARRIERS) operates a leg departing from a
             Turkish airport.
               - Single-flight journeys (2.4): the one leg qualifies directly.
               - Multi-leg journeys (2.5): only LegNo == 1 (the first leg)
                 qualifies — a later leg departing Turkey does not trigger this.

    3. RULE 1 OVERRIDE - LH/XQ/QR-from-Turkey carve-out:
       Unlike the other Rule 2 carve-outs, 2.4/2.5 are NOT suppressed when
       Rule 1 applies. If ANY leg in the journey satisfies 2.4/2.5, the
       journey is forced to Eligible = True regardless of what Rule 1
       decided (including a Rule 1 = False verdict). This is the one
       exception to "Rule 1 always wins."

    4. FINAL OVERRIDE - Domestic Turkey Exclusion:
       If EVERY leg in the journey is Turkey -> Turkey (a purely domestic
       Turkish trip), the journey is forced to Eligible = False, no matter
       what Rules 1-3 decided. This override is applied last and always
       wins — it takes priority even over the LH/XQ/QR-from-Turkey carve-out.
    """
    def _vectorized_eligibility(self, df: pd.DataFrame) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=bool)

        uid_col = "ConnectionID"
        grp_uid = df[uid_col]

        airline = df["AirlineCode"]
        from_ap = df["FromAirport"]
        to_ap = df["ToAirport"]

        from_is_eu = from_ap.isin(self.eu_airports)
        to_is_eu = to_ap.isin(self.eu_airports)
        from_is_tr = from_ap.isin(self.tr_airports)

        is_special_carrier = airline.isin(SPECIAL_NON_EU_CARRIERS)
        is_eu_carrier = airline.isin(self.eu_carriers)
        is_tr_special_carrier = airline.isin(SPECIAL_CARRIERS)  # LH/XQ/QR

        # ==================================================
        # Journey structure: leg counts, first/last leg lookups
        # ==================================================
        leg_counts = grp_uid.map(grp_uid.value_counts())
        is_multi_leg = leg_counts > 1
        is_first_leg = df["LegNo"] == 1

        first_idx = df.groupby(uid_col, sort=False)["LegNo"].idxmin()
        last_idx = df.groupby(uid_col, sort=False)["LegNo"].idxmax()

        first_from_noneu_by_uid = ~from_ap.loc[first_idx].isin(self.eu_airports)
        first_from_noneu_by_uid.index = df.loc[first_idx, uid_col].values

        last_to_noneu_by_uid = ~to_ap.loc[last_idx].isin(self.eu_airports)
        last_to_noneu_by_uid.index = df.loc[last_idx, uid_col].values

        first_leg_from_noneu = grp_uid.map(first_from_noneu_by_uid).fillna(False)
        last_leg_to_noneu = grp_uid.map(last_to_noneu_by_uid).fillna(False)

        # ==================================================
        # RULE 1: multi-leg nonEU -> nonEU "bookend" journey
        # Eligible only if a special carrier operates somewhere in the journey.
        # ==================================================
        rule1_applies = is_multi_leg & first_leg_from_noneu & last_leg_to_noneu
        journey_has_special_carrier = is_special_carrier.groupby(grp_uid, sort=False).transform("any")
        rule1_eligible = rule1_applies & journey_has_special_carrier

        # ==================================================
        # RULE 2: per-leg eligibility (used when Rule 1 does not apply)
        # ==================================================
        rule2_leg_eligible = pd.Series(False, index=df.index)

        rule2_noneu_to_noneu = (~from_is_eu) & (~to_is_eu)
        rule2_noneu_to_eu = (~from_is_eu) & to_is_eu
        rule2_eu_departure = from_is_eu

        rule2_leg_eligible.loc[rule2_noneu_to_noneu] = is_special_carrier.loc[rule2_noneu_to_noneu]
        rule2_leg_eligible.loc[rule2_noneu_to_eu] = (is_special_carrier | is_eu_carrier).loc[rule2_noneu_to_eu]
        rule2_leg_eligible.loc[rule2_eu_departure] = True

        # --- Turkish SHY carve-out: TR carrier operating a leg that touches Turkey ---
        is_tr_carrier = airline.isin(TR_CARRIERS)
        touches_tr = from_ap.isin(self.tr_airports) | to_ap.isin(self.tr_airports)
        rule2_tr_shy = is_tr_carrier & touches_tr

        # --- Serbian carve-out: SRB carrier operating a leg that touches Serbia ---
        is_srb_carrier = airline.isin(SRB_CARRIERS)
        touches_srb = from_ap.isin(SRB_AIRPORTS) | to_ap.isin(SRB_AIRPORTS)
        rule2_serbian = is_srb_carrier & touches_srb

        # --- LH/XQ/QR-from-Turkey carve-out ---
        # Single-flight journeys: the one leg qualifies directly.
        # Multi-leg journeys: only the first leg (LegNo == 1) qualifies.
        rule2_tr_special_single = from_is_tr & is_tr_special_carrier & (~is_multi_leg)
        rule2_tr_special_multi = from_is_tr & is_tr_special_carrier & is_multi_leg & is_first_leg
        rule2_tr_special = rule2_tr_special_single | rule2_tr_special_multi

        # Fold all carve-outs into the per-leg result, then roll up to journey level:
        # any eligible leg makes the whole journey eligible.
        rule2_leg_eligible = rule2_leg_eligible | rule2_tr_shy | rule2_serbian | rule2_tr_special
        rule2_journey_eligible = rule2_leg_eligible.groupby(grp_uid, sort=False).transform("any")

        # ==================================================
        # Combine: Rule 1 wins over Rule 2 where it applies
        # ==================================================
        eligible = np.where(rule1_applies, rule1_eligible, rule2_journey_eligible)
        eligible = pd.Series(eligible, index=df.index).astype(bool)

        # ==================================================
        # RULE 3 (OVERRIDE): LH/XQ/QR-from-Turkey beats Rule 1
        # If any leg in the journey satisfies the TR-special carve-out,
        # force the journey eligible even if Rule 1 said False.
        # ==================================================
        journey_has_tr_special = rule2_tr_special.groupby(grp_uid, sort=False).transform("any")
        eligible = eligible | journey_has_tr_special

        # ==================================================
        # RULE 4 (FINAL OVERRIDE): Domestic Turkey exclusion
        # Purely Turkey -> Turkey journeys are always ineligible, no matter
        # what the rules above decided — this always wins.
        # ==================================================
        leg_is_tr_domestic = from_ap.isin(self.tr_airports) & to_ap.isin(self.tr_airports)
        is_purely_tr_domestic = leg_is_tr_domestic.groupby(grp_uid, sort=False).all()
        is_domestic_tr = grp_uid.map(is_purely_tr_domestic).fillna(False)

        return eligible & (~is_domestic_tr)
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
