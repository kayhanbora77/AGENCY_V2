import duckdb
import pandas as pd
import uuid
from typing import Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==================================================
# CONFIG
# ==================================================
DB_PATH = r"C:\DuckDB\my_db.duckdb"

THREADS = 8
MEMORY_LIMIT = "8GB"
TEMP_DIR = "/tmp/duckdb_temp"
TEMP_DIR_PATH = Path(TEMP_DIR)

SOURCE_TABLE = "MIDDLEEAST_CLEANED"
TARGET_TABLE = "TA_STANDARD_MIDDLEEAST"

# Source chunk size — rows read from DB per iteration
READ_CHUNK = 200_000
# Max parallel workers for chunk processing (CPU-bound parsing)
PARSE_WORKERS = 4

SPECIAL_NON_EU_CARRIERS = {"BA", "TK", "PC", "JU", "FH", "VF", "VS"}

MAX_LEGS = 4  # FlightNo1..FlightNo4

Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
TEMP_DIR_PATH.mkdir(parents=True, exist_ok=True)

TARGET_COLUMNS = [
    "Id",
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
]


# ==================================================
# REFERENCE DATA
# ==================================================
class ReferenceData:
    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.eu_airports = self._load_airports(con)
        self.eu_carriers = self._load_carriers(con)

    def _load_airports(self, con):
        rows = con.execute(
            "SELECT CodeIataAirport FROM AIRPORTS WHERE CodeIso2Country NOT IN ('TR','MA')"
        ).fetchall()
        return {r[0].strip().upper() for r in rows if r[0]}

    def _load_carriers(self, con):
        rows = con.execute(
            "SELECT IataCode FROM AIRLINES WHERE IsInUnion = 1"
        ).fetchall()
        return {r[0].strip().upper() for r in rows if r[0]}


# ==================================================
# VECTORIZED CHUNK PROCESSOR
# ==================================================
class ChunkProcessor:
    """
    Processes one DataFrame chunk entirely with vectorized pandas ops.
    No Python-level row loop — each leg number is handled as a column operation.
    """

    def __init__(self, eu_airports: set, eu_carriers: set):
        self.eu_airports = eu_airports
        self.eu_carriers = eu_carriers

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        self._normalize_strings(df)

        leg_frames = []
        for i in range(1, MAX_LEGS + 1):
            leg_df = self._extract_leg(df, i)
            if leg_df is not None:
                leg_frames.append(leg_df)

        if not leg_frames:
            return pd.DataFrame(columns=TARGET_COLUMNS)

        all_legs = pd.concat(leg_frames, ignore_index=True)

        # Count legs per source row to decide ConnectionID
        leg_counts = all_legs.groupby("_row_idx")["LegNo"].count().rename("_leg_count")
        all_legs = all_legs.join(leg_counts, on="_row_idx")

        # Last leg airport per journey
        last_airports = (
            all_legs.sort_values("LegNo")
            .groupby("_row_idx")["ToAirport"]
            .last()
            .rename("LastLegAirport")
        )
        all_legs = all_legs.join(last_airports, on="_row_idx")

        # EUEligible — vectorized
        all_legs["EUEligible"] = self._vectorized_eligibility(all_legs)

        # ConnectionID: NULL for single-leg, shared UUID per journey for multi-leg
        multi_mask = all_legs["_leg_count"] > 1
        # Generate one UUID per unique _row_idx that has multi-leg
        multi_row_idxs = all_legs.loc[multi_mask, "_row_idx"].unique()
        conn_id_map = {idx: str(uuid.uuid4()) for idx in multi_row_idxs}
        all_legs["ConnectionID"] = all_legs["_row_idx"].map(conn_id_map)

        # Row-level UUIDs for Id
        all_legs["Id"] = [str(uuid.uuid4()) for _ in range(len(all_legs))]

        # Fixed defaults for enrichment columns
        all_legs["AgencyRefNumber"] = None
        all_legs["EUEligibleDuration"] = 0
        all_legs["ExtraNote"] = None
        all_legs["FlightFound"] = False
        all_legs["IsTimeLimitL1"] = False
        all_legs["IsTimeLimitL2"] = False
        all_legs["EUFlights_Id"] = None
        all_legs["Link_Id"] = None
        all_legs["DelayInSecond"] = None
        all_legs["Status"] = None

        return all_legs[TARGET_COLUMNS].reset_index(drop=True)

    # ------------------------------------------------------------------
    # Normalize strings up front (avoid repeated .str ops per leg)
    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_strings(df: pd.DataFrame):
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].where(df[col].notna(), other=None)

    # ------------------------------------------------------------------
    # Extract one leg number as a sub-DataFrame aligned to source rows
    # ------------------------------------------------------------------
    def _extract_leg(self, df: pd.DataFrame, i: int) -> Optional[pd.DataFrame]:
        fn_col = f"FlightNo{i}"
        fd_col = f"FlightDate{i}"
        ap_from = f"Airport{i}"
        ap_to = f"Airport{i + 1}"

        for col in [fn_col, fd_col, ap_from, ap_to]:
            if col not in df.columns:
                return None

        # Mask: rows where this leg exists
        mask = (
            df[fn_col].notna()
            & (df[fn_col].astype(str).str.strip() != "")
            & df[ap_from].notna()
            & (df[ap_from].astype(str).str.strip() != "")
            & df[ap_to].notna()
            & (df[ap_to].astype(str).str.strip() != "")
        )

        sub = df.loc[mask].copy()
        if sub.empty:
            return None

        # Parse dates vectorized
        dates = pd.to_datetime(sub[fd_col], errors="coerce")
        date_valid = dates.notna()
        sub = sub.loc[date_valid]
        dates = dates.loc[date_valid]

        if sub.empty:
            return None

        leg = pd.DataFrame(index=sub.index)
        leg["_row_idx"] = sub.index  # original source row index
        leg["LegNo"] = i
        leg["FlightNumber"] = (
            sub[fn_col].astype(str).str.replace(" ", "", regex=False).str.upper()
        )
        leg["DepartureDate"] = dates.values
        leg["FromAirport"] = sub[ap_from].astype(str).str.strip().str.upper()
        leg["ToAirport"] = sub[ap_to].astype(str).str.strip().str.upper()
        leg["AirlineCode"] = sub["AirlineCodes"].astype(str).str.strip().str.upper()
        leg["PaxName"] = sub["PaxName"].fillna("").astype(str).str.strip()
        leg["ETicketNo"] = (
            sub["TRNN"].fillna(sub["TDNR"]).fillna("").astype(str).str.strip()
        )
        leg["BookingRef"] = (
            sub["PNRR"]
            .fillna(sub["TRNC"])
            .where(sub["PNRR"].notna() | sub["TRNC"].notna(), other=None)
        )
        leg["FileName"] = sub["_SourceFile"].fillna("").astype(str).str.strip()

        return leg.reset_index(drop=True)

    # ------------------------------------------------------------------
    # Vectorized EU eligibility
    # ------------------------------------------------------------------
    def _vectorized_eligibility(self, df: pd.DataFrame) -> pd.Series:
        """
        Per-journey eligibility determined by checking conditions across all legs
        grouped by _row_idx, using vectorized set lookups.
        """
        eu_dep = df["FromAirport"].isin(self.eu_airports)
        eu_arr = df["ToAirport"].isin(self.eu_airports)
        eu_carrier = df["AirlineCode"].isin(self.eu_carriers)
        special = df["AirlineCode"].isin(SPECIAL_NON_EU_CARRIERS)

        # Per-leg eligibility signals
        df2 = df[["_row_idx", "FromAirport", "ToAirport", "AirlineCode"]].copy()
        df2["eu_dep"] = eu_dep.values
        df2["eu_arr"] = eu_arr.values
        df2["eu_carrier"] = eu_carrier.values
        df2["special"] = special.values

        # First-leg departure and last-leg arrival per journey
        first = (
            df2.groupby("_row_idx")
            .first()[["eu_dep"]]
            .rename(columns={"eu_dep": "first_eu_dep"})
        )
        last = (
            df2.groupby("_row_idx")
            .last()[["eu_arr"]]
            .rename(columns={"eu_arr": "last_eu_arr"})
        )

        # Journey-level aggregates
        journey = first.join(last)
        journey["any_special"] = df2.groupby("_row_idx")["special"].any()
        journey["any_eu_dep_mid"] = df2.groupby("_row_idx")["eu_dep"].any()
        # Leg where non-EU dep → EU arr and (EU carrier or special)
        df2["inbound_ok"] = (
            ~df2["eu_dep"] & df2["eu_arr"] & (df2["eu_carrier"] | df2["special"])
        )
        journey["any_inbound_ok"] = df2.groupby("_row_idx")["inbound_ok"].any()

        # Apply eligibility rules
        eligible = (
            journey["first_eu_dep"]  # Rule 1: departs EU
            | (
                ~journey["first_eu_dep"]
                & ~journey["last_eu_arr"]
                & journey["any_special"]
            )  # Rule 2
            | (
                ~journey["first_eu_dep"]
                & journey["last_eu_arr"]
                & (  # Rule 3
                    journey["any_eu_dep_mid"]
                    | journey["any_inbound_ok"]
                    | journey["any_special"]
                )
            )
        )

        # Map back to leg rows
        return df["_row_idx"].map(eligible).astype(bool)


# ==================================================
# IMPORTER
# ==================================================
class Create_TA_STANDARD_TABLE:
    def __init__(self):
        self.con = duckdb.connect(DB_PATH)
        self.con.execute(f"SET threads={THREADS}")
        self.con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
        self.con.execute("SET preserve_insertion_order=false")
        self.con.execute("SET enable_progress_bar=false")
        self.con.execute(f"SET temp_directory='{TEMP_DIR}'")

        ref = ReferenceData(self.con)
        self.processor = ChunkProcessor(ref.eu_airports, ref.eu_carriers)
        self._create_table()

    # ------------------------------------------------------------------
    # Table Creation
    # ------------------------------------------------------------------
    def _create_table(self):
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                Id                  VARCHAR PRIMARY KEY,
                ConnectionID        VARCHAR,
                PaxName             VARCHAR,
                AgencyRefNumber     VARCHAR,
                ETicketNo           VARCHAR,
                FlightNumber        VARCHAR,
                DepartureDate       TIMESTAMP,
                FileName            VARCHAR,
                BookingRef          VARCHAR,
                AirlineCode         VARCHAR,
                FromAirport         VARCHAR,
                ToAirport           VARCHAR,
                LastLegAirport      VARCHAR,
                EUEligible          BOOLEAN,
                EUEligibleDuration  INTEGER,
                ExtraNote           VARCHAR,
                FlightFound         BOOLEAN,
                LegNo               INTEGER,
                IsTimeLimitL1       BOOLEAN,
                IsTimeLimitL2       BOOLEAN,
                EUFlights_Id        VARCHAR,
                Link_Id             VARCHAR,
                DelayInSecond       INTEGER,
                Status              VARCHAR
            )
        """)
        print(f"✓ Table '{TARGET_TABLE}' ready.")

    # ------------------------------------------------------------------
    # Main Import
    # ------------------------------------------------------------------
    def run(self):
        total_rows = self.con.execute(
            f"SELECT COUNT(*) FROM {SOURCE_TABLE}"
        ).fetchone()[0]
        print(f"Source table '{SOURCE_TABLE}' has {total_rows:,} rows.")
        print(f"Read chunk: {READ_CHUNK:,} | Parse workers: {PARSE_WORKERS}\n")

        offset = 0
        total_inserted = 0

        # Pre-fetch chunks and process in parallel
        with ThreadPoolExecutor(max_workers=PARSE_WORKERS) as executor:
            while True:
                # Read next batch of chunks to keep workers busy
                futures = {}
                chunks_read = 0

                for _ in range(PARSE_WORKERS):
                    df = self.con.execute(
                        f"SELECT * FROM {SOURCE_TABLE} LIMIT {READ_CHUNK} OFFSET {offset}"
                    ).df()

                    if df.empty:
                        break

                    future = executor.submit(self.processor.process, df)
                    futures[future] = (offset, len(df))
                    offset += len(df)
                    chunks_read += 1

                    if len(df) < READ_CHUNK:
                        break  # last chunk

                if not futures:
                    break

                for future in as_completed(futures):
                    src_offset, src_len = futures[future]
                    result_df = future.result()
                    if not result_df.empty:
                        self._insert(result_df)
                        total_inserted += len(result_df)
                    print(
                        f"  Processed ~{src_offset + src_len:,} / {total_rows:,} source rows"
                        f" | Leg records so far: {total_inserted:,}"
                    )

                if chunks_read < PARSE_WORKERS:
                    break  # exhausted source

        print(
            f"\n✓ Done. Total leg records inserted into '{TARGET_TABLE}': {total_inserted:,}"
        )

    # ------------------------------------------------------------------
    # Insert
    # ------------------------------------------------------------------
    def _insert(self, df: pd.DataFrame):
        try:
            self.con.register("tmp_df", df)
            self.con.execute(f"""
                INSERT INTO {TARGET_TABLE} ({", ".join(TARGET_COLUMNS)})
                SELECT {", ".join(TARGET_COLUMNS)} FROM tmp_df
            """)
        except Exception as e:
            print(f"  ✗ Insert error: {e}")
        finally:
            self.con.unregister("tmp_df")


# ==================================================
# MAIN
# ==================================================
def main():
    standard_table = Create_TA_STANDARD_TABLE()
    try:
        standard_table.run()
    finally:
        standard_table.con.close()


if __name__ == "__main__":
    main()
