import time
import uuid
from pathlib import Path

import duckdb
import pandas as pd


# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path(r"C:\DuckDB")
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME
THREADS = 4
MEMORY_LIMIT = "6GB"
TEMP_DIR = "/tmp/duckdb_temp"
CSV_FILE = Path(r"C:\Users\cagri\Desktop\Gelen_Datalar\Akbar\Filter_0(Merged)\Akbar_Merged.csv")
TABLE_NAME = "AKBAR_RAW"


def log(msg: str) -> None:
    print(msg, flush=True)


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def connect_db() -> duckdb.DuckDBPyConnection:
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads TO {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    return con


def load_csv_file(con) -> None:
    DATE_COLUMNS = [
        "DAIS",
        "FirstSectordate",
        "LastSectordate",
        "FlightDate1",
        "FlightDate2",
        "FlightDate3",
        "FlightDate4",
    ]

    if not CSV_FILE.exists():
        raise FileNotFoundError(f"CSV file not found: {CSV_FILE}")

    log(f"[LOADING] {CSV_FILE.name}")

    final_df = pd.read_csv(
        CSV_FILE,
        dtype=str,
        encoding="latin-1",
        low_memory=False
    )

    # ✅ Trim ONLY string columns (correct way)
    for col in final_df.select_dtypes(include="object").columns:
        final_df[col] = final_df[col].str.strip()

    # ✅ Convert date columns AFTER trimming
    for col in DATE_COLUMNS:
        if col in final_df.columns:
            final_df[col] = pd.to_datetime(final_df[col], errors="coerce")

    # ✅ Create UUID column
    final_df.insert(
        0,
        "Id",
        [uuid.uuid4() for _ in range(len(final_df))]
    )

    log(f"[INFO] Rows loaded: {len(final_df):,}")

    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.register("final_df", final_df)

    # ✅ Force UUID type in DuckDB
    con.execute(f"""
        CREATE TABLE {TABLE_NAME} AS
        SELECT
            CAST(Id AS UUID) AS Id,
            * EXCLUDE (Id)
        FROM final_df
    """)


def main() -> None:
    start = time.time()
    log(f"Starting at {now_str()}")

    con = connect_db()
    load_csv_file(con)
    con.close()

    elapsed = time.time() - start
    log(f"Finished at {now_str()}")
    log(f"[DONE] Loading completed in {int(elapsed // 60)}m {elapsed % 60:.2f}s")


if __name__ == "__main__":
    main()