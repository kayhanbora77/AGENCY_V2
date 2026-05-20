import time
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
EXCEL_DIR = Path("C:\Users\cagri\Desktop\Gelen_Datalar\Akbar\")
TABLE_NAME = "AKBAR_2020"


def log(msg: str) -> None:
    print(msg, flush=True)


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def connect_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads TO {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    return con


def load_excel_files(con) -> None:
    dfs = []

    DATE_COLUMNS = [
        "DAIS",
        "FirstSectordate",
        "LastSectordate",
        "FlightDate1",
        "FlightDate2",
        "FlightDate3",
        "FlightDate4",
        "FlightDate5",
        "FlightDate6",
        "FlightDate7",
        "FlightDate8",
    ]

    for file in sorted(EXCEL_DIR.glob("*.xlsx")):
        log(f"⏰ Loading {file.name}")

        sheets = pd.read_excel(file, sheet_name=None, dtype=str)

        for sheet_name, df in sheets.items():
            if df.empty:
                continue

            for col in DATE_COLUMNS:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

            dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True)

    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.register("final_df", final_df)

    con.execute(f"""
        CREATE TABLE {TABLE_NAME} AS
        SELECT * FROM final_df
    """)


def main() -> None:
    start = time.time()
    log(f"Starting at {now_str()}")

    con = connect_db()
    load_excel_files(con)
    con.close()

    log(f"Finished at {now_str()}")
    elapsed = time.time() - start
    log(f"🎉 Loading completed in {int(elapsed // 60)}m {elapsed % 60:.2f}s")


if __name__ == "__main__":
    main()
