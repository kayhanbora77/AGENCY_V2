import duckdb
import pandas as pd
from pathlib import Path

# ==================================================
# CONFIG
# ==================================================
DB_PATH = r"C:\DuckDB\my_db.duckdb"
CSV_PATH = r"C:\Users\cagri\Desktop\Agency\Riya_CANADA\filter-0\merged_Riya_Canada.csv"
TABLE_NAME = "RIYA_CANADA_RAW"


# ==================================================
# CREATE TABLE
# ==================================================
def create_table(con):
    con.execute(f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
        
        CREATE TABLE {TABLE_NAME} (
            id                   UUID DEFAULT gen_random_uuid(),
            DocumentReferenceNo  VARCHAR,
            PNRNo                VARCHAR,
            PaxName              VARCHAR,
            SupplierCode         VARCHAR,
            FlightNumber1        VARCHAR,
            FlightNumber2        VARCHAR,
            FlightNumber3        VARCHAR,
            FlightNumber4        VARCHAR,
            FlightNumber5        VARCHAR,
            FlightNumber6        VARCHAR,
            FlightNumber7        VARCHAR,
            FlightNumber8        VARCHAR,
            
            DepartureDate1       TIMESTAMP,
            DepartureDate2       TIMESTAMP,
            DepartureDate3       TIMESTAMP,
            DepartureDate4       TIMESTAMP,
            DepartureDate5       TIMESTAMP,
            DepartureDate6       TIMESTAMP,
            DepartureDate7       TIMESTAMP,
            DepartureDate8       TIMESTAMP,
            
            Airport1             VARCHAR,
            Airport2             VARCHAR,
            Airport3             VARCHAR,
            Airport4             VARCHAR,
            Airport5             VARCHAR,
            Airport6             VARCHAR,
            Airport7             VARCHAR,
            Airport8             VARCHAR,
            Airport9             VARCHAR
        )
    """)
    print(f"✅ Table '{TABLE_NAME}' created successfully.")


# ==================================================
# LOAD CSV WITH PROPER DATE PARSING
# ==================================================
def load_and_insert(con):
    if not Path(CSV_PATH).exists():
        print(f"❌ File not found: {CSV_PATH}")
        return

    print(f"Loading CSV: {Path(CSV_PATH).name} ...")

    df = pd.read_csv(CSV_PATH, dtype=str, low_memory=False)
    print(f"✅ Loaded {len(df):,} rows.")

    df.columns = df.columns.str.strip().str.replace(" ", "")

    print("Columns:", df.columns.tolist())

    con.execute(f"DELETE FROM {TABLE_NAME}")

    con.register("temp_df", df)

    con.execute(f"""
        INSERT INTO {TABLE_NAME} (
            DocumentReferenceNo, PNRNo, PaxName, SupplierCode,
            FlightNumber1, FlightNumber2, FlightNumber3, FlightNumber4,
            FlightNumber5, FlightNumber6, FlightNumber7, FlightNumber8,
            DepartureDate1, DepartureDate2, DepartureDate3, DepartureDate4,
            DepartureDate5, DepartureDate6, DepartureDate7, DepartureDate8,
            Airport1, Airport2, Airport3, Airport4, Airport5,
            Airport6, Airport7, Airport8, Airport9
        )
        SELECT
            DocumentReferenceNo, PNRNo, PaxName, SupplierCode,
            FlightNumber1, FlightNumber2, FlightNumber3, FlightNumber4,
            FlightNumber5, FlightNumber6, FlightNumber7, FlightNumber8,

            TRY_STRPTIME(DepartureDate1, '%m/%d/%Y %H:%M') AS DepartureDate1,
            TRY_STRPTIME(DepartureDate2, '%m/%d/%Y %H:%M') AS DepartureDate2,
            TRY_STRPTIME(DepartureDate3, '%m/%d/%Y %H:%M') AS DepartureDate3,
            TRY_STRPTIME(DepartureDate4, '%m/%d/%Y %H:%M') AS DepartureDate4,
            TRY_STRPTIME(DepartureDate5, '%m/%d/%Y %H:%M') AS DepartureDate5,
            TRY_STRPTIME(DepartureDate6, '%m/%d/%Y %H:%M') AS DepartureDate6,
            TRY_STRPTIME(DepartureDate7, '%m/%d/%Y %H:%M') AS DepartureDate7,
            TRY_STRPTIME(DepartureDate8, '%m/%d/%Y %H:%M') AS DepartureDate8,

            Airport1, Airport2, Airport3, Airport4, Airport5,
            Airport6, Airport7, Airport8, Airport9
        FROM temp_df
    """)

    con.unregister("temp_df")

    print(f"✅ Successfully inserted {len(df):,} rows.")


def main():
    con = duckdb.connect(DB_PATH)
    try:
        create_table(con)
        load_and_insert(con)

        count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        print(f"\n🎉 Final row count: {count:,}")

        # Check how many dates were successfully parsed
        print("\nDate parsing check:")
        for i in range(1, 9):
            col = f"DepartureDate{i}"
            valid = con.execute(
                f"SELECT COUNT(*) FROM RIYA_CANADA_RAW WHERE {col} IS NOT NULL"
            ).fetchone()[0]
            print(f"  {col}: {valid:,} valid timestamps")

        # Show sample
        print("\nSample data:")
        con.sql(f"""
            SELECT id, PaxName, DepartureDate1 
            FROM {TABLE_NAME} 
            WHERE DepartureDate1 IS NOT NULL 
            LIMIT 5
        """).show()

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
