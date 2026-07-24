import duckdb
import pandas as pd
from pathlib import Path

# ==================================================
# CONFIG
# ==================================================
DB_PATH = r"C:\DuckDB\my_db.duckdb"
CSV_PATH = r"C:\Users\cagri\Desktop\Agency_Data\TBO\filter-0\BookingData_DeptDate_01Aug2025_30Apr2026.csv"
TABLE_NAME = "TBO_RAW"

# ==================================================
# CREATE TABLE
# ==================================================
def create_table(con):
    con.execute(f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
        
        CREATE TABLE {TABLE_NAME} (
            id                   UUID DEFAULT gen_random_uuid(),
            PaxName              VARCHAR,
            BookingRef           VARCHAR,
            ETicketNo            VARCHAR,
            ClientCode           VARCHAR,
            Airline              VARCHAR,
            JourneyType          VARCHAR,
            
            FlightNumber1        VARCHAR,
            FlightNumber2        VARCHAR,
            FlightNumber3        VARCHAR,
            FlightNumber4        VARCHAR,
            FlightNumber5        VARCHAR,
            FlightNumber6        VARCHAR,
            FlightNumber7        VARCHAR,
            
            DepartureDateLocal1  TIMESTAMP,
            DepartureDateLocal2  TIMESTAMP,
            DepartureDateLocal3  TIMESTAMP,
            DepartureDateLocal4  TIMESTAMP,
            DepartureDateLocal5  TIMESTAMP,
            DepartureDateLocal6  TIMESTAMP,
            DepartureDateLocal7  TIMESTAMP,
            
            Airport1             VARCHAR,
            Airport2             VARCHAR,
            Airport3             VARCHAR,
            Airport4             VARCHAR,
            Airport5             VARCHAR,
            Airport6             VARCHAR,
            Airport7             VARCHAR,
            Airport8             VARCHAR
        )
    """)
    print("✅ Table 'TBO_RAW' created successfully.")


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
            PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
            FlightNumber1, FlightNumber2, FlightNumber3, FlightNumber4, 
            FlightNumber5, FlightNumber6, FlightNumber7,
            DepartureDateLocal1, DepartureDateLocal2, DepartureDateLocal3, 
            DepartureDateLocal4, DepartureDateLocal5, DepartureDateLocal6, 
            DepartureDateLocal7,
            Airport1, Airport2, Airport3, Airport4, Airport5, 
            Airport6, Airport7, Airport8
        )
        SELECT 
            PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
            FlightNumber1, FlightNumber2, FlightNumber3, FlightNumber4, 
            FlightNumber5, FlightNumber6, FlightNumber7,
            
            -- Stronger date parsing
            STRPTIME(DepartureDateLocal1, '%m/%d/%Y %H:%M') AS DepartureDateLocal1,
            STRPTIME(DepartureDateLocal2, '%m/%d/%Y %H:%M') AS DepartureDateLocal2,
            STRPTIME(DepartureDateLocal3, '%m/%d/%Y %H:%M') AS DepartureDateLocal3,
            STRPTIME(DepartureDateLocal4, '%m/%d/%Y %H:%M') AS DepartureDateLocal4,
            STRPTIME(DepartureDateLocal5, '%m/%d/%Y %H:%M') AS DepartureDateLocal5,
            STRPTIME(DepartureDateLocal6, '%m/%d/%Y %H:%M') AS DepartureDateLocal6,
            STRPTIME(DepartureDateLocal7, '%m/%d/%Y %H:%M') AS DepartureDateLocal7,
            
            Airport1, Airport2, Airport3, Airport4, Airport5, 
            Airport6, Airport7, Airport8
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
        for i in range(1, 8):
            col = f"DepartureDateLocal{i}"
            valid = con.execute(
                f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE {col} IS NOT NULL"
            ).fetchone()[0]
            print(f"  {col}: {valid:,} valid timestamps")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
