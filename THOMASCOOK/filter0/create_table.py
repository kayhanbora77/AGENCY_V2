import duckdb

csv_file_path = r"C:\Users\cagri\Desktop\Agency_Data\ThomasCook\filter-0\SEP_25-MAR_26\ThomasCook.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"


def create_table():
    con = duckdb.connect(DB_PATH)

    print("Analyzing CSV data to find exact unique column counts...")

    # 1. Query max lengths using the de-duplication logic inside the scan phase
    max_lengths = con.execute(f"""
        WITH scanned_data AS (
            SELECT 
                string_to_array(regexp_replace(FLIGHTNO, ' ', '', 'g'), '/') AS f_arr,
                regexp_extract_all(SECTOR, '[A-Z]{{3}}') AS raw_airports,
                string_to_array(DEPDATETIME, ' / ') AS d_arr
            FROM read_csv_auto('{csv_file_path}')
        ),
        cleaned_lengths AS (
            SELECT
                len(f_arr) AS f_len,
                len(list_filter(raw_airports, (x, idx) -> idx = 1 OR x != raw_airports[idx - 1])) AS a_len,
                len(d_arr) AS d_len
            FROM scanned_data
        )
        SELECT max(f_len), max(a_len), max(d_len) FROM cleaned_lengths;
    """).fetchone()

    # Assign variables with defaults if the file is empty
    max_flights = max_lengths[0] if max_lengths and max_lengths[0] is not None else 1
    max_airports = max_lengths[1] if max_lengths and max_lengths[1] is not None else 1
    max_dates = max_lengths[2] if max_lengths and max_lengths[2] is not None else 1

    print(
        f"Detected Dynamic Counts -> Flights: {max_flights}, De-duplicated Airports: {max_airports}, Dates: {max_dates}"
    )

    # 2. Programmatically build the FLIGHTNO projection columns
    flight_cols = []
    for i in range(1, max_flights + 1):
        flight_cols.append(f"trim(flight_array[{i}]) AS FLIGHTNO{i}")

    # 3. Programmatically build Sequential Unique Airport columns
    airport_cols = []
    for i in range(1, max_airports + 1):
        airport_cols.append(f"clean_airports[{i}] AS AIRPORT{i}")

    # 4. Programmatically build TIMESTAMP formatting logic for Departure Dates
    date_cols = []
    for i in range(1, max_dates + 1):
        date_cols.append(f"""
            CASE 
                WHEN date_array[{i}] IS NULL THEN NULL 
                ELSE strptime(regexp_replace(trim(date_array[{i}]), '([0-9]{{2}})([0-9]{{2}})$', '\\1:\\2'), '%Y-%m-%d - %H:%M')
            END AS DEPARTURE_DATE{i}
        """)

    # Join column strings together into clean query blocks
    flight_sql_block = ",\n        ".join(flight_cols)
    airport_sql_block = ",\n        ".join(airport_cols)
    date_sql_block = ",\n        ".join(date_cols)

    print("Creating dynamic table schema in DuckDB...")

    # 5. Execute table creation query with matching dynamic arrays
    con.execute(f"""
        CREATE OR REPLACE TABLE THOMASCOOK_RAW AS
        WITH raw_data AS (
            SELECT 
                uuid() AS Id,
                COMPANY, AIRLINE_PNR, GDS_PNR, TICKET_NO, INVOICE_AND_REFUNDID, 
                GROUP_NAME, AIRLINE_CARRIER_CODE, AIRLINE_CARRIER_NAME, 
                STATUS, ARRVLDATETIME,
                
                string_to_array(regexp_replace(FLIGHTNO, ' ', '', 'g'), '/') AS flight_array,
                regexp_extract_all(SECTOR, '[A-Z]{{3}}') AS raw_airports,
                string_to_array(DEPDATETIME, ' / ') AS date_array
                
            FROM read_csv_auto('{csv_file_path}')
        ),
        deduped_airports AS (
            SELECT 
                *,
                list_filter(
                    raw_airports, 
                    (x, idx) -> idx = 1 OR x != raw_airports[idx - 1]
                ) AS clean_airports
            FROM raw_data
        )
        SELECT 
            Id,
            COMPANY, AIRLINE_PNR, GDS_PNR, TICKET_NO, INVOICE_AND_REFUNDID, 
            GROUP_NAME, AIRLINE_CARRIER_CODE, AIRLINE_CARRIER_NAME, STATUS, ARRVLDATETIME,
            
            -- Dynamic Flights
            {flight_sql_block},
            
            -- Dynamic Unique Consecutive Airports (No extra trailing NULL columns)
            {airport_sql_block},
            
            -- Dynamic Timestamps
            {date_sql_block}

        FROM deduped_airports;
    """)

    print("Table successfully created with structural data limits matching cleanly!")

    # Verify structural schema updates
    print("\nGenerated Schema Columns Verification:")
    cols = con.execute("PRAGMA table_info('THOMASCOOK_RAW');").fetchall()
    for col in cols:
        print(f"Column: {col[1]:<18} | Type: {col[2]}")

    con.close()


def main():
    create_table()


if __name__ == "__main__":
    main()
