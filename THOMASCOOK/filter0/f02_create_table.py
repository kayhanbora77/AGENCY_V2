import duckdb

csv_file_path = r"C:\Users\cagri\Desktop\Agency_Data\ThomasCook\filter-0\SEP_25-MAR_26\ThomasCook.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"


def create_table():
    con = duckdb.connect(DB_PATH)

    print("Analyzing CSV data to find exact unique column counts...")

    # 1. Query max lengths using the split logic inside the scan phase (Airports removed)
    max_lengths = con.execute(f"""
        WITH scanned_data AS (
            SELECT 
                string_to_array(regexp_replace(FLIGHTNO, ' ', '', 'g'), '/') AS f_arr,
                string_to_array(DEPDATETIME, ' / ') AS d_arr,
                string_to_array(ARRVLDATETIME, ' / ') AS arr_arr,
                string_to_array(SECTOR, ' / ') AS s_arr
            FROM read_csv_auto('{csv_file_path}')
        ),
        cleaned_lengths AS (
            SELECT
                len(f_arr) AS f_len,
                len(d_arr) AS d_len,
                len(arr_arr) AS arr_len,
                len(s_arr) AS s_len
            FROM scanned_data
        )
        SELECT max(f_len), max(d_len), max(arr_len), max(s_len) FROM cleaned_lengths;
    """).fetchone()

    max_flights = max_lengths[0] if max_lengths and max_lengths[0] is not None else 1
    max_dates = max_lengths[1] if max_lengths and max_lengths[1] is not None else 1
    max_arrival_dates = (
        max_lengths[2] if max_lengths and max_lengths[2] is not None else 1
    )
    max_sectors = max_lengths[3] if max_lengths and max_lengths[3] is not None else 1

    print(
        f"Detected Max Counts -> Flights: {max_flights}, "
        f"Departure Dates: {max_dates}, Arrival Dates: {max_arrival_dates}, Sectors: {max_sectors}"
    )

    # 2. Programmatically build the dynamic SQL projection columns
    flight_cols = [
        f"trim(flight_array[{i}]) AS FLIGHTNO{i}" for i in range(1, max_flights + 1)
    ]
    date_cols = [
        f"CASE WHEN date_array[{i}] IS NULL THEN NULL ELSE strptime(regexp_replace(trim(date_array[{i}]), '([0-9]{{2}})([0-9]{{2}})$', '\\1:\\2'), '%Y-%m-%d - %H:%M') END AS DEPARTURE_DATE{i}"
        for i in range(1, max_dates + 1)
    ]
    arrival_date_cols = [
        f"CASE WHEN arrival_date_array[{i}] IS NULL THEN NULL ELSE strptime(regexp_replace(trim(arrival_date_array[{i}]), '([0-9]{{2}})([0-9]{{2}})$', '\\1:\\2'), '%Y-%m-%d - %H:%M') END AS ARRIVAL_DATE{i}"
        for i in range(1, max_arrival_dates + 1)
    ]
    sector_cols = [
        f"trim(sector_array[{i}]) AS SECTOR{i}" for i in range(1, max_sectors + 1)
    ]

    flight_sql_block = ",\n            ".join(flight_cols)
    date_sql_block = ",\n            ".join(date_cols)
    arrival_date_sql_block = ",\n            ".join(arrival_date_cols)
    sector_sql_block = ",\n            ".join(sector_cols)

    print("Staging all data into a temporary view...")

    # 3. Create a flattened staging view containing all segmented columns
    con.execute(f"""
        CREATE OR REPLACE TEMPORARY VIEW staging_view AS
        SELECT 
            uuid() AS Id,
            COMPANY, AIRLINE_PNR, GDS_PNR, TICKET_NO, INVOICE_AND_REFUNDID, 
            GROUP_NAME, AIRLINE_CARRIER_CODE, AIRLINE_CARRIER_NAME, 
            STATUS, DEPDATETIME, ARRVLDATETIME, FLIGHTNO, SECTOR,
            string_to_array(regexp_replace(FLIGHTNO, ' ', '', 'g'), '/') AS flight_array,
            string_to_array(DEPDATETIME, ' / ') AS date_array,
            string_to_array(ARRVLDATETIME, ' / ') AS arrival_date_array,
            string_to_array(SECTOR, ' / ') AS sector_array
        FROM read_csv_auto('{csv_file_path}');
    """)

    # 3b. Apply the projection blocks directly onto the staging view
    con.execute(f"""
        CREATE OR REPLACE TEMPORARY VIEW projected_view AS
        SELECT 
            *,
            {flight_sql_block},
            {date_sql_block},
            {arrival_date_sql_block},
            {sector_sql_block}
        FROM staging_view;
    """)

    print("Inserting all records directly into THOMASCOOK_RAW...")

    # 4. Insert All Records -> THOMASCOOK_RAW
    flight_select = ", ".join([f"FLIGHTNO{i}" for i in range(1, max_flights + 1)])
    date_select = ", ".join([f"DEPARTURE_DATE{i}" for i in range(1, max_dates + 1)])
    arrival_date_select = ", ".join(
        [f"ARRIVAL_DATE{i}" for i in range(1, max_arrival_dates + 1)]
    )
    sector_select = ", ".join([f"SECTOR{i}" for i in range(1, max_sectors + 1)])

    con.execute(f"""
        CREATE OR REPLACE TABLE THOMASCOOK_RAW AS
        SELECT 
            Id, COMPANY, AIRLINE_PNR, GDS_PNR, TICKET_NO, INVOICE_AND_REFUNDID, 
            GROUP_NAME, AIRLINE_CARRIER_CODE, AIRLINE_CARRIER_NAME, STATUS,
            {flight_select},
            {date_select},
            {arrival_date_select},
            {sector_select}
        FROM projected_view;
    """)

    # 5. Print execution report summaries
    raw_count = con.execute("SELECT count(*) FROM THOMASCOOK_RAW").fetchone()[0]

    print("\n--- Post-Generation Columns Audit Report ---")
    print(f"Successfully Processed Rows (THOMASCOOK_RAW): {raw_count}")

    con.close()


def main():
    create_table()


if __name__ == "__main__":
    main()
