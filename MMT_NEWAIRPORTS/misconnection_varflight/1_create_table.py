import duckdb

# =====================================================
# CONFIG
# =====================================================
CSV_FILE = r"C:\Users\cagri\Desktop\MMT\MMT_VARIFLIGHT\MMT_MISSCONN.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "MMT_MC_VARFLIGHT"

con = duckdb.connect(str(DB_PATH))

con.execute(f"""
CREATE OR REPLACE TABLE {TABLE_NAME} AS
SELECT
    CAST(src.Id AS VARCHAR)                         AS Id,
    CAST(src.ConnectionID AS VARCHAR)               AS ConnectionID,
    
    -- FlightNumber: Just clean it, no scientific notation conversion needed
    -- Handle "#####" and keep normal flight codes like "EY84", "D83194"
    CASE 
        WHEN src.FlightNumber IS NULL 
             OR src.FlightNumber = '' 
             OR src.FlightNumber LIKE '%#%' THEN NULL
        ELSE TRIM(CAST(src.FlightNumber AS VARCHAR))
    END                                              AS FlightNumber,
    
    -- DepartureDate: Parse multiple date formats
    COALESCE(
        TRY_STRPTIME(src.DepartureDate, '%Y-%m-%d'),
        TRY_STRPTIME(src.DepartureDate, '%m/%d/%Y'),
        TRY_STRPTIME(src.DepartureDate, '%d-%m-%Y'),
        TRY_STRPTIME(src.DepartureDate, '%d/%m/%Y'),
        TRY_STRPTIME(src.DepartureDate, '%Y/%m/%d')
    )::DATE                                         AS DepartureDate,
    
    -- LegNo: Handle "#####" as NULL
    CASE 
        WHEN src.LegNo IS NULL 
             OR src.LegNo = '' 
             OR src.LegNo LIKE '%#%' THEN NULL
        ELSE TRY_CAST(src.LegNo AS INTEGER)
    END                                              AS LegNo,
    
    -- EUEligible
    CASE 
        WHEN src.EUEligible IS NULL 
             OR src.EUEligible = '' 
             OR src.EUEligible LIKE '%#%' THEN NULL
        ELSE TRY_CAST(src.EUEligible AS INTEGER)
    END                                              AS EUEligible,
    
    -- AirlineCode: Handle "#####" as NULL
    CASE 
        WHEN src.AirlineCode IS NULL 
             OR src.AirlineCode = '' 
             OR src.AirlineCode LIKE '%#%' THEN NULL
        ELSE TRIM(CAST(src.AirlineCode AS VARCHAR))
    END                                              AS AirlineCode,

    -- ActualDepartureTime: Parse multiple timestamp formats
    COALESCE(
        TRY_STRPTIME(src.ActualDepartureTime, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ActualDepartureTime, '%Y-%m-%d %H:%M:%S.%f'),
        TRY_STRPTIME(src.ActualDepartureTime, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ActualDepartureTime, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ActualDepartureTime, '%Y-%m-%d %H:%M'),
        TRY_STRPTIME(src.ActualDepartureTime, '%d-%m-%Y %H:%M:%S'),
        TRY_STRPTIME(src.ActualDepartureTime, '%d/%m/%Y %H:%M:%S')
    )::TIMESTAMP                                     AS ActualDepartureTime,

    -- ActualArrivalTime
    COALESCE(
        TRY_STRPTIME(src.ActualArrivalTime, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ActualArrivalTime, '%Y-%m-%d %H:%M:%S.%f'),
        TRY_STRPTIME(src.ActualArrivalTime, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ActualArrivalTime, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ActualArrivalTime, '%Y-%m-%d %H:%M'),
        TRY_STRPTIME(src.ActualArrivalTime, '%d-%m-%Y %H:%M:%S'),
        TRY_STRPTIME(src.ActualArrivalTime, '%d/%m/%Y %H:%M:%S')
    )::TIMESTAMP                                     AS ActualArrivalTime,

    -- ScheduledDepartureTime
    COALESCE(
        TRY_STRPTIME(src.ScheduledDepartureTime, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledDepartureTime, '%Y-%m-%d %H:%M:%S.%f'),
        TRY_STRPTIME(src.ScheduledDepartureTime, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledDepartureTime, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ScheduledDepartureTime, '%Y-%m-%d %H:%M'),
        TRY_STRPTIME(src.ScheduledDepartureTime, '%d-%m-%Y %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledDepartureTime, '%d/%m/%Y %H:%M:%S')
    )::TIMESTAMP                                     AS ScheduledDepartureTime,

    -- ScheduledArrivalTime
    COALESCE(
        TRY_STRPTIME(src.ScheduledArrivalTime, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledArrivalTime, '%Y-%m-%d %H:%M:%S.%f'),
        TRY_STRPTIME(src.ScheduledArrivalTime, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledArrivalTime, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ScheduledArrivalTime, '%Y-%m-%d %H:%M'),
        TRY_STRPTIME(src.ScheduledArrivalTime, '%d-%m-%Y %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledArrivalTime, '%d/%m/%Y %H:%M:%S')
    )::TIMESTAMP                                     AS ScheduledArrivalTime,

    -- DelayInSecond: Handle "#####" as NULL
    CASE 
        WHEN src.DelayInSecond IS NULL 
             OR src.DelayInSecond = '' 
             OR src.DelayInSecond LIKE '%#%' THEN NULL
        ELSE TRY_CAST(src.DelayInSecond AS BIGINT)
    END                                              AS DelayInSecond,
    
    -- Status: Handle "#####" as NULL
    CASE 
        WHEN src.Status IS NULL 
             OR src.Status = '' 
             OR src.Status LIKE '%#%' THEN NULL
        ELSE TRIM(CAST(src.Status AS VARCHAR))
    END                                              AS Status,
    
    CAST(NULL AS BIGINT)                             AS DelayMissConnection,
    CAST(NULL AS BOOLEAN)                            AS IsMissConnection

FROM read_csv_auto(
    '{CSV_FILE}',
    delim=',',
    header=true,
    ignore_errors=true,
    nullstr=['NULL', 'null', 'N/A', '', '#####', '######', '#######'],
    sample_size=-1,
    all_varchar=true
) AS src;
""")

row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
print(f"Table created : {TABLE_NAME}")
print(f"Rows loaded   : {row_count:,}")

# Verify FlightNumber - show sample values
df = con.execute(f"""
    SELECT FlightNumber, COUNT(*) as cnt
    FROM {TABLE_NAME}
    GROUP BY FlightNumber
    ORDER BY cnt DESC
    LIMIT 15
""").df()
print("\nFlightNumber distribution:")
print(df)

# Check for NULL counts in key columns
df_nulls = con.execute(f"""
    SELECT 
        SUM(CASE WHEN FlightNumber IS NULL THEN 1 ELSE 0 END) AS FlightNumber_NULL,
        SUM(CASE WHEN DepartureDate IS NULL THEN 1 ELSE 0 END) AS DepartureDate_NULL,
        SUM(CASE WHEN ActualDepartureTime IS NULL THEN 1 ELSE 0 END) AS ActualDepartureTime_NULL,
        SUM(CASE WHEN ScheduledDepartureTime IS NULL THEN 1 ELSE 0 END) AS ScheduledDepartureTime_NULL,
        SUM(CASE WHEN Status IS NULL THEN 1 ELSE 0 END) AS Status_NULL
    FROM {TABLE_NAME}
""").df()
print("\nNULL counts by column:")
print(df_nulls)

# Show sample timestamp values
df_ts = con.execute(f"""
    SELECT ActualDepartureTime, ScheduledDepartureTime, DepartureDate
    FROM {TABLE_NAME}
    WHERE ActualDepartureTime IS NOT NULL
    LIMIT 5
""").df()
print("\nSample timestamp values:")
print(df_ts)

con.close()
print("\n✓ Import completed successfully!")