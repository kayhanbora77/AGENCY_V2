import duckdb

# =====================================================
# CONFIG
# =====================================================
CSV_FILE = r"C:\Users\cagri\Desktop\Generated_Data\MMT\MMT_MISSCONNECTION.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "MMT_MISSCONNECTION"

con = duckdb.connect(str(DB_PATH))

con.execute(f"""
CREATE OR REPLACE TABLE {TABLE_NAME} AS
SELECT
    CAST(src.Id AS VARCHAR)                         AS Id,
    CAST(src.ConnectionID AS VARCHAR)               AS ConnectionID,
    
    -- Clean FlightNumber: remove trailing zeros, decimal point, and + sign from exponent
    CAST(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        src.FlightNumber,
                        '(\.[0-9]*[1-9])0+E', '\1E'  -- Remove trailing zeros: 1.2300E -> 1.23E
                    ),
                    '\.0+E', 'E'                      -- Remove .00E: 6.00E -> 6E
                ),
                'E\+0*', 'E'                          -- Remove + and leading zeros: E+032 -> E32
            ),
            'E\-0*', 'E-'                             -- Handle negative: E-032 -> E-32
        )
    AS VARCHAR)                                       AS FlightNumber,
    
    CAST(src.DepartureDate AS VARCHAR)              AS DepartureDate,
    CAST(src.LegNo AS INTEGER)                      AS LegNo,
    TRY_CAST(src.EUEligible AS INTEGER)             AS EUEligible,
    CAST(src.AirlineCode AS VARCHAR)                AS AirlineCode,

    COALESCE(
        TRY_STRPTIME(src.ActualDepartureTime, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ActualDepartureTime, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ActualDepartureTime, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ActualDepartureTime, '%Y-%m-%d %H:%M')
    ) AS ActualDepartureTime,

    COALESCE(
        TRY_STRPTIME(src.ActualArrivalTime, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ActualArrivalTime, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ActualArrivalTime, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ActualArrivalTime, '%Y-%m-%d %H:%M')
    ) AS ActualArrivalTime,

    COALESCE(
        TRY_STRPTIME(src.ScheduledDepartureTime, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledDepartureTime, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledDepartureTime, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ScheduledDepartureTime, '%Y-%m-%d %H:%M')
    ) AS ScheduledDepartureTime,

    COALESCE(
        TRY_STRPTIME(src.ScheduledArrivalTime, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledArrivalTime, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledArrivalTime, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ScheduledArrivalTime, '%Y-%m-%d %H:%M')
    ) AS ScheduledArrivalTime,

    TRY_CAST(src.DelayInSecond AS BIGINT)           AS DelayInSecond,
    CAST(src.Status AS VARCHAR)                     AS Status,
    CAST(NULL AS BIGINT)  AS DelayMissConnection,
    CAST(NULL AS BOOLEAN) AS IsMissConnection

FROM read_csv_auto(
    '{CSV_FILE}',
    delim=',',
    header=true,
    ignore_errors=true,
    nullstr=['NULL', 'null', 'N/A', ''],
    sample_size=-1,
    all_varchar=true
) AS src;
""")

row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
print(f"Table created : {TABLE_NAME}")
print(f"Rows loaded   : {row_count:,}")

# Verify FlightNumber cleaning
df = con.execute(f"""
    SELECT DISTINCT FlightNumber
    FROM {TABLE_NAME}
    WHERE FlightNumber LIKE '%E%'
    LIMIT 10
""").df()
print("\nCleaned FlightNumber samples:")
print(df)

con.close()