
import duckdb

# =====================================================
# CONFIG
# =====================================================
CSV_FILE = r"C:\Users\cagri\Desktop\Tripjack\Tripjack_MissConn_Candidate.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "TRIPJACK_MISSCONNECTION"

con = duckdb.connect(str(DB_PATH))

con.execute(f"""
CREATE OR REPLACE TABLE {TABLE_NAME} AS
SELECT
    CAST(src.Id AS VARCHAR)                           AS Id,
    CAST(src.ConnectionID AS VARCHAR)                 AS ConnectionID,
    CAST(src.PaxName AS VARCHAR)                      AS PaxName,
    CAST(src.AgencyRefNumber AS VARCHAR)              AS AgencyRefNumber,
    CAST(src.ETicketNo AS VARCHAR)                    AS ETicketNo,

    -- Clean FlightNumber: remove trailing zeros, decimal point, and + sign from exponent
    CAST(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        src.FlightNumber,
                        '(\\.[0-9]*[1-9])0+E', '\\1E'  -- Remove trailing zeros: 1.2300E -> 1.23E
                    ),
                    '\\.0+E', 'E'                      -- Remove .00E: 6.00E -> 6E
                ),
                'E\\+0*', 'E'                          -- Remove + and leading zeros: E+032 -> E32
            ),
            'E\\-0*', 'E-'                             -- Handle negative: E-032 -> E-32
        ) 
    AS VARCHAR)                                       AS FlightNumber,

    CAST(src.DepartureDate AS VARCHAR)                AS DepartureDate,
    CAST(src.FileName AS VARCHAR)                     AS FileName,
    CAST(src.BookingRef AS VARCHAR)                   AS BookingRef,
    CAST(src.AirlineCode AS VARCHAR)                  AS AirlineCode,
    CAST(src.FromAirport AS VARCHAR)                  AS FromAirport,
    CAST(src.ToAirport AS VARCHAR)                    AS ToAirport,
    CAST(src.LastLegAirport AS VARCHAR)               AS LastLegAirport,
    
    TRY_CAST(src.EUEligible AS INTEGER)               AS EUEligible,
    TRY_CAST(src.EUEligibleDuration AS BIGINT)        AS EUEligibleDuration,
    CAST(src.ExtraNote AS VARCHAR)                    AS ExtraNote,
    TRY_CAST(src.FlightFound AS INTEGER)              AS FlightFound,
    TRY_CAST(src.LegNo AS INTEGER)                    AS LegNo,
    TRY_CAST(src.IsTimeLimitL1 AS INTEGER)            AS IsTimeLimitL1,
    TRY_CAST(src.IsTimeLimitL2 AS INTEGER)            AS IsTimeLimitL2,
    CAST(src.EUFlights_Id AS VARCHAR)                 AS EUFlights_Id,
    CAST(src.Link_Id AS VARCHAR)                      AS Link_Id,
    TRY_CAST(src.DelayInSecond AS BIGINT)             AS DelayInSecond,
    CAST(src.Status AS VARCHAR)                       AS Status,
    TRY_CAST(src.IsSingleFlight AS INTEGER)           AS IsSingleFlight,
    TRY_CAST(src.IsMultiSegment AS INTEGER)           AS IsMultiSegment,
    CAST(src.OperatingFlightNo AS VARCHAR)            AS OperatingFlightNo,

    COALESCE(
        TRY_STRPTIME(src.ScheduledDeparture, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledDeparture, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledDeparture, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ScheduledDeparture, '%Y-%m-%d %H:%M')
    ) AS ScheduledDeparture,

    COALESCE(
        TRY_STRPTIME(src.ScheduledArrival, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledArrival, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledArrival, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ScheduledArrival, '%Y-%m-%d %H:%M')
    ) AS ScheduledArrival,

    COALESCE(
        TRY_STRPTIME(src.ActualDeparture, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ActualDeparture, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ActualDeparture, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ActualDeparture, '%Y-%m-%d %H:%M')
    ) AS ActualDeparture,

    COALESCE(
        TRY_STRPTIME(src.ActualArrival, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ActualArrival, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ActualArrival, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ActualArrival, '%Y-%m-%d %H:%M')
    ) AS ActualArrival,

    CAST(src.SourceData AS VARCHAR)                   AS SourceData,

    -- Custom calculated place-holder columns
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

con.close()