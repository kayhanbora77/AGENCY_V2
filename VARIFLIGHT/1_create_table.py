import os
import pandas as pd
import duckdb

# Directory containing Excel files
EXCEL_DIR = r"C:\Users\cagri\Desktop\VariFlight"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "VARIFLIGHT"

# List all Excel files in the directory
excel_files = [f for f in os.listdir(EXCEL_DIR) if f.endswith(".xlsx")]

# Initialize an empty list to store DataFrames
dfs = []

# Read each Excel file and append to the list
for file in excel_files:
    file_path = os.path.join(EXCEL_DIR, file)
    df = pd.read_excel(file_path)

    # Set AgencyName based on filename
    if file == "vari_canada.xlsx":
        df["AgencyName"] = "MMT_CANADA"
    elif file == "vari_shy.xlsx":
        df["AgencyName"] = "MMT_SHY"
    # For vari_mix, use existing AgencyName values (no action needed)

    dfs.append(df)

# Combine all DataFrames into one
combined_df = pd.concat(dfs, ignore_index=True)

# Drop irrelevant columns (if any)
combined_df = combined_df.drop(columns=["Unnamed: 3", "Unnamed: 4", "Unnamed: 5", "EUFlightId"], errors="ignore")

# Ensure all columns in the DataFrame match the table schema
# Define the expected columns in the correct order
expected_columns = [
    "Id", "AgencyName", "flight_number", "date", "DepartureAirport", "ArrivalAirport",
    "AirlineCode", "AirlineName", "ScheduledDeparture", "ActualDeparture",
    "DepartureDelayMinutes", "ScheduledArrival", "ActualArrival", "ArrivalDelayMinutes",
    "FlightStatus", "AircraftType", "DistanceKm", "HistoricalOnTimeRate",
    "DepartureTimezone", "ArrivalTimezone", "ActualGateArrival", "ActualGateDeparture",
    "StopCount", "SegmentType", "OperatingFlightNo", "IsCodeShare", "CancellationTime",
    "ReplacementFlightNo", "WasDiverted", "DiversionDetails", "DepartureWeather",
    "ArrivalWeather", "ErrorMessage", "IsProcessed"
]

# Reorder the DataFrame columns to match the schema
combined_df = combined_df.reindex(columns=expected_columns, fill_value=None)

# Connect to DuckDB
conn = duckdb.connect(database=DB_PATH)

# Create a DuckDB table with the appropriate schema
conn.execute(f"""
CREATE OR REPLACE TABLE {TABLE_NAME} (
    Id VARCHAR,
    AgencyName VARCHAR,
    flight_number VARCHAR,
    date TIMESTAMP,
    DepartureAirport VARCHAR,
    ArrivalAirport VARCHAR,
    AirlineCode VARCHAR,
    AirlineName VARCHAR,
    ScheduledDeparture TIMESTAMP,
    ActualDeparture TIMESTAMP,
    DepartureDelayMinutes INTEGER,
    ScheduledArrival TIMESTAMP,
    ActualArrival TIMESTAMP,
    ArrivalDelayMinutes INTEGER,
    FlightStatus VARCHAR,
    AircraftType VARCHAR,
    DistanceKm INTEGER,
    HistoricalOnTimeRate INTEGER,
    DepartureTimezone VARCHAR,
    ArrivalTimezone VARCHAR,
    ActualGateArrival TIMESTAMP,
    ActualGateDeparture TIMESTAMP,
    StopCount INTEGER,
    SegmentType VARCHAR,
    OperatingFlightNo VARCHAR,
    IsCodeShare INTEGER,
    CancellationTime TIMESTAMP,
    ReplacementFlightNo VARCHAR,
    WasDiverted INTEGER,
    DiversionDetails VARCHAR,
    DepartureWeather VARCHAR,
    ArrivalWeather VARCHAR,
    ErrorMessage VARCHAR,
    IsProcessed VARCHAR
)
""")

# Insert the combined data into the DuckDB table
# Use the correct column order and explicitly map DataFrame columns to table columns
conn.register("combined_df", combined_df)
conn.execute(f"""
INSERT INTO {TABLE_NAME}
SELECT
    Id, AgencyName, flight_number, date, DepartureAirport, ArrivalAirport,
    AirlineCode, AirlineName, ScheduledDeparture, ActualDeparture,
    DepartureDelayMinutes, ScheduledArrival, ActualArrival, ArrivalDelayMinutes,
    FlightStatus, AircraftType, DistanceKm, HistoricalOnTimeRate,
    DepartureTimezone, ArrivalTimezone, ActualGateArrival, ActualGateDeparture,
    StopCount, SegmentType, OperatingFlightNo, IsCodeShare, CancellationTime,
    ReplacementFlightNo, WasDiverted, DiversionDetails, DepartureWeather,
    ArrivalWeather, ErrorMessage, IsProcessed
FROM combined_df
""")

# Verify the data was inserted
result = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchall()
print(f"Total rows inserted: {result[0][0]}")

# Close the connection
conn.close()