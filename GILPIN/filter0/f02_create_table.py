import duckdb

# File paths
CSV_FILE = r"C:\Users\cagri\Desktop\Agency_Data\GULPIN\filter0\GILPIN_MERGED.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "GILPIN_RAW"

def insert_csv_to_duckdb(csv_path, db_path, table_name):
    # Connect to DuckDB
    con = duckdb.connect(db_path)
    
    print(f"Reading CSV and creating table '{table_name}' with UUIDs...")
    
    # SQL query with the fix: explicitly define TicketNumber as VARCHAR
    query = f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT 
            uuid() AS Id, 
            * 
        FROM read_csv_auto(
            ?, 
            all_varchar=false, 
            types={{'TicketNumber': 'VARCHAR'}}  -- THE FIX IS HERE
        );
    """
    
    # Execute the query
    con.execute(query, [csv_path])
    
    print("Data successfully inserted!")
    
    # --- Verification Step ---
    print("\n--- Sample Data from DuckDB ---")
    sample_df = con.execute(f"SELECT Id, TicketNumber, Passenger, AirlineCode FROM {table_name} LIMIT 5").fetchdf()
    print(sample_df.to_string(index=False))
    
    # Optional: Show the table schema to prove 'TicketNumber' is now VARCHAR and 'Id' is UUID
    print("\n--- Table Schema ---")
    schema_df = con.execute(f"DESCRIBE {table_name}").fetchdf()
    print(schema_df.to_string(index=False))

    # Close the connection
    con.close()

if __name__ == "__main__":
    insert_csv_to_duckdb(CSV_FILE, DB_PATH, TABLE_NAME)