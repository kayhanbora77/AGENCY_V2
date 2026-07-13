import duckdb

CSV_PATH = r"C:\Users\cagri\Desktop\Agency_Data\Tripjack\filter0\TRIPJACK_ALL.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
con = duckdb.connect(DB_PATH)

# Actual row count in the CSV on disk, read by DuckDB (not Excel)
total = con.execute(f"""
    SELECT COUNT(*) FROM read_csv_auto('{CSV_PATH}')
""").fetchone()[0]
print(f"Total rows in CSV (via DuckDB): {total:,}")

# Break it down by source file to see if one file is short
by_file = con.execute(f"""
    SELECT SourceFile, COUNT(*) AS rows
    FROM read_csv_auto('{CSV_PATH}')
    GROUP BY SourceFile
    ORDER BY rows DESC
""").df()
print(by_file.to_string(index=False))

con.close()