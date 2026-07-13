import duckdb

CSV_PATH = r"C:\Users\cagri\Desktop\Agency_Data\Tripjack\filter0\TRIPJACK_ALL.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "TRIPJACK_RAW"

DATE_COLUMNS = [
    "DepartureDateLocal1",
    "DepartureDateLocal2",
    "DepartureDateLocal3",
    "DepartureDateLocal4",
    "DepartureDateLocal5",
]

con = duckdb.connect(DB_PATH)

# Discover column names/order without loading data (LIMIT 0)
cols = con.execute(
    f"SELECT * FROM read_csv_auto('{CSV_PATH}', SAMPLE_SIZE=1000) LIMIT 0"
).df().columns.tolist()

# Everything defaults to VARCHAR (matches how it was written),
# except the 5 date columns which get typed as DATE.
dtype_map = {c: ("DATE" if c in DATE_COLUMNS else "VARCHAR") for c in cols}
dtype_struct = ", ".join(f"'{col}': '{typ}'" for col, typ in dtype_map.items())

con.execute(f"""
    CREATE OR REPLACE TABLE {TABLE_NAME} AS
    SELECT * FROM read_csv(
        '{CSV_PATH}',
        header    = True,
        columns   = {{{dtype_struct}}},
        nullstr   = '',
        quote     = '"',
        escape    = '"'
    )
""")

row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
print(f"Loaded {row_count:,} rows into '{TABLE_NAME}' at {DB_PATH}")

con.close()