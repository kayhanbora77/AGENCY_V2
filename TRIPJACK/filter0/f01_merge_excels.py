import os
import glob
import uuid
import pandas as pd

INPUT_FOLDER = r"C:\Users\cagri\Desktop\Agency_Data\Tripjack\filter0"
OUTPUT_CSV = r"C:\Users\cagri\Desktop\Agency_Data\Tripjack\filter0\TRIPJACK_ALL.csv"

DATE_COLUMNS = [
    "DepartureDateLocal1",
    "DepartureDateLocal2",
    "DepartureDateLocal3",
    "DepartureDateLocal4",
    "DepartureDateLocal5",
]


def parse_date_column(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    parsed = pd.to_datetime(s, format="%d %B, %Y", errors="coerce")
    mask = parsed.isna() & (s != "") & (s.str.lower() != "nan")
    if mask.any():
        parsed.loc[mask] = pd.to_datetime(s[mask], errors="coerce")
    return parsed.dt.strftime("%Y-%m-%d").fillna("")


excel_files = glob.glob(os.path.join(INPUT_FOLDER, "*.xlsx"))
excel_files += glob.glob(os.path.join(INPUT_FOLDER, "*.xls"))
print(f"Found {len(excel_files)} Excel files")

# =====================================================
# PASS 1: scan headers only (nrows=0) to build one master
# column list, preserving first-seen order across all
# files/sheets. This is what guarantees every appended
# chunk lines up under the same header later.
# =====================================================
master_cols = []
seen = set()

for file in excel_files:
    sheets = pd.read_excel(file, sheet_name=None, dtype=str, nrows=0)
    for sheet_name, df in sheets.items():
        for c in df.columns:
            if c not in seen:
                seen.add(c)
                master_cols.append(c)

for c in DATE_COLUMNS:
    if c not in seen:
        master_cols.append(c)
        seen.add(c)

# Final on-disk column order: Id, all source columns, SourceFile, SourceSheet
final_cols = ["Id"] + master_cols + ["SourceFile", "SourceSheet"]
print(f"Master schema has {len(master_cols)} source columns")

# =====================================================
# PASS 2: real read + write, every chunk reindexed to
# final_cols so columns can never silently shift.
# =====================================================
first_write = True
total_rows = 0

if os.path.exists(OUTPUT_CSV):
    os.remove(OUTPUT_CSV)

for file in excel_files:
    print(f"Reading {os.path.basename(file)}")
    sheets = pd.read_excel(file, sheet_name=None, dtype=str)

    for sheet_name, df in sheets.items():
        if df.empty:
            continue

        df = df.fillna("")

        for col in DATE_COLUMNS:
            if col in df.columns:
                df[col] = parse_date_column(df[col])

        df["Id"] = [str(uuid.uuid4()) for _ in range(len(df))]
        df["SourceFile"] = os.path.basename(file)
        df["SourceSheet"] = sheet_name

        # Force every chunk onto the identical column set/order.
        # Any column this sheet doesn't have becomes "" (not shifted).
        df = df.reindex(columns=final_cols, fill_value="")

        df.to_csv(
            OUTPUT_CSV,
            mode="a",
            index=False,
            header=first_write,
            encoding="utf-8-sig" if first_write else "utf-8",  # BOM once only
        )
        first_write = False
        total_rows += len(df)

        print(f"   {sheet_name}: {len(df):,} rows (running total: {total_rows:,})")

print(f"\nFinished!")
print(f"Rows : {total_rows:,}")
print(f"CSV  : {OUTPUT_CSV}")