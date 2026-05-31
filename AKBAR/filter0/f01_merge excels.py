"""
merge_excel.py (Optimized for 7M+ rows)
----------------------------------------
Streams data to disk incrementally. Never loads full dataset into RAM.
Vectorized date parsing replaces slow .apply() loops.
"""

import os
import sys
import glob
import argparse
import pandas as pd
import gc
import warnings

# Suppress openpyxl date serial warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ── CONFIGURATION ────────────────────────────────────────────────────────────
SOURCE_FOLDER = r"C:\Users\cagri\Desktop\Agency\Akbar\filter-0"
OUTPUT_FILE = r"C:\Users\cagri\Desktop\Agency\Akbar\filter-0\merged_Akbar.csv"
ADD_SOURCE_COLS = True

# Hard-coded date columns from your sample data
DATE_COLUMNS = ["DAIS", "FlightDate", "FirstSectordate", "LastSectordate"]
# ─────────────────────────────────────────────────────────────────────────────


def fast_date_optimization(df, date_cols):
    """Vectorized date parsing: 10-50x faster than .apply()"""
    for col in date_cols:
        if col in df.columns:
            # Primary format: 05-Jan-2020
            converted = pd.to_datetime(df[col], format="%d-%b-%Y", errors="coerce")

            # Fallback for 2-digit years: 05-Jan-20
            mask = converted.isna() & df[col].ne("")
            if mask.any():
                converted[mask] = pd.to_datetime(
                    df[col][mask], format="%d-%b-%y", errors="coerce"
                )

            # Convert to YYYY-MM-DD string. Preserves original text for complex/multi-date cells.
            df[col] = converted.dt.strftime("%Y-%m-%d").fillna(df[col])
    return df


def process_and_write(filepath, output_path, add_source_cols, date_cols, first_write):
    """Read one file, optimize dates, append to CSV, free memory."""
    ext = os.path.splitext(filepath)[1].lower()
    fname = os.path.basename(filepath)

    if ext == ".csv":
        df = pd.read_csv(filepath, dtype=str, low_memory=False).fillna("")
        df.columns = [c.strip() for c in df.columns]
        sheets = {fname: df}
    else:
        # Read all sheets as strings to prevent Excel auto-conversion corruption
        sheets = pd.read_excel(filepath, sheet_name=None, dtype=str, engine="openpyxl")
        cleaned = {}
        for name, sheet_df in sheets.items():
            sheet_df = sheet_df.fillna("")
            sheet_df.columns = [c.strip() for c in sheet_df.columns]
            cleaned[name] = sheet_df
        sheets = cleaned

    rows_processed = 0
    for sheet_name, df in sheets.items():
        if df.empty:
            continue

        if add_source_cols:
            df["_SourceFile"] = fname
            df["_SourceSheet"] = sheet_name

        if date_cols:
            df = fast_date_optimization(df, date_cols)

        # Stream to disk: header only on first chunk
        df.to_csv(
            output_path, mode="a", header=first_write, index=False, encoding="utf-8-sig"
        )
        first_write = False
        rows_processed += len(df)

    # Force memory release before next file
    del sheets, df
    gc.collect()
    return rows_processed, first_write


def merge(input_files, output_path, add_source_cols=True, date_cols=None):
    output_path = os.path.splitext(output_path)[0] + ".csv"

    # Prevent appending to previous runs
    if os.path.exists(output_path):
        os.remove(output_path)

    total_rows = 0
    first_write = True
    files_processed = 0

    for filepath in input_files:
        try:
            rows, first_write = process_and_write(
                filepath, output_path, add_source_cols, date_cols, first_write
            )
            total_rows += rows
            files_processed += 1
            print(f"  ✓ {os.path.basename(filepath)} (+{rows:,} rows)")
        except Exception as e:
            print(f"  ✗ SKIP {os.path.basename(filepath)}: {e}")

    size_mb = (
        os.path.getsize(output_path) / (1024 * 1024)
        if os.path.exists(output_path)
        else 0
    )
    print("\n✅ Merge Complete")
    print(f"   Total Rows : {total_rows:,}")
    print(f"   Files      : {files_processed}")
    print(f"   Output Size: {size_mb:.1f} MB")
    print(f"   Path       : {os.path.abspath(output_path)}")


def collect_files(folder):
    exts = ("*.xlsx", "*.xls", "*.csv")
    found = []
    for ext in exts:
        found.extend(glob.glob(os.path.join(folder, ext)))
    return sorted(set(f for f in found if not os.path.basename(f).startswith("~$")))


def main():
    parser = argparse.ArgumentParser(
        description="Merge Excel/CSV files (streaming mode for large datasets)."
    )
    parser.add_argument("--folder", default=None, help="Override SOURCE_FOLDER")
    parser.add_argument("--output", default=None, help="Override OUTPUT_FILE")
    parser.add_argument(
        "--no-source-cols", action="store_true", help="Skip tracking columns"
    )
    args = parser.parse_args()

    folder = args.folder or SOURCE_FOLDER
    output_path = args.output or OUTPUT_FILE
    src_cols = (not args.no_source_cols) and ADD_SOURCE_COLS

    if not os.path.isdir(folder):
        print(f"ERROR: Source folder not found: {folder}")
        sys.exit(1)

    input_files = collect_files(folder)
    if not input_files:
        print(f"No Excel/CSV files found in: {folder}")
        sys.exit(1)

    print(f" Source   : {os.path.abspath(folder)}")
    print(f"📄 Files    : {len(input_files)}")
    print(f"📅 Dates    : {', '.join(DATE_COLUMNS) if DATE_COLUMNS else 'None'}")
    print("💾 Mode     : Streaming (low memory)\n")

    merge(input_files, output_path, add_source_cols=src_cols, date_cols=DATE_COLUMNS)


if __name__ == "__main__":
    main()
