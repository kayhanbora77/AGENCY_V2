"""
merge_excel.py
--------------
Merges multiple Excel (.xlsx, .xls) and CSV files — including all sheets —
into a single output CSV file. No row limit.

Temp/lock files (~$filename.xlsx) are automatically skipped.

Configuration (edit the block below):
    SOURCE_FOLDER    : folder containing the Excel/CSV files to merge
    OUTPUT_FILE      : output CSV path (always .csv)
    ADD_SOURCE_COLS  : True = add _SourceFile and _SourceSheet tracking columns

Usage:
    python merge_excel.py

Optional CLI overrides:
    python merge_excel.py --folder C:/data --output result.csv
    python merge_excel.py --no-source-cols
"""

import os
import sys
import glob
import argparse
import pandas as pd

# ── CONFIGURATION ────────────────────────────────────────────────────────────
SOURCE_FOLDER = r"C:\Users\cagri\Desktop\Agency\Riya_USA\filter-0"
OUTPUT_FILE = r"C:\Users\cagri\Desktop\Agency\Riya_USA\filter-0\merged_Riya_USA.csv"
ADD_SOURCE_COLS = True
# ─────────────────────────────────────────────────────────────────────────────


def read_file(filepath):
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(filepath, dtype=str).fillna("")
        df.columns = [c.strip() for c in df.columns]
        return {os.path.basename(filepath): df}
    else:
        sheets = pd.read_excel(filepath, sheet_name=None, dtype=str)
        cleaned = {}
        for name, df in sheets.items():
            df = df.fillna("")
            df.columns = [c.strip() for c in df.columns]
            cleaned[name] = df
        return cleaned


def merge(input_files, output_path, add_source_cols=True):
    # Ensure output is always .csv
    output_path = os.path.splitext(output_path)[0] + ".csv"

    all_frames = []
    total_rows = 0
    total_sheets_read = 0

    for filepath in input_files:
        fname = os.path.basename(filepath)
        try:
            sheets = read_file(filepath)
        except Exception as e:
            print(f"  [SKIP] {fname}: {e}")
            continue

        for sheet_name, df in sheets.items():
            if df.empty:
                continue
            if add_source_cols:
                df = df.copy()
                df["_SourceFile"] = fname
                df["_SourceSheet"] = sheet_name
            all_frames.append(df)
            total_rows += len(df)
            total_sheets_read += 1
            print(f"  {fname} › {sheet_name}: {len(df):,} rows")

    if not all_frames:
        print("No data found. Nothing to merge.")
        return

    print(f"\nTotal rows: {total_rows:,}")
    print(f"Writing {output_path} ...")

    merged = pd.concat(all_frames, ignore_index=True)
    merged.to_csv(output_path, index=False, encoding="utf-8-sig")

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(
        f"\nDone: {total_rows:,} rows from {len(input_files)} file(s), "
        f"{total_sheets_read} source sheet(s)"
    )
    print(f"File size : {size_mb:.1f} MB")
    print(f"Output    : {os.path.abspath(output_path)}")


def collect_files(folder):
    exts = ("*.xlsx", "*.xls", "*.csv")
    found = []
    for ext in exts:
        found.extend(glob.glob(os.path.join(folder, ext)))
    found = [f for f in found if not os.path.basename(f).startswith("~$")]
    return sorted(set(found))


def main():
    parser = argparse.ArgumentParser(
        description="Merge multiple Excel/CSV files into one CSV."
    )
    parser.add_argument("files", nargs="*", help="Specific files to merge")
    parser.add_argument(
        "--folder", default=None, help="Folder to scan (overrides SOURCE_FOLDER)"
    )
    parser.add_argument(
        "--output", default=None, help="Output CSV path (overrides OUTPUT_FILE)"
    )
    parser.add_argument(
        "--no-source-cols",
        action="store_true",
        help="Do not add _SourceFile / _SourceSheet columns",
    )
    args = parser.parse_args()

    folder = args.folder or SOURCE_FOLDER
    output_path = args.output or OUTPUT_FILE
    src_cols = (not args.no_source_cols) and ADD_SOURCE_COLS

    if args.files:
        input_files = [f for f in args.files if os.path.isfile(f)]
    else:
        if not os.path.isdir(folder):
            print(f"ERROR: Source folder not found: {folder}")
            print("Edit SOURCE_FOLDER at the top of the script, or pass --folder PATH")
            sys.exit(1)
        input_files = collect_files(folder)

    if not input_files:
        print(f"No Excel/CSV files found in: {folder}")
        sys.exit(1)

    print(f"Source folder : {os.path.abspath(folder)}")
    print(f"Found {len(input_files)} file(s) to merge:\n")
    for f in input_files:
        print(f"  {os.path.basename(f)}")
    print()

    merge(input_files, output_path=output_path, add_source_cols=src_cols)


if __name__ == "__main__":
    main()
