"""
Collapse sequential/duplicate FlightNumber values within a journey row.

Rule
----
FlightNumber1..5, DepartureDateLocal1..5, AirportIATACode1..6 describe a
chain of up to 5 legs:
    leg_k : AirportIATACode[k] -> AirportIATACode[k+1]  on FlightNumber[k]
            departing DepartureDateLocal[k]

If FlightNumber[k] == FlightNumber[k+1] (a *sequential* duplicate), the two
legs are really one physical flight recorded twice. For each maximal run of
identical, adjacent FlightNumber values:
    - keep the flight number once
    - keep DepartureDateLocal of the FIRST leg in the run
    - keep AirportIATACode of the run's ORIGIN and the run's FINAL airport
      (any airports strictly inside the run are dropped)

Everything is then compacted left (no gaps) and unused trailing
FlightNumber/DepartureDateLocal/AirportIATACode slots are blanked.
Rows with no adjacent duplicates are returned unchanged.
"""

import numpy as np
import pandas as pd
import duckdb

N_FLIGHTS = 5
N_AIRPORTS = 6
DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "TRIPJACK_SPLIT"
TARGET_TABLE = "TRIPJACK_SPLIT2"


def _collapse_leg_arrays(flight_numbers, dep_dates, airports):
    """
    flight_numbers: list[5], dep_dates: list[5], airports: list[6]
    Empty/NaN entries are treated as "no leg". Returns the same-shaped,
    collapsed & left-compacted lists.
    """
    def is_empty(v):
        return v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == ""

    # number of valid, contiguous legs from the left
    n = 0
    while n < N_FLIGHTS and not is_empty(flight_numbers[n]):
        n += 1

    if n == 0:
        return flight_numbers, dep_dates, airports  # nothing to do

    new_fn, new_dl = [], []
    new_ac = [airports[0]]

    i = 0
    while i < n:
        j = i
        while j + 1 < n and flight_numbers[j + 1] == flight_numbers[i]:
            j += 1
        new_fn.append(flight_numbers[i])
        new_dl.append(dep_dates[i])
        new_ac.append(airports[j + 1])
        i = j + 1

    # pad back out to fixed width
    new_fn += [None] * (N_FLIGHTS - len(new_fn))
    new_dl += [None] * (N_FLIGHTS - len(new_dl))
    new_ac += [None] * (N_AIRPORTS - len(new_ac))
    return new_fn, new_dl, new_ac


def collapse_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vectorized-ish implementation: pulls each column group out as a raw
    NumPy object array and loops in plain Python over rows, writing into
    pre-allocated output arrays. This avoids the per-row Series-construction
    overhead of DataFrame.apply(axis=1), which is ~50x slower at this scale
    (measured: ~46 min projected for 7M rows via .apply vs. <1 min this way).
    """
    fn_cols = [f"FlightNumber{i}" for i in range(1, N_FLIGHTS + 1)]
    dl_cols = [f"DepartureDateLocal{i}" for i in range(1, N_FLIGHTS + 1)]
    ac_cols = [f"AirportIATACode{i}" for i in range(1, N_AIRPORTS + 1)]

    fn_arr = df[fn_cols].to_numpy(dtype=object)
    dl_arr = df[dl_cols].to_numpy(dtype=object)
    ac_arr = df[ac_cols].to_numpy(dtype=object)
    n_rows = len(df)

    out_fn = np.empty_like(fn_arr)
    out_dl = np.empty_like(dl_arr)
    out_ac = np.empty_like(ac_arr)

    for r in range(n_rows):
        new_fn, new_dl, new_ac = _collapse_leg_arrays(fn_arr[r], dl_arr[r], ac_arr[r])
        out_fn[r] = new_fn
        out_dl[r] = new_dl
        out_ac[r] = new_ac

    result = df.copy()
    result[fn_cols] = out_fn
    result[dl_cols] = out_dl
    result[ac_cols] = out_ac
    return result


def collapse_duckdb_table(con: duckdb.DuckDBPyConnection, source_table: str, target_table: str):
    """
    Reads `source_table` from the given DuckDB connection, applies the
    collapse, and writes the result to `target_table` (created/replaced).
    """
    df = con.execute(f"SELECT * FROM {source_table}").df()
    result = collapse_dataframe(df)
    con.register("collapsed_df_tmp", result)
    con.execute(f"CREATE OR REPLACE TABLE {target_table} AS SELECT * FROM collapsed_df_tmp")
    con.unregister("collapsed_df_tmp")


if __name__ == "__main__":
    con = duckdb.connect(DB_PATH)
    collapse_duckdb_table(con, SOURCE_TABLE, TARGET_TABLE)
    con.close()

    