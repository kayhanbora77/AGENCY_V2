import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time, random
import pyodbc

HEADERS = {"User-Agent": "Mozilla/5.0"}
HOURS = [0, 6, 12, 18]

CANADA_AIRPORTS = [
    "YYC", "YEG", "YFC", "YQX", "YHZ", "YQM", "YQB", "YUL", "YOW", "YYT", "YYZ", "YVR", "YWG"
]


def canada_airports():
    return CANADA_AIRPORTS


def eu_airports(conn):
    """Reuses the already-open SQL Server connection instead of opening a new one."""
    cur = conn.cursor()
    cur.execute("select * from dbo.eu_airports")
    rows = cur.fetchall()
    return [row[0] for row in rows]


def get_sqlserver_connection():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=AgentDataProcessDB;"
        "UID=C2RFlightDataUser;"
        "PWD=C2RFlightDataServerP0ss!"
    )

    try:
        conn = pyodbc.connect(conn_str)
        print("Connected successfully using SQL Authentication!")
        return conn
    except Exception as e:
        print("Connection failed:", e)
        raise


def request_with_backoff(url, params=None, max_retries=2):
    for attempt in range(max_retries):
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        if r.status_code == 403:
            wait = 30 * (attempt + 1)
            print(f"  403 on {url} -- backing off {wait}s (attempt {attempt+1}/{max_retries})")
            time.sleep(wait)
            continue
        if r.status_code in (500, 502, 503, 504):
            print(f"  {r.status_code} on {url} -- skipping, no retry")
            return None  # signal "skip", don't raise -- caller won't count this as a failure
        r.raise_for_status()
        return r.json().get("data", {})
    raise RuntimeError(f"Repeated failures, giving up on {url}")


def fetch_departures(airport, date, hour):
    url = f"https://www.flightstats.com/v2/api-next/flight-tracker/dep/{airport}/{date.year}/{date.month}/{date.day}/{hour}"
    data = request_with_backoff(url, {"carrierCode": "", "numHours": 6})
    return data.get("flights", []) if data else []


def fetch_status(carrier_fs, flight_number, date):
    url = f"https://www.flightstats.com/v2/api-next/flight-tracker/{carrier_fs}/{flight_number}/{date.year}/{date.month}/{date.day}"
    return request_with_backoff(url)


def extract_record(base, status_data):
    sched = status_data.get("schedule", {})
    st = status_data.get("status", {})
    note = status_data.get("flightNote", {})
    dep_actual = sched.get("estimatedActualDeparture") if sched.get("estimatedActualDepartureTitle") == "Actual" else None
    arr_actual = sched.get("estimatedActualArrival") if sched.get("estimatedActualArrivalTitle") == "Actual" else None
    return {
        **base,
        "departure_airport_iatacode": status_data.get("departureAirport", {}).get("fs"),
        "arrival_airport_iatacode": status_data.get("arrivalAirport", {}).get("fs"),
        "scheduled_departure": sched.get("scheduledDeparture"),
        "actual_departure": dep_actual,
        "scheduled_arrival": sched.get("scheduledArrival"),
        "actual_arrival": arr_actual,
        "status_code": st.get("statusCode"),
        "status": st.get("status"),
        "final_status": st.get("finalStatus"),
        "delay_departure_min": st.get("delay", {}).get("departure", {}).get("minutes"),
        "delay_arrival_min": st.get("delay", {}).get("arrival", {}).get("minutes"),
        "canceled": note.get("canceled"),
        "diverted": st.get("diverted"),
    }


def process_flight(f, date, target_airport):
    carrier = f.get("carrier", {})
    fs, num = carrier.get("fs"), carrier.get("flightNumber")
    if f.get("isCodeshare") or not fs or not num:
        return None
    base = {
        "date": date.strftime("%Y-%m-%d"),
        "flight_number": fs + str(num),
        "carrier_code": fs,
        "airline": carrier.get("name"),
        "destination_airport_iatacode": f.get("airport", {}).get("fs"),
    }
    status_data = fetch_status(fs, num, date)
    if status_data is None:  # skipped due to 500
        return None
    record = extract_record(base, status_data)
    record["departure_airport_iatacode"] = target_airport  # trust the queried airport, not FlightStats' code
    return record


def build_dataframe(records):
    df = pd.DataFrame(records)
    df.rename(columns={
        "date": "FlightDate",
        "flight_number": "FlightNumber",
        "carrier_code": "AirlineCode",
        "departure_airport_iatacode": "DepartureAirport",
        "arrival_airport_iatacode": "ArrivalAirport",
        "scheduled_departure": "ScheduledDeparture",
        "actual_departure": "ActualDeparture",
        "scheduled_arrival": "ScheduledArrival",
        "actual_arrival": "ActualArrival",
        "status_code": "StatusCode",
        "status": "Status",
        "final_status": "FinalStatus",
        "delay_departure_min": "DelayDepartureMin",
        "delay_arrival_min": "DelayArrivalMin",
        "canceled": "IsCanceled",
        "diverted": "IsDiverted",
    }, inplace=True)

    for c in ["ScheduledDeparture", "ActualDeparture", "ScheduledArrival", "ActualArrival"]:
        df[c] = pd.to_datetime(df[c], errors="coerce")

    df["FlightDate"] = pd.to_datetime(df["FlightDate"]).dt.date
    df["IsCanceled"] = df["IsCanceled"].fillna(False).astype(bool)
    df["IsDiverted"] = df["IsDiverted"].fillna(False).astype(bool)
    df["IsDelayed"] = (df["DelayArrivalMin"].fillna(0) > 0) | (df["DelayDepartureMin"].fillna(0) > 0)
    df["DelayInSecond"] = (df["ActualArrival"] - df["ScheduledArrival"]).dt.total_seconds()
    df.loc[df["DelayInSecond"] < 0, "DelayInSecond"] = 0

    return df[[
        "FlightDate", "FlightNumber", "AirlineCode", "DepartureAirport", "ArrivalAirport",
        "ScheduledDeparture", "ActualDeparture", "ScheduledArrival", "ActualArrival",
        "StatusCode", "Status", "FinalStatus", "DelayDepartureMin", "DelayArrivalMin",
        "IsCanceled", "IsDiverted", "IsDelayed", "DelayInSecond",
    ]]


def _to_native(value):
    """pyodbc needs plain python types -- convert numpy/pandas objects and NaT/NaN to None."""
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.to_pydatetime()
    if isinstance(value, (np.floating, float)):
        if pd.isna(value):
            return None
        return float(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if pd.isna(value) if not isinstance(value, (list, dict)) else False:
        return None
    return value


INSERT_SQL = """
    INSERT INTO IRREGULAR_FLIGHTS
    (FlightDate, FlightNumber, AirlineCode, DepartureAirport, ArrivalAirport,
     ScheduledDeparture, ActualDeparture, ScheduledArrival, ActualArrival,
     StatusCode, Status, FinalStatus, DelayDepartureMin, DelayArrivalMin,
     IsCanceled, IsDiverted, IsDelayed, DelayInSecond)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def save_delaycanceldivert(conn, records):
    if not records:
        return 0, 0

    df = build_dataframe(records)
    irregular_df = df[df["IsCanceled"] | df["IsDiverted"] | df["IsDelayed"]]
    n_total = len(df)
    n_irregular = len(irregular_df)

    if n_irregular == 0:
        return n_total, 0

    rows = [
        tuple(_to_native(v) for v in row)
        for row in irregular_df.itertuples(index=False, name=None)
    ]

    cursor = conn.cursor()
    cursor.fast_executemany = True
    try:
        cursor.executemany(INSERT_SQL, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()

    return n_total, n_irregular


# ---- run ----
def main():
    conn = get_sqlserver_connection()
    airports = eu_airports(conn) + canada_airports()
    print(f"Loaded {len(airports)} airports: {airports[:10]}{'...' if len(airports) > 10 else ''}")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)  # keep narrow while testing

    consecutive_failures = 0
    STOP_AFTER_N_FAILURES = 2
    total_flights, total_irregular = 0, 0

    for airport in airports:
        current_date = start_date
        while current_date < end_date:
            day_flights = []
            for hour in HOURS:
                try:
                    day_flights.extend(fetch_departures(airport, current_date, hour))
                    consecutive_failures = 0
                except Exception as e:
                    print(f"List error {airport} {current_date:%Y-%m-%d} hour {hour}: {e}")
                    consecutive_failures += 1
                time.sleep(2)

            to_process, seen_today = [], set()
            for f in day_flights:
                carrier = f.get("carrier", {})
                key = (carrier.get("fs"), carrier.get("flightNumber"))
                if key in seen_today or not all(key):
                    continue
                seen_today.add(key)
                to_process.append(f)

            print(f"{airport} {current_date:%Y-%m-%d}: {len(to_process)} unique flights to check")

            day_records = []
            for i, f in enumerate(to_process, 1):
                try:
                    rec = process_flight(f, current_date, airport)
                    if rec:
                        day_records.append(rec)
                    consecutive_failures = 0
                except Exception as e:
                    print(f"  Status error: {e}")
                    consecutive_failures += 1

                if i % 25 == 0 or i == len(to_process):
                    print(f"  ...{i}/{len(to_process)} flights checked")

                time.sleep(random.uniform(0.8, 1.5))

                if consecutive_failures >= STOP_AFTER_N_FAILURES:
                    print(f"Hit {STOP_AFTER_N_FAILURES} consecutive failures -- stopping early.")
                    n, ni = save_delaycanceldivert(conn, day_records)
                    total_flights += n; total_irregular += ni
                    conn.close()
                    raise SystemExit("Stopped: likely rate-limited. Re-run later / resume.")

            n, ni = save_delaycanceldivert(conn, day_records)
            total_flights += n; total_irregular += ni
            current_date += timedelta(days=1)

    conn.close()
    print(f"Done. Inserted {total_flights} flights, {total_irregular} irregular into SQL Server.")


if __name__ == "__main__":
    main()