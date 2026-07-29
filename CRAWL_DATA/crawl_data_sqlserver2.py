import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time, random, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import pyodbc

HEADERS = {"User-Agent": "Mozilla/5.0"}
HOURS = [0, 6, 12, 18]

CANADA_AIRPORTS = [
    "YYC", "YEG", "YFC", "YQX", "YHZ", "YQM", "YQB", "YUL", "YOW", "YYT", "YYZ", "YVR", "YWG"
]

# ---- concurrency / rate-limit tuning knobs ----
AIRPORT_WORKERS = 6      # how many airports processed in parallel
STATUS_WORKERS = 20      # how many flight-status lookups in flight at once, per airport-day
MAX_REQUESTS_PER_SEC = 12  # global cap across ALL threads -- start conservative, raise if you don't see 403s
STOP_AFTER_N_FAILURES = 6  # per-thread circuit breaker (was global before; now local so one bad airport doesn't kill the run)


def canada_airports():
    return CANADA_AIRPORTS


def eu_airports(conn):
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


# ---------------------------------------------------------------------------
# Token-bucket rate limiter shared across every thread. This replaces the old
# time.sleep(2) / time.sleep(random.uniform(0.8, 1.5)) calls. Instead of each
# thread pacing itself blindly, every thread pulls a "token" from the same
# bucket before making a request, so the TOTAL request rate across the whole
# program is capped at MAX_REQUESTS_PER_SEC -- not the per-thread rate.
# ---------------------------------------------------------------------------
class TokenBucket:
    def __init__(self, rate_per_sec):
        self.rate = rate_per_sec
        self.tokens = rate_per_sec
        self.lock = threading.Lock()
        self.last = time.monotonic()

    def acquire(self):
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last
                self.last = now
                self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / self.rate
            time.sleep(wait)


RATE_LIMITER = TokenBucket(MAX_REQUESTS_PER_SEC)

# One requests.Session per thread so TCP connections get reused (keep-alive)
# instead of every single request paying a fresh connection-setup cost.
_thread_local = threading.local()


def get_session():
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        _thread_local.session.headers.update(HEADERS)
    return _thread_local.session


def request_with_backoff(url, params=None, max_retries=2):
    session = get_session()
    for attempt in range(max_retries):
        RATE_LIMITER.acquire()
        r = session.get(url, params=params, timeout=15)
        if r.status_code == 403:
            wait = 30 * (attempt + 1)
            print(f"  403 on {url} -- backing off {wait}s (attempt {attempt+1}/{max_retries})")
            time.sleep(wait)
            continue
        if r.status_code in (500, 502, 503, 504):
            print(f"  {r.status_code} on {url} -- skipping, no retry")
            return None
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
    if status_data is None:
        return None
    record = extract_record(base, status_data)
    record["departure_airport_iatacode"] = target_airport
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

# pyodbc connections are NOT safe for concurrent use from multiple threads.
# Since airports are now processed in parallel, guard every DB write with a lock.
DB_LOCK = threading.Lock()


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

    with DB_LOCK:
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


def process_airport_day(conn, airport, current_date):
    """Everything needed for one airport on one day. Runs inside the outer
    airport-level thread pool. Internally fans out the flight-status lookups
    across STATUS_WORKERS threads."""
    airport_start = time.monotonic()
    # fetch all 4 hour-blocks concurrently instead of one-by-one with sleep(2) between
    day_flights = []
    with ThreadPoolExecutor(max_workers=len(HOURS)) as hour_pool:
        futures = {hour_pool.submit(fetch_departures, airport, current_date, hour): hour for hour in HOURS}
        for fut in as_completed(futures):
            hour = futures[fut]
            try:
                day_flights.extend(fut.result())
            except Exception as e:
                print(f"List error {airport} {current_date:%Y-%m-%d} hour {hour}: {e}")

    to_process, seen_today = [], set()
    for f in day_flights:
        carrier = f.get("carrier", {})
        key = (carrier.get("fs"), carrier.get("flightNumber"))
        if key in seen_today or not all(key):
            continue
        seen_today.add(key)
        to_process.append(f)

    print(f"{airport} {current_date:%Y-%m-%d}: {len(to_process)} unique flights to check")

    if not to_process:
        elapsed = time.monotonic() - airport_start
        return 0, 0, elapsed

    day_records = []
    consecutive_failures = 0
    stopped_early = False

    with ThreadPoolExecutor(max_workers=STATUS_WORKERS) as status_pool:
        futures = {status_pool.submit(process_flight, f, current_date, airport): f for f in to_process}
        done_count = 0
        for fut in as_completed(futures):
            done_count += 1
            try:
                rec = fut.result()
                if rec:
                    day_records.append(rec)
                consecutive_failures = 0
            except Exception as e:
                print(f"  Status error at {airport} {current_date:%Y-%m-%d}: {e}")
                consecutive_failures += 1
                if consecutive_failures >= STOP_AFTER_N_FAILURES:
                    print(f"  Hit {STOP_AFTER_N_FAILURES} consecutive failures on {airport} -- "
                          f"stopping this airport's day early (other airports keep going).")
                    stopped_early = True
                    break
            if done_count % 25 == 0 or done_count == len(to_process):
                print(f"  ...{done_count}/{len(to_process)} flights checked ({airport} {current_date:%Y-%m-%d})")

    n, ni = save_delaycanceldivert(conn, day_records)
    if stopped_early:
        print(f"  (partial results saved for {airport} {current_date:%Y-%m-%d} due to early stop)")

    elapsed = time.monotonic() - airport_start
    print(f"  [{airport} {current_date:%Y-%m-%d}] done in {elapsed:.1f}s "
          f"({len(to_process)} flights, {elapsed / max(len(to_process), 1):.2f}s/flight)")
    return n, ni, elapsed


def _format_duration(seconds):
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def main():
    run_start = time.monotonic()
    run_start_wall = datetime.now()

    conn = get_sqlserver_connection()
    airports = eu_airports(conn) + canada_airports()
    print(f"Loaded {len(airports)} airports: {airports[:10]}{'...' if len(airports) > 10 else ''}")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)  # keep narrow while testing

    # Flatten to (airport, date) work units so airports run in parallel too.
    tasks = []
    for airport in airports:
        current_date = start_date
        while current_date < end_date:
            tasks.append((airport, current_date))
            current_date += timedelta(days=1)

    total_flights, total_irregular = 0, 0
    airport_timings = []  # (airport, date, elapsed_seconds) -- for the slowest-airports report at the end
    failed = []

    with ThreadPoolExecutor(max_workers=AIRPORT_WORKERS) as airport_pool:
        futures = {
            airport_pool.submit(process_airport_day, conn, airport, date): (airport, date)
            for airport, date in tasks
        }
        for fut in as_completed(futures):
            airport, date = futures[fut]
            try:
                n, ni, elapsed = fut.result()
                total_flights += n
                total_irregular += ni
                airport_timings.append((airport, date, elapsed))
            except Exception as e:
                print(f"Airport-day failed entirely: {airport} {date:%Y-%m-%d}: {e}")
                failed.append((airport, date))

    conn.close()

    total_elapsed = time.monotonic() - run_start
    run_end_wall = datetime.now()

    print("\n" + "=" * 60)
    print("RUN SUMMARY")
    print("=" * 60)
    print(f"Started:  {run_start_wall:%Y-%m-%d %H:%M:%S}")
    print(f"Finished: {run_end_wall:%Y-%m-%d %H:%M:%S}")
    print(f"Total execution time: {_format_duration(total_elapsed)} ({total_elapsed:.1f}s)")
    print(f"Airport-days processed: {len(airport_timings)} / {len(tasks)}  (failed: {len(failed)})")
    print(f"Flights inserted (total / irregular): {total_flights} / {total_irregular}")

    if airport_timings:
        avg_per_airport_day = sum(t for _, _, t in airport_timings) / len(airport_timings)
        print(f"Avg time per airport-day: {avg_per_airport_day:.1f}s")
        slowest = sorted(airport_timings, key=lambda x: x[2], reverse=True)[:5]
        print("Slowest airport-days:")
        for airport, date, elapsed in slowest:
            print(f"  {airport} {date:%Y-%m-%d}: {elapsed:.1f}s")

    if failed:
        print("Failed airport-days:")
        for airport, date in failed:
            print(f"  {airport} {date:%Y-%m-%d}")
    print("=" * 60)

    print(f"Done. Inserted {total_flights} flights, {total_irregular} irregular into SQL Server.")


if __name__ == "__main__":
    main()