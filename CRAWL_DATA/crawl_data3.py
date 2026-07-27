import requests
import pandas as pd
from datetime import datetime, timedelta
import time, random, os

HEADERS = {"User-Agent": "Mozilla/5.0"}
AIRPORT = "CCU"
HOURS = [0, 6, 12, 18]
OUT_FILE = "ccu_arrivals_full.csv"

def request_with_backoff(url, params=None, max_retries=4):
    for attempt in range(max_retries):
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        if r.status_code == 403:
            wait = 30 * (attempt + 1)  # 30s, 60s, 90s, 120s
            print(f"  403 on {url} -- backing off {wait}s (attempt {attempt+1}/{max_retries})")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json().get("data", {})
    raise RuntimeError(f"Repeated 403s, giving up on {url}")

def fetch_arrivals(date, hour):
    url = f"https://www.flightstats.com/v2/api-next/flight-tracker/arr/{AIRPORT}/{date.year}/{date.month}/{date.day}/{hour}"
    return request_with_backoff(url, {"carrierCode": "", "numHours": 6}).get("flights", [])

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
        "departure_airport_code": status_data.get("departureAirport", {}).get("fs"),
        "arrival_airport_code": status_data.get("arrivalAirport", {}).get("fs"),
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

def process_flight(f, date):
    carrier = f.get("carrier", {})
    fs, num = carrier.get("fs"), carrier.get("flightNumber")
    if f.get("isCodeshare") or not fs or not num:
        return None
    base = {
        "date": date.strftime("%Y-%m-%d"),
        "flight_number": num,
        "carrier_code": fs,
        "airline": carrier.get("name"),
        "origin_city": f.get("airport", {}).get("city"),
        "origin_airport_code": f.get("airport", {}).get("fs"),
    }
    status_data = fetch_status(fs, num, date)
    return extract_record(base, status_data)

def save_incremental(records):
    if not records:
        return
    df_new = pd.DataFrame(records)
    if os.path.exists(OUT_FILE):
        df_new.to_csv(OUT_FILE, mode="a", header=False, index=False)
    else:
        df_new.to_csv(OUT_FILE, index=False)

# ---- run ----
end_date = datetime.now()
start_date = end_date - timedelta(days=1)

seen_keys = set()
current_date = start_date
consecutive_failures = 0
STOP_AFTER_N_FAILURES = 8  # circuit breaker

while current_date < end_date:
    day_flights = []
    for hour in HOURS:
        try:
            day_flights.extend(fetch_arrivals(current_date, hour))
            consecutive_failures = 0
        except Exception as e:
            print(f"List error {current_date:%Y-%m-%d} hour {hour}: {e}")
            consecutive_failures += 1
        time.sleep(2)  # much gentler pacing between list calls

    to_process, seen_today = [], set()
    for f in day_flights:
        carrier = f.get("carrier", {})
        key = (carrier.get("fs"), carrier.get("flightNumber"))
        if key in seen_today or not all(key):
            continue
        seen_today.add(key)
        to_process.append(f)

    print(f"{current_date:%Y-%m-%d}: {len(to_process)} unique flights to check")

    day_records = []
    for f in to_process:
        try:
            rec = process_flight(f, current_date)
            if rec:
                day_records.append(rec)
            consecutive_failures = 0
        except Exception as e:
            print(f"  Status error: {e}")
            consecutive_failures += 1
        time.sleep(random.uniform(0.8, 1.5))  # serial, human-ish pacing

        if consecutive_failures >= STOP_AFTER_N_FAILURES:
            print(f"Hit {STOP_AFTER_N_FAILURES} consecutive failures -- stopping early to avoid a longer block.")
            save_incremental(day_records)
            raise SystemExit("Stopped: likely rate-limited. Re-run later / resume from this date.")

    save_incremental(day_records)
    current_date += timedelta(days=1)

print("Done. Building irregular-flights summary...")
if os.path.exists(OUT_FILE):
    df = pd.read_csv(OUT_FILE)
    if "final_status" in df.columns:
        irregular = df[df["final_status"].astype(str).str.contains("Cancel|Divert|Delay", case=False, na=False)]
        irregular.to_csv("ccu_irregular_flights.csv", index=False)
        print(f"{len(df)} total, {len(irregular)} cancel/delay/divert.")
    else:
        print("No successful status records were collected -- check the errors above.")
else:
    print("No output file was created.")