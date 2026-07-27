import requests
from datetime import datetime, timedelta

HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_flight_status(carrier_fs, flight_number, year, month, day):
    url = f"https://www.flightstats.com/v2/api-next/flight-tracker/{carrier_fs}/{flight_number}/{year}/{month}/{day}"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json().get("data", {})

def extract_record(data):
    dep = data.get("departureAirport", {}).get("times", {})
    arr = data.get("arrivalAirport", {}).get("times", {})
    st = data.get("status", {})
    note = data.get("flightNote", {})

    dep_est = dep.get("estimatedActual", {})
    arr_est = arr.get("estimatedActual", {})

    return {
        "scheduled_departure": dep.get("scheduled", {}).get("time24"),
        "actual_departure": dep_est.get("time24") if dep_est.get("title") == "Actual" else None,
        "estimated_departure": dep_est.get("time24") if dep_est.get("title") == "Estimated" else None,
        "departure_runway": dep_est.get("runway"),  # True once it's an actual takeoff/runway time

        "scheduled_arrival": arr.get("scheduled", {}).get("time24"),
        "actual_arrival": arr_est.get("time24") if arr_est.get("title") == "Actual" else None,
        "estimated_arrival": arr_est.get("time24") if arr_est.get("title") == "Estimated" else None,
        "arrival_runway": arr_est.get("runway"),

        "status_code": st.get("statusCode"),
        "status": st.get("status"),
        "canceled": note.get("canceled"),
        "diverted": st.get("diverted"),
        "delay_departure_min": st.get("delay", {}).get("departure", {}).get("minutes"),
        "delay_arrival_min": st.get("delay", {}).get("arrival", {}).get("minutes"),
    }

# --- test ---
d = datetime.now() - timedelta(days=1)
list_url = f"https://www.flightstats.com/v2/api-next/flight-tracker/arr/CCU/{d.year}/{d.month}/{d.day}/0"
resp = requests.get(list_url, params={"carrierCode": "", "numHours": 6}, headers=HEADERS, timeout=15)
resp.raise_for_status()
flights = resp.json().get("data", {}).get("flights", [])

for f in flights[:10]:
    carrier = f.get("carrier", {})
    fs, num = carrier.get("fs"), carrier.get("flightNumber")
    if not fs or not num:
        continue
    try:
        data = get_flight_status(fs, num, d.year, d.month, d.day)
        rec = extract_record(data)
        print(f"{fs}{num} -> {rec}")
    except Exception as e:
        print(f"{fs}{num} ERROR: {e}")