import requests
import pandas as pd
from datetime import datetime, timedelta
import json

BASE_URL = "https://www.flightstats.com/v2/api-next/flight-tracker/arr/CCU/{year}/{month}/{day}/{hour}"
HOURS = [0, 6, 12, 18]

# FlightStats only serves data for roughly 3 days before departure
# through ~7 days after arrival -- use a recent window, not a fixed past date.
end_date = datetime.now()
start_date = end_date - timedelta(days=6)

all_flights = []
raw_json_store = []
debug_dumped = False
current_date = start_date

while current_date <= end_date:
    year, month, day = current_date.year, current_date.month, current_date.day
    for hour in HOURS:
        url = BASE_URL.format(year=year, month=month, day=day, hour=hour)
        params = {"carrierCode": "", "numHours": 6}
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            response = requests.get(url, params=params, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                print(f"API Error on {current_date:%Y-%m-%d} hour {hour}: {data['error'].get('message')}")
                continue

            raw_json_store.append({"date": current_date.strftime("%Y-%m-%d"), "hour_block": hour, "response": data})
            flights = data.get("data", {}).get("flights", [])

            # One-time dump so you can see the real field names for status/delay/cancel
            if flights and not debug_dumped:
                print("---- SAMPLE FLIGHT RECORD (first one found) ----")
                print(json.dumps(flights[0], indent=2)[:3000])
                print("-------------------------------------------------")
                debug_dumped = True

            for f in flights:
                if f.get("isCodeshare"):
                    continue

                status_block = f.get("status", {}) or {}
                record = {
                    "date": current_date.strftime("%Y-%m-%d"),
                    "hour_block": hour,
                    "flight_number": f.get("carrier", {}).get("flightNumber"),
                    "airline": f.get("carrier", {}).get("name"),
                    "origin_city": f.get("airport", {}).get("city"),
                    "origin_airport_code": f.get("airport", {}).get("fs"),
                    "departure_time": f.get("departureTime", {}).get("time24"),
                    "arrival_time": f.get("arrivalTime", {}).get("time24"),
                    # Best-guess status fields -- confirm/adjust against the debug dump above
                    "status_code": status_block.get("statusCode") or f.get("statusCode"),
                    "status_name": status_block.get("status") or f.get("statusName"),
                    "delay_minutes": (status_block.get("delay", {}) or {}).get("arrival", {}).get("minutes"),
                }
                all_flights.append(record)
        except requests.exceptions.HTTPError as e:
            # Print response body too -- helps tell "date out of range" from a real bug
            body = getattr(e.response, "text", "")[:200]
            print(f"HTTP error on {current_date:%Y-%m-%d} hour {hour}: {e} | body: {body}")
        except Exception as e:
            print(f"Error on {current_date:%Y-%m-%d} hour {hour}: {e}")

    current_date += timedelta(days=1)

df = pd.DataFrame(all_flights)
if not df.empty:
    df = df.drop_duplicates()

    # Filter to cancel/delay/divert once you've confirmed the real status field names
    status_col = df["status_name"].astype(str).str.lower()
    irregular = df[
        status_col.str.contains("cancel")
        | status_col.str.contains("divert")
        | (df["delay_minutes"].fillna(0) > 0)
    ]
    irregular.to_csv("ccu_irregular_flights.csv", index=False)
    df.to_csv("ccu_arrivals_all.csv", index=False)
    print(f"Saved {len(df)} total, {len(irregular)} cancel/delay/divert records.")
else:
    print("No flight records retrieved. CSV was not saved.")