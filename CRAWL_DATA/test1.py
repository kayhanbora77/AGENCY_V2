import requests, json

data = requests.get(
    "https://www.flightstats.com/v2/api-next/flight-tracker/6E/389/2026/7/23",
    headers={"User-Agent": "Mozilla/5.0"}
).json().get("data", {})

print(json.dumps(data.get("schedule", {}), indent=2))
print(json.dumps(data.get("departureAirport", {}).get("times", {}), indent=2))
print(json.dumps(data.get("status", {}), indent=2))