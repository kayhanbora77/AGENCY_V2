import csv

file_path = r"C:\Users\cagri\Desktop\Agency\Akbar\filter-0\merged_Akbar.csv"
max_flights = 0

print("Processing file line-by-line...")
with open(file_path, mode="r", encoding="utf-8", errors="ignore") as f:
    # DictReader reads headers automatically and maps columns
    reader = csv.DictReader(f)

    for row in reader:
        flight_no_val = row.get("FlightNo")
        if flight_no_val:
            # .split() without arguments splits by any consecutive whitespace
            flight_count = len(flight_no_val.strip().split())
            if flight_count > max_flights:
                max_flights = flight_count

print(f"Maximum FlightNo count found: {max_flights}")
