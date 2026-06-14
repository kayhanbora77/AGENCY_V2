import pandas as pd

# 1. Read both Excel Binary (.xlsb) files using the pyxlsb engine
df1 = pd.read_excel(
    r"C:\Users\cagri\Desktop\Agency_Data\ThomasCook\filter-0\SEP_25-MAR_26\set1.xlsb",
    engine="pyxlsb",
)
df2 = pd.read_excel(
    r"C:\Users\cagri\Desktop\Agency_Data\ThomasCook\filter-0\SEP_25-MAR_26\set2.xlsb",
    engine="pyxlsb",
)

# 2. Combine the dataframes vertically
combined_df = pd.concat([df1, df2], ignore_index=True)

# 3. Export the combined data to a single CSV file
combined_df.to_csv(
    r"C:\Users\cagri\Desktop\Agency_Data\ThomasCook\filter-0\SEP_25-MAR_26\ThomasCook.csv",
    index=False,
)

print("CSV file created successfully!")
