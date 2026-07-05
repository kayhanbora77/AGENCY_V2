import pandas as pd

# Input files
file1 = r"C:\Users\cagri\Desktop\MMT\MMT_APPROVED\MMT_CANCEL_DIVERT_DELAY.csv"
file2 = r"C:\Users\cagri\Desktop\MMT\MMT_APPROVED\MMT_MissConnection.csv"

df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

merged = pd.concat([df1, df2], ignore_index=True, sort=False)

merged.to_csv(r"C:\Users\cagri\Desktop\MMT\MMT_APPROVED\MMT_CANCEL_DIVERT_DELAY_MISCONNECT.csv", index=False)

print("Done.")
print(f"Rows: {len(merged):,}")
print(f"Columns: {len(merged.columns)}")