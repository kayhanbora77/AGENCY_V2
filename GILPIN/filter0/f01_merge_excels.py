import pandas as pd
import numpy as np

def process_excel_to_csv(input_file, output_file):
    # Read all sheets from the Excel file into a dictionary of DataFrames
    excel_data = pd.read_excel(input_file, sheet_name=None)
    
    # --- THE FIX ---
    cleaned_sheets = []
    for sheet_df in excel_data.values():
        # 1. Strip spaces from column names on EACH sheet individually BEFORE combining
        sheet_df.columns = sheet_df.columns.str.strip()
        cleaned_sheets.append(sheet_df)
    
    # 2. Combine all sheets into a single DataFrame
    combined_df = pd.concat(cleaned_sheets, ignore_index=True)
    
    # 3. Drop exact duplicate columns (e.g., if Excel had two 'PNR' columns side-by-side)
    # ~ means "NOT", so this keeps columns that are NOT duplicated
    combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]
    # ---------------

    # Rename columns to standard names to match your rules
    col_mapping = {
        'Doc Date': 'DocDate',
        'Flight No 1': 'FlightNo1', 'Flight Date 1': 'FlightDate1',
        'Flight No 2': 'FlightNo2', 'Flight Date 2': 'FlightDate2',
        'Flight No 3': 'FlightNo3', 'Flight Date 3': 'FlightDate3',
        'Flight No 4': 'FlightNo4', 'Flight Date 4': 'FlightDate4',
        'airlineCode': 'AirlineCode',
        'Airline Code': 'AirlineCode'  
    }
    combined_df.rename(columns=col_mapping, inplace=True)

    # ---------------------------------------------------------
    # RULE 3: Update AirlineCode to first 2 characters
    # ---------------------------------------------------------
    if 'AirlineCode' in combined_df.columns:
        combined_df['AirlineCode'] = combined_df['AirlineCode'].astype(str).str.strip().str[:2]
        combined_df['AirlineCode'] = combined_df['AirlineCode'].replace('nan', '')
    else:
        combined_df['AirlineCode'] = ''

    # ---------------------------------------------------------
    # RULE 5: Format FlightNo1 to FlightNo4 (FIXED .0 ISSUE)
    # ---------------------------------------------------------
    def format_flightno(row, col_name):
        # Safely get code, ensuring it's a string
        code = str(row['AirlineCode']).strip()
        if code.lower() == 'nan' or code == '':
            code = ''
        else:
            code = code[:2] 
            
        # Safely get flight number, ensuring it's a string
        fn = str(row[col_name]).strip() if col_name in row.index else ''
        
        if fn == '' or fn.lower() == 'nan':
            return ''
        
        # --- THE FIX ---
        # If pandas read the Excel number as a float (e.g., '2439.0'), 
        # convert it to a float, then to an int, then back to string.
        # This strips the '.0' safely.
        try:
            fn = str(int(float(fn)))
        except ValueError:
            pass # If it fails, it means it wasn't a number, so leave it as is
        # ---------------

        # Drop leading zeros
        fn = fn.lstrip('0')
        
        # If it was all zeros (edge case), keep one zero
        if fn == '':
            fn = '0'
            
        return code + fn

    for i in range(1, 5):
        col = f'FlightNo{i}'
        if col in combined_df.columns:
            combined_df[col] = combined_df.apply(lambda row: format_flightno(row, col), axis=1)

    # ---------------------------------------------------------
    # RULE 2: Change Date Formats to yyyy-mm-dd
    # ---------------------------------------------------------
    date_cols = ['DocDate', 'FlightDate1', 'FlightDate2', 'FlightDate3', 'FlightDate4']
    for col in date_cols:
        if col in combined_df.columns:
            combined_df[col] = pd.to_datetime(combined_df[col], dayfirst=True, errors='coerce')
            combined_df[col] = combined_df[col].dt.strftime('%Y-%m-%d')
            combined_df[col] = combined_df[col].fillna('')

    # ---------------------------------------------------------
    # RULE 4: Split Sector into Airport1, Airport2, etc.
    # ---------------------------------------------------------
    if 'Sector' in combined_df.columns:
        sectors_split = combined_df['Sector'].astype(str).str.split('/')
        max_airports = sectors_split.apply(len).max()
        
        for i in range(max_airports):
            airport_col = f'Airport{i+1}'
            combined_df[airport_col] = sectors_split.apply(lambda x: x[i].strip() if i < len(x) and x[i].lower() != 'nan' else '')

    # ---------------------------------------------------------
    # RULE 1: Save to CSV
    # ---------------------------------------------------------
    combined_df.to_csv(output_file, index=False)
    print(f"Successfully processed and saved to {output_file}")

if __name__ == "__main__":
    INPUT_EXCEL = r"C:\Users\cagri\Desktop\Agency_Data\GULPIN\filter0\GILPIN.xlsx"  
    OUTPUT_CSV = r"C:\Users\cagri\Desktop\Agency_Data\GULPIN\filter0\GILPIN_MERGED.csv"
    
    process_excel_to_csv(INPUT_EXCEL, OUTPUT_CSV)