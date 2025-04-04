# --- START OF FILE transform_revised.py ---

import pandas as pd
import numpy as np
import os
import glob
import re # Regular expressions for parsing

# --- Configuration ---
# <<< IMPORTANT: Set this to the directory containing your original CSV files >>>
input_directory = 'C:\\Users\\Asus\\Desktop\\Scrapping\\Prev UnProcessed Data' # Use '.' for current directory or provide the full path

# Directory where cleaned files will be saved (subdirectory within input_directory)
output_directory = os.path.join(input_directory, 'cleaned_data_revised')

# Define the FULL range of years we want in the final output
FINAL_YEARS = list(range(2001, 2023)) # 2001 to 2022 inclusive

# --- Helper Functions ---
def find_header_info(filepath, max_header_rows=10):
    """
    Analyzes the first few rows to find the data start row and year columns.

    Returns:
        tuple: (data_start_row_index, year_cols_map)
        year_cols_map (dict): {year: {'montant_col_idx': idx, 'indice_col_idx': idx}}
        Returns None, None if header info cannot be reliably determined.
    """
    try:
        # Read potential header rows with basic settings
        header_df = pd.read_csv(
            filepath,
            nrows=max_header_rows,
            header=None,
            encoding='utf-8',
            skipinitialspace=True,
            sep=',' # Read as comma-separated initially
        )
    except Exception as e:
        print(f"  Error reading header rows for {os.path.basename(filepath)}: {e}")
        return None, None

    year_cols_map = {}
    data_start_row_index = -1
    year_row_index = -1

    # Find the row that looks like the year header (e.g., contains 2001, 2002...)
    for idx, row in header_df.iterrows():
        years_found = 0
        potential_year_indices = {}
        # Check columns starting from index 3 (after identifiers)
        for col_idx in range(3, len(row)):
            val = str(row[col_idx]).strip()
            if re.fullmatch(r'20[0-2]\d', val): # Match years 2000-2029
                try:
                    year = int(val)
                    if 2000 < year < 2030:
                       potential_year_indices[year] = col_idx
                       years_found += 1
                except ValueError:
                    continue
        # If we found multiple years, assume this is the year header row
        if years_found > 5: # Require finding at least a few years
            year_row_index = idx
            # Find the actual start of data (first row after year header that doesn't look like a header)
            for data_idx in range(year_row_index + 1, max_header_rows):
                 # A simple check: if first column is blank or looks like text ID it might be data
                 first_val = str(header_df.iloc[data_idx, 0]).strip()
                 third_val = str(header_df.iloc[data_idx, 2]).strip()
                 # Check if the 'Envergure' column seems populated (like PT/MT/GT)
                 if first_val == '' or pd.isna(header_df.iloc[data_idx, 0]) or len(third_val) <= 3:
                      data_start_row_index = data_idx
                      break
            break # Stop searching for year row

    if year_row_index == -1 or data_start_row_index == -1:
        print(f"  Warning: Could not reliably determine year header or data start row in {os.path.basename(filepath)}.")
        # Fallback: Use original script's assumption
        return 5, None # Return original data start assumption, but signal map failure

    # Now map years to columns for Montants and Indices
    years_in_header = sorted(potential_year_indices.keys())
    num_years = len(years_in_header)
    montant_indices = list(potential_year_indices.values())

    # Heuristic: Assume Indices start after Montants + potentially one blank/separator column
    first_indice_col_start_guess1 = montant_indices[-1] + 1
    first_indice_col_start_guess2 = montant_indices[-1] + 2 # Check one further

    indice_indices = []

    # Try guess 2 first (accounts for separator)
    if first_indice_col_start_guess2 < header_df.shape[1] - num_years + 1 :
        indice_indices = list(range(first_indice_col_start_guess2, first_indice_col_start_guess2 + num_years))
        # Basic validation: check if number of indice columns matches year count
        if len(indice_indices) != num_years:
             indice_indices = [] # Reset if mismatch

    # If guess 2 failed or seemed wrong, try guess 1
    if not indice_indices and first_indice_col_start_guess1 < header_df.shape[1] - num_years + 1 :
         indice_indices = list(range(first_indice_col_start_guess1, first_indice_col_start_guess1 + num_years))
         if len(indice_indices) != num_years:
              print(f"  Warning: Indice column count mismatch using guess 1 for {os.path.basename(filepath)}. Indice data might be incorrect.")
              indice_indices = [] # Indicate failure

    if not indice_indices:
        print(f"  Warning: Could not determine indice column indices for {os.path.basename(filepath)}. Proceeding without indice mapping.")

    # Build the map
    for i, year in enumerate(years_in_header):
        year_info = {'montant_col_idx': montant_indices[i]}
        if i < len(indice_indices):
            year_info['indice_col_idx'] = indice_indices[i]
        else:
            year_info['indice_col_idx'] = None # Mark as not found
        year_cols_map[year] = year_info

    return data_start_row_index, year_cols_map


def clean_value(value):
    """Cleans string values for numeric conversion."""
    if pd.isna(value):
        return np.nan
    if isinstance(value, (int, float)):
        return value # Already numeric
    text = str(value).strip()
    if text == '-' or text == '':
        return np.nan
    # Remove thousands separators (,) and spaces, keep decimal (.)
    # If the input uses ',' as decimal, pandas read_csv should handle it
    # This cleaning is more for any stray spaces or thousand-like commas
    text = text.replace(' ', '')
    # We assume read_csv already handled the specified decimal separator (',')
    # So, we don't replace ',' with '.' here during cleaning.
    return text

# --- Main Processing Logic ---
os.makedirs(output_directory, exist_ok=True)
print(f"Output directory set to: {output_directory}")

csv_files = glob.glob(os.path.join(input_directory, 'montants-*.csv'))

if not csv_files:
    print(f"No CSV files matching 'montants-*.csv' found in '{input_directory}'. Please check the path and filenames.")
    exit()

print(f"Found {len(csv_files)} CSV files to process:")
for f in csv_files:
    print(f"  - {os.path.basename(f)}")
print("-" * 30)

all_cleaned_data = [] # List to hold DataFrames from each file

for input_csv_file in csv_files:
    base_filename = os.path.basename(input_csv_file)
    output_filename = base_filename.replace('.csv', '_cleaned.csv')
    output_csv_file = os.path.join(output_directory, output_filename)

    print(f"Processing '{base_filename}'...")

    try:
        # --- Determine Header Info ---
        data_start_index, year_col_map = find_header_info(input_csv_file)

        if data_start_index is None:
            print(f"  Skipping file {base_filename} due to header parsing error.")
            continue

        print(f"  Determined data starts at row index: {data_start_index}")
        if year_col_map:
             print(f"  Mapped columns for {len(year_col_map)} years.")
        else:
             print(f"  Warning: Could not map year columns reliably. Column names might be generic.")


        # --- Read Main Data ---
        df = pd.read_csv(
            input_csv_file,
            skiprows=data_start_index,
            header=None,       # Read without header initially
            decimal=',',       # Use comma as decimal separator
            thousands=None,    # Do not assume a thousands separator during read
            skipinitialspace=True,
            encoding='utf-8',
            low_memory=False
        )
        print(f"  Read {len(df)} data rows starting from index {data_start_index}.")

        # --- Assign Column Names ---
        # Construct column names based on the map or generic if map failed
        base_identifiers = ['Agglomeration', 'Type_Habitat', 'Envergure']
        column_names = base_identifiers.copy()
        max_cols_needed = 3 # Start with identifiers

        input_years_found = []
        montant_cols_map_idx_name = {}
        indice_cols_map_idx_name = {}

        if year_col_map:
            input_years_found = sorted(year_col_map.keys())
            for year in input_years_found:
                 info = year_col_map[year]
                 montant_idx = info['montant_col_idx']
                 indice_idx = info['indice_col_idx']
                 montant_name = f'Montant_{year}'
                 indice_name = f'Indice_{year}'
                 montant_cols_map_idx_name[montant_idx] = montant_name
                 if indice_idx is not None:
                     indice_cols_map_idx_name[indice_idx] = indice_name
                 max_cols_needed = max(max_cols_needed, montant_idx + 1, (indice_idx + 1) if indice_idx is not None else 0)

            # Build the final name list ensuring correct order based on index
            temp_names = {}
            for idx, name in montant_cols_map_idx_name.items():
                 temp_names[idx] = name
            for idx, name in indice_cols_map_idx_name.items():
                 temp_names[idx] = name

            # Add identifier names at the beginning
            full_col_names = base_identifiers.copy()
            # Add the mapped names in order of their original column index
            for idx in sorted(temp_names.keys()):
                 full_col_names.append(temp_names[idx])

            # Assign names, handling potential extra columns read
            num_cols_read = df.shape[1]
            if len(full_col_names) <= num_cols_read:
                 df.columns = full_col_names + [f'Unknown_{i}' for i in range(num_cols_read - len(full_col_names))]
            else:
                 print(f"  Error: More columns mapped ({len(full_col_names)}) than read ({num_cols_read}). Check header parsing. Skipping.")
                 continue
            print(f"  Assigned specific column names based on header.")

        else:
            # Fallback to generic naming if header parsing failed
            num_cols_to_name = df.shape[1]
            generic_names = base_identifiers + [f'Column_{i+1}' for i in range(num_cols_to_name - len(base_identifiers))]
            df.columns = generic_names
            print(f"  Assigned generic column names due to header parsing issues.")


        # --- Data Cleaning ---
        # 1. Drop fully empty rows
        df.dropna(how='all', inplace=True)

        # 2. Forward fill Agglomeration and Type_Habitat
        df['Group_ID'] = df['Agglomeration'].notna().cumsum()
        df['Agglomeration'] = df.groupby('Group_ID')['Agglomeration'].ffill()
        df['Type_Habitat'] = df.groupby('Group_ID')['Type_Habitat'].ffill()
        df.drop(columns=['Group_ID'], inplace=True)
        print(f"  Forward filled 'Agglomeration' and 'Type_Habitat'.")
        # print(df[['Agglomeration', 'Type_Habitat', 'Envergure']].head(10)) # Debug print

        # 3. Filter out structural/summary rows AFTER ffill
        structural_rows = ['Agglomération', 'Région', 'Total ville', 'Total région', 'Fès Meknès'] # Add more if needed
        df = df[df['Agglomeration'].notna()] # Should be filled now
        df = df[~df['Agglomeration'].isin(structural_rows)]
        df = df[df['Type_Habitat'].notna()] # Ensure Type_Habitat is also filled
        # Filter rows where Envergure is blank or looks like a total indicator itself
        df = df[df['Envergure'].notna() & (~df['Envergure'].isin(['VIL', 'APS', 'APE', 'MLE', '']))]
        print(f"  Filtered structural rows. Shape after filtering: {df.shape}")
        # print(df[['Agglomeration', 'Type_Habitat', 'Envergure']].head(10)) # Debug print


        # 4. Clean and convert numeric columns
        numeric_cols_in_df = [col for col in df.columns if 'Montant_' in col or 'Indice_' in col]
        if not numeric_cols_in_df and not year_col_map: # Handle generic column case if needed
             # Attempt conversion on columns presumed to be numeric (e.g., Column_4 onwards)
             potential_numeric_cols = [col for col in df.columns if col.startswith('Column_') and int(col.split('_')[1]) >= 4]
             numeric_cols_in_df.extend(potential_numeric_cols) # Extend, don't overwrite

        print(f"  Attempting numeric conversion for columns: {numeric_cols_in_df}")
        for col in numeric_cols_in_df:
            if col in df.columns:
                # Apply cleaning: replace '-', remove spaces
                df[col] = df[col].astype(str).replace('-', np.nan, regex=False)
                df[col] = df[col].str.replace(' ', '', regex=False)
                # Convert to numeric. Pandas read_csv with decimal=',' should handle the input decimal.
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                 print(f"    Column {col} not found for numeric conversion.")


        # 5. Drop rows with no numeric data AFTER conversion attempt
        df.dropna(subset=numeric_cols_in_df, how='all', inplace=True)
        df.reset_index(drop=True, inplace=True)
        print(f"  Cleaned numeric values and dropped rows with no data. Final shape for file: {df.shape}")

        # --- Standardize Columns to Final Year Range (2001-2022) ---
        final_montant_cols = [f'Montant_{year}' for year in FINAL_YEARS]
        final_indice_cols = [f'Indice_{year}' for year in FINAL_YEARS]
        final_col_order = base_identifiers + final_montant_cols + final_indice_cols

        # Add missing year columns (initialize with NaN)
        for col in final_col_order:
            if col not in df.columns:
                df[col] = np.nan
                # print(f"  Added missing column: {col}")

        # Ensure correct final column order and drop any extra/unknown columns
        # Only keep columns that are part of the final desired structure
        df = df[[col for col in final_col_order if col in df.columns]]

        # Add the processed DataFrame to the list
        all_cleaned_data.append(df)

        # --- Output Individual Cleaned File ---
        df.to_csv(output_csv_file, index=False, decimal='.', encoding='utf-8-sig')
        print(f"  Successfully cleaned and saved individual file to '{output_filename}'")

    except FileNotFoundError:
        print(f"  Error: Input file not found: {input_csv_file}")
    except pd.errors.EmptyDataError:
        print(f"  Error: The file '{base_filename}' is empty.")
    except Exception as e:
        print(f"  An unexpected error occurred processing file '{base_filename}': {e}")
        import traceback
        traceback.print_exc()

    print("-" * 30)

# --- Combine all DataFrames ---
if all_cleaned_data:
    print("Combining data from all processed files...")
    combined_df = pd.concat(all_cleaned_data, ignore_index=True)

    # Final check for duplicates just in case
    combined_df.drop_duplicates(subset=base_identifiers + [final_montant_cols[0]], keep='first', inplace=True) # Check based on first year

    # Save the combined file
    combined_output_file = os.path.join(output_directory, 'COMBINED_montants_loyers_cleaned.csv')
    combined_df.to_csv(combined_output_file, index=False, decimal='.', encoding='utf-8-sig')
    print(f"Successfully combined {len(all_cleaned_data)} files into '{os.path.basename(combined_output_file)}'")
else:
    print("No data was successfully processed to combine.")

print("Script finished.")

# --- END OF FILE transform_revised.py ---