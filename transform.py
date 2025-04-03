import pandas as pd
import numpy as np
import os
import glob # To find files matching a pattern

# --- Configuration ---
# Directory containing the original CSV files
# <<< IMPORTANT: Make sure this path is correct for your system >>>
input_directory = r'C:\Users\Asus\Desktop\Dataset Loyer'

# Directory where cleaned files will be saved
output_directory = os.path.join(input_directory, 'cleaned_data')

# Define the FULL range of years we want in the final output
final_years = list(range(2001, 2023)) # 2001 to 2022 inclusive

# --- Helper Functions ---
def clean_numeric_string(value):
    """Removes spaces and handles potential non-string inputs."""
    if isinstance(value, str):
        return value.replace(' ', '').strip()
    return value

# --- Main Processing Logic ---

# Create the output directory if it doesn't exist
os.makedirs(output_directory, exist_ok=True)
print(f"Output directory set to: {output_directory}")

# Find all relevant CSV files in the input directory
csv_files = glob.glob(os.path.join(input_directory, 'montants-*.csv'))

if not csv_files:
    print(f"No CSV files matching 'montants-*.csv' found in '{input_directory}'. Please check the path and filenames.")
    exit()

print(f"Found {len(csv_files)} CSV files to process:")
for f in csv_files:
    print(f"  - {os.path.basename(f)}")
print("-" * 30)

# Loop through each found CSV file
for input_csv_file in csv_files:
    base_filename = os.path.basename(input_csv_file)
    output_filename = base_filename.replace('.csv', '_cleaned.csv')
    output_csv_file = os.path.join(output_directory, output_filename)

    print(f"Processing '{base_filename}'...")

    try:
        # --- Read CSV ---
        # Read the data, skipping initial header rows
        df = pd.read_csv(
            input_csv_file,
            skiprows=4,
            header=None,
            decimal=',',
            quotechar='"',
            thousands=None,
            skipinitialspace=True,
            encoding='utf-8',
            low_memory=False # Helps with potential mixed types or ragged rows
        )
        print(f"  Read {len(df)} data rows.")

        # --- Determine Input Years and Column Names ---
        num_cols_read = len(df.columns)

        # Estimate the number of year columns present in this specific file
        # Assuming structure: 3 ID, X Amounts, 1 Sep, X Indices, Y Trail
        # Simplified approach: Check how many columns likely belong to 'Montants'
        # A more robust way might be needed if structure varies wildly, but let's try this:
        # Assume at least 1 separator and maybe 1 trailing column minimum.
        # Max possible years = (num_cols_read - 3 ID - 1 Sep - 1 Trail) / 2 roughly
        # Let's dynamically assign based on column position for robustness

        base_identifiers = ['Agglomeration', 'Type_Habitat', 'Envergure']
        temp_col_names = base_identifiers.copy()
        col_idx = 3 # Start index after identifiers

        # Identify Montant columns (read until a likely non-numeric/separator column)
        input_montant_cols = []
        start_year = 2001
        year_counter = 0
        while col_idx < num_cols_read:
            # Try converting first few data rows to see if it looks numeric
            try:
                 # Check a sample, ignore header/summary remnants
                 sample_numeric = pd.to_numeric(df.iloc[5:15, col_idx].apply(clean_numeric_string), errors='coerce')
                 # If more than half are numeric, assume it's a data year column
                 if sample_numeric.notna().sum() > len(sample_numeric) / 2 :
                     current_year = start_year + year_counter
                     col_name = f'Montant_{current_year}'
                     temp_col_names.append(col_name)
                     input_montant_cols.append(col_name)
                     year_counter += 1
                     col_idx += 1
                 else:
                     # Likely separator or start of indices/trailing
                     break
            except IndexError: # Reached end of columns or rows
                 break
            except Exception: # General conversion error
                 break # Stop assuming Montant columns

        # Add separator column name (we'll drop it later)
        if col_idx < num_cols_read:
             temp_col_names.append('Separator_Col_1')
             col_idx += 1
        else:
             print(f"  Warning: Unexpected end of columns after Montant for {base_filename}")

        # Add Indice columns (should match number of Montant columns found)
        input_indice_cols = []
        for i in range(len(input_montant_cols)):
             if col_idx < num_cols_read:
                 montant_col_name = input_montant_cols[i]
                 year = montant_col_name.split('_')[1]
                 col_name = f'Indice_{year}'
                 temp_col_names.append(col_name)
                 input_indice_cols.append(col_name)
                 col_idx += 1
             else:
                 print(f"  Warning: Ran out of columns while assigning Indice names for {base_filename}")
                 break

        # Add names for any remaining trailing columns
        while col_idx < num_cols_read:
            temp_col_names.append(f'Trailing_Col_{col_idx - len(temp_col_names) + len(base_identifiers) + 1}') # Adjust index naming
            col_idx += 1

        # Assign temporary names
        if len(temp_col_names) == df.shape[1]:
             df.columns = temp_col_names
        else:
            print(f"  Error: Column name count ({len(temp_col_names)}) doesn't match read columns ({df.shape[1]}) for {base_filename}. Skipping file.")
            continue # Skip to next file


        # --- Clean Data ---
        df.dropna(how='all', inplace=True) # Drop fully empty rows

        cols_to_drop = [col for col in df.columns if 'Separator_Col' in col or 'Trailing_Col' in col]
        df.drop(columns=cols_to_drop, inplace=True, errors='ignore')

        numeric_cols_in_df = [col for col in df.columns if 'Montant_' in col or 'Indice_' in col]
        for col in numeric_cols_in_df:
            df[col] = df[col].apply(clean_numeric_string)
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df.replace(['', ' ', '-'], np.nan, inplace=True)

        df['Group_ID'] = df['Agglomeration'].notna().cumsum()
        df['Agglomeration'] = df.groupby('Group_ID')['Agglomeration'].ffill()
        df['Type_Habitat'] = df.groupby('Group_ID')['Type_Habitat'].ffill()
        df.drop(columns=['Group_ID'], inplace=True)

        # Generic filtering for summary rows (might need adjustment if patterns differ significantly)
        structural_rows = ['Agglomération', 'Région', 'Total ville', 'Total région', 'Fès Meknès'] # Add known summary terms
        df = df[df['Agglomeration'].notna()]
        df = df[~df['Agglomeration'].isin(structural_rows)]
        # Also remove rows where essential identifiers might be missing after cleaning
        df.dropna(subset=['Agglomeration', 'Type_Habitat', 'Envergure'], how='any', inplace=True)
        # Remove rows that have NO numeric data at all after conversion
        df.dropna(subset=numeric_cols_in_df, how='all', inplace=True)

        df.reset_index(drop=True, inplace=True)
        print(f"  Cleaned identifiers and filtered rows. Shape: {df.shape}")

        # --- Standardize Columns to Final Year Range (2001-2022) ---
        final_montant_cols = [f'Montant_{year}' for year in final_years]
        final_indice_cols = [f'Indice_{year}' for year in final_years]
        final_col_order = base_identifiers + final_montant_cols + final_indice_cols

        # Add missing year columns (initialize with NaN)
        for col in final_col_order:
            if col not in df.columns:
                df[col] = np.nan
        print(f"  Added missing year columns up to {final_years[-1]}.")

        # Ensure correct final column order and drop any extra columns not in the final list
        df = df[final_col_order]

        # --- Output ---
        df.to_csv(output_csv_file, index=False, decimal='.', encoding='utf-8-sig')
        print(f"  Successfully cleaned and saved to '{output_filename}'")

    except FileNotFoundError:
         print(f"  Error: Input file not found during processing loop (should not happen with glob): {input_csv_file}")
    except pd.errors.EmptyDataError:
        print(f"  Error: The file '{base_filename}' is empty or couldn't be parsed.")
    except Exception as e:
        print(f"  Error processing file '{base_filename}': {e}")
        import traceback
        traceback.print_exc() # Print detailed traceback for debugging

    print("-" * 30)

print("Script finished.")