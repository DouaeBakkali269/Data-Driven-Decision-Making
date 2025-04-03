import pandas as pd
import numpy as np
import os
import glob # To find files matching a pattern

# --- Configuration ---
# Directory where the cleaned CSV files are located
# (This should be the 'cleaned_data' folder created by the previous script)
# <<< IMPORTANT: Make sure this path is correct for your system >>>
cleaned_data_directory = r'C:\Users\Asus\Desktop\Dataset Loyer\cleaned_data'

# Directory and filename for the final combined output file
# It will be saved in the *parent* directory of 'cleaned_data'
output_directory = r'C:\Users\Asus\Desktop\Dataset Loyer'
combined_filename = 'combined_loyers_all_regions_2001-2022.csv'
output_csv_file = os.path.join(output_directory, combined_filename)

# --- Main Processing Logic ---

# Check if the cleaned data directory exists
if not os.path.isdir(cleaned_data_directory):
    print(f"Error: Cleaned data directory not found at '{cleaned_data_directory}'")
    print("Please ensure the first script (cleaning) ran successfully and created this folder.")
    exit()

# Find all cleaned CSV files in the specified directory
cleaned_csv_files = glob.glob(os.path.join(cleaned_data_directory, '*_cleaned.csv'))

if not cleaned_csv_files:
    print(f"No cleaned CSV files ('*_cleaned.csv') found in '{cleaned_data_directory}'.")
    print("Make sure the cleaning script ran and produced output files.")
    exit()

print(f"Found {len(cleaned_csv_files)} cleaned CSV files to combine:")
for f in cleaned_csv_files:
    print(f"  - {os.path.basename(f)}")
print("-" * 30)

# List to hold individual DataFrames
all_dataframes = []

# Loop through each cleaned CSV file and read it into a DataFrame
print("Reading cleaned files...")
for file_path in cleaned_csv_files:
    try:
        df = pd.read_csv(
            file_path,
            encoding='utf-8',
            low_memory=False # Good practice for potentially large files or mixed types
            )
        # Optional: Add a column to indicate the source file/region if needed
        # source_name = os.path.basename(file_path).replace('_cleaned.csv', '').replace('montants-moyens-des-loyers-region-', '').replace('montants-loyers-region-', '')
        # df['Source_Region'] = source_name
        all_dataframes.append(df)
        print(f"  Read '{os.path.basename(file_path)}' ({len(df)} rows)")
    except pd.errors.EmptyDataError:
        print(f"  Warning: Skipping empty file '{os.path.basename(file_path)}'.")
    except Exception as e:
        print(f"  Error reading file '{os.path.basename(file_path)}': {e}")
        print("    Skipping this file.")

# Check if any DataFrames were successfully read
if not all_dataframes:
    print("\nError: No dataframes were successfully read. Cannot combine.")
    exit()

# Concatenate all DataFrames into one
print("\nConcatenating DataFrames...")
try:
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    print("Concatenation successful.")
except Exception as e:
    print(f"Error during concatenation: {e}")
    exit()

# --- Output ---
print(f"\nSaving combined data to '{output_csv_file}'...")
try:
    # Save the combined DataFrame to a new CSV file
    combined_df.to_csv(output_csv_file, index=False, encoding='utf-8-sig')
    print("Combined file saved successfully.")
except Exception as e:
    print(f"\nError writing combined CSV: {e}")

# Display info about the final combined DataFrame
print("\n--- Combined DataFrame Info ---")
print(f"Final shape: {combined_df.shape} (rows, columns)")
print("Columns:", combined_df.columns.tolist())
print(combined_df.info()) # Provides more detail on columns and non-null counts
print("\nSample of combined data (first 5 rows):")
print(combined_df.head())

print("\nScript finished.")