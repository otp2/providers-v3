import pandas as pd
import uuid
import os

# Define input and output file paths
input_file = '00_source_data/identifiers/idenitfying-information.csv'
output_dir = '01_processed_data/main_provider_table'
output_file = os.path.join(output_dir, '01_providers_with_uids.csv')

# Ensure output directory exists (it should have been created by previous steps)
if not os.path.exists(output_dir):
    print(f"Error: Output directory {output_dir} does not exist. Please create it first.")
    exit()

# Load the CSV
try:
    df = pd.read_csv(input_file)
except FileNotFoundError:
    print(f"Error: Input file '{input_file}' not found.")
    exit()
except Exception as e:
    print(f"Error reading CSV file '{input_file}': {e}")
    exit()

# Ensure 'uiud' column exists, if not, create it
if 'uiud' not in df.columns:
    print("Warning: 'uiud' column not found. Creating it.")
    df['uiud'] = None # Initialize with None

# Function to generate UUID if 'uiud' is missing
def generate_uuid_if_missing(value):
    if pd.isna(value) or str(value).strip() == '':
        return str(uuid.uuid4())
    return str(value).strip() # Ensure existing UUIDs are also strings and stripped

# Apply the function to the 'uiud' column
try:
    df['uiud'] = df['uiud'].apply(generate_uuid_if_missing)
except Exception as e:
    print(f"Error processing 'uiud' column: {e}")
    exit()

# Save the updated DataFrame
try:
    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Successfully processed identifiers and saved to '{output_file}'")
except Exception as e:
    print(f"Error writing updated CSV to '{output_file}': {e}")
    exit()

# Verify by printing the first few rows of the 'uiud' column from the saved file
try:
    df_check = pd.read_csv(output_file)
    print("\nVerification: First 5 UIUDs from the saved file:")
    print(df_check['uiud'].head())
    if df_check['uiud'].isnull().any():
        print("Warning: Some UIUDs are still null in the saved file.")
    else:
        print("All UIUDs appear to be populated in the saved file.")
except Exception as e:
    print(f"Error during verification: {e}") 