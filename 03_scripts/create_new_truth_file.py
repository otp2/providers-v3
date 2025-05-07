import pandas as pd
import numpy as np

# --- Configuration ---
OUTPUT_FILE = "01_processed_data/new_provider_truth_file.csv"

PULSE_NAMES_FILE = "05_airtable_and_mapping/02_pulse/pulse_consolidated_names.csv"
NORTHSHORE_NAMES_FILE = "05_airtable_and_mapping/03_northshore/northshore_names.csv"
UNMATCHED_FILE = "05_airtable_and_mapping/04_not_in_pulse_or_northshore/unmatched_providers.csv"
OLD_TRUTH_FOR_NPI_LOOKUP = "05_airtable_and_mapping/01_name_npi_airtable/provider_ids_for_mapping.csv"

# --- Column Names ---
COL_UIUD = "uiud"
COL_FIRST = "First Name"
COL_LAST = "Last Name"
COL_NPI_OUT = "NPI Number"
COL_LABEL = "Internal Label"

COL_PULSE_LABEL = "Pulse Label" # From pulse_consolidated_names.csv
COL_NPI_IN = "National Provider Identifier (NPI)" # From unmatched_providers.csv and old truth file

def normalize_name_part(name_part):
    """Normalize a single part of a name (first or last)."""
    if pd.isna(name_part):
        return ""
    # Lowercase, strip, replace hyphens/periods/commas with space, then condense multiple spaces
    normalized = str(name_part).lower().strip()
    normalized = normalized.replace('-', ' ').replace('.', ' ').replace(',', ' ')
    return ' '.join(normalized.split())

def create_name_key(df):
    """Create a consistent search key from first and last name columns."""
    first = df[COL_FIRST].apply(normalize_name_part)
    last = df[COL_LAST].apply(normalize_name_part)
    return first + "_" + last

# --- Load Data --- 
def load_data():
    data_frames = {}
    try:
        data_frames['pulse'] = pd.read_csv(PULSE_NAMES_FILE)
        data_frames['northshore'] = pd.read_csv(NORTHSHORE_NAMES_FILE)
        data_frames['unmatched'] = pd.read_csv(UNMATCHED_FILE, dtype={COL_NPI_IN: str})
        data_frames['old_truth'] = pd.read_csv(OLD_TRUTH_FOR_NPI_LOOKUP, dtype={COL_NPI_IN: str})
        print("Successfully loaded all input files.")
        return data_frames
    except FileNotFoundError as e:
        print(f"Error loading file: {e}. Please ensure all input files exist.")
        return None
    except Exception as e:
        print(f"An error occurred during file loading: {e}")
        return None

# --- Main Processing Logic ---
def main():
    print("Starting creation of new truth file...")
    loaded_data = load_data()
    if loaded_data is None:
        return

    df_pulse = loaded_data['pulse']
    df_northshore = loaded_data['northshore']
    df_unmatched = loaded_data['unmatched']
    df_old_truth = loaded_data['old_truth']

    # --- Prepare NPI Lookup from Old Truth --- 
    df_old_truth[COL_NPI_IN] = df_old_truth[COL_NPI_IN].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_old_truth['name_key'] = create_name_key(df_old_truth)
    npi_lookup = df_old_truth.set_index('name_key')[COL_NPI_IN].to_dict()
    print(f"Created NPI lookup dictionary from {len(npi_lookup)} unique names in old truth file.")

    # --- Standardize Input DataFrames --- 
    # Pulse
    df_pulse[COL_LABEL] = df_pulse[COL_PULSE_LABEL]
    df_pulse = df_pulse[[COL_FIRST, COL_LAST, COL_LABEL]].copy()
    df_pulse[COL_NPI_OUT] = np.nan # Add NPI column
    df_pulse['source_priority'] = 1 # Higher priority

    # Northshore
    df_northshore[COL_LABEL] = 'Northshore'
    df_northshore = df_northshore[[COL_FIRST, COL_LAST, COL_LABEL]].copy()
    df_northshore[COL_NPI_OUT] = np.nan
    df_northshore['source_priority'] = 2

    # Unmatched/Legacy
    df_unmatched[COL_LABEL] = 'Legacy/Unmatched'
    # Rename NPI column for consistency before merging
    df_unmatched.rename(columns={COL_NPI_IN: COL_NPI_OUT}, inplace=True)
    df_unmatched[COL_NPI_OUT] = df_unmatched[COL_NPI_OUT].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_unmatched = df_unmatched[[COL_FIRST, COL_LAST, COL_NPI_OUT, COL_LABEL]].copy()
    df_unmatched['source_priority'] = 3

    # --- Combine DataFrames --- 
    df_combined = pd.concat([df_pulse, df_northshore, df_unmatched], ignore_index=True)
    print(f"Combined data from sources. Total rows before deduplication: {len(df_combined)}")

    # --- Create Name Key for Combined Data --- 
    df_combined['name_key'] = create_name_key(df_combined)

    # --- Lookup NPIs --- 
    # Fill NPIs from lookup where they are currently missing
    missing_npi_mask = df_combined[COL_NPI_OUT].isna()
    df_combined.loc[missing_npi_mask, COL_NPI_OUT] = df_combined.loc[missing_npi_mask, 'name_key'].map(npi_lookup)
    print(f"Attempted NPI lookup. {missing_npi_mask.sum()} entries had NPI looked up.")

    # --- Deduplicate --- 
    # Sort by priority (Pulse > Northshore > Unmatched), then drop duplicates based on name_key
    df_combined.sort_values(by=['name_key', 'source_priority'], ascending=[True, True], inplace=True)
    df_final = df_combined.drop_duplicates(subset=['name_key'], keep='first').copy()
    print(f"Deduplicated based on First/Last Name. Total rows after deduplication: {len(df_final)}")

    # --- Final Structure --- 
    df_final[COL_UIUD] = '' # Add blank UIUD column
    # Ensure NPI is string and handle potential NaN/None after merge/lookup
    df_final[COL_NPI_OUT] = df_final[COL_NPI_OUT].fillna('').astype(str)
    
    # Select and reorder columns
    final_columns = [COL_UIUD, COL_FIRST, COL_LAST, COL_NPI_OUT, COL_LABEL]
    df_final = df_final[final_columns]

    # --- Save Output --- 
    try:
        df_final.to_csv(OUTPUT_FILE, index=False)
        print(f"Successfully saved new truth file to: {OUTPUT_FILE}")
    except Exception as e:
        print(f"Error saving output file: {e}")

if __name__ == "__main__":
    main() 