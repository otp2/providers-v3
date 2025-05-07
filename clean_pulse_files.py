import pandas as pd
import re
import os

def clean_multi_value_col(series, sep=';#', item_transform_func=None):
    """Splits by separator, applies optional transform, trims, filters empty, rejoins."""
    if series is None:
        return None
        
    def process_cell(cell):
        if pd.isna(cell):
            return None
        items = str(cell).split(sep)
        cleaned_items = []
        for item in items:
            stripped_item = item.strip()
            if stripped_item: # Filter out empty strings resulting from separators
                if item_transform_func:
                    try:
                        transformed_item = item_transform_func(stripped_item)
                        if transformed_item: # Ensure transform didn't result in None/empty
                             cleaned_items.append(str(transformed_item)) # Ensure back to string
                    except Exception as e:
                        # Handle potential errors during transformation, keep original
                        print(f"  - Warning: Error transforming item '{stripped_item}': {e}. Keeping original.")
                        cleaned_items.append(stripped_item)
                else:
                    cleaned_items.append(stripped_item)
        # Return None if no valid items remain, otherwise rejoin
        return sep.join(cleaned_items) if cleaned_items else None

    return series.apply(process_cell)

def clean_provider_name(name_series):
    """Removes quoted nicknames like ""Jim""."""
    if name_series is None:
        return None
    # Regex to find "nickname" pattern, replace with nothing
    # It handles potential spaces around the quotes
    cleaned_series = name_series.str.replace(r'\s*""(.*?)""\s*', '', regex=True)
    # Clean up potential double spaces left behind
    cleaned_series = cleaned_series.str.replace(r'\s{2,}', ' ', regex=True)
    return cleaned_series.str.strip()

def clean_newlines_in_df(df):
    """Replaces newlines in all object columns."""
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].notna().any(): # Check if column has any non-NA string data
             # ReplaceCRLF first, then LF, then CR
            df[col] = df[col].str.replace(r'\r\n', ' ', regex=True)
            df[col] = df[col].str.replace(r'\n', ' ', regex=True)
            df[col] = df[col].str.replace(r'\r', ' ', regex=True)
            # Replace multiple spaces resulting from newline replacement
            df[col] = df[col].str.replace(r'\s{2,}', ' ', regex=True)
    return df


# --- Main Script ---
file_paths = [
    '00_source_data/pulse_data/pulse_mm/pulse_mm.csv',
    '00_source_data/pulse_data/pulse_counseling/pulse_counseling.csv',
    '00_source_data/pulse_data/pulse_bhi/pulse_bhi.csv'
]

# Columns known to potentially use ';#' separator
multi_value_cols = ['Availability', 'Ages', 'Specialties', 'Do NOT Refer', 'Services Offered', 'Specialities/Preference'] # Combine potential names

for filepath in file_paths:
    print(f"--- Processing: {filepath} ---")
    if not os.path.exists(filepath):
        print(f"ERROR: File not found: {filepath}. Skipping.")
        continue

    try:
        # Read CSV, treating all as string initially
        df = pd.read_csv(filepath, dtype=str, skipinitialspace=True)
        print(f"Read file. Shape: {df.shape}")

        # --- Apply Fixes ---

        # 0. Basic whitespace trim on columns and data (redundant with previous but safe)
        df.columns = [col.strip() for col in df.columns]
        for col in df.select_dtypes(include=['object']).columns:
             df[col] = df[col].str.strip()

        # 1. Standardize Provider Name Column
        if 'Therapist Name' in df.columns:
            df.rename(columns={'Therapist Name': 'Provider Name'}, inplace=True)
            print("- Renamed 'Therapist Name' to 'Provider Name'.")
        if 'BHIC' in df.columns:
            df.rename(columns={'BHIC': 'Provider Name'}, inplace=True)
            print("- Renamed 'BHIC' to 'Provider Name'.")

        # 2. Clean Embedded Quotes in Provider Name
        if 'Provider Name' in df.columns:
            original_names = df['Provider Name'].copy()
            df['Provider Name'] = clean_provider_name(df['Provider Name'])
            if not original_names.equals(df['Provider Name']):
                 print("- Cleaned quoted nicknames from 'Provider Name'.")

        # 3. Replace Newlines Within Cells
        # Run this early before splitting columns that might contain newlines
        print("- Replacing newlines within cells...")
        df = clean_newlines_in_df(df)

        # 4. Handle Multi-value Columns (Standardize splitting & trimming)
        print("- Standardizing multi-value columns (splitting by ';#', trimming items)...")
        for col_name in multi_value_cols:
             if col_name in df.columns:
                 # Apply basic split/trim/rejoin first
                 df[col_name] = clean_multi_value_col(df[col_name], sep=';#')
                 print(f"  - Processed '{col_name}' for consistent splitting/trimming.")

        # 5. Standardize Case for 'Availability' (Lowercase)
        if 'Availability' in df.columns:
             print("- Standardizing case for 'Availability' (lowercase & multi-value clean)...")
             # Apply transform *within* the multi-value cleaner
             df['Availability'] = clean_multi_value_col(df['Availability'], sep=';#', item_transform_func=lambda x: x.lower())


        # 6. Standardize Case for 'Credentials' (Uppercase)
        if 'Credentials' in df.columns:
             print("- Standardizing case for 'Credentials' (uppercase & multi-value clean)...")
             # Apply transform *within* the multi-value cleaner
             df['Credentials'] = clean_multi_value_col(df['Credentials'], sep=';#', item_transform_func=lambda x: x.upper())


        # 7. Standardize Case for 'Location' (Title Case)
        if 'Location' in df.columns:
             print("- Standardizing case for 'Location' (title case)...")
             # Location is likely single value, but apply transform safely
             df['Location'] = df['Location'].str.title()


        # 8. Final whitespace trim on all string columns (just in case)
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip()


        # Remove completely blank rows if any were created/existed
        original_rows = df.shape[0]
        df.dropna(how='all', inplace=True)
        if df.shape[0] < original_rows:
            print(f"- Removed {original_rows - df.shape[0]} fully blank rows.")


        # Save cleaned file
        df.to_csv(filepath, index=False, encoding='utf-8')
        print(f"Successfully cleaned and saved '{filepath}'. New shape: {df.shape}")

    except Exception as e:
        print(f"ERROR: Failed to process file {filepath}. Error: {e}")
        # Optionally re-raise e if debugging: raise e
    
    print("-" * (len(filepath) + 18)) # Separator for clarity

print("--- All Pulse files processed. ---") 