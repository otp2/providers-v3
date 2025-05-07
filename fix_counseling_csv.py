# Script: fix_counseling_csv.py
import pandas as pd
import re
import os

filepath = '00_source_data/pulse_data/pulse_counseling/pulse_counseling.csv'

def clean_credentials_specifically(series):
    """Handles both comma and ;# separation, uppercases, removes duplicates."""
    if series is None:
        return None
    
    processed_cells = []
    for cell in series.fillna(''): # Fill NaN to allow processing
        items_final = set() # Use a set to handle duplicates
        if isinstance(cell, str) and cell.strip():
            # Split by comma first, then by ;# within each part
            parts = str(cell).split(',')
            for part in parts:
                sub_parts = part.split(';#')
                for sub_part in sub_parts:
                    cleaned = sub_part.strip().upper()
                    if cleaned:
                        # Further clean potential leftover quotes if needed
                        cleaned = cleaned.replace('"', '') 
                        if cleaned:
                           items_final.add(cleaned)
        
        processed_cells.append(';#'.join(sorted(list(items_final))) if items_final else None)
        
    return pd.Series(processed_cells, index=series.index)

def enhance_name_cleaning(name_series):
    """Removes quoted nicknames and common suffixes."""
    if name_series is None: return None
    
    # Remove nicknames first
    cleaned = name_series.str.replace(r'\s*""(.*?)""\s*', '', regex=True)
    # Remove common suffixes like Jr., Sr., III etc. (case-insensitive)
    cleaned = cleaned.str.replace(r'\s*,?\s*(?:Jr|Sr|III|II|IV)\.?$', '', regex=True, case=False)
     # Remove potential leading/trailing spaces and multiple internal spaces
    cleaned = cleaned.str.replace(r'\s{2,}', ' ', regex=True).str.strip()
    return cleaned

def fix_url(url_series):
    """Trims URL and attempts to prepend https:// if missing."""
    if url_series is None: return None

    def process_url(url):
        if pd.isna(url): return None
        cleaned_url = str(url).strip()
        if cleaned_url and not cleaned_url.lower().startswith(('http://', 'https://')):
            # Basic check if it looks like a domain part exists
            if '.' in cleaned_url:
                return 'https://' + cleaned_url
        return cleaned_url if cleaned_url else None # Return None if empty after strip
        
    return url_series.apply(process_url)

def normalize_special_whitespace(series):
     """Replaces non-breaking spaces and zero-width spaces."""
     if series is None: return None
     cleaned = series.astype(str).str.replace('\u00A0', ' ', regex=False) # Non-breaking space
     cleaned = cleaned.str.replace('\u200b', '', regex=False)     # Zero-width space (remove)
     # Replace multiple spaces that might result
     cleaned = cleaned.str.replace(r'\s{2,}', ' ', regex=True) 
     return cleaned

def clean_newlines_in_df(df):
    """Replaces newlines in all object columns."""
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].notna().any(): 
            df[col] = df[col].str.replace(r'\r\n', ' ', regex=True)
            df[col] = df[col].str.replace(r'\n', ' ', regex=True)
            df[col] = df[col].str.replace(r'\r', ' ', regex=True)
            df[col] = df[col].str.replace(r'\s{2,}', ' ', regex=True)
    return df

print(f"--- Processing specific fixes for: {filepath} ---")
if not os.path.exists(filepath):
    print(f"ERROR: File not found: {filepath}. Cannot process.")
    exit()

try:
    df = pd.read_csv(filepath, dtype=str, skipinitialspace=True)
    print(f"Read file. Shape: {df.shape}")
    
    # Clean column headers first
    df.columns = [col.strip() for col in df.columns]

    # --- Apply Specific Fixes ---

    # Fix 5 (Early pass): Normalize special whitespace/invisible chars first
    print("- Normalizing special whitespace/invisible characters...")
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = normalize_special_whitespace(df[col])

    # Fix 3: Rename 'CT Gender ID' to 'Gender'
    if 'CT Gender ID' in df.columns:
        df.rename(columns={'CT Gender ID': 'Gender'}, inplace=True)
        print("- Renamed 'CT Gender ID' to 'Gender'.")

    # Fix 4: Standardize 'Title'
    if 'Title' in df.columns:
        # Remove "CT* - " prefix, trim, then Title Case
        df['Title'] = df['Title'].str.replace(r'^CT\d\s*-\s*', '', regex=True).str.strip().str.title()
        print("- Standardized 'Title' column (removed prefix, title case).")

    # Fix 1: Clean Credentials (handle commas, ;, case)
    if 'Credentials' in df.columns:
        df['Credentials'] = clean_credentials_specifically(df['Credentials'])
        print("- Cleaned 'Credentials' (handled commas, ;#, case, duplicates).")

    # Fix 2: Enhance 'Provider Name' cleaning (suffixes, re-check nicknames)
    if 'Provider Name' in df.columns:
         df['Provider Name'] = enhance_name_cleaning(df['Provider Name'])
         print("- Enhanced 'Provider Name' cleaning (removed suffixes/nicknames).")

    # Fix 6: Fix 'Link to Website' format
    if 'Link to Website' in df.columns:
        df['Link to Website'] = fix_url(df['Link to Website'])
        print("- Standardized 'Link to Website' format.")
        
    # General Re-apply: Clean newlines and consolidate spaces (covers issue 8)
    print("- Re-applying newline removal and space consolidation...")
    df = clean_newlines_in_df(df) # Use function defined below

    # Final trim for all string columns
    print("- Final whitespace trim for all string columns...")
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()
        
    # Remove fully blank rows
    original_rows = df.shape[0]
    df.dropna(how='all', inplace=True)
    if df.shape[0] < original_rows:
        print(f"- Removed {original_rows - df.shape[0]} fully blank rows.")

    # Save the specifically cleaned file
    df.to_csv(filepath, index=False, encoding='utf-8')
    print(f"Successfully applied specific fixes and saved '{filepath}'. New shape: {df.shape}")

except Exception as e:
    print(f"ERROR: Failed to process file {filepath}. Error: {e}")

print("--- Specific processing finished. ---") 