import pandas as pd
import numpy as np
import re

# --- Configuration ---
TRUTH_FILE = "01_processed_data/new_provider_truth_file.csv"
BHI_FILE = "00_source_data/pulse_data/pulse_bhi/pulse_bhi.csv"
LEGACY_AIRTABLE_FILE = "00_source_data/airtable_monolithic/Providers-All Providers.csv"
OUTPUT_FILE = "01_processed_data/new_provider_truth_file.csv" # Overwrite the existing file

# --- Column Names --- 
# Truth File Columns (Existing)
COL_UIUD = "uiud"
COL_FIRST = "First Name"
COL_LAST = "Last Name"
COL_NPI = "NPI Number"
COL_LABEL = "Internal Label"
# New Columns to Add
COL_PHONE = "Phone Number"
COL_WEB = "Web Address"

# BHI File Columns
BHI_PROVIDER_NAME = "Provider Name"
BHI_PHONE = "Patient Facing Number"

# Legacy File Columns
LEGACY_FIRST = "First Name"
LEGACY_LAST = "Last Name"
LEGACY_NPI = "National Provider Identifier (NPI)"
LEGACY_WEB = "Profile Link - Legacy Site"

# --- Helper Functions --- 
def normalize_name_part(name_part):
    """Normalize a single part of a name (first or last)."""
    if pd.isna(name_part):
        return ""
    normalized = str(name_part).lower().strip()
    normalized = normalized.replace('-', ' ').replace('.', ' ').replace(',', ' ')
    return ' '.join(normalized.split())

def create_name_key(first_name, last_name):
    """Create a consistent search key from first and last name."""
    return f"{normalize_name_part(first_name)}_{normalize_name_part(last_name)}"

def clean_and_split_full_name(full_name_str):
    """(Copied/adapted from name_consistency_analyzer.py) Cleans and splits a full name string."""
    if pd.isna(full_name_str):
        return "", ""
    name = str(full_name_str)
    credentials_suffixes = [
        r",\s*PhD", r",\s*MD", r",\s*LCSW", r",\s*LCPC", r",\s*PsyD", r",\s*PMHNP-BC",
        r",\s*APN", r",\s*PA-C", r",\s*PA", r",\s*DO", r",\s*FNP-BC", r",\s*DNP", r",\s*APRN",
        r",\s*LSW", r",\s*LPC", r",\s*CADC", r",\s*BCBA", r",\s*QIDP", r",\s*LMFT",
        r"\s+\(.*\)", r"\s+PhD", r"\s+MD", r"\s+LCSW", r"\s+LCPC", r"\s+PsyD", r"\s+PMHNP-BC",
        r"\s+APN", r"\s+PA-C", r"\s+PA", r"\s+DO", r"\s+FNP-BC", r"\s+DNP", r"\s+APRN",
        r"\s+LSW", r"\s+LPC", r"\s+CADC", r"\s+BCBA", r"\s+QIDP", r"\s+LMFT"
    ]
    for pattern in credentials_suffixes:
        name = re.sub(pattern, "", name, flags=re.IGNORECASE)
    name = name.strip()
    parts = name.split()
    if not parts:
        return "", ""
    first_name = parts[0]
    last_name = " ".join(parts[1:])
    if ',' in name and len(parts) > 1 and parts[0].endswith(','):
        last_name = parts[0].replace(',', '').strip()
        first_name = " ".join(parts[1:]).strip()
    return first_name.strip(), last_name.strip()

def format_phone_number(number_str):
    """Cleans and formats a phone number string to (XXX) XXX-XXXX."""
    if pd.isna(number_str):
        return ""
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', str(number_str))
    # Check if we have 10 digits
    if len(digits) == 10:
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
    elif len(digits) == 11 and digits.startswith('1'): # Handle leading 1
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:11]}"
    else:
        # print(f"Skipping invalid phone number format: {number_str}")
        return "" # Return empty string for invalid formats

# --- Main Logic --- 
def main():
    print(f"Loading current truth file: {TRUTH_FILE}")
    try:
        df_truth = pd.read_csv(TRUTH_FILE, dtype={COL_NPI: str})
        df_truth[COL_NPI] = df_truth[COL_NPI].fillna('').astype(str) # Ensure NPI is string
    except FileNotFoundError:
        print(f"ERROR: Truth file not found: {TRUTH_FILE}")
        return
    except Exception as e:
        print(f"ERROR loading truth file: {e}")
        return

    print(f"Loading BHI file for phone numbers: {BHI_FILE}")
    try:
        df_bhi = pd.read_csv(BHI_FILE)
    except FileNotFoundError:
        print(f"ERROR: BHI file not found: {BHI_FILE}")
        return # Cannot proceed without phone source
    except Exception as e:
        print(f"ERROR loading BHI file: {e}")
        return
        
    print(f"Loading Legacy Airtable file for web addresses: {LEGACY_AIRTABLE_FILE}")
    try:
        df_legacy = pd.read_csv(LEGACY_AIRTABLE_FILE, dtype={LEGACY_NPI: str})
        df_legacy[LEGACY_NPI] = df_legacy[LEGACY_NPI].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).fillna('')
    except FileNotFoundError:
        print(f"Warning: Legacy Airtable file not found ({LEGACY_AIRTABLE_FILE}). Web addresses will not be populated.")
        df_legacy = pd.DataFrame() # Create empty df to avoid errors later
    except Exception as e:
        print(f"Warning: Error loading Legacy Airtable file ({LEGACY_AIRTABLE_FILE}): {e}. Web addresses might not be populated.")
        df_legacy = pd.DataFrame()

    # --- Create BHI Phone Lookup --- 
    bhi_phone_lookup = {}
    if not df_bhi.empty:
        for index, row in df_bhi.iterrows():
            provider_name_raw = row.get(BHI_PROVIDER_NAME)
            phone_raw = row.get(BHI_PHONE)
            
            first, last = clean_and_split_full_name(provider_name_raw)
            name_key = create_name_key(first, last)
            formatted_phone = format_phone_number(phone_raw)
            
            if name_key != "_" and formatted_phone: # Only add if valid name and phone
                if name_key in bhi_phone_lookup:
                    # Handle potential duplicate names in BHI file - keep first found for simplicity
                    # print(f"Warning: Duplicate name key '{name_key}' found in BHI file. Keeping first phone number.")
                    pass 
                else:
                    bhi_phone_lookup[name_key] = formatted_phone
        print(f"Created BHI phone lookup with {len(bhi_phone_lookup)} entries.")

    # --- Create Legacy Web Address Lookups --- 
    legacy_web_lookup_by_name = {}
    legacy_web_lookup_by_npi = {}
    if not df_legacy.empty and LEGACY_WEB in df_legacy.columns:
        df_legacy_filtered = df_legacy[[LEGACY_FIRST, LEGACY_LAST, LEGACY_NPI, LEGACY_WEB]].dropna(subset=[LEGACY_WEB]).copy()
        df_legacy_filtered['name_key'] = df_legacy_filtered.apply(lambda row: create_name_key(row[LEGACY_FIRST], row[LEGACY_LAST]), axis=1)
        
        # NPI lookup (more reliable) - keep first NPI match if duplicates exist
        legacy_web_lookup_by_npi = df_legacy_filtered.drop_duplicates(subset=[LEGACY_NPI], keep='first').set_index(LEGACY_NPI)[LEGACY_WEB].to_dict()
        
        # Name lookup (fallback)
        legacy_web_lookup_by_name = df_legacy_filtered.drop_duplicates(subset=['name_key'], keep='first').set_index('name_key')[LEGACY_WEB].to_dict()
        print(f"Created Legacy web address lookups: {len(legacy_web_lookup_by_npi)} by NPI, {len(legacy_web_lookup_by_name)} by Name.")
    else:
        print("Skipping Legacy web address lookup creation (file missing, empty, or lacks web column).")

    # --- Enrich Truth DataFrame --- 
    df_truth[COL_PHONE] = ""
    df_truth[COL_WEB] = ""
    df_truth['name_key'] = df_truth.apply(lambda row: create_name_key(row[COL_FIRST], row[COL_LAST]), axis=1)

    phone_added_count = 0
    web_added_by_npi_count = 0
    web_added_by_name_count = 0

    for index, row in df_truth.iterrows():
        name_key = row['name_key']
        npi = row[COL_NPI]
        label = row.get(COL_LABEL, '')

        # Add Phone Number (only for BHI)
        # Check if label contains 'BHI' (case-insensitive)
        if isinstance(label, str) and 'bhi' in label.lower():
            phone = bhi_phone_lookup.get(name_key)
            if phone:
                df_truth.loc[index, COL_PHONE] = phone
                phone_added_count += 1

        # Add Web Address (prioritize NPI match)
        web_address = None
        if npi and npi in legacy_web_lookup_by_npi:
            web_address = legacy_web_lookup_by_npi[npi]
            df_truth.loc[index, COL_WEB] = web_address
            web_added_by_npi_count += 1
        elif name_key in legacy_web_lookup_by_name: # Fallback to name match
             web_address = legacy_web_lookup_by_name[name_key]
             df_truth.loc[index, COL_WEB] = web_address
             web_added_by_name_count += 1

    print(f"Enrichment complete. Added {phone_added_count} phone numbers (for BHI).")
    print(f"Added {web_added_by_npi_count} web addresses via NPI match, {web_added_by_name_count} via name match.")

    # --- Final Cleanup and Save --- 
    final_columns = [COL_UIUD, COL_FIRST, COL_LAST, COL_NPI, COL_LABEL, COL_PHONE, COL_WEB]
    df_output = df_truth[final_columns]

    try:
        df_output.to_csv(OUTPUT_FILE, index=False)
        print(f"Successfully updated and saved truth file to: {OUTPUT_FILE}")
    except Exception as e:
        print(f"ERROR saving updated file: {e}")

if __name__ == "__main__":
    main() 