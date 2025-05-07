import pandas as pd
import numpy as np
import re

# --- Configuration ---
TRUTH_FILE = "01_processed_data/new_provider_truth_file.csv"
SALESFORCE_CREDS_FILE = "02_salesforce_picklist/salesforce_credentials.csv"
PULSE_BHI_FILE = "00_source_data/pulse_data/pulse_bhi/pulse_bhi.csv"
PULSE_COUNSELING_FILE = "00_source_data/pulse_data/pulse_counseling/pulse_counseling.csv"
PULSE_MM_FILE = "00_source_data/pulse_data/pulse_mm/pulse_mm.csv"
LEGACY_AIRTABLE_FILE = "00_source_data/airtable_monolithic/Providers-All Providers.csv"
OUTPUT_FILE = "01_processed_data/new_provider_truth_file.csv" # Overwrite

# --- Column Names ---
# Truth File
COL_TRUTH_FIRST = "First Name"
COL_TRUTH_LAST = "Last Name"
COL_TRUTH_NPI = "NPI Number"
COL_SF_CREDENTIAL_OUT = "Salesforce Credential"

# Salesforce Credentials File
COL_SF_CRED = "salesforce_credentials" # Header of the single column

# Pulse Files
COL_PULSE_PROVIDER_NAME = "Provider Name"
COL_PULSE_CREDENTIALS = "Credentials"

# Legacy Airtable File
COL_LEGACY_FIRST = "First Name"
COL_LEGACY_LAST = "Last Name"
COL_LEGACY_NPI = "National Provider Identifier (NPI)"
COL_LEGACY_CREDENTIALS = "Credentials"


# --- Helper Functions (copied/adapted from previous scripts) ---
def normalize_name_part(name_part):
    if pd.isna(name_part):
        return ""
    normalized = str(name_part).lower().strip()
    normalized = normalized.replace('-', ' ').replace('.', ' ').replace(',', ' ')
    return ' '.join(normalized.split())

def create_name_key(first_name, last_name):
    return f"{normalize_name_part(first_name)}_{normalize_name_part(last_name)}"

def clean_and_split_full_name(full_name_str):
    if pd.isna(full_name_str):
        return "", ""
    name = str(full_name_str)
    # More comprehensive list of credentials/suffixes to remove for name splitting
    credentials_suffixes = [
        r",\s*PhD", r",\s*MD", r",\s*LCSW", r",\s*LCPC", r",\s*PsyD", r",\s*PMHNP-BC",
        r",\s*APN", r",\s*PA-C", r",\s*PA", r",\s*DO", r",\s*FNP-BC", r",\s*DNP", r",\s*APRN",
        r",\s*LSW", r",\s*LPC", r",\s*CADC", r",\s*BCBA", r",\s*QIDP", r",\s*LMFT", r",\s*RN",
        r",\s*MA", r",\s*MS", r",\s*MBA", r",\s*MEd", r",\s*MSW", r",\s*MPH",
        r"\s+\(.*\)", # Content in parentheses
        # Common standalone credentials (ensure space prefixed)
        r"\s+PhD", r"\s+MD", r"\s+LCSW", r"\s+LCPC", r"\s+PsyD", r"\s+PMHNP-BC",
        r"\s+APN", r"\s+PA-C", r"\s+PA", r"\s+DO", r"\s+FNP-BC", r"\s+DNP", r"\s+APRN",
        r"\s+LSW", r"\s+LPC", r"\s+CADC", r"\s+BCBA", r"\s+QIDP", r"\s+LMFT", r"\s+RN",
        r"\s+MA", r"\s+MS", r"\s+MBA", r"\s+MEd", r"\s+MSW", r"\s+MPH"
    ]
    for pattern in credentials_suffixes:
        name = re.sub(pattern, "", name, flags=re.IGNORECASE)
    
    # Remove bracketed content e.g. [Trial Staff]
    name = re.sub(r"\s*\[.*\]", "", name)
    name = name.strip()
    
    parts = name.split(',')
    if len(parts) == 2: # Likely "Last, First"
        last_name = parts[0].strip()
        first_name = parts[1].strip()
    else: # Assume "First Last" or "First Middle Last"
        name_parts = name.split()
        if not name_parts:
            return "", ""
        first_name = name_parts[0]
        last_name = " ".join(name_parts[1:])
        
    return first_name.strip(), last_name.strip()

def parse_credentials(cred_str):
    """Parses a raw credential string into a list of individual credentials."""
    if pd.isna(cred_str):
        return []
    # Delimiters: ',', ';', '/', '&', ' and ' (with spaces), '#'
    # Removed: cred_str_spaced = re.sub(r"([a-zA-Z])([A-Z])", r"\1 \2", str(cred_str))
    
    # Replace multiple delimiters with a single common one (e.g., '|')
    # Normalize ' and ' to a delimiter as well.
    normalized_str = re.sub(r'\s+and\s+', '|', str(cred_str), flags=re.IGNORECASE)
    normalized_str = re.sub(r'[,;/&#]', '|', normalized_str)
    
    creds = [c.strip().upper().replace(".", "") for c in normalized_str.split('|') if c.strip()]
    return creds

# --- Load Data ---
def load_data():
    data = {}
    try:
        data['truth'] = pd.read_csv(TRUTH_FILE, dtype={COL_TRUTH_NPI: str})
        data['truth'][COL_TRUTH_NPI] = data['truth'][COL_TRUTH_NPI].fillna('').astype(str)
        print(f"Successfully loaded truth file: {TRUTH_FILE}")
    except FileNotFoundError:
        print(f"ERROR: Truth file not found: {TRUTH_FILE}")
        return None
    except Exception as e:
        print(f"Error loading truth file {TRUTH_FILE}: {e}")
        return None

    try:
        df_sf_creds = pd.read_csv(SALESFORCE_CREDS_FILE)
        # Store original values for assignment, and cleaned values for matching
        data['salesforce_creds_original'] = df_sf_creds[COL_SF_CRED].dropna().unique().tolist()
        data['salesforce_creds_set'] = {str(cred).strip().upper().replace(".", "") for cred in data['salesforce_creds_original']}
        print(f"Successfully loaded Salesforce credentials: {SALESFORCE_CREDS_FILE} ({len(data['salesforce_creds_set'])} unique)")
    except FileNotFoundError:
        print(f"ERROR: Salesforce credentials file not found: {SALESFORCE_CREDS_FILE}")
        return None
    except Exception as e:
        print(f"Error loading Salesforce credentials {SALESFORCE_CREDS_FILE}: {e}")
        return None

    source_files_config = {
        'bhi': {'path': PULSE_BHI_FILE, 'name_col': COL_PULSE_PROVIDER_NAME, 'cred_col': COL_PULSE_CREDENTIALS, 'npi_col': None},
        'counseling': {'path': PULSE_COUNSELING_FILE, 'name_col': COL_PULSE_PROVIDER_NAME, 'cred_col': COL_PULSE_CREDENTIALS, 'npi_col': None},
        'mm': {'path': PULSE_MM_FILE, 'name_col': COL_PULSE_PROVIDER_NAME, 'cred_col': COL_PULSE_CREDENTIALS, 'npi_col': None},
        'legacy': {'path': LEGACY_AIRTABLE_FILE, 'first_col': COL_LEGACY_FIRST, 'last_col': COL_LEGACY_LAST, 'cred_col': COL_LEGACY_CREDENTIALS, 'npi_col': COL_LEGACY_NPI}
    }

    data['source_creds_by_name'] = {}
    data['source_creds_by_npi'] = {}

    for key, config in source_files_config.items():
        try:
            df_source = pd.read_csv(config['path'], dtype={config.get('npi_col'): str} if config.get('npi_col') else None)
            print(f"Loaded {key} data from {config['path']}")
            
            for _, row in df_source.iterrows():
                creds_raw = row.get(config['cred_col'])
                if pd.isna(creds_raw):
                    continue

                npi = None
                if config.get('npi_col') and pd.notna(row.get(config['npi_col'])):
                    npi = str(row.get(config['npi_col'])).strip().replace('.0', '')
                    if npi:
                        if npi not in data['source_creds_by_npi']:
                            data['source_creds_by_npi'][npi] = []
                        data['source_creds_by_npi'][npi].append(creds_raw)
                
                name_key_source = None
                if config.get('name_col'): # Pulse files with "Provider Name"
                    first, last = clean_and_split_full_name(row.get(config['name_col']))
                    if first or last: # Ensure at least one part of the name is present
                         name_key_source = create_name_key(first, last)
                elif config.get('first_col') and config.get('last_col'): # Legacy file
                    first = row.get(config['first_col'])
                    last = row.get(config['last_col'])
                    if pd.notna(first) or pd.notna(last):
                        name_key_source = create_name_key(first, last)
                
                if name_key_source and name_key_source != "_":
                    if name_key_source not in data['source_creds_by_name']:
                        data['source_creds_by_name'][name_key_source] = []
                    data['source_creds_by_name'][name_key_source].append(creds_raw)

        except FileNotFoundError:
            print(f"Warning: Source file not found: {config['path']}. Skipping.")
        except Exception as e:
            print(f"Warning: Error loading source file {config['path']}: {e}. Skipping.")
            
    print(f"Created source credential lookups: {len(data['source_creds_by_name'])} by name, {len(data['source_creds_by_npi'])} by NPI.")
    return data

# --- Main Logic ---
def main():
    loaded_data = load_data()
    if not loaded_data:
        return

    df_truth = loaded_data['truth']
    sf_creds_set = loaded_data['salesforce_creds_set']
    # Create a lookup from cleaned sf_cred to original sf_cred
    sf_creds_original_map = {str(cred).strip().upper().replace(".", ""): cred for cred in loaded_data['salesforce_creds_original']}
    
    source_creds_by_name = loaded_data['source_creds_by_name']
    source_creds_by_npi = loaded_data['source_creds_by_npi']

    df_truth[COL_SF_CREDENTIAL_OUT] = ""
    df_truth['name_key_truth'] = df_truth.apply(lambda row: create_name_key(row[COL_TRUTH_FIRST], row[COL_TRUTH_LAST]), axis=1)
    
    credentials_added_count = 0
    processed_aaron_huth_debug = False # Debug flag

    for index, row_truth in df_truth.iterrows():
        truth_npi = str(row_truth[COL_TRUTH_NPI]).strip()
        truth_first_name = row_truth[COL_TRUTH_FIRST]
        truth_last_name = row_truth[COL_TRUTH_LAST]
        truth_name_key = row_truth['name_key_truth']
        
        # --- DEBUGGING FOR AARON HUTH ---
        is_aaron_huth = (truth_first_name == "Aaron" and truth_last_name == "Huth")
        if is_aaron_huth and not processed_aaron_huth_debug:
            print(f"--- DEBUG START: Aaron Huth ---")
            print(f"Truth NPI: '{truth_npi}', Truth Name Key: '{truth_name_key}'")

        found_sf_cred = None
        
        # Gather all potential credential strings for this provider
        all_raw_creds_for_provider = []
        
        if truth_npi and truth_npi in source_creds_by_npi:
            all_raw_creds_for_provider.extend(source_creds_by_npi[truth_npi])
            if is_aaron_huth and not processed_aaron_huth_debug:
                print(f"  NPI '{truth_npi}' found in source_creds_by_npi. Creds: {source_creds_by_npi[truth_npi]}")
        elif is_aaron_huth and not processed_aaron_huth_debug:
            print(f"  NPI '{truth_npi}' NOT found in source_creds_by_npi.")
            
        if truth_name_key in source_creds_by_name:
            all_raw_creds_for_provider.extend(source_creds_by_name[truth_name_key])
            if is_aaron_huth and not processed_aaron_huth_debug:
                print(f"  Name key '{truth_name_key}' found in source_creds_by_name. Creds: {source_creds_by_name[truth_name_key]}")
        elif is_aaron_huth and not processed_aaron_huth_debug:
             print(f"  Name key '{truth_name_key}' NOT found in source_creds_by_name.")
        
        # Deduplicate raw credential strings to avoid parsing the same string multiple times
        unique_raw_creds = list(set(all_raw_creds_for_provider))

        if is_aaron_huth and not processed_aaron_huth_debug:
            print(f"  Unique raw credentials collected: {unique_raw_creds}")

        if not unique_raw_creds:
            # print(f"No source credentials found for {truth_name_key} / NPI {truth_npi}")
            if is_aaron_huth and not processed_aaron_huth_debug:
                print(f"  No unique raw credentials found. Skipping further processing for Aaron Huth debug.")
                processed_aaron_huth_debug = True # Mark as processed for debug
            continue

        for raw_cred_str in unique_raw_creds:
            if is_aaron_huth and not processed_aaron_huth_debug:
                print(f"  Processing raw_cred_str: '{raw_cred_str}'")
            parsed_list = parse_credentials(raw_cred_str)
            if is_aaron_huth and not processed_aaron_huth_debug:
                print(f"    Parsed list: {parsed_list}")
            for cred_candidate in parsed_list:
                if is_aaron_huth and not processed_aaron_huth_debug:
                    print(f"      Checking cred_candidate: '{cred_candidate}'")
                # cred_candidate is already uppercased and stripped by parse_credentials
                if cred_candidate in sf_creds_set:
                    found_sf_cred = sf_creds_original_map[cred_candidate] # Get the original casing
                    if is_aaron_huth and not processed_aaron_huth_debug:
                        print(f"        MATCH FOUND! Salesforce original: '{found_sf_cred}'")
                    break # Found a valid Salesforce credential for this source string
                elif is_aaron_huth and not processed_aaron_huth_debug:
                    print(f"        No match in sf_creds_set.")
            if found_sf_cred:
                break # Found a valid Salesforce credential for this provider
        
        if is_aaron_huth and not processed_aaron_huth_debug:
            print(f"  Final found_sf_cred for Aaron Huth: {found_sf_cred}")
            processed_aaron_huth_debug = True # Mark as processed for debug
            print(f"--- DEBUG END: Aaron Huth ---")


        if found_sf_cred:
            df_truth.loc[index, COL_SF_CREDENTIAL_OUT] = found_sf_cred
            credentials_added_count += 1
            # print(f"Assigned '{found_sf_cred}' to {truth_name_key} / NPI {truth_npi}")
        # else:
            # print(f"No Salesforce credential match for {truth_name_key} / NPI {truth_npi} from raw: {unique_raw_creds}")


    print(f"Enrichment complete. Added Salesforce credentials to {credentials_added_count} providers.")

    # --- Final Cleanup and Save ---
    final_cols = [col for col in df_truth.columns if col != 'name_key_truth'] # Keep original columns + new one
    if COL_SF_CREDENTIAL_OUT not in final_cols: # Should be there, but as a safeguard
        final_cols.append(COL_SF_CREDENTIAL_OUT)
        
    df_output = df_truth[final_cols]

    try:
        df_output.to_csv(OUTPUT_FILE, index=False)
        print(f"Successfully updated and saved truth file to: {OUTPUT_FILE}")
    except Exception as e:
        print(f"ERROR saving updated file: {e}")

if __name__ == "__main__":
    main() 