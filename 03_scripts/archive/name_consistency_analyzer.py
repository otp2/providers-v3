import pandas as pd
import re

# Define the source of truth file and its columns
TRUTH_FILE = "05_airtable_and_mapping/01_name_npi_airtable/provider_ids_for_mapping.csv"
TRUTH_NPI_COL = "National Provider Identifier (NPI)"
TRUTH_FIRST_NAME_COL = "First Name"
TRUTH_LAST_NAME_COL = "Last Name"

# Define other files to check and their relevant columns
FILES_TO_CHECK = {
    "northshore_names": {
        "path": "05_airtable_and_mapping/03_northshore/northshore_names.csv",
        "npi_col": None,  # No NPI in this file as per current structure
        "first_name_col": "First Name",
        "last_name_col": "Last Name",
        "full_name_col": None,
    },
    "providers_with_uids": {
        "path": "01_processed_data/main_provider_table/01_providers_with_uids.csv",
        "npi_col": "National Provider Identifier (NPI)",
        "first_name_col": "First Name",
        "last_name_col": "Last Name",
        "full_name_col": "Provider Full Name",
    },
    "pulse_mm": {
        "path": "00_source_data/pulse_data/pulse_mm/pulse_mm.csv",
        "npi_col": None, # NPI not directly available, would need lookup if strict matching is required
        "first_name_col": None, # To be extracted from Provider Name
        "last_name_col": None,  # To be extracted from Provider Name
        "full_name_col": "Provider Name",
    },
    "pulse_bhi": {
        "path": "00_source_data/pulse_data/pulse_bhi/pulse_bhi.csv",
        "npi_col": None, # NPI not directly available
        "first_name_col": None, # To be extracted from Provider Name
        "last_name_col": None,  # To be extracted from Provider Name
        "full_name_col": "Provider Name",
    },
    "pulse_counseling": {
        "path": "00_source_data/pulse_data/pulse_counseling/pulse_counseling.csv",
        "npi_col": None, # NPI not directly available
        "first_name_col": None, # To be extracted from Therapist Name
        "last_name_col": None,  # To be extracted from Therapist Name
        "full_name_col": "Therapist Name",
    },
}

def normalize_name_part(name_part):
    """Normalize a single part of a name (first or last)."""
    if pd.isna(name_part):
        return ""
    return str(name_part).lower().strip().replace('-', ' ').replace('.', '').replace(',', '')

def create_search_key(first_name, last_name):
    """Create a consistent search key from first and last name."""
    return f"{normalize_name_part(first_name)}_{normalize_name_part(last_name)}"

def clean_and_split_full_name(full_name_str):
    """
    Cleans and splits a full name string into first and last names.
    Attempts to remove credentials and handles common patterns.
    """
    if pd.isna(full_name_str):
        return "", ""

    name = str(full_name_str)
    # Remove common credentials and suffixes - this list can be expanded
    credentials_suffixes = [
        r",\s*PhD", r",\s*MD", r",\s*LCSW", r",\s*LCPC", r",\s*PsyD", r",\s*PMHNP-BC",
        r",\s*APN", r",\s*PA-C", r",\s*PA", r",\s*DO", r",\s*FNP-BC", r",\s*DNP", r",\s*APRN",
        r",\s*LSW", r",\s*LPC", r",\s*CADC", r",\s*BCBA", r",\s*QIDP", r",\s*LMFT",
        r"\s+\(.*\)", # content in parentheses
        r"\s+PhD", r"\s+MD", r"\s+LCSW", r"\s+LCPC", r"\s+PsyD", r"\s+PMHNP-BC",
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
    
    # Handle cases like "Lastname, Firstname"
    if ',' in name and len(parts) > 1 and parts[0].endswith(','):
        last_name = parts[0].replace(',', '').strip()
        first_name = " ".join(parts[1:]).strip()

    # Simple heuristic for multi-part last names if a comma wasn't the primary separator
    # This will need refinement based on actual data patterns.
    # For now, assume the first word is first name, rest is last.
    # If last name appears to be a single initial (e.g. "Smith J"), try to flip.
    if len(parts) > 1 and len(parts[-1]) == 1 and parts[-1].isalpha() and len(parts[0]) > 1 :
         # Check if the part before the initial could be a first name
         if len(parts) > 2 and len(parts[-2]) > 1: # e.g. Van Der Meer J
            pass # Keep as is
         elif len(parts) == 2 : # e.g. Smith J
            # This is tricky, could be "J Smith" or "Smith J."
            # For now, we assume it's Last, First Initial.
            # A more robust solution needs NPI linking or more rules.
             pass


    return first_name.strip(), last_name.strip()

def load_truth_data():
    """Loads and prepares the source of truth data."""
    try:
        df_truth = pd.read_csv(TRUTH_FILE, dtype={TRUTH_NPI_COL: str})
    except FileNotFoundError:
        print(f"ERROR: Source of truth file not found: {TRUTH_FILE}")
        return None

    df_truth[TRUTH_NPI_COL] = df_truth[TRUTH_NPI_COL].astype(str).str.strip().str.replace(r'\\.0$', '', regex=True)
    df_truth['search_key_name'] = df_truth.apply(
        lambda row: create_search_key(row[TRUTH_FIRST_NAME_COL], row[TRUTH_LAST_NAME_COL]), axis=1
    )
    df_truth['search_key_npi'] = df_truth[TRUTH_NPI_COL].fillna('').astype(str).str.lower().str.strip()
    # Create a dictionary for quick NPI to name lookup
    truth_npi_to_name = df_truth.set_index('search_key_npi')['search_key_name'].to_dict()
    truth_name_to_npi = df_truth.set_index('search_key_name')['search_key_npi'].to_dict()
    
    # For faster lookups
    truth_by_npi = df_truth.set_index('search_key_npi').to_dict('index')
    truth_by_name_key = df_truth.set_index('search_key_name').to_dict('index')
    
    return df_truth, truth_by_npi, truth_by_name_key, truth_npi_to_name, truth_name_to_npi

def analyze_file(file_key, config, truth_by_npi, truth_by_name_key, truth_npi_to_name, truth_name_to_npi):
    """Analyzes a single file against the source of truth."""
    print(f"\\n--- Analyzing File: {file_key} ({config['path']}) ---")
    
    try:
        df_check = pd.read_csv(config['path'], on_bad_lines='skip') # Skip bad lines for robustness
    except FileNotFoundError:
        print(f"ERROR: File not found: {config['path']}")
        return []
    except Exception as e:
        print(f"ERROR: Could not read {config['path']}: {e}")
        return []

    discrepancies = []

    for index, row_check in df_check.iterrows():
        npi_check_raw = row_check.get(config['npi_col']) if config['npi_col'] else None
        
        npi_check = None
        if pd.notna(npi_check_raw):
            npi_check_str = str(npi_check_raw).strip()
            # Remove trailing '.0' if NPI was incorrectly read as float
            npi_check_str = re.sub(r'\\.0$', '', npi_check_str)
            npi_check = npi_check_str.lower()
        

        first_name_check, last_name_check = "", ""

        if config['first_name_col'] and config['last_name_col']:
            first_name_check = row_check.get(config['first_name_col'], "")
            last_name_check = row_check.get(config['last_name_col'], "")
        elif config['full_name_col']:
            full_name_raw = row_check.get(config['full_name_col'])
            if pd.notna(full_name_raw):
                first_name_check, last_name_check = clean_and_split_full_name(full_name_raw)
            else: # Handle cases where the full_name_col might be empty
                 first_name_check, last_name_check = "", ""


        if not first_name_check and not last_name_check and not npi_check:
            # print(f"Skipping row {index+2} in {file_key} due to insufficient data (no name or NPI).")
            continue

        name_key_check = create_search_key(first_name_check, last_name_check)
        
        truth_entry_by_npi = truth_by_npi.get(npi_check) if npi_check else None
        truth_entry_by_name = truth_by_name_key.get(name_key_check) if name_key_check else None

        # Provider details from the file being checked
        current_file_details = {
            "file": file_key,
            "row_num": index + 2,
            "npi_in_file": npi_check_raw,
            "first_name_in_file": first_name_check,
            "last_name_in_file": last_name_check,
            "full_name_in_file": row_check.get(config['full_name_col'], "N/A")
        }
        
        # Standardization: Convert truth NPI to string for comparison if it's a number
        if truth_entry_by_npi and isinstance(truth_entry_by_npi.get(TRUTH_NPI_COL), (int, float)):
            truth_entry_by_npi[TRUTH_NPI_COL] = str(int(truth_entry_by_npi[TRUTH_NPI_COL]))
            
        if truth_entry_by_name and isinstance(truth_entry_by_name.get(TRUTH_NPI_COL), (int, float)):
             truth_entry_by_name[TRUTH_NPI_COL] = str(int(truth_entry_by_name[TRUTH_NPI_COL]))


        if npi_check and truth_entry_by_npi: # Match by NPI
            truth_first = truth_entry_by_npi.get(TRUTH_FIRST_NAME_COL)
            truth_last = truth_entry_by_npi.get(TRUTH_LAST_NAME_COL)
            truth_name_key = create_search_key(truth_first, truth_last)

            if name_key_check != truth_name_key:
                discrepancies.append({
                    **current_file_details,
                    "issue": "NPI Match, Name Mismatch",
                    "truth_npi": truth_entry_by_npi.get(TRUTH_NPI_COL),
                    "truth_first_name": truth_first,
                    "truth_last_name": truth_last,
                })
        elif name_key_check and truth_entry_by_name: # Match by Name
            truth_npi = truth_entry_by_name.get(TRUTH_NPI_COL)
            if npi_check and str(npi_check).lower() != str(truth_npi).lower():
                discrepancies.append({
                    **current_file_details,
                    "issue": "Name Match, NPI Mismatch",
                    "truth_npi": truth_npi,
                    "truth_first_name": truth_entry_by_name.get(TRUTH_FIRST_NAME_COL),
                    "truth_last_name": truth_entry_by_name.get(TRUTH_LAST_NAME_COL),
                })
            elif not npi_check and pd.notna(truth_npi) and str(truth_npi).strip() != "": # NPI missing in current file but exists in truth
                discrepancies.append({
                    **current_file_details,
                    "issue": "Name Match, NPI Missing in File (Present in Truth)",
                    "truth_npi": truth_npi,
                    "truth_first_name": truth_entry_by_name.get(TRUTH_FIRST_NAME_COL),
                    "truth_last_name": truth_entry_by_name.get(TRUTH_LAST_NAME_COL),
                })
        elif npi_check: # NPI in file but not found in truth
            discrepancies.append({
                **current_file_details,
                "issue": "NPI in File Not Found in Truth File",
            })
        elif name_key_check and name_key_check != "_": # Name in file but not found in truth (and not an empty name key)
             discrepancies.append({
                **current_file_details,
                "issue": "Name in File Not Found in Truth File",
            })
        # else: No NPI and no valid name in current file, or no match at all - already handled by continue or no action

    if discrepancies:
        print(f"Found {len(discrepancies)} discrepancies in {file_key}:")
        for i, d in enumerate(discrepancies):
            print(f"  Discrepancy {i+1}:")
            print(f"    File Row: {d['row_num']}")
            print(f"    Issue: {d['issue']}")
            print(f"    Details in File: NPI='{d.get('npi_in_file', 'N/A')}', Name='{d.get('first_name_in_file')} {d.get('last_name_in_file')}' (Full: '{d.get('full_name_in_file', 'N/A')}')")
            if "truth_npi" in d:
                print(f"    Details in Truth:  NPI='{d['truth_npi']}', Name='{d['truth_first_name']} {d['truth_last_name']}'")
    else:
        print(f"No discrepancies found in {file_key} based on NPI or exact name match after normalization.")
    return discrepancies

def main():
    print("Starting Provider Name Consistency Analysis...")
    
    load_result = load_truth_data()
    if load_result is None:
        return
    
    df_truth, truth_by_npi, truth_by_name_key, truth_npi_to_name, truth_name_to_npi = load_result
    
    all_discrepancies = []

    for file_key, config in FILES_TO_CHECK.items():
        file_discrepancies = analyze_file(file_key, config, truth_by_npi, truth_by_name_key, truth_npi_to_name, truth_name_to_npi)
        all_discrepancies.extend(file_discrepancies)

    print("\\n--- Summary of Analysis ---")
    if not all_discrepancies:
        print("No discrepancies found across all checked files. Names and NPIs appear consistent with the source of truth.")
    else:
        print(f"Found a total of {len(all_discrepancies)} discrepancies across all files.")
        # Further summary can be added here, e.g., by issue type or by file.
        
        # Example: Group by issue type
        issues_summary = {}
        for d in all_discrepancies:
            issue_type = d['issue']
            issues_summary[issue_type] = issues_summary.get(issue_type, 0) + 1
        
        print("\\nDiscrepancies by Type:")
        for issue, count in issues_summary.items():
            print(f"  - {issue}: {count}")

    print("\\nAnalysis Complete.")
    print(f"Source of Truth: {TRUTH_FILE}")
    print("Checked Files:")
    for f_key, f_config in FILES_TO_CHECK.items():
        print(f"  - {f_key}: {f_config['path']}")

    # You can save 'all_discrepancies' to a CSV or JSON file for easier review
    # For example:
    # import json
    # with open("discrepancy_report.json", "w") as f:
    #     json.dump(all_discrepancies, f, indent=4)
    # print("\\nDiscrepancy report saved to discrepancy_report.json")

    # df_discrepancies = pd.DataFrame(all_discrepancies)
    # if not df_discrepancies.empty:
    #     df_discrepancies.to_csv("discrepancy_report.csv", index=False)
    #     print("\\nDiscrepancy report saved to discrepancy_report.csv")


if __name__ == "__main__":
    main() 