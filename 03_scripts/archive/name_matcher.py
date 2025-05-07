import pandas as pd

def normalize_name_parts(df, first_col, last_col):
    # Ensure columns exist before trying to access them
    if first_col not in df.columns:
        raise KeyError(f"Column '{first_col}' not found in DataFrame.")
    if last_col not in df.columns:
        raise KeyError(f"Column '{last_col}' not found in DataFrame.")
        
    df_copy = df.copy()
    df_copy['normalized_first'] = df_copy[first_col].astype(str).str.lower().str.strip()
    df_copy['normalized_last'] = df_copy[last_col].astype(str).str.lower().str.strip()
    # Create a search key by concatenating and removing all spaces
    df_copy['search_key'] = (df_copy['normalized_first'] + df_copy['normalized_last']).str.replace(' ', '', regex=False)
    return df_copy

# File paths
northshore_file = '05_airtable_and_mapping/03_northshore/northshore_names.csv'
provider_file = '05_airtable_and_mapping/01_name_npi_airtable/provider_ids_for_mapping.csv'

try:
    northshore_df = pd.read_csv(northshore_file)
    provider_df = pd.read_csv(provider_file)
except FileNotFoundError as e:
    print(f"ERROR: Could not find a file: {e}")
    exit()
except Exception as e:
    print(f"ERROR: Could not read CSV files: {e}")
    exit()

# Store original names for reporting
northshore_df['original_full_name_ns'] = northshore_df['First Name'] + " " + northshore_df['Last Name']
provider_df['original_full_name_p'] = provider_df['First Name'] + " " + provider_df['Last Name']

# Normalize names
try:
    northshore_df = normalize_name_parts(northshore_df, 'First Name', 'Last Name')
    provider_df = normalize_name_parts(provider_df, 'First Name', 'Last Name')
except KeyError as e:
    print(f"ERROR: Missing expected column in one of the CSVs. {e}")
    exit()


# Initialize match tracking
northshore_df['matched_to_provider_name'] = None
northshore_df['match_type'] = None
provider_df['matched_from_northshore'] = False

# --- Phase 1: Exact Matches ---
for ns_idx, ns_row in northshore_df.iterrows():
    for p_idx, p_row in provider_df.iterrows():
        if not provider_df.at[p_idx, 'matched_from_northshore']: # Only match if provider is not already matched
            if ns_row['search_key'] == p_row['search_key']:
                northshore_df.at[ns_idx, 'matched_to_provider_name'] = p_row['original_full_name_p']
                northshore_df.at[ns_idx, 'match_type'] = 'Exact'
                provider_df.at[p_idx, 'matched_from_northshore'] = True
                break # Move to next northshore name

# --- Phase 2: Substring Matches for remaining unmatched Northshore names ---
for ns_idx, ns_row in northshore_df.iterrows():
    if pd.isna(ns_row['match_type']): # If not already matched exactly
        for p_idx, p_row in provider_df.iterrows():
            if not provider_df.at[p_idx, 'matched_from_northshore']: # Only match if provider is not already matched
                ns_key = ns_row['search_key']
                p_key = p_row['search_key']
                if ns_key and p_key: # Ensure keys are not empty
                    if ns_key.startswith(p_key) or p_key.startswith(ns_key):
                        northshore_df.at[ns_idx, 'matched_to_provider_name'] = p_row['original_full_name_p']
                        northshore_df.at[ns_idx, 'match_type'] = 'Substring'
                        provider_df.at[p_idx, 'matched_from_northshore'] = True
                        break # Move to next northshore name

print("--- Name Matching Report (First/Last Name Only) ---")

print("\nMatched names from Northshore list ('northshore_names.csv'):")
matched_ns = northshore_df[northshore_df['match_type'].notna()]
if not matched_ns.empty:
    for _, row in matched_ns.iterrows():
        print(f"- '{row['original_full_name_ns']}' matched to '{row['matched_to_provider_name']}' - Type: {row['match_type']}")
else:
    print("  No names from Northshore list were matched.")

print("\nUnmatched names from Northshore list (not found in 'provider_ids_for_mapping.csv'):")
unmatched_ns = northshore_df[northshore_df['match_type'].isna()]
if not unmatched_ns.empty:
    for _, row in unmatched_ns.iterrows():
        print(f"- '{row['original_full_name_ns']}'")
else:
    print("  All names from Northshore list were matched.")

print("\nUnmatched names from Provider IDs list ('provider_ids_for_mapping.csv') (not found in 'northshore_names.csv'):")
unmatched_p = provider_df[~provider_df['matched_from_northshore']]
if not unmatched_p.empty:
    for _, row in unmatched_p.iterrows():
        print(f"- '{row['original_full_name_p']}'") # NPI removed here
else:
    print("  All names from Provider IDs list were matched in the Northshore list.")

print("\n--- End of Report ---") 