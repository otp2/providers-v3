'''
Script: fix_final_pulse_mismatches.py
Corrects the last two specific name mismatches in provider_ids_for_mapping.csv.
'''
import pandas as pd
import os

mapping_file = '05_airtable_and_mapping/01_name_npi_airtable/provider_ids_for_mapping.csv'

# Final corrections (NPI: [Correct First Name, Correct Last Name])
corrections = {
    '1588405831': ['Kathleen', 'Ordiway'],  # Kathryn -> Kathleen
    '1720885601': ['Stephanie', 'Stanislawcyzk'] # Stanislawczyk -> Stanislawcyzk
}

try:
    df = pd.read_csv(mapping_file, dtype=str)
    expected_cols = ['National Provider Identifier (NPI)', 'First Name', 'Last Name']
    if not all(col in df.columns for col in expected_cols):
        print(f"ERROR: Mapping file {mapping_file} missing expected columns or wrong order. Expected: {expected_cols}, Found: {df.columns.tolist()}")
        exit(1)

    print(f"Applying final corrections to {mapping_file}...")
    correction_count = 0
    for npi, names in corrections.items():
        mask = df['National Provider Identifier (NPI)'] == npi
        if mask.any():
            df.loc[mask, 'First Name'] = names[0]
            df.loc[mask, 'Last Name'] = names[1]
            print(f"  Corrected NPI {npi} to {names[0]} {names[1]}")
            correction_count += 1
        else:
            print(f"  Warning: NPI {npi} not found for correction.")
            
    if correction_count > 0:
        df.to_csv(mapping_file, index=False)
        print(f"Successfully applied {correction_count} corrections to {mapping_file}.")
    else:
        print("No final corrections were needed or applied.")

except FileNotFoundError:
    print(f"ERROR: Mapping file not found: {mapping_file}")
except Exception as e:
    print(f"ERROR: Could not process mapping file {mapping_file}: {e}")

print("Script finished.") 