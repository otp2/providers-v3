import pandas as pd
import os

# Define output directory
output_base_dir = 'picklists_generated'
# Ensure this base directory is created by a separate terminal command

# Load the CSV
try:
    df = pd.read_csv('airtable/Providers-All Providers.csv', low_memory=False)
except FileNotFoundError:
    print("Error: 'airtable/Providers-All Providers.csv' not found. Make sure the file exists in the 'airtable' directory.")
    exit()

def generate_picklist(column_name, output_subdir, output_filename):
    if column_name not in df.columns:
        print(f"Warning: Column '{column_name}' not found in CSV. Skipping.")
        return

    # Construct the full path for the output directory
    full_output_dir = os.path.join(output_base_dir, output_subdir)
    # Ensure subdirectories are created by separate terminal commands

    unique_items = set()
    # Drop NaN values before processing and convert to string to handle mixed types gracefully
    series = df[column_name].dropna().astype(str)

    for item_list_str in series:
        # Normalize delimiters: replace semicolon with comma, then split by comma
        normalized_str = item_list_str.replace(';', ',')
        items = normalized_str.split(',')
        for item in items:
            stripped_item = item.strip()
            if stripped_item: # Avoid adding empty strings
                unique_items.add(stripped_item)

    if not unique_items:
        print(f"No unique items found for column '{column_name}' after processing. Skipping file generation.")
        return

    sorted_items = sorted(list(unique_items))

    output_file_path = os.path.join(full_output_dir, output_filename)
    try:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            for item in sorted_items:
                f.write(f"{item}\n")
        print(f"Generated picklist for '{column_name}' in '{output_file_path}'")
    except Exception as e:
        print(f"Error writing picklist for '{column_name}' to '{output_file_path}': {e}")


columns_to_process = [
    # Provider Demographics and Attributes
    {'col': 'Legacy Region', 'subdir': 'Provider_Attributes', 'file': 'legacy_region.txt'},
    {'col': 'Gender', 'subdir': 'Provider_Attributes', 'file': 'gender.txt'},
    {'col': 'Languages', 'subdir': 'Provider_Attributes', 'file': 'languages.txt'},
    {'col': 'Ages Seen', 'subdir': 'Provider_Attributes', 'file': 'ages_seen.txt'},

    # Clinical Information
    {'col': 'Web Specialty', 'subdir': 'Clinical_Information', 'file': 'web_specialty.txt'},
    {'col': 'Provider Type', 'subdir': 'Clinical_Information', 'file': 'provider_type.txt'},
    {'col': 'Level(s) of Care - BHSL List', 'subdir': 'Clinical_Information', 'file': 'levels_of_care_bhsl.txt'},
    {'col': 'Clinical Interests - Legacy Sites', 'subdir': 'Clinical_Information', 'file': 'clinical_interests_legacy.txt'},
    {'col': 'Clinical Focus', 'subdir': 'Clinical_Information', 'file': 'clinical_focus.txt'},
    {'col': 'Conditions Treated', 'subdir': 'Clinical_Information', 'file': 'conditions_treated.txt'},
    {'col': 'Treatment Modalities', 'subdir': 'Clinical_Information', 'file': 'treatment_modalities.txt'},
    {'col': 'Board Specialties', 'subdir': 'Clinical_Information', 'file': 'board_specialties.txt'},

    # LOMG Grid Options
    {'col': 'LOMG Grid - Label', 'subdir': 'LOMG_Grid_Options', 'file': 'lomg_grid_label.txt'},
    {'col': 'LOMG Grid - Location', 'subdir': 'LOMG_Grid_Options', 'file': 'lomg_grid_location.txt'},
    {'col': 'LOMG Grid - Availability', 'subdir': 'LOMG_Grid_Options', 'file': 'lomg_grid_availability.txt'},
    {'col': 'LOMG Grid - Ages Treated', 'subdir': 'LOMG_Grid_Options', 'file': 'lomg_grid_ages_treated.txt'},
    {'col': 'LOMG Grid - Specialties', 'subdir': 'LOMG_Grid_Options', 'file': 'lomg_grid_specialties.txt'},
    {'col': 'LOMG Grid - Do NOT Refer', 'subdir': 'LOMG_Grid_Options', 'file': 'lomg_grid_do_not_refer.txt'},
    {'col': 'LOMG Grid - Insurance Restrictions', 'subdir': 'LOMG_Grid_Options', 'file': 'lomg_grid_insurance_restrictions.txt'},
    {'col': 'LOMG Grid - Other Considerations', 'subdir': 'LOMG_Grid_Options', 'file': 'lomg_grid_other_considerations.txt'},
]

# This script assumes that the base directory 'picklists_generated' and its subdirectories
# 'Provider_Attributes', 'Clinical_Information', and 'LOMG_Grid_Options'
# have already been created in the workspace root.

for config in columns_to_process:
    generate_picklist(config['col'], config['subdir'], config['file'])

print("Picklist generation process complete.") 