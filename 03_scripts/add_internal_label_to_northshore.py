import csv
import os

# Define file paths
northshore_names_file = os.path.join('05_airtable_and_mapping', '03_northshore', 'northshore_names.csv')
guidebook_file = os.path.join('00_source_data', 'guidebook', 'guidebook.csv')
log_file_path = os.path.join('03_scripts', 'add_internal_label_to_northshore_log.txt')

def write_log(message, handle):
    print(message)
    handle.write(message + '\n')

def normalize_name_component(name_str):
    if not name_str:
        return ""
    return str(name_str).lower().strip()

with open(log_file_path, 'w', encoding='utf-8') as log_f:
    write_log("--- Running script: add_internal_label_to_northshore.py ---", log_f)

    # --- Step 1: Load Guidebook data to create a lookup for Internal Labels ---
    guidebook_label_lookup = {}
    guidebook_provider_name_col_idx = -1 # For fallback if first/last name columns are insufficient
    guidebook_internal_label_col_idx = -1
    guidebook_first_name_col_idx = 0 # Defaulting to first column for Last Name
    guidebook_last_name_col_idx = 0   # Defaulting to second column for First Name

    write_log(f"Attempting to load Guidebook data from: {guidebook_file}", log_f)
    try:
        with open(guidebook_file, 'r', newline='', encoding='utf-8') as f_guidebook:
            reader = csv.reader(f_guidebook)
            header = next(reader, None)
            if not header:
                write_log(f"  Error: Guidebook file {guidebook_file} is empty or has no header.", log_f)
                exit()
            
            try:
                # Primary keys for matching from guidebook (now at the start of the file)
                # Based on previous steps, guidebook.csv has Last Name in col 0, First Name in col 1
                guidebook_last_name_col_idx = 0 # Explicitly set based on known structure
                guidebook_first_name_col_idx = 1 # Explicitly set
                
                guidebook_internal_label_col_idx = header.index('Internal Label')
                # For logging/fallback, also find the original provider name column
                guidebook_provider_name_col_idx = header.index('Provider Name (hyperlink to Practice Brochure)')

            except ValueError as ve:
                write_log(f"  Error: Required columns not found in Guidebook header: {header}. Error: {ve}", log_f)
                exit()

            for i, row in enumerate(reader):
                if len(row) <= max(guidebook_first_name_col_idx, guidebook_last_name_col_idx, guidebook_internal_label_col_idx):
                    write_log(f"  Warning: Row {i+2} in Guidebook is too short. Skipping. Row: {row}", log_f)
                    continue

                # Use the corrected First and Last names from the beginning of guidebook.csv
                gb_first_name = normalize_name_component(row[guidebook_first_name_col_idx])
                gb_last_name = normalize_name_component(row[guidebook_last_name_col_idx])
                internal_label = row[guidebook_internal_label_col_idx].strip()
                original_provider_name_field = row[guidebook_provider_name_col_idx].strip()

                if gb_first_name and gb_last_name: # Only consider if both names are present
                    lookup_key = (gb_first_name, gb_last_name)
                    if lookup_key in guidebook_label_lookup and guidebook_label_lookup[lookup_key] != internal_label:
                        write_log(f"  Warning: Duplicate name {lookup_key} in Guidebook with different labels. Keeping first one: '{guidebook_label_lookup[lookup_key]}'. Ignoring new: '{internal_label}' from provider entry: '{original_provider_name_field}'", log_f)
                    elif lookup_key not in guidebook_label_lookup: # Add if not already there
                         guidebook_label_lookup[lookup_key] = internal_label
                else:
                    write_log(f"  Warning: Row {i+2} in Guidebook ('{original_provider_name_field}') has missing first/last name components after normalization. Cannot use for lookup.", log_f)
        
        write_log(f"Successfully loaded {len(guidebook_label_lookup)} unique (First, Last) -> Internal Label entries from Guidebook.", log_f)
    except FileNotFoundError:
        write_log(f"FATAL ERROR: Guidebook file not found at {guidebook_file}", log_f)
        exit()
    except Exception as e:
        write_log(f"FATAL ERROR reading Guidebook file: {e}", log_f)
        import traceback
        write_log(traceback.format_exc(), log_f)
        exit()

    if not guidebook_label_lookup:
        write_log("FATAL ERROR: No lookup data loaded from Guidebook. Exiting.", log_f)
        exit()

    # --- Step 2: Read Northshore names, add Internal Label, and write back ---
    updated_northshore_rows = []
    northshore_fieldnames = None
    rows_read_northshore = 0
    labels_added_count = 0
    labels_not_found_count = 0

    write_log(f"\nAttempting to process Northshore names file: {northshore_names_file}", log_f)
    try:
        with open(northshore_names_file, 'r', newline='', encoding='utf-8') as f_northshore_in:
            reader = csv.DictReader(f_northshore_in)
            northshore_fieldnames = reader.fieldnames
            if not northshore_fieldnames or 'First Name' not in northshore_fieldnames or 'Last Name' not in northshore_fieldnames:
                write_log(f"  Error: Northshore file {northshore_names_file} is missing 'First Name' or 'Last Name' columns.", log_f)
                exit()
            
            if 'Internal Label' not in northshore_fieldnames:
                new_fieldnames = list(northshore_fieldnames) + ['Internal Label']
            else:
                new_fieldnames = list(northshore_fieldnames) # Use existing fieldnames
            
            updated_northshore_rows.append(new_fieldnames) # Header for the output

            for i, row_dict in enumerate(reader):
                rows_read_northshore += 1
                ns_first_name_orig = row_dict.get('First Name', '').strip()
                ns_last_name_orig = row_dict.get('Last Name', '').strip()
                
                ns_first_name_norm = normalize_name_component(ns_first_name_orig)
                ns_last_name_norm = normalize_name_component(ns_last_name_orig)

                # Initialize new_row_dict using new_fieldnames to ensure 'Internal Label' key exists
                # And copy over existing values from original northshore row_dict
                current_new_row = {field: row_dict.get(field, '') for field in northshore_fieldnames}
                current_new_row['Internal Label'] = '' # Ensure it has a default blank value

                if ns_first_name_norm and ns_last_name_norm:
                    lookup_key = (ns_first_name_norm, ns_last_name_norm)
                    if lookup_key in guidebook_label_lookup:
                        internal_label = guidebook_label_lookup[lookup_key]
                        current_new_row['Internal Label'] = internal_label
                        labels_added_count += 1
                        # write_log(f"  Row {i+2} Northshore: Added label '{internal_label}' for ({ns_first_name_orig}, {ns_last_name_orig})", log_f) # Verbose
                    else:
                        labels_not_found_count += 1
                        # current_new_row['Internal Label'] is already '' (set above)
                        write_log(f"  Warning: Row {i+2} Northshore: No Internal Label found in Guidebook lookup for ({ns_first_name_orig}, {ns_last_name_orig}) (normalized: {lookup_key})", log_f)
                else:
                    labels_not_found_count += 1 # Or handle as error / skip
                    # current_new_row['Internal Label'] is already '' (set above)
                    write_log(f"  Warning: Row {i+2} Northshore: ('{ns_first_name_orig}', '{ns_last_name_orig}') has missing name components. Cannot find label.", log_f)
                
                updated_northshore_rows.append(current_new_row)

        write_log(f"Finished reading Northshore names. Processed {rows_read_northshore} data rows.", log_f)

    except FileNotFoundError:
        write_log(f"FATAL ERROR: Northshore file not found at {northshore_names_file}", log_f)
        exit()
    except Exception as e:
        write_log(f"FATAL ERROR processing Northshore file: {e}", log_f)
        import traceback
        write_log(traceback.format_exc(), log_f)
        exit()

    # Write updated Northshore data back
    if updated_northshore_rows and len(updated_northshore_rows) > 1:
        write_log(f"\nAttempting to write {len(updated_northshore_rows)-1} data rows (plus header) back to: {northshore_names_file}", log_f)
        try:
            with open(northshore_names_file, 'w', newline='', encoding='utf-8') as f_out:
                # updated_northshore_rows[0] is the new_fieldnames list
                writer = csv.DictWriter(f_out, fieldnames=updated_northshore_rows[0])
                writer.writeheader()
                writer.writerows(updated_northshore_rows[1:])
            write_log(f"Successfully wrote updated data to {northshore_names_file}", log_f)
        except Exception as e:
            write_log(f"FATAL ERROR writing updated Northshore file: {e}", log_f)
            import traceback
            write_log(traceback.format_exc(), log_f)
    else:
        write_log("\nSkipping write to Northshore file: No data to write or header missing.", log_f)

    # --- Final Summary ---
    write_log("\n--- Add Internal Label to Northshore Summary ---", log_f)
    write_log(f"Total rows processed from Northshore names: {rows_read_northshore}", log_f)
    write_log(f"Internal Labels successfully added/updated: {labels_added_count}", log_f)
    write_log(f"Internal Labels NOT found (or name missing in Northshore): {labels_not_found_count}", log_f)

    write_log("\n--- Script add_internal_label_to_northshore.py finished ---", log_f) 