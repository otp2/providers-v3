import csv
import os
import re

# Define file paths
northshore_names_file = os.path.join('05_airtable_and_mapping', '03_northshore', 'northshore_names.csv')
guidebook_file = os.path.join('00_source_data', 'guidebook', 'guidebook.csv')
log_file_path = os.path.join('03_scripts', 'update_guidebook_names_log.txt')

def write_log(message, handle):
    print(message) # Also print to console for immediate feedback if possible
    handle.write(message + '\n')

def normalize_name_for_matching(name_str):
    if not name_str:
        return ""
    name_str = str(name_str).lower()
    # Remove extra whitespace (leading, trailing, multiple internal)
    name_str = ' '.join(name_str.split())
    # Optional: remove common punctuation like periods, commas if they cause issues,
    # but for "First Last" vs "First Last" this might be enough.
    # name_str = re.sub(r'[.\-,]', '', name_str) # Example: remove .,-
    return name_str.strip()

with open(log_file_path, 'w', encoding='utf-8') as log_f:
    write_log("--- Running script: update_guidebook_names_from_northshore.py (v2 reader/writer) ---", log_f)

    # --- Step 1: Load Northshore names (source of truth) ---
    northshore_lookup = {}
    # This map will help bridge known variants from guidebook.csv to the normalized key used in northshore_lookup
    # Key: normalized_name_as_in_guidebook, Value: normalized_key_as_in_northshore_lookup
    guidebook_to_northshore_alias_map = {
        normalize_name_for_matching("Jon Chernaik"): normalize_name_for_matching("Jonathan Chernaik"),
        normalize_name_for_matching("Rob Marvin"): normalize_name_for_matching("Robert Marvin"),
        normalize_name_for_matching("Susie Lesher"): normalize_name_for_matching("Susan Lesher"),
        normalize_name_for_matching("Jenni Nierstheimer"): normalize_name_for_matching("Jennifer Nierstheimer"),
        normalize_name_for_matching("Alex Schade"): normalize_name_for_matching("Alex Elstein"), # Northshore was updated to Alex Elstein
        normalize_name_for_matching("Jeff Sholemson"): normalize_name_for_matching("Jeffrey Sholemson"),
        normalize_name_for_matching("Chris Williams"): normalize_name_for_matching("Christopher Williams"),
    }
    write_log(f"Attempting to load Northshore names from: {northshore_names_file}", log_f)
    try:
        with open(northshore_names_file, 'r', newline='', encoding='utf-8') as f_northshore:
            reader = csv.DictReader(f_northshore)
            if not reader.fieldnames or 'First Name' not in reader.fieldnames or 'Last Name' not in reader.fieldnames:
                write_log(f"  Error: Northshore names file {northshore_names_file} is missing 'First Name' or 'Last Name' columns.", log_f)
                exit()
            
            for i, row in enumerate(reader):
                first_name = row.get('First Name', '').strip()
                last_name = row.get('Last Name', '').strip()
                
                if not first_name and not last_name: # Skip if both are empty
                    write_log(f"  Warning: Skipping row {i+2} in Northshore names due to empty First and Last Name: {row}", log_f)
                    continue

                # Key is normalized "firstname lastname"
                normalized_key = normalize_name_for_matching(f"{first_name} {last_name}")
                if not normalized_key: # handles cases where one name part might be missing and results in empty after normalization
                    write_log(f"  Warning: Skipping row {i+2} in Northshore names due to empty normalized key for ('{first_name}', '{last_name}')", log_f)
                    continue

                if normalized_key in northshore_lookup:
                    write_log(f"  Warning: Duplicate normalized key '{normalized_key}' found in Northshore names. Original: ('{first_name}', '{last_name}'). Previous: {northshore_lookup[normalized_key]}. Overwriting.", log_f)
                northshore_lookup[normalized_key] = (first_name, last_name) # Store original casing
        write_log(f"Successfully loaded {len(northshore_lookup)} unique name entries from Northshore names.", log_f)
    except FileNotFoundError:
        write_log(f"FATAL ERROR: Northshore names file not found at {northshore_names_file}", log_f)
        exit()
    except Exception as e:
        write_log(f"FATAL ERROR reading Northshore names file: {e}", log_f)
        exit()

    if not northshore_lookup:
        write_log("FATAL ERROR: No truth data loaded from Northshore names. Exiting.", log_f)
        exit()

    # --- Step 2: Process Guidebook CSV using csv.reader and csv.writer ---
    updated_guidebook_data = [] # Will store list of lists
    rows_processed = 0
    rows_matched_updated = 0
    rows_not_found = 0
    rows_empty_name_skipped = 0
    provider_name_col_idx = -1

    write_log(f"\nAttempting to process Guidebook file: {guidebook_file} using positional writing", log_f)
    try:
        original_guidebook_rows = []
        with open(guidebook_file, 'r', newline='', encoding='utf-8') as f_guidebook:
            reader = csv.reader(f_guidebook)
            for row in reader:
                original_guidebook_rows.append(row)
        
        if not original_guidebook_rows:
            write_log(f"  Error: Guidebook file {guidebook_file} is empty.", log_f)
            exit()

        header = original_guidebook_rows[0]
        updated_guidebook_data.append(header) # Add header to output

        try:
            # Expected header: "Provider Name (hyperlink to Practice Brochure)"
            provider_name_col_header = "Provider Name (hyperlink to Practice Brochure)"
            provider_name_col_idx = header.index(provider_name_col_header)
        except ValueError:
            write_log(f"  Error: Column '{provider_name_col_header}' not found in Guidebook header: {header}", log_f)
            exit()

        for i, original_row in enumerate(original_guidebook_rows[1:]): # Skip header
            row_num_for_log = i + 2 # 1-based index for data rows, plus 1 for header
            rows_processed += 1
            
            # Ensure row has enough columns
            if len(original_row) < max(2, provider_name_col_idx + 1): # Need at least 2 cols for F/L name, and provider_name_col_idx
                 write_log(f"  Warning: Row {row_num_for_log} in Guidebook is too short or malformed. Skipping. Row: {original_row}", log_f)
                 updated_guidebook_data.append(original_row) # Append original malformed row
                 continue

            provider_name_from_guidebook = original_row[provider_name_col_idx].strip()
            
            # Create a mutable copy for the new row, defaulting to original values
            new_row = list(original_row) 

            if not provider_name_from_guidebook:
                write_log(f"  Info: Row {row_num_for_log} in Guidebook has empty '{provider_name_col_header}'. Skipping name update.", log_f)
                rows_empty_name_skipped +=1
                # First two columns might be blank or whatever they were, rest are original
            else:
                normalized_guidebook_key = normalize_name_for_matching(provider_name_from_guidebook)
                northshore_key_to_use = normalized_guidebook_key

                if northshore_key_to_use not in northshore_lookup:
                    if normalized_guidebook_key in guidebook_to_northshore_alias_map:
                        aliased_key = guidebook_to_northshore_alias_map[normalized_guidebook_key]
                        write_log(f"  Info: Row {row_num_for_log} Guidebook name '{provider_name_from_guidebook}' aliased to Northshore key '{aliased_key}'", log_f)
                        northshore_key_to_use = aliased_key
                
                if northshore_key_to_use in northshore_lookup:
                    correct_first, correct_last = northshore_lookup[northshore_key_to_use]
                    
                    new_row[0] = correct_last  # Corrected Last Name in Column 1 (index 0)
                    new_row[1] = correct_first # Corrected First Name in Column 2 (index 1)
                    
                    rows_matched_updated += 1
                    write_log(f"  Match: Row {row_num_for_log} Guidebook ('{provider_name_from_guidebook}') -> Northshore ('{correct_first}', '{correct_last}')", log_f)
                else:
                    rows_not_found += 1
                    write_log(f"  No Match: Row {row_num_for_log} Guidebook name '{provider_name_from_guidebook}' (normalized: '{normalized_guidebook_key}') not found.", log_f)
                    # If no match, new_row[0] and new_row[1] will retain original blank/values
            
            updated_guidebook_data.append(new_row)
        
        write_log(f"Finished processing Guidebook. Processed {rows_processed} data rows.", log_f)

    except FileNotFoundError:
        write_log(f"FATAL ERROR: Guidebook file not found at {guidebook_file}", log_f)
        exit()
    except Exception as e:
        write_log(f"FATAL ERROR processing Guidebook file: {e}", log_f)
        import traceback
        write_log(traceback.format_exc(), log_f)
        exit()

    # --- Step 3: Write updated data back to Guidebook CSV using csv.writer ---
    if updated_guidebook_data:
        write_log(f"\nAttempting to write {len(updated_guidebook_data)} rows back to: {guidebook_file}", log_f)
        try:
            with open(guidebook_file, 'w', newline='', encoding='utf-8') as f_out:
                writer = csv.writer(f_out)
                writer.writerows(updated_guidebook_data)
            write_log(f"Successfully wrote updated data to {guidebook_file}", log_f)
        except Exception as e:
            write_log(f"FATAL ERROR writing updated Guidebook file: {e}", log_f)
            import traceback
            write_log(traceback.format_exc(), log_f)
    else:
        write_log("\nSkipping write to Guidebook file: No data processed.", log_f)

    # --- Final Summary ---
    write_log("\n--- Update Guidebook Names Summary (v2 reader/writer) ---", log_f)
    write_log(f"Total rows processed from Guidebook: {rows_processed}", log_f)
    write_log(f"Rows matched with Northshore and First/Last Name updated: {rows_matched_updated}", log_f)
    write_log(f"Rows where '{provider_name_col_header}' was empty (skipped name update): {rows_empty_name_skipped}", log_f)
    write_log(f"Rows where Guidebook name was NOT found in Northshore lookup: {rows_not_found}", log_f)
    write_log(f"Number of entries in Northshore lookup: {len(northshore_lookup)}", log_f)
    
    write_log("\n--- Script update_guidebook_names_from_northshore.py (v2 reader/writer) finished ---", log_f) 