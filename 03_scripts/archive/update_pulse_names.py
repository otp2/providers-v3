import csv
import os

# Define file paths
consolidated_names_file = os.path.join('05_airtable_and_mapping', '02_pulse', 'pulse_consolidated_names.csv')
pulse_bhi_file = os.path.join('00_source_data', 'pulse_data', 'pulse_bhi', 'pulse_bhi.csv')
pulse_counseling_file = os.path.join('00_source_data', 'pulse_data', 'pulse_counseling', 'pulse_counseling.csv')
pulse_mm_file = os.path.join('00_source_data', 'pulse_data', 'pulse_mm', 'pulse_mm.csv')
log_file_path = os.path.join('03_scripts', 'update_pulse_names_log.txt')

# Helper function to normalize names for matching
def normalize_name(name_str):
    if name_str is None:
        return ""
    
    # Attempt to handle "Last, First" if present, otherwise assume "First Last"
    # This part primarily normalizes names from source files if they have commas.
    parts = [p.strip() for p in name_str.split(',')]
    if len(parts) == 2: # Likely "Last, First"
        processed_name_str = f"{parts[1]} {parts[0]}"
    else:
        processed_name_str = name_str

    return ''.join(filter(str.isalnum, processed_name_str)).lower()

# Function to log messages to both console and file
def log_message(message, log_file_handle):
    print(message)
    log_file_handle.write(message + '\n')

with open(log_file_path, 'w', encoding='utf-8') as log_f:
    log_message(f"Log file created at {log_file_path}", log_f)

    corrected_names_lookup = {}
    known_variants_to_correct_key_map = {}

    try:
        with open(consolidated_names_file, 'r', newline='', encoding='utf-8') as f_consolidated:
            reader = csv.DictReader(f_consolidated)
            for row in reader:
                first_name = row['First Name'].strip()
                last_name = row['Last Name'].strip()
                pulse_label = row['Pulse Label'].strip()
                
                if not first_name or not last_name:
                    log_message(f"Warning: Missing first or last name in consolidated file for label {pulse_label}: First='{first_name}', Last='{last_name}'", log_f)
                    continue

                # This is the key we will use for matching. It's based on the *corrected* names.
                normalized_key_name = normalize_name(f"{first_name}{last_name}") # Concatenate first and last, then normalize
                corrected_names_lookup[(normalized_key_name, pulse_label)] = f"{first_name} {last_name}"

                # Populate the known_variants_to_correct_key_map for specific issues
                if first_name == 'Martha' and last_name == 'Trujillo' and pulse_label == 'BHI':
                    known_variants_to_correct_key_map[(normalize_name("MarthaTrujilo"), 'BHI')] = normalized_key_name
                if first_name == 'Martin' and last_name == 'Beirne' and pulse_label == 'Counseling':
                    known_variants_to_correct_key_map[(normalize_name("MartyBeirne"), 'Counseling')] = normalized_key_name
                if first_name == 'Michael' and last_name == 'Geraci' and pulse_label == 'Counseling':
                    known_variants_to_correct_key_map[(normalize_name("MikeGeraci"), 'Counseling')] = normalized_key_name
                if first_name == 'Kathryn' and last_name == 'Ordiway' and pulse_label == 'Counseling':
                    known_variants_to_correct_key_map[(normalize_name("KathyOrdiway"), 'Counseling')] = normalized_key_name
                if first_name == 'Sarah' and last_name == 'Mulligan' and pulse_label == 'Counseling': # Correct form
                    known_variants_to_correct_key_map[(normalize_name("SarahMulliganDenman"), 'Counseling')] = normalized_key_name
                if first_name == 'Radostina' and last_name == 'Yakimova-Marfoe' and pulse_label == 'Counseling': # Correct form
                    known_variants_to_correct_key_map[(normalize_name("RadostinaInaYakimovaMarfoe"), 'Counseling')] = normalized_key_name
                if first_name == 'Pagel' and last_name == 'Palmer' and pulse_label == 'Counseling': # Correct form
                    known_variants_to_correct_key_map[(normalize_name("PagelPalmerJr"), 'Counseling')] = normalized_key_name

    except FileNotFoundError:
        log_message(f"Error: Consolidated names file not found at {consolidated_names_file}", log_f)
        exit()
    except Exception as e:
        log_message(f"Error reading consolidated names file: {e}", log_f)
        exit()

    if not corrected_names_lookup:
        log_message("Error: No names loaded from consolidated file. Exiting.", log_f)
        exit()

    log_message(f"Loaded {len(corrected_names_lookup)} unique name entries from {consolidated_names_file}", log_f)

    files_to_process = [
        {'path': pulse_bhi_file, 'name_col_header': 'Provider Name', 'label': 'BHI'},
        {'path': pulse_counseling_file, 'name_col_header': 'Therapist Name', 'label': 'Counseling'},
        {'path': pulse_mm_file, 'name_col_header': 'Provider Name', 'label': 'MM'}
    ]

    for file_info in files_to_process:
        input_file_path = file_info['path']
        # output_file_path = input_file_path # Overwrite the original file
        name_col_header = file_info['name_col_header']
        current_pulse_label = file_info['label']
        
        log_message(f"\nProcessing file: {input_file_path} for label: {current_pulse_label}", log_f)
        
        updated_rows = []
        names_updated_count = 0
        names_not_found_count = 0
        not_found_names_list = [] # Added to store not found names
        
        if not os.path.exists(input_file_path):
            log_message(f"  Error: File not found at {input_file_path}. Skipping.", log_f)
            continue

        try:
            with open(input_file_path, 'r', newline='', encoding='utf-8') as f_in:
                reader = csv.reader(f_in)
                header = next(reader)
                updated_rows.append(header)
                
                if name_col_header not in header:
                    log_message(f"  Error: Name column '{name_col_header}' not found in header of {input_file_path}. Header: {header}. Skipping this file.", log_f)
                    continue
                name_col_idx = header.index(name_col_header)

                for row_idx, row in enumerate(reader):
                    if name_col_idx >= len(row):
                        # print(f"  Warning: Row {row_idx+2} in {input_file_path} is shorter than expected, name column index out of bounds. Row: {row}")
                        updated_rows.append(row)
                        continue

                    original_name_in_file = row[name_col_idx]
                    normalized_name_from_file = normalize_name(original_name_in_file)
                    
                    target_corrected_name_string = None
                    lookup_key_to_check = (normalized_name_from_file, current_pulse_label)

                    # ---- START DEBUG BLOCK FOR MARTHA TRUJILO ----
                    if original_name_in_file == "Martha Trujilo" and current_pulse_label == "BHI":
                        log_message(f"  DEBUG: Checking for Martha Trujilo (BHI) in {os.path.basename(input_file_path)}:", log_f)
                        log_message(f"    DEBUG:   Original raw string from CSV: '{original_name_in_file}' (len {len(original_name_in_file)})", log_f)
                        log_message(f"    DEBUG:   Normalized from CSV: '{normalized_name_from_file}'", log_f)
                        log_message(f"    DEBUG:   Lookup key being checked: {lookup_key_to_check}", log_f)
                        log_message(f"    DEBUG:   Is key in corrected_names_lookup? {lookup_key_to_check in corrected_names_lookup}", log_f)
                        
                        # Forcing what the key from consolidated *should* be for the corrected entry
                        martha_trujillo_corrected_consolidated_key_name = normalize_name("MarthaTrujillo") # Note: No space
                        martha_trujillo_corrected_consolidated_tuple_key = (martha_trujillo_corrected_consolidated_key_name, "BHI")
                        log_message(f"    DEBUG:   Expected key from consolidated for CORRECT 'Martha Trujillo': {martha_trujillo_corrected_consolidated_tuple_key}", log_f)
                        log_message(f"    DEBUG:   Is *expected* consolidated key in lookup? {martha_trujillo_corrected_consolidated_tuple_key in corrected_names_lookup}", log_f)
                        if martha_trujillo_corrected_consolidated_tuple_key in corrected_names_lookup:
                            log_message(f"    DEBUG:     Value for expected key '{martha_trujillo_corrected_consolidated_tuple_key}': '{corrected_names_lookup[martha_trujillo_corrected_consolidated_tuple_key]}'", log_f)
                        else:
                            log_message(f"    DEBUG:     Expected key '{martha_trujillo_corrected_consolidated_tuple_key}' NOT FOUND in lookup.", log_f)
                    # ---- END DEBUG BLOCK FOR MARTHA TRUJILO ----

                    actual_key_to_use_in_lookup = None

                    # Attempt 1: Direct match of normalized name from file against keys in lookup
                    if lookup_key_to_check in corrected_names_lookup:
                        actual_key_to_use_in_lookup = lookup_key_to_check
                    
                    # Attempt 2: Check against known variants map
                    elif lookup_key_to_check in known_variants_to_correct_key_map:
                        corrected_entry_normalized_name = known_variants_to_correct_key_map[lookup_key_to_check]
                        actual_key_to_use_in_lookup = (corrected_entry_normalized_name, current_pulse_label)
                        log_message(f"  INFO: Matched '{original_name_in_file}' to corrected form via known_variants_map. Using key {actual_key_to_use_in_lookup} for lookup.", log_f)

                    # Attempt 3: Lenient match (if other attempts failed)
                    # This is less likely to be needed if known_variants map is comprehensive for these cases
                    if not actual_key_to_use_in_lookup:
                        name_parts_from_file = original_name_in_file.replace('-', ' ').split() # Basic split
                        if len(name_parts_from_file) >= 2:
                            potential_first = name_parts_from_file[0]
                            potential_last = name_parts_from_file[-1]
                            normalized_lenient_key_from_file = normalize_name(f"{potential_first}{potential_last}")
                            
                            lenient_lookup_key_to_check = (normalized_lenient_key_from_file, current_pulse_label)
                            if lenient_lookup_key_to_check in corrected_names_lookup:
                                actual_key_to_use_in_lookup = lenient_lookup_key_to_check
                                log_message(f"  INFO: Matched '{original_name_in_file}' via lenient match. Using key {actual_key_to_use_in_lookup} for lookup.", log_f)
                            elif lenient_lookup_key_to_check in known_variants_to_correct_key_map: # Lenient match against variant map
                                corrected_entry_normalized_name = known_variants_to_correct_key_map[lenient_lookup_key_to_check]
                                actual_key_to_use_in_lookup = (corrected_entry_normalized_name, current_pulse_label)
                                log_message(f"  INFO: Matched '{original_name_in_file}' to corrected form via lenient known_variants_map. Using key {actual_key_to_use_in_lookup} for lookup.", log_f)

                    if actual_key_to_use_in_lookup and actual_key_to_use_in_lookup in corrected_names_lookup:
                        target_corrected_name_string = corrected_names_lookup[actual_key_to_use_in_lookup]
                        if row[name_col_idx] != target_corrected_name_string:
                            # print(f"  Updating Name in {os.path.basename(input_file_path)}: Row {row_idx+2} '{row[name_col_idx]}' -> '{target_corrected_name_string}'")
                            row[name_col_idx] = target_corrected_name_string
                            names_updated_count += 1
                        else:
                            # Name is already correct, no action needed, not counted as "updated"
                            pass
                    else:
                        # print(f"  Warning: Name '{original_name_in_file}' (normalized: {normalized_name_from_file}) in {os.path.basename(input_file_path)} not found in consolidated list for label '{current_pulse_label}'. Keeping original.")
                        names_not_found_count +=1
                        # Log the original and normalized name that wasn't found
                        not_found_detail = f"Original: '{original_name_in_file}', Normalized: '{normalized_name_from_file}'"
                        not_found_names_list.append(not_found_detail)
                            
                    updated_rows.append(row)

            # Write the updated rows back to the same file
            with open(input_file_path, 'w', newline='', encoding='utf-8') as f_out:
                writer = csv.writer(f_out)
                writer.writerows(updated_rows)
            
            log_message(f"  Finished processing {input_file_path}.", log_f)
            log_message(f"  Names updated: {names_updated_count}", log_f)
            log_message(f"  Names not found/kept original: {names_not_found_count}", log_f)
            if not_found_names_list:
                log_message(f"    Names not found in {os.path.basename(input_file_path)} (original string and its normalized form shown):", log_f)
                for name_detail in not_found_names_list:
                    log_message(f"      - {name_detail}", log_f)

        except Exception as e:
            log_message(f"  Error processing file {input_file_path}: {e}", log_f)

    log_message("\nScript finished.", log_f) 