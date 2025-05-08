import csv
import os
import re

# Define file paths
consolidated_names_file = os.path.join('05_airtable_and_mapping', '02_pulse', 'pulse_consolidated_names.csv')
pulse_bhi_file = os.path.join('00_source_data', 'pulse_data', 'pulse_bhi', 'pulse_bhi.csv')
pulse_counseling_file = os.path.join('00_source_data', 'pulse_data', 'pulse_counseling', 'pulse_counseling.csv')
pulse_mm_file = os.path.join('00_source_data', 'pulse_data', 'pulse_mm', 'pulse_mm.csv')
log_file_path = os.path.join('03_scripts', 'split_pulse_names_log.txt')

def write_log(message, handle):
    print(message)
    handle.write(message + '\n')

# Helper function to normalize full names for lookup key
def normalize_full_name_key(name_str):
    if name_str is None:
        return ""
    # Lowercase, remove extra whitespace
    normalized = ' '.join(name_str.strip().lower().split())
    return normalized

# --- Step 1: Load consolidated names as the source of truth for First/Last names ---
# Key: (normalized_full_name, pulse_label)
# Value: {'first': CorrectFirstName, 'last': CorrectLastName}
name_split_lookup = {}

with open(log_file_path, 'w', encoding='utf-8') as log_f:
    write_log(f"--- Running script: split_pulse_names.py ---", log_f)
    write_log(f"Attempting to load consolidated names from: {consolidated_names_file}", log_f)
    try:
        with open(consolidated_names_file, 'r', newline='', encoding='utf-8') as f_consolidated:
            reader = csv.DictReader(f_consolidated)
            for row in reader:
                first_name = row.get('First Name','').strip()
                last_name = row.get('Last Name','').strip()
                pulse_label = row.get('Pulse Label','').strip()
                
                if not first_name or not last_name or not pulse_label:
                    write_log(f"  Warning: Skipping row in consolidated file due to missing data: {row}", log_f)
                    continue
                    
                full_name_str = f"{first_name} {last_name}"
                normalized_key = normalize_full_name_key(full_name_str)
                lookup_tuple = (normalized_key, pulse_label)
                
                if lookup_tuple in name_split_lookup:
                     write_log(f"  Warning: Duplicate entry found in consolidated file for key {lookup_tuple}. Overwriting previous entry.", log_f)
                name_split_lookup[lookup_tuple] = {'first': first_name, 'last': last_name}
                
        write_log(f"Successfully loaded {len(name_split_lookup)} entries into name split lookup.", log_f)

    except FileNotFoundError:
        write_log(f"FATAL ERROR: Consolidated names file not found at {consolidated_names_file}", log_f)
        exit()
    except Exception as e:
        write_log(f"FATAL ERROR reading consolidated names file: {e}", log_f)
        exit()

    if not name_split_lookup:
        write_log("FATAL ERROR: No data loaded from consolidated names file. Exiting.", log_f)
        exit()

    # --- Step 2: Process each Pulse file ---
    files_to_process = [
        {'path': pulse_bhi_file, 'name_col_header': 'Provider Name', 'label': 'BHI'},
        {'path': pulse_counseling_file, 'name_col_header': 'Therapist Name', 'label': 'Counseling'},
        {'path': pulse_mm_file, 'name_col_header': 'Provider Name', 'label': 'MM'}
    ]

    for file_info in files_to_process:
        input_file_path = file_info['path']
        original_name_col_header = file_info['name_col_header']
        current_pulse_label = file_info['label']
        
        write_log(f"\nProcessing file: {input_file_path}", log_f)
        
        if not os.path.exists(input_file_path):
            write_log(f"  Error: File not found at {input_file_path}. Skipping.", log_f)
            continue

        output_rows = []
        not_found_count = 0

        try:
            with open(input_file_path, 'r', newline='', encoding='utf-8') as f_in:
                reader = csv.reader(f_in)
                try:
                    header = next(reader)
                except StopIteration: # Handle empty file
                    write_log(f"  Warning: File {input_file_path} is empty. Skipping.", log_f)
                    continue

                # Find original name column index
                try:
                    name_col_idx = header.index(original_name_col_header)
                except ValueError:
                    write_log(f"  Error: Column '{original_name_col_header}' not found in header of {input_file_path}. Header: {header}. Skipping file.", log_f)
                    continue

                # Create new header
                new_header = header[:name_col_idx] + ['First Name', 'Last Name'] + header[name_col_idx+1:]
                output_rows.append(new_header)
                
                # Process data rows
                for i, row in enumerate(reader):
                    if name_col_idx >= len(row):
                        write_log(f"  Warning: Row {i+2} in {input_file_path} is shorter than expected. Skipping name split. Row: {row}", log_f)
                        # Pad row if necessary to match new header length before appending
                        padded_row = row[:name_col_idx] + ['', ''] + row[name_col_idx+1:]
                        while len(padded_row) < len(new_header):
                            padded_row.append('')
                        output_rows.append(padded_row)
                        continue

                    full_name_str = row[name_col_idx].strip()
                    normalized_lookup_key = normalize_full_name_key(full_name_str)
                    lookup_tuple = (normalized_lookup_key, current_pulse_label)
                    
                    first_name_to_insert = ''
                    last_name_to_insert = ''
                    
                    # Look up in the consolidated data
                    if lookup_tuple in name_split_lookup:
                        split_data = name_split_lookup[lookup_tuple]
                        first_name_to_insert = split_data['first']
                        last_name_to_insert = split_data['last']
                    else:
                        not_found_count += 1
                        write_log(f"  Warning: Row {i+2} - Name '{full_name_str}' (Label: {current_pulse_label}, NormKey: {normalized_lookup_key}) not found in consolidated lookup. Attempting basic split.", log_f)
                        # Fallback: Basic split on first space
                        parts = full_name_str.split(' ', 1)
                        first_name_to_insert = parts[0]
                        if len(parts) > 1:
                            last_name_to_insert = parts[1]
                        else:
                            last_name_to_insert = '' # Or handle single names differently?
                            write_log(f"    -> Could only find single name part '{first_name_to_insert}' during basic split.", log_f)

                    # Create the new row structure
                    new_row = row[:name_col_idx] + [first_name_to_insert, last_name_to_insert] + row[name_col_idx+1:]
                    output_rows.append(new_row)
            
            # Write the updated rows back to the same file
            with open(input_file_path, 'w', newline='', encoding='utf-8') as f_out:
                writer = csv.writer(f_out)
                writer.writerows(output_rows)
            
            write_log(f"  Finished processing {input_file_path}.", log_f)
            if not_found_count > 0:
                 write_log(f"  NOTE: {not_found_count} names were not found in the consolidated list and used a basic split fallback.", log_f)
            else:
                 write_log(f"  Successfully split names based on consolidated list.", log_f)

        except Exception as e:
            write_log(f"  Error processing file {input_file_path}: {e}", log_f)
            import traceback
            write_log(traceback.format_exc(), log_f)

    write_log("\n--- Script split_pulse_names.py finished ---", log_f) 