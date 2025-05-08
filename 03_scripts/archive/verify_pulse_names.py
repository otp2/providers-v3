import csv
import os

# Define file paths
consolidated_names_file = os.path.join('05_airtable_and_mapping', '02_pulse', 'pulse_consolidated_names.csv')
pulse_bhi_file = os.path.join('00_source_data', 'pulse_data', 'pulse_bhi', 'pulse_bhi.csv')
pulse_counseling_file = os.path.join('00_source_data', 'pulse_data', 'pulse_counseling', 'pulse_counseling.csv')
pulse_mm_file = os.path.join('00_source_data', 'pulse_data', 'pulse_mm', 'pulse_mm.csv')
log_file_path = os.path.join('03_scripts', 'verify_pulse_names_log.txt')

def write_log(message, handle):
    print(message)
    handle.write(message + '\n')

# --- Step 1: Load the truth data --- 
truth_data = set()

with open(log_file_path, 'w', encoding='utf-8') as log_f:
    write_log(f"--- Running script: verify_pulse_names.py ---", log_f)
    write_log(f"Attempting to load truth data from: {consolidated_names_file}", log_f)
    try:
        with open(consolidated_names_file, 'r', newline='', encoding='utf-8') as f_consolidated:
            reader = csv.DictReader(f_consolidated)
            for i, row in enumerate(reader):
                first_name = row.get('First Name','').strip()
                last_name = row.get('Last Name','').strip()
                pulse_label = row.get('Pulse Label','').strip()
                
                if not first_name or not last_name or not pulse_label:
                    write_log(f"  Warning: Skipping row {i+2} in consolidated file due to missing data: {row}", log_f)
                    continue
                
                truth_tuple = (first_name, last_name, pulse_label)
                if truth_tuple in truth_data:
                    write_log(f"  Warning: Duplicate entry found in consolidated file: {truth_tuple}", log_f)
                truth_data.add(truth_tuple)
        write_log(f"Successfully loaded {len(truth_data)} unique truth entries.", log_f)
    except FileNotFoundError:
        write_log(f"FATAL ERROR: Consolidated names file not found at {consolidated_names_file}", log_f)
        exit()
    except Exception as e:
        write_log(f"FATAL ERROR reading consolidated names file: {e}", log_f)
        exit()

    if not truth_data:
        write_log("FATAL ERROR: No truth data loaded. Exiting.", log_f)
        exit()

    # --- Step 2: Verify each Pulse file against the truth data ---
    files_to_verify = [
        {'path': pulse_bhi_file, 'label': 'BHI'},
        {'path': pulse_counseling_file, 'label': 'Counseling'},
        {'path': pulse_mm_file, 'label': 'MM'}
    ]

    total_discrepancies = 0
    discrepancy_details = []

    for file_info in files_to_verify:
        input_file_path = file_info['path']
        current_pulse_label = file_info['label']
        rows_checked = 0
        file_discrepancies = 0
        
        write_log(f"\nVerifying file: {input_file_path} (Label: {current_pulse_label})", log_f)
        
        if not os.path.exists(input_file_path):
            write_log(f"  Error: File not found at {input_file_path}. Skipping verification.", log_f)
            continue

        try:
            with open(input_file_path, 'r', newline='', encoding='utf-8') as f_in:
                reader = csv.reader(f_in)
                try:
                    header = next(reader)
                except StopIteration:
                    write_log(f"  Warning: File {input_file_path} is empty. Skipping verification.", log_f)
                    continue

                # Find First Name and Last Name column indices
                try:
                    first_name_col_idx = header.index('First Name')
                    last_name_col_idx = header.index('Last Name')
                except ValueError as ve:
                    write_log(f"  Error: Required columns 'First Name' or 'Last Name' not found in header of {input_file_path}. Header: {header}. Error: {ve}. Skipping verification.", log_f)
                    continue

                # Process data rows
                for i, row in enumerate(reader):
                    rows_checked += 1
                    if first_name_col_idx >= len(row) or last_name_col_idx >= len(row):
                        write_log(f"  Warning: Row {i+2} in {input_file_path} is shorter than expected. Cannot verify names. Row: {row}", log_f)
                        continue

                    first_name_from_file = row[first_name_col_idx].strip()
                    last_name_from_file = row[last_name_col_idx].strip()
                    
                    # Form the tuple to check against truth data
                    check_tuple = (first_name_from_file, last_name_from_file, current_pulse_label)
                    
                    if check_tuple not in truth_data:
                        file_discrepancies += 1
                        discrepancy_msg = f"  DISCREPANCY Found: Row {i+2} in {os.path.basename(input_file_path)} - Name ('{first_name_from_file}', '{last_name_from_file}', '{current_pulse_label}') not found in consolidated list."
                        write_log(discrepancy_msg, log_f)
                        discrepancy_details.append(discrepancy_msg)
                        
            write_log(f"  Finished verifying {input_file_path}. Checked {rows_checked} data rows. Found {file_discrepancies} discrepancies in this file.", log_f)
            total_discrepancies += file_discrepancies

        except Exception as e:
            write_log(f"  Error processing file {input_file_path} during verification: {e}", log_f)
            import traceback
            write_log(traceback.format_exc(), log_f)

    # --- Step 3: Final Summary --- 
    write_log("\n--- Verification Summary ---", log_f)
    write_log(f"Total Discrepancies Found (across all Pulse files): {total_discrepancies}", log_f)
    if total_discrepancies == 0:
        write_log("Verification PASSED: All names in Pulse files match the consolidated list.", log_f)
    else:
        write_log("Verification FAILED: Discrepancies found. See details above.", log_f)
        
    write_log("\n--- Script verify_pulse_names.py finished ---", log_f) 