import csv
import os

# Define file paths
pulse_file = os.path.join('05_airtable_and_mapping', '02_pulse', 'pulse_consolidated_names.csv')
northshore_file = os.path.join('05_airtable_and_mapping', '03_northshore', 'northshore_names.csv')
output_airtable_file = os.path.join('05_airtable_and_mapping', 'working_airtable.csv')
log_file_path = os.path.join('03_scripts', 'create_working_airtable_log.txt')

def write_log(message, handle):
    print(message)
    handle.write(message + '\n')

with open(log_file_path, 'w', encoding='utf-8') as log_f:
    write_log("--- Running script: create_working_airtable.py ---", log_f)

    all_output_rows = []
    output_header = ["UID", "Last Name", "First Name", "Internal Label"]
    all_output_rows.append(output_header)

    pulse_rows_processed = 0
    northshore_rows_processed = 0

    # --- Step 1: Process Pulse Consolidated Names ---
    write_log(f"Attempting to process Pulse file: {pulse_file}", log_f)
    try:
        with open(pulse_file, 'r', newline='', encoding='utf-8') as f_pulse:
            reader = csv.DictReader(f_pulse)
            if not reader.fieldnames or not all(col in reader.fieldnames for col in ['First Name', 'Last Name', 'Pulse Label']):
                write_log(f"  Error: Pulse file {pulse_file} is missing required columns ('First Name', 'Last Name', 'Pulse Label').", log_f)
            else:
                for row in reader:
                    first_name = row.get('First Name', '').strip()
                    last_name = row.get('Last Name', '').strip()
                    pulse_label = row.get('Pulse Label', '').strip() # This becomes the Internal Label
                    
                    if first_name and last_name: # Only add if name is present
                        all_output_rows.append([
                            "",                # Blank UID
                            last_name,
                            first_name,
                            pulse_label        # Use Pulse Label as Internal Label
                        ])
                        pulse_rows_processed += 1
                    else:
                        write_log(f"  Warning: Skipping row in Pulse file due to missing First/Last Name: {row}", log_f)
        write_log(f"Successfully processed {pulse_rows_processed} rows from {pulse_file}.", log_f)
    except FileNotFoundError:
        write_log(f"  Error: Pulse file not found at {pulse_file}. Skipping this source.", log_f)
    except Exception as e:
        write_log(f"  Error processing Pulse file {pulse_file}: {e}", log_f)
        import traceback
        write_log(traceback.format_exc(), log_f)

    # --- Step 2: Process Northshore Names ---
    write_log(f"\nAttempting to process Northshore file: {northshore_file}", log_f)
    try:
        with open(northshore_file, 'r', newline='', encoding='utf-8') as f_northshore:
            reader = csv.DictReader(f_northshore)
            if not reader.fieldnames or not all(col in reader.fieldnames for col in ['First Name', 'Last Name', 'Internal Label']):
                write_log(f"  Error: Northshore file {northshore_file} is missing required columns ('First Name', 'Last Name', 'Internal Label').", log_f)
            else:
                for row in reader:
                    first_name = row.get('First Name', '').strip()
                    last_name = row.get('Last Name', '').strip()
                    internal_label = row.get('Internal Label', '').strip()

                    if first_name and last_name: # Only add if name is present
                        all_output_rows.append([
                            "",               # Blank UID
                            last_name,
                            first_name,
                            internal_label
                        ])
                        northshore_rows_processed += 1
                    else:
                        write_log(f"  Warning: Skipping row in Northshore file due to missing First/Last Name: {row}", log_f)
        write_log(f"Successfully processed {northshore_rows_processed} rows from {northshore_file}.", log_f)
    except FileNotFoundError:
        write_log(f"  Error: Northshore file not found at {northshore_file}. Skipping this source.", log_f)
    except Exception as e:
        write_log(f"  Error processing Northshore file {northshore_file}: {e}", log_f)
        import traceback
        write_log(traceback.format_exc(), log_f)

    # --- Step 3: Write combined data to Working Airtable file ---
    total_data_rows = len(all_output_rows) - 1 # Subtract header
    if total_data_rows > 0:
        write_log(f"\nAttempting to write {total_data_rows} combined data rows to: {output_airtable_file}", log_f)
        try:
            with open(output_airtable_file, 'w', newline='', encoding='utf-8') as f_out:
                writer = csv.writer(f_out)
                writer.writerows(all_output_rows)
            write_log(f"Successfully wrote {total_data_rows} data rows (plus header) to {output_airtable_file}", log_f)
        except Exception as e:
            write_log(f"FATAL ERROR writing to {output_airtable_file}: {e}", log_f)
            import traceback
            write_log(traceback.format_exc(), log_f)
    else:
        write_log("\nNo data processed from sources. Output file will not be created or will be empty (header only).", log_f)

    # --- Final Summary ---
    write_log("\n--- Create Working Airtable Summary ---", log_f)
    write_log(f"Rows processed from Pulse file: {pulse_rows_processed}", log_f)
    write_log(f"Rows processed from Northshore file: {northshore_rows_processed}", log_f)
    write_log(f"Total data rows written to {os.path.basename(output_airtable_file)}: {total_data_rows}", log_f)

    write_log("\n--- Script create_working_airtable.py finished ---", log_f) 