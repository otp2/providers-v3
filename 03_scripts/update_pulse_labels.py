import csv
import os

# Define file paths
consolidated_names_file = os.path.join('05_airtable_and_mapping', '02_pulse', 'pulse_consolidated_names.csv')
log_file_path = os.path.join('03_scripts', 'update_pulse_labels_log.txt')

def write_log(message, handle):
    print(message)
    handle.write(message + '\n')

# Define the label transformations
label_transformation_map = {
    "BHI": "Behavioral Health Integration",
    "MM": "Medication Management"
    # Counseling remains Counseling, so no explicit mapping needed for it to stay the same
}

with open(log_file_path, 'w', encoding='utf-8') as log_f:
    write_log("--- Running script: update_pulse_labels.py ---", log_f)
    write_log(f"Attempting to read and update labels in: {consolidated_names_file}", log_f)

    updated_rows = []
    fieldnames = None
    rows_read = 0
    labels_changed_count = 0

    try:
        with open(consolidated_names_file, 'r', newline='', encoding='utf-8') as f_in:
            reader = csv.DictReader(f_in)
            fieldnames = reader.fieldnames
            if not fieldnames or 'Pulse Label' not in fieldnames:
                write_log(f"  Error: {consolidated_names_file} is missing 'Pulse Label' column or has no header.", log_f)
                exit()
            
            updated_rows.append(fieldnames) # Keep header for writing later

            for i, row_dict in enumerate(reader):
                rows_read += 1
                original_label = row_dict.get('Pulse Label', '').strip()
                new_label = original_label # Default to original

                if original_label in label_transformation_map:
                    new_label = label_transformation_map[original_label]
                    if new_label != original_label:
                        labels_changed_count += 1
                        write_log(f"  Row {i+2}: Changed label for ('{row_dict.get('First Name')}', '{row_dict.get('Last Name')}') from '{original_label}' -> '{new_label}'", log_f)
                
                # Create a new dictionary for the updated row to ensure order and all fields are kept
                updated_row_dict = {}
                for field in fieldnames: # Iterate in original field order
                    if field == 'Pulse Label':
                        updated_row_dict[field] = new_label
                    else:
                        updated_row_dict[field] = row_dict.get(field, '') # Get original value or empty string if missing
                updated_rows.append(updated_row_dict)

        write_log(f"Successfully read {rows_read} data rows from {consolidated_names_file}.", log_f)

    except FileNotFoundError:
        write_log(f"Error: File not found at {consolidated_names_file}", log_f)
        exit()
    except Exception as e:
        write_log(f"Error reading {consolidated_names_file}: {e}", log_f)
        import traceback
        write_log(traceback.format_exc(), log_f)
        exit()

    if not fieldnames or len(updated_rows) <= 1: # Only header or empty
        write_log("No data processed or fieldnames not available. Exiting without writing.", log_f)
        exit()

    # Write the updated rows back to the file
    try:
        with open(consolidated_names_file, 'w', newline='', encoding='utf-8') as f_out:
            # updated_rows[0] is the fieldnames list
            writer = csv.DictWriter(f_out, fieldnames=updated_rows[0])
            writer.writeheader()
            # Write data rows (all rows after the header, which are already dicts)
            writer.writerows(updated_rows[1:])
            
        write_log(f"Successfully wrote {rows_read} data rows back to {consolidated_names_file}", log_f)
        write_log(f"Total Pulse Labels changed: {labels_changed_count}", log_f)
    except Exception as e:
        write_log(f"Error writing to {consolidated_names_file}: {e}", log_f)
        import traceback
        write_log(traceback.format_exc(), log_f)

    write_log("--- Script update_pulse_labels.py finished ---", log_f) 