import csv
import os

# Define file paths
northshore_names_file = os.path.join('05_airtable_and_mapping', '03_northshore', 'northshore_names.csv')
log_file_path = os.path.join('03_scripts', 'correct_northshore_names_log.txt')

def write_log(message, handle):
    print(message)
    handle.write(message + '\n')

# Define the corrections
# Format: (Current LastName, Current FirstName_or_None_if_any, New FirstName, New LastName_or_None_if_no_change)
corrections_map = {
    ('Chernaik', None): ('Jonathan', None),
    ('Marvin', None): ('Robert', None),
    ('Lesher', None): ('Susan', None),
    ('Nierstheimer', None): ('Jennifer', None),
    ('Sholemson', None): ('Jeffrey', None),
    ('Williams', None): ('Christopher', None),
    ('Lee-Elstein', 'Alexandra'): ('Alex', 'Elstein') # Special case: change both First and Last name based on current pair
}

with open(log_file_path, 'w', encoding='utf-8') as log_f:
    write_log("--- Running script: correct_northshore_names.py ---", log_f)
    write_log(f"Attempting to read and correct: {northshore_names_file}", log_f)

    rows = []
    fieldnames = None
    corrections_applied_count = 0

    try:
        with open(northshore_names_file, 'r', newline='', encoding='utf-8') as f_in:
            reader = csv.DictReader(f_in)
            fieldnames = reader.fieldnames
            if not fieldnames or 'First Name' not in fieldnames or 'Last Name' not in fieldnames:
                write_log(f"  Error: {northshore_names_file} is missing 'First Name' or 'Last Name' columns.", log_f)
                exit()
            
            for row_num, row_dict in enumerate(reader):
                current_first = row_dict.get('First Name', '').strip()
                current_last = row_dict.get('Last Name', '').strip()
                original_row_tuple = (current_first, current_last)
                updated = False

                # Check for specific full name change first (Lee-Elstein to Elstein)
                if (current_last, current_first) == ('Lee-Elstein', 'Alexandra'):
                    new_first, new_last = corrections_map[('Lee-Elstein', 'Alexandra')]
                    row_dict['First Name'] = new_first
                    row_dict['Last Name'] = new_last
                    write_log(f"  Corrected Row {row_num+2}: ('{current_first}', '{current_last}') -> ('{new_first}', '{new_last}')", log_f)
                    corrections_applied_count += 1
                    updated = True
                else:
                    # Check for other corrections based on LastName only
                    for (match_last, match_first_optional), (new_first, new_last_optional) in corrections_map.items():
                        if match_first_optional is None and current_last == match_last:
                            if row_dict['First Name'] != new_first:
                                row_dict['First Name'] = new_first
                                write_log(f"  Corrected Row {row_num+2}: First Name for '{current_last}' from '{current_first}' -> '{new_first}'", log_f)
                                corrections_applied_count += 1
                            # No change to last name in these cases (new_last_optional is None)
                            updated = True # Mark as processed even if first name was already correct
                            break 
                rows.append(row_dict)
        write_log(f"Successfully read {len(rows)} rows from {northshore_names_file}.", log_f)

    except FileNotFoundError:
        write_log(f"Error: File not found at {northshore_names_file}", log_f)
        exit()
    except Exception as e:
        write_log(f"Error reading {northshore_names_file}: {e}", log_f)
        import traceback
        write_log(traceback.format_exc(), log_f)
        exit()

    if not fieldnames or not rows:
        write_log("No data processed or fieldnames not available. Exiting without writing.", log_f)
        exit()

    # Write the updated rows back to the file
    try:
        with open(northshore_names_file, 'w', newline='', encoding='utf-8') as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        write_log(f"Successfully wrote {len(rows)} rows back to {northshore_names_file}", log_f)
        write_log(f"Total corrections applied: {corrections_applied_count}", log_f)
    except Exception as e:
        write_log(f"Error writing to {northshore_names_file}: {e}", log_f)
        import traceback
        write_log(traceback.format_exc(), log_f)

    write_log("--- Script correct_northshore_names.py finished ---", log_f) 