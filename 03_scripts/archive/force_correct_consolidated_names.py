import csv
import os

consolidated_names_file = os.path.join('05_airtable_and_mapping', '02_pulse', 'pulse_consolidated_names.csv')
log_file = os.path.join('03_scripts', 'force_correct_consolidated_names_log.txt')

def write_log(message, handle):
    print(message)
    handle.write(message + '\n')

# Define the known correct names and their desired state
# Format: (CorrectFirstName, CorrectLastName, PulseLabel)
explicitly_correct_names = [
    ('Lia', 'Panos', 'BHI'),
    ('Martha', 'Trujillo', 'BHI'),
    ('Martha', 'Trujillo', 'Counseling'), # Assuming Martha Trujillo is also in Counseling correctly
    ('Sarah', 'Mulligan', 'Counseling'),
    ('Martin', 'Beirne', 'Counseling'),
    ('Kathryn', 'Ordiway', 'Counseling'),
    ('Radostina', 'Yakimova-Marfoe', 'Counseling'),
    ('Pagel', 'Palmer', 'Counseling'),
    ('Michael', 'Geraci', 'Counseling') # For Mike Geraci
]

# Define known incorrect forms to be replaced by the correct ones above
# Format: (IncorrectFirstName, IncorrectLastName, PulseLabel)
known_incorrect_forms = [
    ('Martha', 'Trujilo', 'BHI'),
    ('Sarah', 'Mulligan (Denman)', 'Counseling'),
    ('Marty', 'Beirne', 'Counseling'),
    ('Kathy', 'Ordiway', 'Counseling'),
    ('Radostina "Ina"', 'Yakimova-Marfoe', 'Counseling'),
    ('Pagel', 'Palmer Jr.', 'Counseling'),
    ('Mike', 'Geraci', 'Counseling')
]

name_to_remove = ('Amalia', 'Demopolus', 'BHI')

with open(log_file, 'w', encoding='utf-8') as clf:
    write_log(f"--- Running script: force_correct_consolidated_names.py ---", clf)
    write_log(f"Attempting to read: {consolidated_names_file}", clf)

    original_rows = []
    fieldnames = None
    try:
        with open(consolidated_names_file, 'r', newline='', encoding='utf-8') as f_in:
            reader = csv.DictReader(f_in)
            fieldnames = reader.fieldnames
            if not fieldnames:
                write_log(f"Error: {consolidated_names_file} is empty or has no header.", clf)
                exit()
            for row in reader:
                original_rows.append(row)
        write_log(f"Successfully read {len(original_rows)} original rows from {consolidated_names_file}. Fieldnames: {fieldnames}", clf)
    except FileNotFoundError:
        write_log(f"Error: File not found at {consolidated_names_file}", clf)
        exit()
    except Exception as e:
        write_log(f"Error reading {consolidated_names_file}: {e}", clf)
        exit()

    if not fieldnames: # Should be caught above, but defensive
        write_log("Error: CSV fieldnames were not captured. Exiting.", clf)
        exit() 

    write_log("--- Rebuilding list with forced corrections ---", clf)
    
    # This map will store the data for the new CSV. Key: (First, Last, Label), Value: full row dict
    # This inherently handles deduplication of the final correct forms.
    rebuilt_data_map = {}

    # 1. Add all explicitly correct names first to ensure they are prioritized.
    write_log("Step 1: Seeding rebuilt list with EXPLICITLY CORRECT names.", clf)
    for first, last, label in explicitly_correct_names:
        # Create a full row dict for these, ensuring all original fieldnames are present
        row_to_add = {fn: '' for fn in fieldnames} # Initialize with all fields
        row_to_add['First Name'] = first
        row_to_add['Last Name'] = last
        row_to_add['Pulse Label'] = label
        rebuilt_data_map[(first, last, label)] = row_to_add
        write_log(f"  Ensured correct entry: {first}, {last}, {label}", clf)

    # 2. Process original rows: carry over good data, skip bad/superfluous data.
    write_log("Step 2: Processing original rows from input file.", clf)
    for i, original_row_dict in enumerate(original_rows):
        current_first = original_row_dict.get('First Name', '').strip()
        current_last = original_row_dict.get('Last Name', '').strip()
        current_label = original_row_dict.get('Pulse Label', '').strip()
        current_tuple = (current_first, current_last, current_label)

        # Check if this row is the one to remove
        if current_tuple == name_to_remove:
            write_log(f"  Original Row {i+1} ({current_first} {current_last}, {current_label}): REMOVING (matches name_to_remove).", clf)
            continue

        # Check if this row is one of the known incorrect forms
        if current_tuple in known_incorrect_forms:
            write_log(f"  Original Row {i+1} ({current_first} {current_last}, {current_label}): SKIPPING (known incorrect form, corrected version already seeded).", clf)
            continue
        
        # If it's not explicitly removed or a known incorrect form, 
        # add it to our map. If it's a corrected form that was already seeded, this will just update the row with any other fields from this instance.
        # If it is a completely different provider, it gets added.
        # This preserves other data fields from the original file if a name was already correct.
        if current_tuple not in rebuilt_data_map: # Add if it's a new, unrelated entry
             write_log(f"  Original Row {i+1} ({current_first} {current_last}, {current_label}): KEEPING (not targeted for removal/correction, and not already present as a corrected form).", clf)
        rebuilt_data_map[current_tuple] = original_row_dict # Add/overwrite to preserve other columns if name was already correct from seed

    final_rows_to_write = list(rebuilt_data_map.values())
    write_log(f"Total rows after processing and deduplication by map key: {len(final_rows_to_write)}", clf)

    write_log(f"--- Writing {len(final_rows_to_write)} rows to {consolidated_names_file} --- ", clf)
    try:
        with open(consolidated_names_file, 'w', newline='', encoding='utf-8') as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(final_rows_to_write)
        write_log(f"Successfully updated {consolidated_names_file} using force_correct script.", clf)
    except Exception as e:
        write_log(f"Error writing {consolidated_names_file}: {e}", clf)

    write_log("--- Script force_correct_consolidated_names.py finished ---", clf) 