import csv
import os

consolidated_names_file = os.path.join('05_airtable_and_mapping', '02_pulse', 'pulse_consolidated_names.csv')
correction_log_file = os.path.join('03_scripts', 'correct_consolidated_names_log.txt')

def log_correction_message(message, log_handle):
    print(message)
    log_handle.write(message + '\n')

with open(correction_log_file, 'w', encoding='utf-8') as clf:
    log_correction_message(f"--- Running script: correct_consolidated_names.py (rebuild strategy) ---", clf)
    log_correction_message(f"Attempting to read: {consolidated_names_file}", clf)

    original_rows = []
    fieldnames = None
    try:
        with open(consolidated_names_file, 'r', newline='', encoding='utf-8') as f_in:
            reader = csv.DictReader(f_in)
            fieldnames = reader.fieldnames
            if not fieldnames:
                log_correction_message(f"Error: {consolidated_names_file} is empty or has no header.", clf)
                exit()
            for row in reader:
                original_rows.append(row)
        log_correction_message(f"Successfully read {len(original_rows)} original rows from {consolidated_names_file}.", clf)
    except FileNotFoundError:
        log_correction_message(f"Error: File not found at {consolidated_names_file}", clf)
        exit()
    except Exception as e:
        log_correction_message(f"Error reading {consolidated_names_file}: {e}", clf)
        exit()

    if not fieldnames:
        log_correction_message(f"Error: fieldnames not set. Exiting.", clf)
        exit()

    log_correction_message("--- Rebuilding and Correcting List ---", clf)
    rebuilt_rows = []
    lia_panos_bhi_added_to_rebuilt_list = False

    for i, row_dict in enumerate(original_rows):
        # Work with a copy for modification
        corrected_row = dict(row_dict)
        first_name = corrected_row.get('First Name', '').strip()
        last_name = corrected_row.get('Last Name', '').strip()
        pulse_label = corrected_row.get('Pulse Label', '').strip()
        
        action_taken = False

        # 1. Lia Panos / Amalia Demopolus (BHI)
        if pulse_label == 'BHI':
            if first_name == 'Amalia' and last_name == 'Demopolus':
                log_correction_message(f"  Original Row {i+1}: Removing Amalia Demopolus, BHI.", clf)
                action_taken = True
                continue # Skip adding this row to rebuilt_rows
            elif first_name == 'Lia' and last_name == 'Panos':
                lia_panos_bhi_added_to_rebuilt_list = True # Mark that correct Lia is present

        # 2. Martha Trujillo (BHI) - Trujilo -> Trujillo
        if first_name == 'Martha' and last_name == 'Trujilo' and pulse_label == 'BHI':
            corrected_row['Last Name'] = 'Trujillo'
            log_correction_message(f"  Original Row {i+1}: Corrected Martha Trujilo (BHI) -> Martha Trujillo", clf)
            action_taken = True

        # 3. Sarah Mulligan (Counseling) - remove (Denman)
        if first_name == 'Sarah' and last_name == 'Mulligan (Denman)' and pulse_label == 'Counseling':
            corrected_row['Last Name'] = 'Mulligan'
            log_correction_message(f"  Original Row {i+1}: Corrected Sarah Mulligan (Denman) (Counseling) -> Sarah Mulligan", clf)
            action_taken = True

        # 4. Marty Beirne (Counseling) -> Martin Beirne
        if first_name == 'Marty' and last_name == 'Beirne' and pulse_label == 'Counseling':
            corrected_row['First Name'] = 'Martin'
            log_correction_message(f"  Original Row {i+1}: Corrected Marty Beirne (Counseling) -> Martin Beirne", clf)
            action_taken = True
        
        # 5. Kathy Ordiway (Counseling) -> Kathryn Ordiway
        if first_name == 'Kathy' and last_name == 'Ordiway' and pulse_label == 'Counseling':
            corrected_row['First Name'] = 'Kathryn'
            log_correction_message(f"  Original Row {i+1}: Corrected Kathy Ordiway (Counseling) -> Kathryn Ordiway", clf)
            action_taken = True

        # 6. Radostina "Ina" Yakimova-Marfoe (Counseling) -> Radostina Yakimova-Marfoe
        if first_name == 'Radostina "Ina"' and last_name == 'Yakimova-Marfoe' and pulse_label == 'Counseling':
            corrected_row['First Name'] = 'Radostina'
            log_correction_message(f"  Original Row {i+1}: Corrected Radostina \"Ina\" Yakimova-Marfoe (Counseling) -> Radostina Yakimova-Marfoe", clf)
            action_taken = True
        
        # 7. Pagel Palmer Jr. (Counseling) -> Pagel Palmer
        if first_name == 'Pagel' and last_name == 'Palmer Jr.' and pulse_label == 'Counseling':
            corrected_row['Last Name'] = 'Palmer'
            log_correction_message(f"  Original Row {i+1}: Corrected Pagel Palmer Jr. (Counseling) -> Pagel Palmer", clf)
            action_taken = True
            
        rebuilt_rows.append(corrected_row)

    # After processing all original rows, ensure Lia Panos (BHI) is in the rebuilt list if Amalia was supposed to be replaced by her.
    if not lia_panos_bhi_added_to_rebuilt_list:
        # This covers cases where Amalia was removed (or not present) AND Lia was also not present.
        log_correction_message(f"  Adding Lia Panos, BHI to rebuilt list as she was not found.", clf)
        rebuilt_rows.append({'First Name': 'Lia', 'Last Name': 'Panos', 'Pulse Label': 'BHI'})

    log_correction_message("--- Deduplicating Rebuilt List --- ", clf)
    final_rows = []
    seen_entries = set()
    for row_dict in rebuilt_rows:
        entry_tuple = (
            row_dict.get('First Name', '').strip(), 
            row_dict.get('Last Name', '').strip(), 
            row_dict.get('Pulse Label', '').strip()
        )
        if entry_tuple not in seen_entries:
            # Ensure all original fieldnames are present
            complete_row = {fn: row_dict.get(fn, '') for fn in fieldnames}
            final_rows.append(complete_row)
            seen_entries.add(entry_tuple)
        else:
            log_correction_message(f"  Duplicate removed from rebuilt list: {entry_tuple[0]} {entry_tuple[1]}, {entry_tuple[2]}", clf)
    log_correction_message(f"Deduplication complete. Final rebuilt row count: {len(final_rows)}", clf)

    log_correction_message(f"--- Writing {len(final_rows)} rows to {consolidated_names_file} --- ", clf)
    try:
        with open(consolidated_names_file, 'w', newline='', encoding='utf-8') as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(final_rows)
        log_correction_message(f"Successfully updated {consolidated_names_file} using rebuild strategy.", clf)
    except Exception as e:
        log_correction_message(f"Error writing {consolidated_names_file}: {e}", clf)

    log_correction_message("--- Script correct_consolidated_names.py (rebuild strategy) finished ---", clf) 