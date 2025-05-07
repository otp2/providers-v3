import pandas as pd
import os
import re

def clean_and_extract_specialties_pandas(input_filepath, output_filepath, column_name):
    """Reads a CSV using pandas, extracts, cleans, and saves unique specialties."""
    if not os.path.exists(input_filepath):
        print(f"Error: Input file '{input_filepath}' not found.")
        return

    try:
        df = pd.read_csv(input_filepath, dtype=str) 
    except Exception as e:
        print(f"Error reading CSV file '{input_filepath}' with pandas: {e}")
        return

    if column_name not in df.columns:
        print(f"Error: Column '{column_name}' not found in '{input_filepath}'. Available columns: {df.columns.tolist()}")
        return

    unique_specialties = set()
    line_num_for_error = 0 
    try:
        # Use iterrows to get an index for error reporting, approximating line number
        for index, row_content in df.iterrows():
            line_num_for_error = index + 2 # +1 for 0-index, +1 for header
            item_list_str = row_content[column_name]
            
            if pd.isna(item_list_str): # Explicitly skip NaN values after selecting the series content
                continue

            cleaned_str = str(item_list_str).strip()
            if cleaned_str.startswith('"') and cleaned_str.endswith('"'):
                cleaned_str = cleaned_str[1:-1]
            cleaned_str = re.sub(r'"?=$|=', '', cleaned_str) 
            cleaned_str = cleaned_str.strip()
            if not cleaned_str:
                continue
            items = cleaned_str.split(',')
            for item in items:
                stripped_item = item.strip()
                if stripped_item.startswith('"') and stripped_item.endswith('"'):
                    stripped_item = stripped_item[1:-1]
                elif stripped_item.startswith('"'):
                    stripped_item = stripped_item[1:]
                elif stripped_item.endswith('"'):
                    stripped_item = stripped_item[:-1]
                stripped_item = stripped_item.strip()
                if stripped_item:
                    unique_specialties.add(stripped_item)
    except Exception as e:
        print(f"Error processing pandas DataFrame for '{input_filepath}' at/near source line {line_num_for_error}: {e}")
        return
    
    if not unique_specialties:
        print(f"No unique specialties found for column '{column_name}' in '{input_filepath}'.")
        return

    sorted_specialties = sorted(list(unique_specialties))
    output_dir = os.path.dirname(output_filepath)
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            print(f"Created directory: {output_dir}")
        except Exception as e:
            print(f"Error creating directory {output_dir}: {e}")
            return
            
    try:
        with open(output_filepath, 'w', encoding='utf-8') as f:
            for specialty in sorted_specialties:
                f.write(f"{specialty}\n")
        print(f"Generated picklist for '{column_name}' from '{input_filepath}' in '{output_filepath}' (using pandas method)")
    except Exception as e:
        print(f"Error writing picklist to '{output_filepath}': {e}")


def clean_and_extract_specialties_raw_lines(input_filepath, output_filepath):
    """Reads a single-column CSV line by line (skipping header), extracts, cleans, and saves unique specialties."""
    if not os.path.exists(input_filepath):
        print(f"Error: Input file '{input_filepath}' not found.")
        return

    unique_specialties = set()
    line_num_for_error = 0 # Initialize for error reporting
    try:
        with open(input_filepath, 'r', encoding='utf-8') as f:
            next(f) # Skip header line
            for line_num, line_content in enumerate(f, 2): # Start line count from 2 for errors
                line_num_for_error = line_num
                cleaned_str = line_content.strip()
                # Clean the whole line first
                if cleaned_str.startswith('"') and cleaned_str.endswith('"'):
                    cleaned_str = cleaned_str[1:-1]
                cleaned_str = re.sub(r'"?=$|=', '', cleaned_str) 
                cleaned_str = cleaned_str.strip()

                if not cleaned_str:
                    continue
                
                items = cleaned_str.split(',')
                for item in items:
                    stripped_item = item.strip()
                    # Further clean each individual item
                    if stripped_item.startswith('"') and stripped_item.endswith('"'):
                        stripped_item = stripped_item[1:-1]
                    elif stripped_item.startswith('"'): # Handle only leading quote
                        stripped_item = stripped_item[1:]
                    elif stripped_item.endswith('"'): # Handle only trailing quote
                        stripped_item = stripped_item[:-1]
                    
                    stripped_item = stripped_item.strip() # Strip again in case quotes were the only content
                    if stripped_item:
                        unique_specialties.add(stripped_item)
    except Exception as e:
        print(f"Error processing file '{input_filepath}' line by line at/near line {line_num_for_error}: {e}")
        return
    
    if not unique_specialties:
        print(f"No unique specialties found in '{input_filepath}' (using raw lines method).")
        return

    sorted_specialties = sorted(list(unique_specialties))
    output_dir = os.path.dirname(output_filepath)
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            print(f"Created directory: {output_dir}")
        except Exception as e:
            print(f"Error creating directory {output_dir}: {e}")
            return
            
    try:
        with open(output_filepath, 'w', encoding='utf-8') as f:
            for specialty in sorted_specialties:
                f.write(f"{specialty}\n")
        print(f"Generated picklist from '{input_filepath}' in '{output_filepath}' (using raw lines method)")
    except Exception as e:
        print(f"Error writing picklist to '{output_filepath}': {e}")

# Define file configurations
files_to_process = [
    {
        'method': 'raw_lines',
        'input': '00_source_data/pulse_data/pulse specialites.csv',
        'output': '01_processed_data/source_specific_picklists/pulse_extracted_specialties.txt',
        'column': None
    },
    {
        'method': 'raw_lines',
        'input': '00_source_data/website_data/web-specialities.csv',
        'output': '01_processed_data/source_specific_picklists/website_extracted_specialties.txt',
        'column': None
    }
]

# Process each file
for file_config in files_to_process:
    if file_config['method'] == 'pandas':
        clean_and_extract_specialties_pandas(file_config['input'], file_config['output'], file_config['column'])
    elif file_config['method'] == 'raw_lines':
        clean_and_extract_specialties_raw_lines(file_config['input'], file_config['output'])

print("Specialty picklist generation process complete.") 