import os
import csv
from pathlib import Path
import json
import time

input_dir=r"D:\feb_to_apr"
output_dir =r"D:\json_output"

def convert_to_json(input_dir, output_dir):
    print(f"Starting conversion from {input_dir} to {output_dir}")
    start_time = time.time()
    file_count = 0
    
    # Extract the folder name from the input directory path
    folder_name = os.path.basename(input_dir)
    print(f"Processing folder: {folder_name}")
    
    # Create a specific output directory for this input folder
    specific_output_dir = os.path.join(output_dir, folder_name)
    Path(specific_output_dir).mkdir(parents=True, exist_ok=True)
    print(f"Created output directory: {specific_output_dir}")

    # Iterate through all files in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            file_count += 1
            csv_file_path = os.path.join(input_dir, filename)
            json_file_path = os.path.join(specific_output_dir, filename.replace('.csv', '.json'))
            print(f"Converting {filename}...")

            # Read CSV and convert to JSON
            try:
                with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
                    csv_reader = csv.DictReader(csv_file)
                    data = [row for row in csv_reader]
                    row_count = len(data)

                # Write JSON to file
                with open(json_file_path, mode='w', encoding='utf-8') as json_file:
                    json.dump(data, json_file, indent=4)
                
                print(f"  ✓ Converted {filename} with {row_count} records")
            except Exception as e:
                print(f"  ✗ Error converting {filename}: {str(e)}")
    
    elapsed_time = time.time() - start_time
    print(f"Conversion complete: {file_count} files processed in {elapsed_time:.2f} seconds")
    print(f"All files from {input_dir} converted to {specific_output_dir}")

convert_to_json(input_dir, output_dir)