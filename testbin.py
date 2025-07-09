# c:\Users\minht\big_data\bin_to_csv_analyzer.py
import pandas as pd
import numpy as np
import struct
import os
import json
from datetime import datetime, date

def load_metadata(metadata_path):
    """Load metadata from JSON file"""
    try:
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        print(f"Loaded metadata from {metadata_path}")
        return metadata
    except Exception as e:
        print(f"Error loading metadata: {e}")
        return None

def calculate_record_size(data_types):
    """Calculate the size of each record in bytes based on data types"""
    size = 0
    for col, dtype in data_types.items():
        if dtype == 'integer':
            size += 4  # 4 bytes for integer
        elif dtype == 'float':
            size += 4  # 4 bytes for float  
        elif dtype == 'date':
            size += 4  # 4 bytes for date (stored as integer)
        elif dtype == 'string':
            size += 50  # Assume max 50 chars for string
    return size

def parse_record(binary_data, offset, data_types, columns):
    """Parse a single record from binary data"""
    record = []
    current_offset = offset
    
    for col in columns:
        dtype = data_types[col]
        
        if current_offset >= len(binary_data):
            record.append(None)
            continue
            
        try:
            if dtype == 'integer':
                if current_offset + 4 > len(binary_data):
                    record.append(None)
                    current_offset += 4
                else:
                    value = struct.unpack('<i', binary_data[current_offset:current_offset+4])[0]
                    record.append(value)
                    current_offset += 4
            elif dtype == 'float':
                if current_offset + 4 > len(binary_data):
                    record.append(None)
                    current_offset += 4
                else:
                    value = struct.unpack('<f', binary_data[current_offset:current_offset+4])[0]
                    record.append(value)
                    current_offset += 4
            elif dtype == 'date':
                if current_offset + 4 > len(binary_data):
                    record.append(None)
                    current_offset += 4
                else:
                    date_int = struct.unpack('<i', binary_data[current_offset:current_offset+4])[0]
                    try:
                        # Convert to proper date format
                        if date_int > 0:
                            # Try Excel date format (days since 1900-01-01)
                            if date_int < 100000:
                                base_date = datetime(1900, 1, 1)
                                value = (base_date + pd.Timedelta(days=date_int-2)).date()
                            # Try Unix timestamp
                            elif 1000000000 <= date_int <= 2147483647:
                                value = datetime.fromtimestamp(date_int).date()
                            # Try days since Unix epoch
                            elif 0 < date_int < 50000:
                                base_date = datetime(1970, 1, 1)
                                value = (base_date + pd.Timedelta(days=date_int)).date()
                            else:
                                # Try YYYYMMDD format
                                date_str = str(date_int)
                                if len(date_str) == 8:
                                    year = int(date_str[:4])
                                    month = int(date_str[4:6])
                                    day = int(date_str[6:8])
                                    if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                                        value = datetime(year, month, day).date()
                                    else:
                                        value = None
                                else:
                                    value = None
                        else:
                            value = None
                    except (ValueError, OSError, OverflowError):
                        value = None
                    record.append(value)
                    current_offset += 4
            elif dtype == 'string':
                string_length = 50  # Fixed length for strings
                if current_offset + string_length > len(binary_data):
                    record.append('')
                    current_offset += string_length
                else:
                    string_bytes = binary_data[current_offset:current_offset+string_length]
                    value = string_bytes.decode('utf-8', errors='ignore').rstrip('\x00').strip()
                    record.append(value)
                    current_offset += string_length
        except Exception as e:
            print(f"Error parsing field {col} (type: {dtype}): {e}")
            record.append(None)
            # Still need to advance offset
            if dtype in ['integer', 'float', 'date']:
                current_offset += 4
            elif dtype == 'string':
                current_offset += 50
    
    return record

def read_binary_file(file_path, metadata):
    """Read binary file and extract data based on metadata"""
    try:
        print(f"Reading binary file: {file_path}")
        
        # Extract metadata information
        data_types = metadata['file_metadata']['data_types']
        columns = metadata['file_metadata']['columns']
        row_count = metadata['file_metadata']['row_count']
        
        # Filter out date columns
        filtered_columns = [col for col in columns if data_types[col] != 'date']
        filtered_data_types = {col: dtype for col, dtype in data_types.items() if dtype != 'date'}
        
        print(f"Expected columns (excluding dates): {filtered_columns}")
        print(f"Expected row count: {row_count}")
        
        with open(file_path, 'rb') as f:
            binary_data = f.read()
            
        print(f"Binary file size: {len(binary_data)} bytes")
        
        # Parse records
        records = []
        
        # Try CSV-like parsing first (since original was CSV)
        try:
            text_data = binary_data.decode('utf-8', errors='ignore')
            lines = text_data.strip().split('\n')
            
            print(f"Found {len(lines)} lines in text format")
            
            # Skip header if present
            start_idx = 1 if metadata['file_metadata']['has_header'] else 0
            
            for i, line in enumerate(lines[start_idx:], start=start_idx):
                if not line.strip():
                    continue
                    
                values = line.split(metadata['file_metadata']['delimiter'])
                if len(values) == len(columns):
                    record = convert_csv_record(values, data_types, columns, filtered_columns)
                    records.append(record)
                    
                if len(records) >= row_count:
                    break
                    
        except Exception as e:
            print(f"CSV parsing failed: {e}")
            print("Binary parsing not supported without date columns")
                    
        print(f"Successfully parsed {len(records)} records")
        return records, filtered_columns, filtered_data_types
        
    except Exception as e:
        print(f"Error reading file: {e}")
        return [], [], {}

def convert_csv_record(values, data_types, columns, filtered_columns):
    """Convert CSV values to appropriate data types, excluding date columns"""
    record = []
    for i, col in enumerate(columns):
        if col not in filtered_columns:
            continue  # Skip date columns
            
        dtype = data_types[col]
        if i < len(values):
            value = values[i].strip()
            try:
                if dtype == 'integer':
                    record.append(int(value) if value and value != '' else None)
                elif dtype == 'float':
                    record.append(float(value) if value and value != '' else None)
                elif dtype == 'string':
                    record.append(value if value else '')
                else:
                    record.append(value)
            except (ValueError, TypeError):
                record.append(None)
        else:
            record.append(None)
    return record

def create_csv_with_metadata(records, columns, data_types, output_path):
    """Convert records to CSV with metadata column names (excluding date columns)"""
    # Create DataFrame
    df = pd.DataFrame(records, columns=columns)
    
    # Convert data types
    for col, dtype in data_types.items():
        if col not in df.columns:
            continue
            
        try:
            if dtype == 'integer':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif dtype == 'float':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
            elif dtype == 'string':
                df[col] = df[col].astype(str).replace('nan', '')
        except Exception as e:
            print(f"Warning: Could not convert column {col} to {dtype}: {e}")
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"CSV file created: {output_path}")
    
    return df

def analyze_data(df):
    """Analyze the converted data"""
    print("\n" + "="*50)
    print("DATA ANALYSIS REPORT")
    print("="*50)
    
    # Basic statistics
    print(f"Dataset Shape: {df.shape}")
    print(f"\nData Types:")
    print(df.dtypes)
    
    print(f"\nBasic Statistics:")
    print(df.describe())
    
    print(f"\nNull Values:")
    print(df.isnull().sum())
    
    # Specific analysis for assignment data
    if 'assignment_type' in df.columns:
        print(f"\nAssignment Type Distribution:")
        print(df['assignment_type'].value_counts())
    
    # Calculate completion rates if applicable
    if 'started_student_count' in df.columns and 'completed_or_mastered_student_count' in df.columns:
        df['completion_rate'] = (df['completed_or_mastered_student_count'] / df['started_student_count']).fillna(0)
        print(f"\nCompletion Rate Statistics:")
        print(f"Mean: {df['completion_rate'].mean():.3f}")
        print(f"Median: {df['completion_rate'].median():.3f}")
        print(f"Min: {df['completion_rate'].min():.3f}")
        print(f"Max: {df['completion_rate'].max():.3f}")
    
    return df

def main():
    # File paths
    bin_file_path = r"C:\Users\minht\big_data\processed_data_machine2\adets_csv\block_0000.bin"
    metadata_path = r"C:\Users\minht\big_data\processed_data_machine2\adets_csv\block_0000_metadata.json"
    csv_output_path = r"C:\Users\minht\big_data\converted_data.csv"
    
    # Load metadata
    metadata = load_metadata(metadata_path)
    
    if not metadata:
        print("Failed to load metadata. Exiting.")
        return
    
    # Read binary file using metadata (excluding date columns)
    records, filtered_columns, filtered_data_types = read_binary_file(bin_file_path, metadata)
    
    if records:
        # Convert to CSV with proper column names (no date columns)
        df = create_csv_with_metadata(records, filtered_columns, filtered_data_types, csv_output_path)
        
        # Analyze data
        analyzed_df = analyze_data(df)
        
        # Save analysis report
        report_path = r"C:\Users\minht\big_data\analysis_report.txt"
        with open(report_path, 'w') as f:
            f.write("Binary File Analysis Report\n")
            f.write("=" * 50 + "\n")
            f.write(f"Source file: {bin_file_path}\n")
            f.write(f"Total records: {len(records)}\n")
            f.write(f"Columns (excluding dates): {list(df.columns)}\n")
            f.write("\nStatistical Summary:\n")
            f.write(str(df.describe()))
            
        print(f"\nAnalysis report saved to: {report_path}")
    else:
        print("No data found in binary file")

if __name__ == "__main__":
    main()