#!/usr/bin/env python3
"""
Metadata Viewer - View metadata information from processed data blocks
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional
import argparse

def load_metadata(metadata_file: Path) -> Dict:
    """Load metadata from JSON file"""
    try:
        with open(metadata_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading metadata from {metadata_file}: {e}")
        return {}

def display_file_metadata(metadata: Dict, show_details: bool = False):
    """Display file metadata in a formatted way"""
    print(f"\n📁 File: {metadata.get('file_name', 'Unknown')}")
    print(f"   Block: {metadata.get('block_number', 'Unknown')}")
    print(f"   Size: {metadata.get('block_size_bytes', 0) / (1024*1024):.2f} MB")
    print(f"   Checksum: {metadata.get('checksum', 'Unknown')[:16]}...")
    print(f"   Processed by: {metadata.get('machine_id', 'Unknown')}")
    
    # Display basic file metadata if available
    if 'file_metadata' in metadata and metadata['file_metadata']:
        fm = metadata['file_metadata']
        print(f"\n📋 File Structure:")
        print(f"   Type: {fm.get('file_type', 'unknown')}")
        
        if fm.get('columns'):
            print(f"   Columns ({len(fm['columns'])}):")
            for i, col in enumerate(fm['columns'], 1):
                data_type = fm.get('data_types', {}).get(col, 'unknown')
                print(f"     {i:2d}. {col} ({data_type})")
        
        if fm.get('row_count'):
            print(f"   Row Count: {fm['row_count']:,}")
        
        print(f"   Encoding: {fm.get('encoding', 'utf-8')}")
        
        if fm.get('file_type') == 'csv':
            print(f"   Delimiter: '{fm.get('delimiter', ',')}'")
            print(f"   Has Header: {fm.get('has_header', True)}")
    
    if show_details:
        print(f"\n🔍 Full Metadata:")
        print(json.dumps(metadata, indent=2))

def find_metadata_files(output_dir: Path) -> List[Path]:
    """Find all metadata files in output directory"""
    metadata_files = []
    if output_dir.exists():
        for metadata_file in output_dir.rglob("*_metadata.json"):
            metadata_files.append(metadata_file)
    return sorted(metadata_files)

def main():
    parser = argparse.ArgumentParser(description='Metadata Viewer for Distributed Data Processing')
    parser.add_argument('--output-dir', default='processed_data_client1',
                       help='Output directory to scan for metadata files')
    parser.add_argument('--file-name', 
                       help='Show metadata for specific file only')
    parser.add_argument('--details', action='store_true',
                       help='Show detailed metadata information')
    parser.add_argument('--summary', action='store_true',
                       help='Show summary of all files')
    
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    
    if not output_dir.exists():
        print(f"❌ Output directory {output_dir} does not exist!")
        return
    
    metadata_files = find_metadata_files(output_dir)
    
    if not metadata_files:
        print(f"❌ No metadata files found in {output_dir}")
        return
    
    print(f"📂 Found {len(metadata_files)} metadata files in {output_dir}")
    
    # Group by file name
    files_metadata = {}
    for metadata_file in metadata_files:
        metadata = load_metadata(metadata_file)
        file_name = metadata.get('file_name', 'Unknown')
        
        if file_name not in files_metadata:
            files_metadata[file_name] = []
        files_metadata[file_name].append(metadata)
    
    # Show summary
    if args.summary:
        print(f"\n📊 Summary:")
        print(f"   Total files: {len(files_metadata)}")
        total_blocks = sum(len(blocks) for blocks in files_metadata.values())
        print(f"   Total blocks: {total_blocks}")
        
        for file_name, blocks in files_metadata.items():
            print(f"\n   📁 {file_name}:")
            print(f"      Blocks: {len(blocks)}")
            if blocks and 'file_metadata' in blocks[0] and blocks[0]['file_metadata']:
                fm = blocks[0]['file_metadata']
                print(f"      Type: {fm.get('file_type', 'unknown')}")
                if fm.get('columns'):
                    print(f"      Columns: {len(fm['columns'])}")
                if fm.get('row_count'):
                    print(f"      Rows: {fm['row_count']:,}")
    
    # Show specific file or all files
    if args.file_name:
        if args.file_name in files_metadata:
            print(f"\n📋 Metadata for {args.file_name}:")
            for metadata in files_metadata[args.file_name]:
                display_file_metadata(metadata, args.details)
        else:
            print(f"❌ File {args.file_name} not found!")
    else:
        for file_name, blocks in files_metadata.items():
            print(f"\n{'='*60}")
            print(f"📁 File: {file_name} ({len(blocks)} blocks)")
            print(f"{'='*60}")
            
            # Show metadata for first block (contains file structure)
            if blocks:
                display_file_metadata(blocks[0], args.details)
                
                if len(blocks) > 1:
                    print(f"\n📦 Additional blocks: {len(blocks)-1}")
                    for block in blocks[1:]:
                        print(f"   Block {block.get('block_number')}: "
                              f"{block.get('block_size_bytes', 0) / (1024*1024):.2f} MB")

if __name__ == "__main__":
    main() 