#!/usr/bin/env python3
"""
Block Processor - Utilities for managing 128MB data blocks with offset information
Helps with block processing, file reconstruction, and assignment management
"""

import json
import os
import base64
from pathlib import Path
from collections import defaultdict

class BlockProcessor:
    def __init__(self, blocks_dir):
        """
        Initialize block processor
        
        Args:
            blocks_dir: Directory containing received blocks (e.g., 'received_blocks_machine1')
        """
        self.blocks_dir = Path(blocks_dir)
        self.block_info = defaultdict(list)
        self.load_block_info()
    
    def load_block_info(self):
        """Load information about all available blocks"""
        print(f"Loading block information from: {self.blocks_dir}")
        
        if not self.blocks_dir.exists():
            print(f"Directory {self.blocks_dir} does not exist!")
            return
        
        # Find all metadata files
        for metadata_file in self.blocks_dir.rglob("*_metadata.json"):
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                
                file_name = metadata['file_name']
                self.block_info[file_name].append(metadata)
                
            except Exception as e:
                print(f"Error loading {metadata_file}: {e}")
        
        # Sort blocks by block number for each file
        for file_name in self.block_info:
            self.block_info[file_name].sort(key=lambda x: x['block_number'])
        
        print(f"Found {len(self.block_info)} files with {sum(len(blocks) for blocks in self.block_info.values())} total blocks")
    
    def get_file_progress(self, file_name):
        """Get progress information for a specific file"""
        if file_name not in self.block_info:
            return None
        
        blocks = self.block_info[file_name]
        if not blocks:
            return None
        
        total_blocks = blocks[0]['total_blocks']
        received_blocks = len(blocks)
        progress_percent = (received_blocks / total_blocks) * 100
        
        return {
            'file_name': file_name,
            'total_blocks': total_blocks,
            'received_blocks': received_blocks,
            'progress_percent': progress_percent,
            'file_size_bytes': blocks[0]['file_size_bytes'],
            'blocks': blocks
        }
    
    def list_all_files(self):
        """List all files and their progress"""
        print("\n📁 File Progress Summary")
        print("=" * 60)
        
        for file_name in sorted(self.block_info.keys()):
            progress = self.get_file_progress(file_name)
            if progress:
                print(f"📄 {file_name}")
                print(f"   Progress: {progress['received_blocks']}/{progress['total_blocks']} blocks ({progress['progress_percent']:.1f}%)")
                print(f"   Size: {progress['file_size_bytes'] / (1024*1024):.2f} MB")
                print()
    
    def reconstruct_file(self, file_name, output_dir="reconstructed_files"):
        """Reconstruct a complete file from its blocks"""
        if file_name not in self.block_info:
            print(f"❌ File {file_name} not found in blocks!")
            return False
        
        blocks = self.block_info[file_name]
        if not blocks:
            print(f"❌ No blocks found for {file_name}!")
            return False
        
        # Check if we have all blocks
        total_blocks = blocks[0]['total_blocks']
        received_blocks = len(blocks)
        
        if received_blocks < total_blocks:
            print(f"❌ Missing blocks for {file_name}: {received_blocks}/{total_blocks}")
            print("Missing block numbers:")
            received_block_nums = {b['block_number'] for b in blocks}
            for i in range(total_blocks):
                if i not in received_block_nums:
                    print(f"  - Block {i}")
            return False
        
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Reconstruct file
        output_file = output_path / file_name
        print(f"🔧 Reconstructing {file_name}...")
        
        with open(output_file, 'wb') as outfile:
            for block_info in blocks:
                block_file = self.blocks_dir / file_name.replace('.', '_') / f"block_{block_info['block_number']:04d}.bin"
                
                if not block_file.exists():
                    print(f"❌ Block file missing: {block_file}")
                    return False
                
                with open(block_file, 'rb') as infile:
                    data = infile.read()
                    outfile.write(data)
                
                print(f"  ✓ Added block {block_info['block_number']} (offsets {block_info['start_offset']:,} - {block_info['end_offset']:,})")
        
        print(f"✅ File reconstructed: {output_file}")
        print(f"   Size: {output_file.stat().st_size / (1024*1024):.2f} MB")
        return True
    
    def get_block_assignment_summary(self):
        """Get summary of block assignments across machines"""
        print("\n🤖 Block Assignment Summary")
        print("=" * 60)
        
        machine_assignments = defaultdict(lambda: {'files': set(), 'blocks': 0, 'bytes': 0})
        
        for file_name, blocks in self.block_info.items():
            for block in blocks:
                machine_id = block.get('machine_id', 'unknown')
                machine_assignments[machine_id]['files'].add(file_name)
                machine_assignments[machine_id]['blocks'] += 1
                machine_assignments[machine_id]['bytes'] += block['block_size_bytes']
        
        for machine_id, stats in machine_assignments.items():
            print(f"🖥️ Machine: {machine_id}")
            print(f"   Files: {len(stats['files'])}")
            print(f"   Blocks: {stats['blocks']}")
            print(f"   Data: {stats['bytes'] / (1024*1024):.2f} MB")
            print()
    
    def validate_block_integrity(self, file_name):
        """Validate that blocks are in correct order and have proper offsets"""
        if file_name not in self.block_info:
            print(f"❌ File {file_name} not found!")
            return False
        
        blocks = self.block_info[file_name]
        if not blocks:
            print(f"❌ No blocks found for {file_name}!")
            return False
        
        print(f"🔍 Validating {file_name}...")
        
        # Check block order
        for i, block in enumerate(blocks):
            if block['block_number'] != i:
                print(f"❌ Block order error: expected {i}, got {block['block_number']}")
                return False
        
        # Check offset continuity
        for i in range(len(blocks) - 1):
            current_block = blocks[i]
            next_block = blocks[i + 1]
            
            if current_block['end_offset'] + 1 != next_block['start_offset']:
                print(f"❌ Offset gap between blocks {i} and {i+1}")
                print(f"   Block {i} ends at: {current_block['end_offset']}")
                print(f"   Block {i+1} starts at: {next_block['start_offset']}")
                return False
        
        print(f"✅ {file_name} validation passed!")
        return True
    
    def export_block_report(self, output_file="block_report.json"):
        """Export detailed block report"""
        report = {
            'summary': {
                'total_files': len(self.block_info),
                'total_blocks': sum(len(blocks) for blocks in self.block_info.values()),
                'total_bytes': sum(
                    sum(block['block_size_bytes'] for block in blocks)
                    for blocks in self.block_info.values()
                )
            },
            'files': {}
        }
        
        for file_name, blocks in self.block_info.items():
            progress = self.get_file_progress(file_name)
            report['files'][file_name] = {
                'progress': progress,
                'blocks': blocks
            }
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"📊 Block report exported to: {output_file}")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Process and manage 128MB data blocks')
    parser.add_argument('--blocks-dir', default='received_blocks_machine1', help='Directory containing blocks')
    parser.add_argument('--action', choices=['list', 'reconstruct', 'validate', 'report'], default='list', help='Action to perform')
    parser.add_argument('--file-name', help='Specific file name for reconstruct/validate actions')
    parser.add_argument('--output-dir', default='reconstructed_files', help='Output directory for reconstructed files')
    
    args = parser.parse_args()
    
    processor = BlockProcessor(args.blocks_dir)
    
    if args.action == 'list':
        processor.list_all_files()
        processor.get_block_assignment_summary()
    
    elif args.action == 'reconstruct':
        if args.file_name:
            processor.reconstruct_file(args.file_name, args.output_dir)
        else:
            print("Please specify --file-name for reconstruction")
    
    elif args.action == 'validate':
        if args.file_name:
            processor.validate_block_integrity(args.file_name)
        else:
            print("Please specify --file-name for validation")
    
    elif args.action == 'report':
        processor.export_block_report()

if __name__ == "__main__":
    main() 