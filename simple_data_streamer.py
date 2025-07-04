#!/usr/bin/env python3
"""
Simple Data Streamer - Sends 128MB blocks to Kafka
Reads files from D:\feb_to_apr and sends data in 128MB chunks
"""

import os
import json
import time
from pathlib import Path
from kafka import KafkaProducer
from kafka.errors import KafkaError

class SimpleDataStreamer:
    def __init__(self, data_folder="D:\\feb_to_apr", bootstrap_servers=['localhost:29092', 'localhost:29093', 'localhost:29094']):
        self.data_folder = Path(data_folder)
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            max_request_size=209715200,
            compression_type='gzip',
            batch_size=16384,
            linger_ms=10
        )
        
        self.block_size = 128 * 1024 * 1024  # 128MB in bytes
        self.stats = {
            'files_processed': 0,
            'blocks_sent': 0,
            'total_bytes': 0
        }
    
    def get_data_files(self):
        """Get all files from the data folder"""
        if not self.data_folder.exists():
            print(f"ERROR: Folder {self.data_folder} does not exist!")
            return []
        
        files = []
        for file_path in self.data_folder.rglob('*'):
            if file_path.is_file():
                files.append(file_path)
        
        print(f"Found {len(files)} files in {self.data_folder}")
        return files
    
    def send_data_block(self, topic, file_path, block_data, block_number, start_offset, end_offset):
        """Send a 128MB data block to Kafka with offset information"""
        try:
            message = {
                'file_name': file_path.name,
                'file_path': str(file_path),
                'file_size_bytes': file_path.stat().st_size,
                'block_number': block_number,
                'block_size_bytes': len(block_data),
                'start_offset': start_offset,
                'end_offset': end_offset,
                'total_blocks': (file_path.stat().st_size + self.block_size - 1) // self.block_size,
                'timestamp': time.time(),
                'data': block_data
            }
            
            # Use file name as key for partitioning
            key = f"{file_path.stem}-block-{block_number}"
            
            future = self.producer.send(topic, value=message, key=key)
            record_metadata = future.get(timeout=10)
            
            self.stats['blocks_sent'] += 1
            self.stats['total_bytes'] += len(block_data)
            
            print(f"✓ Sent block {block_number} from {file_path.name} ({len(block_data)} bytes)")
            return record_metadata
            
        except KafkaError as e:
            print(f"✗ Failed to send block: {e}")
            return None
    
    def stream_file_in_blocks(self, file_path, topic):
        """Stream a file in 128MB blocks with offset information"""
        try:
            print(f"Processing file: {file_path.name}")
            file_size = file_path.stat().st_size
            print(f"File size: {file_size / (1024*1024):.2f} MB")
            
            block_number = 0
            current_offset = 0
            
            with open(file_path, 'rb') as f:
                while True:
                    # Read 128MB block
                    block_data = f.read(self.block_size)
                    
                    if not block_data:
                        break
                    
                    # Calculate offsets
                    start_offset = current_offset
                    end_offset = current_offset + len(block_data) - 1
                    
                    # Convert to base64 for JSON serialization
                    import base64
                    block_data_b64 = base64.b64encode(block_data).decode('utf-8')
                    
                    # Send block to Kafka with offset information
                    self.send_data_block(topic, file_path, block_data_b64, block_number, start_offset, end_offset)
                    
                    block_number += 1
                    current_offset += len(block_data)
                    
                    # Small delay between blocks
                    time.sleep(0.1)
            
            print(f"✓ Completed file: {file_path.name} ({block_number} blocks)")
            self.stats['files_processed'] += 1
            
        except Exception as e:
            print(f"✗ Error processing file {file_path}: {e}")
    
    def stream_all_data(self, topic='data-blocks'):
        """Stream all files in 128MB blocks"""
        files = self.get_data_files()
        
        if not files:
            print("No files found to process!")
            return
        
        print(f"Starting to stream {len(files)} files in 128MB blocks...")
        print(f"Topic: {topic}")
        print("-" * 50)
        
        start_time = time.time()
        
        for file_num, file_path in enumerate(files, 1):
            print(f"\nFile {file_num}/{len(files)}: {file_path.name}")
            self.stream_file_in_blocks(file_path, topic)
        
        # Flush remaining messages
        self.producer.flush()
        
        # Print final statistics
        runtime = time.time() - start_time
        print("\n" + "=" * 50)
        print("STREAMING COMPLETED!")
        print("=" * 50)
        print(f"Files processed: {self.stats['files_processed']}")
        print(f"Blocks sent: {self.stats['blocks_sent']}")
        print(f"Total bytes: {self.stats['total_bytes'] / (1024*1024):.2f} MB")
        print(f"Runtime: {runtime:.2f} seconds")
        print(f"Throughput: {self.stats['total_bytes'] / runtime / (1024*1024):.2f} MB/s")
    
    def close(self):
        """Close the producer"""
        self.producer.close()
        print("Data streamer closed")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Stream data in 128MB blocks to Kafka')
    parser.add_argument('--data-folder', default='D:\\feb_to_apr', help='Path to data folder')
    parser.add_argument('--topic', default='data-blocks', help='Kafka topic name')
    parser.add_argument('--brokers', nargs='+', default=['localhost:29092'], help='Kafka broker addresses')
    
    args = parser.parse_args()
    
    print("Simple Data Streamer - 128MB Blocks")
    print("=" * 40)
    print(f"Data folder: {args.data_folder}")
    print(f"Topic: {args.topic}")
    print(f"Brokers: {args.brokers}")
    print()
    
    # Create data streamer
    streamer = SimpleDataStreamer(
        data_folder=args.data_folder,
        bootstrap_servers=args.brokers
    )
    
    try:
        # Start streaming
        streamer.stream_all_data(topic=args.topic)
    except KeyboardInterrupt:
        print("\nStreaming interrupted by user")
    finally:
        streamer.close()

if __name__ == "__main__":
    main() 