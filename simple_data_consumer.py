#!/usr/bin/env python3
"""
Simple Data Consumer - Receives 128MB blocks from Kafka
Each machine gets different blocks automatically via consumer groups
"""

import json
import time
import base64
from pathlib import Path
from kafka import KafkaConsumer
from kafka.errors import KafkaError

class SimpleDataConsumer:
    def __init__(self, topic, group_id, machine_id, bootstrap_servers=['192.168.1.160:29092', '192.168.1.160:29093', '192.168.1.160:29094']):
        self.topic = topic
        self.group_id = group_id
        self.machine_id = machine_id
        
        # Create output directory for this machine
        self.output_dir = Path(f"received_blocks_{machine_id}")
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize consumer
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            max_poll_records=1,  # Process one block at a time
            fetch_max_wait_ms=1000
        )
        
        # Statistics
        self.stats = {
            'blocks_received': 0,
            'files_received': set(),
            'total_bytes': 0,
            'start_time': time.time()
        }
    
    def get_partition_assignment(self):
        """Get current partition assignment for this consumer"""
        assignment = self.consumer.assignment()
        return [f"{p.topic}-{p.partition}" for p in assignment]
    
    def save_data_block(self, message_data):
        """Save a received data block to local storage with offset information"""
        try:
            file_name = message_data['file_name']
            block_number = message_data['block_number']
            block_data_b64 = message_data['data']
            start_offset = message_data.get('start_offset', 0)
            end_offset = message_data.get('end_offset', 0)
            file_size = message_data.get('file_size_bytes', 0)
            total_blocks = message_data.get('total_blocks', 0)
            
            # Decode base64 data
            block_data = base64.b64decode(block_data_b64)
            
            # Create file directory
            file_dir = self.output_dir / file_name.replace('.', '_')
            file_dir.mkdir(exist_ok=True)
            
            # Save block to file
            block_file = file_dir / f"block_{block_number:04d}.bin"
            with open(block_file, 'wb') as f:
                f.write(block_data)
            
            # Save offset metadata
            metadata_file = file_dir / f"block_{block_number:04d}_metadata.json"
            metadata = {
                'file_name': file_name,
                'block_number': block_number,
                'start_offset': start_offset,
                'end_offset': end_offset,
                'block_size_bytes': len(block_data),
                'file_size_bytes': file_size,
                'total_blocks': total_blocks,
                'progress_percent': (block_number + 1) / total_blocks * 100 if total_blocks > 0 else 0,
                'received_at': time.time(),
                'machine_id': self.machine_id
            }
            
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            # Update statistics
            self.stats['blocks_received'] += 1
            self.stats['files_received'].add(file_name)
            self.stats['total_bytes'] += len(block_data)
            
            print(f"✓ Saved block {block_number} from {file_name} ({len(block_data)} bytes)")
            print(f"  Offsets: {start_offset:,} - {end_offset:,} (File: {file_size:,} bytes)")
            print(f"  Progress: {block_number + 1}/{total_blocks} ({metadata['progress_percent']:.1f}%)")
            print(f"  Saved to: {block_file}")
            
            return True
            
        except Exception as e:
            print(f"✗ Error saving block: {e}")
            return False
    
    def process_message(self, message):
        """Process a received message"""
        try:
            message_data = message.value
            
            # Save the data block
            success = self.save_data_block(message_data)
            
            if success:
                # Print progress every 10 blocks
                if self.stats['blocks_received'] % 10 == 0:
                    runtime = time.time() - self.stats['start_time']
                    mb_received = self.stats['total_bytes'] / (1024*1024)
                    print(f"\n📊 Progress: {self.stats['blocks_received']} blocks, {mb_received:.2f} MB, {mb_received/runtime:.2f} MB/s")
            
            return success
            
        except Exception as e:
            print(f"✗ Error processing message: {e}")
            return False
    
    def start_consuming(self, max_blocks=None):
        """Start consuming data blocks from Kafka"""
        print(f"Simple Data Consumer - Machine {self.machine_id}")
        print("=" * 50)
        print(f"Topic: {self.topic}")
        print(f"Group ID: {self.group_id}")
        print(f"Output Directory: {self.output_dir}")
        
        # Show partition assignment
        partitions = self.get_partition_assignment()
        print(f"Assigned partitions: {partitions}")
        print()
        
        block_count = 0
        
        try:
            print("Waiting for data blocks...")
            print("Press Ctrl+C to stop")
            print("-" * 50)
            
            for message in self.consumer:
                success = self.process_message(message)
                block_count += 1
                
                if max_blocks and block_count >= max_blocks:
                    print(f"\nReached maximum block count: {max_blocks}")
                    break
                    
        except KeyboardInterrupt:
            print("\nStopping consumer...")
        except KafkaError as e:
            print(f"Kafka error: {e}")
        finally:
            self.print_final_stats()
            self.close()
    
    def print_final_stats(self):
        """Print final statistics"""
        runtime = time.time() - self.stats['start_time']
        
        print("\n" + "=" * 50)
        print(f"FINAL STATISTICS - Machine {self.machine_id}")
        print("=" * 50)
        print(f"Blocks received: {self.stats['blocks_received']}")
        print(f"Files received: {len(self.stats['files_received'])}")
        print(f"Total bytes: {self.stats['total_bytes'] / (1024*1024):.2f} MB")
        print(f"Runtime: {runtime:.2f} seconds")
        print(f"Throughput: {self.stats['total_bytes'] / runtime / (1024*1024):.2f} MB/s")
        print(f"Output directory: {self.output_dir}")
        
        if self.stats['files_received']:
            print(f"\nFiles received:")
            for file_name in sorted(self.stats['files_received']):
                print(f"  - {file_name}")
    
    def close(self):
        """Close the consumer"""
        self.consumer.close()
        print(f"Consumer {self.machine_id} closed")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Consume 128MB data blocks from Kafka')
    parser.add_argument('--topic', default='data-blocks', help='Kafka topic to consume from')
    parser.add_argument('--group-id', default='data-consumers', help='Consumer group ID')
    parser.add_argument('--machine-id', required=True, help='Unique machine ID (e.g., machine1, machine2)')
    parser.add_argument('--max-blocks', type=int, help='Maximum blocks to receive')
    parser.add_argument('--brokers', nargs='+', default=['192.168.1.160:29092'], help='Kafka broker addresses')
    
    args = parser.parse_args()
    
    # Create consumer
    consumer = SimpleDataConsumer(
        topic=args.topic,
        group_id=args.group_id,
        machine_id=args.machine_id,
        bootstrap_servers=args.brokers
    )
    
    try:
        consumer.start_consuming(max_blocks=args.max_blocks)
    except KeyboardInterrupt:
        print("Shutting down...")

if __name__ == "__main__":
    main() 