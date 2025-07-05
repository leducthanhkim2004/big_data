#!/usr/bin/env python3
"""
Distributed Data Processor - Robust big data processing across multiple machines
Features:
- Reliable data distribution with acknowledgments
- Data integrity with checksums
- Progress tracking and monitoring
- Fault tolerance and retry mechanisms
- Distributed processing across multiple machines
"""

import os
import json
import time
import hashlib
import base64
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class DataBlock:
    """Represents a data block with metadata"""
    file_name: str
    file_path: str
    block_number: int
    total_blocks: int
    start_offset: int
    end_offset: int
    data: bytes
    checksum: str
    timestamp: float
    file_size_bytes: int
    block_size_bytes: int

@dataclass
class ProcessingStatus:
    """Processing status for a file"""
    file_name: str
    total_blocks: int
    processed_blocks: int
    failed_blocks: int
    start_time: float
    last_update: float
    status: str  # 'processing', 'completed', 'failed'
    machine_id: str

class DistributedDataProcessor:
    def __init__(self, 
                 bootstrap_servers: List[str],
                 data_topic: str = 'data-blocks',
                 status_topic: str = 'processing-status',
                 ack_topic: str = 'block-acknowledgments'):
        
        self.bootstrap_servers = bootstrap_servers
        self.data_topic = data_topic
        self.status_topic = status_topic
        self.ack_topic = ack_topic
        
        # Initialize producers
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_request_size=209715200,
            compression_type='gzip'
        )
        
        # Processing statistics
        self.stats = {
            'files_processed': 0,
            'blocks_sent': 0,
            'blocks_acknowledged': 0,
            'total_bytes': 0,
            'failed_blocks': 0
        }
        
        # File processing status
        self.file_status: Dict[str, ProcessingStatus] = {}
        
    def calculate_checksum(self, data: bytes) -> str:
        """Calculate SHA-256 checksum for data integrity"""
        return hashlib.sha256(data).hexdigest()
    
    def create_data_block(self, file_path: Path, block_data: bytes, 
                         block_number: int, start_offset: int, end_offset: int) -> DataBlock:
        """Create a data block with metadata and checksum"""
        checksum = self.calculate_checksum(block_data)
        
        return DataBlock(
            file_name=file_path.name,
            file_path=str(file_path),
            block_number=block_number,
            total_blocks=(file_path.stat().st_size + 128*1024*1024 - 1) // (128*1024*1024),
            start_offset=start_offset,
            end_offset=end_offset,
            data=block_data,
            checksum=checksum,
            timestamp=time.time(),
            file_size_bytes=file_path.stat().st_size,
            block_size_bytes=len(block_data)
        )
    
    def send_data_block(self, block: DataBlock) -> bool:
        """Send a data block to Kafka with retry logic"""
        try:
            # Convert block to JSON-serializable format
            message = asdict(block)
            message['data'] = base64.b64encode(block.data).decode('utf-8')
            
            # Use file name as key for partitioning
            key = f"{block.file_name}-block-{block.block_number}"
            
            future = self.producer.send(self.data_topic, value=message, key=key)
            record_metadata = future.get(timeout=30)
            
            self.stats['blocks_sent'] += 1
            self.stats['total_bytes'] += len(block.data)
            
            logger.info(f"✓ Sent block {block.block_number} from {block.file_name} "
                       f"({len(block.data)} bytes) to partition {record_metadata.partition}")
            return True
            
        except KafkaError as e:
            logger.error(f"✗ Failed to send block {block.block_number} from {block.file_name}: {e}")
            self.stats['failed_blocks'] += 1
            return False
    
    def process_file_in_blocks(self, file_path: Path, block_size: int = 128*1024*1024) -> bool:
        """Process a file in blocks with progress tracking"""
        try:
            logger.info(f"Processing file: {file_path.name}")
            file_size = file_path.stat().st_size
            total_blocks = (file_size + block_size - 1) // block_size
            
            # Initialize file status
            self.file_status[file_path.name] = ProcessingStatus(
                file_name=file_path.name,
                total_blocks=total_blocks,
                processed_blocks=0,
                failed_blocks=0,
                start_time=time.time(),
                last_update=time.time(),
                status='processing',
                machine_id='producer'
            )
            
            block_number = 0
            current_offset = 0
            
            with open(file_path, 'rb') as f:
                while True:
                    # Read block
                    block_data = f.read(block_size)
                    
                    if not block_data:
                        break
                    
                    # Calculate offsets
                    start_offset = current_offset
                    end_offset = current_offset + len(block_data) - 1
                    
                    # Create data block
                    block = self.create_data_block(file_path, block_data, block_number, 
                                                 start_offset, end_offset)
                    
                    # Send block with retry logic
                    max_retries = 3
                    for attempt in range(max_retries):
                        if self.send_data_block(block):
                            self.file_status[file_path.name].processed_blocks += 1
                            break
                        elif attempt < max_retries - 1:
                            logger.warning(f"Retrying block {block_number} (attempt {attempt + 1})")
                            time.sleep(2 ** attempt)  # Exponential backoff
                        else:
                            self.file_status[file_path.name].failed_blocks += 1
                            logger.error(f"Failed to send block {block_number} after {max_retries} attempts")
                    
                    block_number += 1
                    current_offset += len(block_data)
                    
                    # Update status
                    self.file_status[file_path.name].last_update = time.time()
                    
                    # Small delay between blocks
                    time.sleep(0.1)
            
            # Mark file as completed
            self.file_status[file_path.name].status = 'completed'
            self.stats['files_processed'] += 1
            
            logger.info(f"✓ Completed file: {file_path.name} ({block_number} blocks)")
            return True
            
        except Exception as e:
            logger.error(f"✗ Error processing file {file_path}: {e}")
            if file_path.name in self.file_status:
                self.file_status[file_path.name].status = 'failed'
            return False
    
    def process_directory(self, data_folder: str) -> None:
        """Process all files in a directory"""
        data_path = Path(data_folder)
        
        if not data_path.exists():
            logger.error(f"ERROR: Folder {data_path} does not exist!")
            return
        
        files = list(data_path.rglob('*'))
        files = [f for f in files if f.is_file()]
        
        logger.info(f"Found {len(files)} files in {data_path}")
        
        start_time = time.time()
        
        for file_num, file_path in enumerate(files, 1):
            logger.info(f"\nFile {file_num}/{len(files)}: {file_path.name}")
            self.process_file_in_blocks(file_path)
        
        # Flush remaining messages
        self.producer.flush()
        
        # Print final statistics
        runtime = time.time() - start_time
        logger.info("\n" + "=" * 50)
        logger.info("PROCESSING COMPLETED!")
        logger.info("=" * 50)
        logger.info(f"Files processed: {self.stats['files_processed']}")
        logger.info(f"Blocks sent: {self.stats['blocks_sent']}")
        logger.info(f"Failed blocks: {self.stats['failed_blocks']}")
        logger.info(f"Total bytes: {self.stats['total_bytes'] / (1024*1024):.2f} MB")
        logger.info(f"Runtime: {runtime:.2f} seconds")
        logger.info(f"Throughput: {self.stats['total_bytes'] / runtime / (1024*1024):.2f} MB/s")
    
    def close(self):
        """Close the processor"""
        self.producer.close()
        logger.info("Distributed data processor closed")

class DistributedDataConsumer:
    def __init__(self, 
                 machine_id: str,
                 group_id: str,
                 bootstrap_servers: List[str],
                 data_topic: str = 'data-blocks',
                 status_topic: str = 'processing-status',
                 ack_topic: str = 'block-acknowledgments'):
        
        self.machine_id = machine_id
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.data_topic = data_topic
        self.status_topic = status_topic
        self.ack_topic = ack_topic
        
        # Create output directory
        self.output_dir = Path(f"processed_data_{machine_id}")
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize consumer
        self.consumer = KafkaConsumer(
            data_topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,  # Manual commit for reliability
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            max_poll_records=1,
            fetch_max_wait_ms=1000
        )
        
        # Initialize acknowledgment producer
        self.ack_producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all'
        )
        
        # Statistics
        self.stats = {
            'blocks_processed': 0,
            'files_processed': set(),
            'total_bytes': 0,
            'failed_blocks': 0,
            'start_time': time.time()
        }
        
        # File processing tracking
        self.file_blocks: Dict[str, Dict[int, bool]] = {}
    
    def verify_checksum(self, data: bytes, expected_checksum: str) -> bool:
        """Verify data integrity using checksum"""
        actual_checksum = hashlib.sha256(data).hexdigest()
        return actual_checksum == expected_checksum
    
    def process_data_block(self, message_data: Dict) -> bool:
        """Process a data block with integrity verification"""
        try:
            file_name = message_data['file_name']
            block_number = message_data['block_number']
            block_data_b64 = message_data['data']
            expected_checksum = message_data['checksum']
            
            # Decode data
            block_data = base64.b64decode(block_data_b64)
            
            # Verify checksum
            if not self.verify_checksum(block_data, expected_checksum):
                logger.error(f"✗ Checksum verification failed for block {block_number} from {file_name}")
                self.stats['failed_blocks'] += 1
                return False
            
            # Create file directory
            file_dir = self.output_dir / file_name.replace('.', '_')
            file_dir.mkdir(exist_ok=True)
            
            # Save block
            block_file = file_dir / f"block_{block_number:04d}.bin"
            with open(block_file, 'wb') as f:
                f.write(block_data)
            
            # Save metadata
            metadata_file = file_dir / f"block_{block_number:04d}_metadata.json"
            metadata = {
                'file_name': file_name,
                'block_number': block_number,
                'checksum': expected_checksum,
                'block_size_bytes': len(block_data),
                'processed_at': time.time(),
                'machine_id': self.machine_id,
                'verified': True
            }
            
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            # Track file progress
            if file_name not in self.file_blocks:
                self.file_blocks[file_name] = {}
            self.file_blocks[file_name][block_number] = True
            
            # Update statistics
            self.stats['blocks_processed'] += 1
            self.stats['files_processed'].add(file_name)
            self.stats['total_bytes'] += len(block_data)
            
            logger.info(f"✓ Processed block {block_number} from {file_name} "
                       f"({len(block_data)} bytes) - Checksum verified")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Error processing block: {e}")
            self.stats['failed_blocks'] += 1
            return False
    
    def send_acknowledgment(self, file_name: str, block_number: int, success: bool):
        """Send acknowledgment for processed block"""
        try:
            ack_message = {
                'file_name': file_name,
                'block_number': block_number,
                'machine_id': self.machine_id,
                'success': success,
                'timestamp': time.time()
            }
            
            key = f"{file_name}-block-{block_number}-{self.machine_id}"
            self.ack_producer.send(self.ack_topic, value=ack_message, key=key)
            
        except Exception as e:
            logger.error(f"Failed to send acknowledgment: {e}")
    
    def start_consuming(self, max_blocks: Optional[int] = None):
        """Start consuming and processing data blocks"""
        logger.info(f"Distributed Data Consumer - Machine {self.machine_id}")
        logger.info("=" * 50)
        logger.info(f"Topic: {self.data_topic}")
        logger.info(f"Group ID: {self.group_id}")
        logger.info(f"Output Directory: {self.output_dir}")
        
        block_count = 0
        
        try:
            logger.info("Waiting for data blocks...")
            logger.info("Press Ctrl+C to stop")
            logger.info("-" * 50)
            
            for message in self.consumer:
                try:
                    message_data = message.value
                    success = self.process_data_block(message_data)
                    
                    # Send acknowledgment
                    self.send_acknowledgment(
                        message_data['file_name'],
                        message_data['block_number'],
                        success
                    )
                    
                    # Manual commit for reliability
                    self.consumer.commit()
                    
                    block_count += 1
                    
                    # Print progress every 10 blocks
                    if block_count % 10 == 0:
                        runtime = time.time() - self.stats['start_time']
                        mb_processed = self.stats['total_bytes'] / (1024*1024)
                        logger.info(f"\n📊 Progress: {block_count} blocks, {mb_processed:.2f} MB, "
                                   f"{mb_processed/runtime:.2f} MB/s")
                    
                    if max_blocks and block_count >= max_blocks:
                        logger.info(f"\nReached maximum block count: {max_blocks}")
                        break
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("\nStopping consumer...")
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
        finally:
            self.print_final_stats()
            self.close()
    
    def print_final_stats(self):
        """Print final statistics"""
        runtime = time.time() - self.stats['start_time']
        
        logger.info("\n" + "=" * 50)
        logger.info(f"FINAL STATISTICS - Machine {self.machine_id}")
        logger.info("=" * 50)
        logger.info(f"Blocks processed: {self.stats['blocks_processed']}")
        logger.info(f"Failed blocks: {self.stats['failed_blocks']}")
        logger.info(f"Files processed: {len(self.stats['files_processed'])}")
        logger.info(f"Total bytes: {self.stats['total_bytes'] / (1024*1024):.2f} MB")
        logger.info(f"Runtime: {runtime:.2f} seconds")
        logger.info(f"Throughput: {self.stats['total_bytes'] / runtime / (1024*1024):.2f} MB/s")
        logger.info(f"Output directory: {self.output_dir}")
        
        if self.stats['files_processed']:
            logger.info(f"\nFiles processed:")
            for file_name in sorted(self.stats['files_processed']):
                logger.info(f"  - {file_name}")
    
    def close(self):
        """Close the consumer"""
        self.consumer.close()
        self.ack_producer.close()
        logger.info(f"Consumer {self.machine_id} closed")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Distributed Data Processing System')
    parser.add_argument('--mode', choices=['producer', 'consumer'], required=True,
                       help='Mode: producer or consumer')
    parser.add_argument('--data-folder', default='D:\\feb_to_apr',
                       help='Path to data folder (producer mode)')
    parser.add_argument('--machine-id', required=True,
                       help='Unique machine ID')
    parser.add_argument('--group-id', default='distributed-processors',
                       help='Consumer group ID (consumer mode)')
    parser.add_argument('--max-blocks', type=int,
                       help='Maximum blocks to process (consumer mode)')
    parser.add_argument('--brokers', nargs='+', 
                       default=['172.16.128.141:29092', '172.16.128.141:29093', '172.16.128.141:29094'],
                       help='Kafka broker addresses')
    
    args = parser.parse_args()
    
    if args.mode == 'producer':
        # Run as producer
        processor = DistributedDataProcessor(args.brokers)
        try:
            processor.process_directory(args.data_folder)
        except KeyboardInterrupt:
            logger.info("\nProcessing interrupted by user")
        finally:
            processor.close()
    
    elif args.mode == 'consumer':
        # Run as consumer
        consumer = DistributedDataConsumer(
            machine_id=args.machine_id,
            group_id=args.group_id,
            bootstrap_servers=args.brokers
        )
        try:
            consumer.start_consuming(max_blocks=args.max_blocks)
        except KeyboardInterrupt:
            logger.info("\nConsumer interrupted by user")

if __name__ == "__main__":
    main() 