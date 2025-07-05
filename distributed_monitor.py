#!/usr/bin/env python3
"""
Distributed Monitor - Tracks progress of distributed data processing
Monitors processing status across multiple machines and provides real-time insights
"""

import json
import time
import threading
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class MachineStatus:
    """Status of a processing machine"""
    machine_id: str
    group_id: str
    blocks_processed: int
    total_bytes: int
    failed_blocks: int
    last_heartbeat: float
    status: str  # 'active', 'inactive', 'failed'
    assigned_partitions: List[str]

@dataclass
class FileProgress:
    """Progress tracking for a file"""
    file_name: str
    total_blocks: int
    processed_blocks: int
    failed_blocks: int
    machines_processing: List[str]
    start_time: float
    estimated_completion: Optional[float]
    status: str  # 'processing', 'completed', 'failed'

class DistributedMonitor:
    def __init__(self, bootstrap_servers: List[str]):
        self.bootstrap_servers = bootstrap_servers
        
        # Initialize Kafka consumers and producers
        self.status_consumer = KafkaConsumer(
            'processing-status',
            bootstrap_servers=bootstrap_servers,
            group_id='monitor-group',
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=True
        )
        
        self.ack_consumer = KafkaConsumer(
            'block-acknowledgments',
            bootstrap_servers=bootstrap_servers,
            group_id='monitor-ack-group',
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=True
        )
        
        # Status tracking
        self.machines: Dict[str, MachineStatus] = {}
        self.files: Dict[str, FileProgress] = {}
        self.acknowledgments: Dict[str, Dict[int, List[str]]] = {}  # file -> block -> machines
        
        # Statistics
        self.stats = {
            'total_blocks_processed': 0,
            'total_bytes_processed': 0,
            'active_machines': 0,
            'files_in_progress': 0,
            'files_completed': 0
        }
        
        # Monitoring thread
        self.monitoring = False
        self.monitor_thread = None
    
    def start_monitoring(self):
        """Start monitoring in a separate thread"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        logger.info("Distributed monitor started")
    
    def stop_monitoring(self):
        """Stop monitoring"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
        logger.info("Distributed monitor stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        try:
            while self.monitoring:
                # Process status messages
                status_messages = self.status_consumer.poll(timeout_ms=1000)
                for tp, messages in status_messages.items():
                    for message in messages:
                        self._process_status_message(message.value)
                
                # Process acknowledgment messages
                ack_messages = self.ack_consumer.poll(timeout_ms=1000)
                for tp, messages in ack_messages.items():
                    for message in messages:
                        self._process_ack_message(message.value)
                
                # Update statistics
                self._update_statistics()
                
                # Print status every 30 seconds
                if int(time.time()) % 30 == 0:
                    self._print_status()
                
        except Exception as e:
            logger.error(f"Error in monitoring loop: {e}")
    
    def _process_status_message(self, message_data: Dict):
        """Process a status message from a machine"""
        machine_id = message_data.get('machine_id')
        if not machine_id:
            return
        
        # Update machine status
        self.machines[machine_id] = MachineStatus(
            machine_id=machine_id,
            group_id=message_data.get('group_id', 'unknown'),
            blocks_processed=message_data.get('blocks_processed', 0),
            total_bytes=message_data.get('total_bytes', 0),
            failed_blocks=message_data.get('failed_blocks', 0),
            last_heartbeat=time.time(),
            status='active',
            assigned_partitions=message_data.get('assigned_partitions', [])
        )
    
    def _process_ack_message(self, message_data: Dict):
        """Process an acknowledgment message"""
        file_name = message_data.get('file_name')
        block_number = message_data.get('block_number')
        machine_id = message_data.get('machine_id')
        success = message_data.get('success', False)
        
        # Type checking
        if not isinstance(file_name, str) or not isinstance(block_number, int) or not isinstance(machine_id, str):
            return
        
        # Track acknowledgment
        if file_name not in self.acknowledgments:
            self.acknowledgments[file_name] = {}
        if block_number not in self.acknowledgments[file_name]:
            self.acknowledgments[file_name][block_number] = []
        
        if success:
            self.acknowledgments[file_name][block_number].append(machine_id)
        
        # Update file progress
        if file_name not in self.files:
            self.files[file_name] = FileProgress(
                file_name=file_name,
                total_blocks=0,
                processed_blocks=0,
                failed_blocks=0,
                machines_processing=[],
                start_time=time.time(),
                estimated_completion=None,
                status='processing'
            )
        
        if success:
            self.files[file_name].processed_blocks += 1
        else:
            self.files[file_name].failed_blocks += 1
    
    def _update_statistics(self):
        """Update monitoring statistics"""
        current_time = time.time()
        
        # Count active machines
        self.stats['active_machines'] = sum(
            1 for machine in self.machines.values()
            if current_time - machine.last_heartbeat < 60  # 60 second timeout
        )
        
        # Update machine status
        for machine in self.machines.values():
            if current_time - machine.last_heartbeat > 60:
                machine.status = 'inactive'
            elif current_time - machine.last_heartbeat > 300:  # 5 minutes
                machine.status = 'failed'
        
        # Count files in progress and completed
        self.stats['files_in_progress'] = sum(
            1 for file in self.files.values()
            if file.status == 'processing'
        )
        self.stats['files_completed'] = sum(
            1 for file in self.files.values()
            if file.status == 'completed'
        )
        
        # Calculate total processed blocks and bytes
        self.stats['total_blocks_processed'] = sum(
            machine.blocks_processed for machine in self.machines.values()
        )
        self.stats['total_bytes_processed'] = sum(
            machine.total_bytes for machine in self.machines.values()
        )
    
    def _print_status(self):
        """Print current monitoring status"""
        logger.info("\n" + "=" * 60)
        logger.info("DISTRIBUTED PROCESSING STATUS")
        logger.info("=" * 60)
        
        # Overall statistics
        logger.info(f"Active Machines: {self.stats['active_machines']}")
        logger.info(f"Total Blocks Processed: {self.stats['total_blocks_processed']:,}")
        logger.info(f"Total Bytes Processed: {self.stats['total_bytes_processed'] / (1024*1024):.2f} MB")
        logger.info(f"Files in Progress: {self.stats['files_in_progress']}")
        logger.info(f"Files Completed: {self.stats['files_completed']}")
        
        # Machine status
        if self.machines:
            logger.info(f"\nMachine Status ({len(self.machines)} machines):")
            for machine_id, machine in self.machines.items():
                status_icon = "🟢" if machine.status == 'active' else "🔴" if machine.status == 'failed' else "🟡"
                logger.info(f"  {status_icon} {machine_id}: {machine.blocks_processed} blocks, "
                           f"{machine.total_bytes / (1024*1024):.2f} MB, {machine.status}")
        
        # File progress
        if self.files:
            logger.info(f"\nFile Progress ({len(self.files)} files):")
            for file_name, file in self.files.items():
                if file.total_blocks > 0:
                    progress = (file.processed_blocks / file.total_blocks) * 100
                    logger.info(f"  📁 {file_name}: {file.processed_blocks}/{file.total_blocks} "
                               f"({progress:.1f}%) - {file.status}")
        
        logger.info("=" * 60)
    
    def get_detailed_status(self) -> Dict:
        """Get detailed status for API or web interface"""
        return {
            'timestamp': time.time(),
            'statistics': self.stats,
            'machines': {mid: asdict(machine) for mid, machine in self.machines.items()},
            'files': {fname: asdict(file) for fname, file in self.files.items()},
            'acknowledgments': self.acknowledgments
        }
    
    def close(self):
        """Close the monitor"""
        self.stop_monitoring()
        self.status_consumer.close()
        self.ack_consumer.close()
        logger.info("Distributed monitor closed")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Distributed Processing Monitor')
    parser.add_argument('--brokers', nargs='+', 
                       default=['172.16.128.141:29092', '172.16.128.141:29093', '172.16.128.141:29094'],
                       help='Kafka broker addresses')
    
    args = parser.parse_args()
    
    monitor = DistributedMonitor(args.brokers)
    
    try:
        monitor.start_monitoring()
        logger.info("Monitor started. Press Ctrl+C to stop.")
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("\nStopping monitor...")
    finally:
        monitor.close()

if __name__ == "__main__":
    main() 