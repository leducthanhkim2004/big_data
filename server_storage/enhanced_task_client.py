#!/usr/bin/env python3
"""
Enhanced Task Client for Distributed Data Analytics
Receives and processes actual data blocks from the scheduler
"""

import socket
import threading
import time
import json
import logging
import psutil
import hashlib
import base64
from datetime import datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

@dataclass
class DataTaskAssignment:
    """Represents a data processing task assignment"""
    task_id: int
    file_name: str
    start_offset: int
    end_offset: int
    block_size: int
    checksum: str
    data_block: bytes
    timestamp: float

class EnhancedTaskClient:
    """Enhanced client for receiving and processing data blocks"""
    
    def __init__(self, 
                 client_id: str,
                 scheduler_host: str = 'localhost',
                 scheduler_port: int = 9000,
                 output_dir: str = 'processed_data'):
        
        self.client_id = client_id
        self.scheduler_host = scheduler_host
        self.scheduler_port = scheduler_port
        self.output_dir = Path(output_dir)
        
        # Create output directory
        self.output_dir.mkdir(exist_ok=True)
        
        # TCP connection
        self.tcp_socket = None
        self.tcp_connected = False
        self.tcp_lock = threading.Lock()
        
        # Task management
        self.current_task: Optional[DataTaskAssignment] = None
        self.task_lock = threading.Lock()
        
        # Statistics
        self.stats = {
            'tasks_received': 0,
            'tasks_completed': 0,
            'tasks_failed': 0,
            'total_bytes_processed': 0,
            'start_time': time.time()
        }
        
        # Threading
        self.running = False
        self.tcp_thread = None
        self.heartbeat_thread = None
        self.processing_thread = None
        
    def start(self):
        """Start the enhanced task client"""
        self.running = True
        
        # Start TCP communication thread
        self.tcp_thread = threading.Thread(target=self._tcp_communication_loop, daemon=True)
        self.tcp_thread.start()
        
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
        
        # Start processing thread
        self.processing_thread = threading.Thread(target=self._processing_loop, daemon=True)
        self.processing_thread.start()
        
        logger.info(f"Enhanced task client {self.client_id} started")
        logger.info(f"Output directory: {self.output_dir}")
    
    def stop(self):
        """Stop the enhanced task client"""
        self.running = False
        
        if self.tcp_thread:
            self.tcp_thread.join()
        
        if self.heartbeat_thread:
            self.heartbeat_thread.join()
        
        if self.processing_thread:
            self.processing_thread.join()
        
        self._disconnect_tcp()
        logger.info(f"Enhanced task client {self.client_id} stopped")
    
    def _tcp_communication_loop(self):
        """TCP communication loop for receiving task assignments"""
        while self.running:
            try:
                if not self.tcp_connected:
                    self._connect_tcp()
                
                if self.tcp_connected:
                    try:
                        data = self.tcp_socket.recv(1024)
                        if not data:
                            self.tcp_connected = False
                            continue
                        
                        message = data.decode().strip()
                        self._handle_tcp_message(message)
                        
                    except socket.error:
                        self.tcp_connected = False
                        logger.warning("TCP connection lost, will reconnect...")
                        time.sleep(5)
                
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error in TCP communication loop: {e}")
                time.sleep(5)
    
    def _connect_tcp(self):
        """Connect to TCP scheduler server"""
        try:
            with self.tcp_lock:
                self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.tcp_socket.settimeout(10)
                self.tcp_socket.connect((self.scheduler_host, self.scheduler_port))
                self.tcp_connected = True
                
            logger.info(f"Connected to enhanced TCP scheduler at {self.scheduler_host}:{self.scheduler_port}")
            
            # Send initial ready message
            self._send_message("READY")
            
        except Exception as e:
            logger.warning(f"Failed to connect to enhanced TCP scheduler: {e}")
            self.tcp_connected = False
    
    def _disconnect_tcp(self):
        """Disconnect from TCP server"""
        with self.tcp_lock:
            if self.tcp_socket:
                try:
                    self.tcp_socket.close()
                except:
                    pass
                self.tcp_socket = None
            self.tcp_connected = False
    
    def _send_message(self, message: str):
        """Send message to scheduler"""
        if not self.tcp_connected:
            return
        
        try:
            with self.tcp_lock:
                self.tcp_socket.sendall((message + "\n").encode())
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            self.tcp_connected = False
    
    def _handle_tcp_message(self, message: str):
        """Handle message received via TCP"""
        try:
            # Try to parse as JSON first (for data blocks)
            try:
                data = json.loads(message)
                if 'task_id' in data and 'data' in data:
                    # This is a data task assignment
                    self._handle_data_task(data)
                    return
            except json.JSONDecodeError:
                pass
            
            # Handle simple text messages
            parts = message.split()
            if not parts:
                return
            
            command = parts[0]
            
            if command == "TASK":
                # Format: TASK <task_id> <file_name> <start_offset> <end_offset>
                # This is for simple task assignments without data
                logger.warning("Received simple TASK command - data blocks should be sent as JSON")
                
        except Exception as e:
            logger.error(f"Error handling TCP message: {e}")
    
    def _handle_data_task(self, task_data: Dict):
        """Handle a data task assignment with actual data"""
        try:
            # Extract task information
            task = DataTaskAssignment(
                task_id=task_data['task_id'],
                file_name=task_data['file_name'],
                start_offset=task_data['start_offset'],
                end_offset=task_data['end_offset'],
                block_size=task_data['block_size'],
                checksum=task_data['checksum'],
                data_block=bytes.fromhex(task_data['data']),  # Convert hex to bytes
                timestamp=time.time()
            )
            
            logger.info(f"Received data task assignment: {task.task_id} for {task.file_name} "
                       f"({len(task.data_block)} bytes)")
            
            # Verify checksum
            calculated_checksum = hashlib.sha256(task.data_block).hexdigest()
            if calculated_checksum != task.checksum:
                logger.error(f"Checksum verification failed for task {task.task_id}")
                self._send_message(f"FAIL {task.task_id} Checksum verification failed")
                return
            
            # Assign task
            with self.task_lock:
                self.current_task = task
                self.stats['tasks_received'] += 1
            
            logger.info(f"Assigned data task {task.task_id} for file {task.file_name}")
            
        except Exception as e:
            logger.error(f"Error handling data task: {e}")
            if 'task_id' in task_data:
                self._send_message(f"FAIL {task_data['task_id']} {str(e)}")
    
    def _processing_loop(self):
        """Main processing loop for handling tasks"""
        while self.running:
            try:
                with self.task_lock:
                    if self.current_task:
                        self._process_current_task()
                
                time.sleep(1)  # Check for tasks every second
                
            except Exception as e:
                logger.error(f"Error in processing loop: {e}")
                time.sleep(5)
    
    def _process_current_task(self):
        """Process the current assigned task"""
        if not self.current_task:
            return
        
        task = self.current_task
        logger.info(f"Processing data task {task.task_id} for file {task.file_name}")
        
        try:
            # Create file directory
            file_dir = self.output_dir / task.file_name.replace('.', '_')
            file_dir.mkdir(exist_ok=True)
            
            # Save the data block
            block_file = file_dir / f"block_{task.task_id:04d}.bin"
            with open(block_file, 'wb') as f:
                f.write(task.data_block)
            
            # Save metadata
            metadata = {
                'task_id': task.task_id,
                'file_name': task.file_name,
                'start_offset': task.start_offset,
                'end_offset': task.end_offset,
                'block_size': task.block_size,
                'checksum': task.checksum,
                'processed_at': time.time(),
                'client_id': self.client_id,
                'verified': True
            }
            
            metadata_file = file_dir / f"block_{task.task_id:04d}_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            # Update statistics
            self.stats['tasks_completed'] += 1
            self.stats['total_bytes_processed'] += len(task.data_block)
            
            # Report completion
            self._send_message(f"DONE {task.task_id}")
            
            logger.info(f"✓ Completed data task {task.task_id} from {task.file_name} "
                       f"({len(task.data_block)} bytes) - Checksum verified")
            
            # Clear current task
            with self.task_lock:
                self.current_task = None
            
        except Exception as e:
            logger.error(f"✗ Error processing data task {task.task_id}: {e}")
            self.stats['tasks_failed'] += 1
            self._send_message(f"FAIL {task.task_id} {str(e)}")
            
            # Clear current task
            with self.task_lock:
                self.current_task = None
    
    def _heartbeat_loop(self):
        """Send periodic heartbeats to scheduler"""
        while self.running:
            try:
                if self.tcp_connected:
                    # Get system memory info
                    memory = psutil.virtual_memory()
                    free_memory_mb = int(memory.available / (1024 * 1024))
                    
                    # Send heartbeat
                    self._send_message(f"HEARTBEAT {free_memory_mb}")
                
                time.sleep(10)  # Send heartbeat every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                time.sleep(30)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get client statistics"""
        runtime = time.time() - self.stats['start_time']
        
        return {
            'client_id': self.client_id,
            'tasks_received': self.stats['tasks_received'],
            'tasks_completed': self.stats['tasks_completed'],
            'tasks_failed': self.stats['tasks_failed'],
            'total_bytes_processed': self.stats['total_bytes_processed'],
            'runtime_seconds': runtime,
            'throughput_mbps': (self.stats['total_bytes_processed'] / runtime / (1024*1024)) if runtime > 0 else 0,
            'tcp_connected': self.tcp_connected,
            'current_task': self.current_task.task_id if self.current_task else None
        }
    
    def print_statistics(self):
        """Print client statistics"""
        stats = self.get_statistics()
        
        print("\n" + "=" * 50)
        print(f"Enhanced Task Client {self.client_id} Statistics")
        print("=" * 50)
        print(f"Tasks Received: {stats['tasks_received']}")
        print(f"Tasks Completed: {stats['tasks_completed']}")
        print(f"Tasks Failed: {stats['tasks_failed']}")
        print(f"Total Bytes Processed: {stats['total_bytes_processed'] / (1024*1024):.2f} MB")
        print(f"Runtime: {stats['runtime_seconds']:.2f} seconds")
        print(f"Throughput: {stats['throughput_mbps']:.2f} MB/s")
        print(f"Current Task: {stats['current_task']}")
        print(f"TCP Connected: {stats['tcp_connected']}")
        print(f"Output Directory: {self.output_dir}")
        print("=" * 50)

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Task Client')
    parser.add_argument('--client-id', required=True, help='Client ID')
    parser.add_argument('--scheduler-host', default='localhost', help='Scheduler host')
    parser.add_argument('--scheduler-port', type=int, default=9000, help='Scheduler port')
    parser.add_argument('--output-dir', default='processed_data', help='Output directory')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create client
    client = EnhancedTaskClient(
        client_id=args.client_id,
        scheduler_host=args.scheduler_host,
        scheduler_port=args.scheduler_port,
        output_dir=args.output_dir
    )
    
    try:
        # Start client
        client.start()
        
        print("=" * 50)
        print(f"Enhanced Task Client {args.client_id}")
        print("=" * 50)
        print(f"Connected to scheduler: {args.scheduler_host}:{args.scheduler_port}")
        print(f"Output directory: {args.output_dir}")
        print("Enhanced task client started")
        print("Press Ctrl+C to stop")
        
        # Main loop
        while True:
            time.sleep(30)
            client.print_statistics()
            
    except KeyboardInterrupt:
        print("\nShutting down enhanced task client...")
    finally:
        client.stop()

if __name__ == "__main__":
    main() 