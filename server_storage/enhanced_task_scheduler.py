#!/usr/bin/env python3
"""
Enhanced Task Scheduler for Distributed Data Analytics
Distributes actual data blocks to clients for processing
"""

import json
import time
import threading
import socket
import math
import os
from pathlib import Path
from collections import deque, OrderedDict
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Default block size for data processing
BLOCK_SIZE = 128 * 1024 * 1024  # 128MB

@dataclass
class DataTask:
    """Represents a data processing task with actual data"""
    task_id: int
    file_name: str
    file_path: str
    start_offset: int
    end_offset: int
    block_size: int
    checksum: str
    status: str = field(default="PENDING")  # PENDING, PROCESSING, DONE, FAIL
    assigned_to: Optional[str] = field(default=None)
    assigned_time: float = field(default_factory=time.time)
    retry_count: int = 0
    data_block: Optional[bytes] = None

@dataclass
class ClientInfo:
    """Information about a processing client"""
    client_id: str
    address: tuple
    free_memory: int = 0
    busy: bool = False
    last_heartbeat: float = field(default_factory=time.time)
    status: str = 'active'  # 'active', 'inactive', 'failed'
    tasks_completed: int = 0
    tasks_failed: int = 0
    total_bytes_processed: int = 0

class EnhancedTaskScheduler:
    """Enhanced task scheduler with data block distribution"""
    
    def __init__(self, 
                 scheduler_host: str = '0.0.0.0', 
                 scheduler_port: int = 9000,
                 block_size: int = BLOCK_SIZE):
        
        self.scheduler_host = scheduler_host
        self.scheduler_port = scheduler_port
        self.block_size = block_size
        
        # Task management
        self.tasks: Dict[int, DataTask] = {}
        self.task_queue = deque()
        self.scheduler_lock = threading.Lock()
        
        # Client tracking
        self.clients: OrderedDict[str, ClientInfo] = OrderedDict()
        self.client_lock = threading.Lock()
        
        # TCP server
        self.tcp_server = None
        self.running = False
        
        # Statistics
        self.stats = {
            'tasks_planned': 0,
            'tasks_assigned': 0,
            'tasks_completed': 0,
            'tasks_failed': 0,
            'clients_connected': 0,
            'total_bytes_distributed': 0
        }
        
        # Threading
        self.scheduling = False
        self.scheduler_thread = None
    
    def plan_tasks_from_directory(self, data_directory: str) -> int:
        """
        Plan tasks for all files in a directory
        Returns the number of tasks planned
        """
        data_path = Path(data_directory)
        if not data_path.exists():
            logger.error(f"Data directory {data_directory} does not exist!")
            return 0
        
        total_tasks = 0
        files = list(data_path.rglob('*'))
        files = [f for f in files if f.is_file()]
        
        logger.info(f"Found {len(files)} files in {data_directory}")
        
        for file_path in files:
            tasks_planned = self.plan_tasks_for_file(str(file_path))
            total_tasks += tasks_planned
        
        logger.info(f"Planned {total_tasks} total tasks for {len(files)} files")
        return total_tasks
    
    def plan_tasks_for_file(self, file_path: str) -> int:
        """
        Plan tasks for a single file
        Returns the number of tasks planned
        """
        try:
            file_path_obj = Path(file_path)
            if not file_path_obj.exists():
                logger.error(f"File {file_path} does not exist!")
                return 0
            
            file_size = file_path_obj.stat().st_size
            num_blocks = math.ceil(file_size / self.block_size)
            
            with self.scheduler_lock:
                for i in range(num_blocks):
                    start = i * self.block_size
                    end = min(start + self.block_size - 1, file_size - 1)
                    
                    # Read the actual data block
                    data_block = self._read_file_block(file_path, start, end)
                    checksum = self._calculate_checksum(data_block)
                    
                    task = DataTask(
                        task_id=len(self.tasks),
                        file_name=file_path_obj.name,
                        file_path=str(file_path),
                        start_offset=start,
                        end_offset=end,
                        block_size=len(data_block),
                        checksum=checksum,
                        data_block=data_block
                    )
                    
                    self.tasks[task.task_id] = task
                    self.task_queue.append(task.task_id)
                
                self.stats['tasks_planned'] += num_blocks
                logger.info(f"Planned {num_blocks} tasks for file {file_path_obj.name} (size: {file_size} bytes)")
            
            return num_blocks
            
        except Exception as e:
            logger.error(f"Error planning tasks for file {file_path}: {e}")
            return 0
    
    def _read_file_block(self, file_path: str, start_offset: int, end_offset: int) -> bytes:
        """Read a block of data from a file"""
        try:
            with open(file_path, 'rb') as f:
                f.seek(start_offset)
                data = f.read(end_offset - start_offset + 1)
                return data
        except Exception as e:
            logger.error(f"Error reading file block: {e}")
            return b''
    
    def _calculate_checksum(self, data: bytes) -> str:
        """Calculate SHA-256 checksum for data integrity"""
        import hashlib
        return hashlib.sha256(data).hexdigest()
    
    def start_scheduling(self):
        """Start task scheduling in a separate thread"""
        self.scheduling = True
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()
        logger.info("Enhanced task scheduler started")
    
    def stop_scheduling(self):
        """Stop task scheduling"""
        self.scheduling = False
        if self.scheduler_thread:
            self.scheduler_thread.join()
        logger.info("Enhanced task scheduler stopped")
    
    def start_tcp_server(self):
        """Start TCP server for client communication"""
        try:
            self.tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.tcp_server.bind((self.scheduler_host, self.scheduler_port))
            self.tcp_server.listen(10)
            self.running = True
            
            logger.info(f"Enhanced TCP server listening on {self.scheduler_host}:{self.scheduler_port}")
            
            # Start accepting clients
            threading.Thread(target=self._accept_clients, daemon=True).start()
            
            # Start cleanup thread
            threading.Thread(target=self._cleanup_inactive_clients, daemon=True).start()
            
        except Exception as e:
            logger.error(f"Failed to start TCP server: {e}")
            raise
    
    def close(self):
        """Close the scheduler and all connections"""
        self.stop_scheduling()
        self.running = False
        if self.tcp_server:
            self.tcp_server.close()
        logger.info("Enhanced task scheduler closed")
    
    def _scheduler_loop(self):
        """Main task scheduling loop"""
        try:
            while self.scheduling:
                self.assign_next_task()
                time.sleep(1)  # Check for new assignments every second
        except Exception as e:
            logger.error(f"Error in scheduler loop: {e}")
    
    def _accept_clients(self):
        """Accept client connections"""
        try:
            while self.running:
                conn, addr = self.tcp_server.accept()
                client_id = f"{addr[0]}:{addr[1]}"
                
                with self.client_lock:
                    self.clients[client_id] = ClientInfo(
                        client_id=client_id,
                        address=addr,
                        last_heartbeat=time.time()
                    )
                    self.stats['clients_connected'] += 1
                
                logger.info(f"Client connected: {client_id}")
                threading.Thread(target=self._handle_client, args=(client_id, conn), daemon=True).start()
                
        except Exception as e:
            if self.running:
                logger.error(f"Error accepting clients: {e}")
    
    def _handle_client(self, client_id: str, conn: socket.socket):
        """Handle client communication"""
        try:
            while self.running:
                try:
                    data = conn.recv(1024)
                    if not data:
                        break
                    
                    message = data.decode().strip()
                    self._process_client_message(client_id, message)
                    
                except socket.error:
                    break
                    
        except Exception as e:
            logger.error(f"Error handling client {client_id}: {e}")
        finally:
            self._handle_client_disconnect(client_id)
            conn.close()
    
    def _process_client_message(self, client_id: str, message: str):
        """Process messages from clients"""
        try:
            parts = message.split()
            if not parts:
                return
            
            command = parts[0]
            
            with self.client_lock:
                if client_id not in self.clients:
                    return
                client = self.clients[client_id]
                client.last_heartbeat = time.time()
            
            if command == "HEARTBEAT":
                # Format: HEARTBEAT <free_memory>
                if len(parts) >= 2:
                    client.free_memory = int(parts[1])
                    client.busy = False
                    logger.debug(f"Client {client_id} heartbeat: {client.free_memory}MB free")
            
            elif command == "READY":
                client.busy = False
                logger.info(f"Client {client_id} ready for tasks")
            
            elif command == "DONE":
                # Format: DONE <task_id>
                if len(parts) >= 2:
                    task_id = int(parts[1])
                    self._mark_task_done(task_id, client_id)
            
            elif command == "FAIL":
                # Format: FAIL <task_id> <error_message>
                if len(parts) >= 2:
                    task_id = int(parts[1])
                    error_msg = " ".join(parts[2:]) if len(parts) > 2 else "Unknown error"
                    self._handle_task_failure(task_id, client_id, error_msg)
            
        except Exception as e:
            logger.error(f"Error processing message from {client_id}: {e}")
    
    def _handle_client_disconnect(self, client_id: str):
        """Handle client disconnection"""
        with self.client_lock:
            if client_id in self.clients:
                del self.clients[client_id]
                logger.info(f"Client disconnected: {client_id}")
    
    def _cleanup_inactive_clients(self):
        """Remove inactive clients"""
        while self.running:
            try:
                current_time = time.time()
                inactive_threshold = 60  # 60 seconds
                
                with self.client_lock:
                    inactive_clients = []
                    for client_id, client in self.clients.items():
                        if current_time - client.last_heartbeat > inactive_threshold:
                            inactive_clients.append(client_id)
                    
                    for client_id in inactive_clients:
                        logger.info(f"Removing inactive client: {client_id}")
                        del self.clients[client_id]
                
                time.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in cleanup: {e}")
    
    def assign_next_task(self):
        """Assign next available task to an available client"""
        if not self.task_queue:
            return
        
        with self.client_lock:
            available_clients = [
                client_id for client_id, client in self.clients.items()
                if not client.busy and client.status == 'active'
            ]
        
        if not available_clients:
            return
        
        # Get next task
        with self.scheduler_lock:
            if not self.task_queue:
                return
            
            task_id = self.task_queue.popleft()
            task = self.tasks[task_id]
            
            if task.status != "PENDING":
                return
        
        # Assign to first available client
        client_id = available_clients[0]
        self._send_task_to_client(client_id, task)
    
    def _send_task_to_client(self, client_id: str, task: DataTask):
        """Send task with data to client"""
        try:
            with self.client_lock:
                if client_id not in self.clients:
                    return
                client = self.clients[client_id]
                client.busy = True
            
            with self.scheduler_lock:
                task.assigned_to = client_id
                task.status = "PROCESSING"
                task.assigned_time = time.time()
            
            # Send task data to client
            task_data = {
                'task_id': task.task_id,
                'file_name': task.file_name,
                'start_offset': task.start_offset,
                'end_offset': task.end_offset,
                'block_size': task.block_size,
                'checksum': task.checksum,
                'data': task.data_block.hex()  # Send as hex string
            }
            
            # Find client connection and send data
            # This would need to be implemented based on your connection management
            
            self.stats['tasks_assigned'] += 1
            self.stats['total_bytes_distributed'] += len(task.data_block)
            
            logger.info(f"Assigned task {task.task_id} to client {client_id} "
                       f"({len(task.data_block)} bytes)")
            
        except Exception as e:
            logger.error(f"Error sending task to client {client_id}: {e}")
            # Return task to queue
            with self.scheduler_lock:
                task.status = "PENDING"
                task.assigned_to = None
                self.task_queue.appendleft(task.task_id)
    
    def _mark_task_done(self, task_id: int, client_id: str):
        """Mark a task as completed"""
        with self.scheduler_lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                task.status = "DONE"
                self.stats['tasks_completed'] += 1
        
        with self.client_lock:
            if client_id in self.clients:
                client = self.clients[client_id]
                client.tasks_completed += 1
                client.total_bytes_processed += task.block_size
        
        logger.info(f"Task {task_id} completed by client {client_id}")
    
    def _handle_task_failure(self, task_id: int, client_id: str, error_msg: str):
        """Handle task failure"""
        with self.scheduler_lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                task.retry_count += 1
                
                if task.retry_count < 3:
                    # Retry the task
                    task.status = "PENDING"
                    task.assigned_to = None
                    self.task_queue.append(task_id)
                    logger.warning(f"Retrying task {task_id} (attempt {task.retry_count})")
                else:
                    # Mark as permanently failed
                    task.status = "FAIL"
                    self.stats['tasks_failed'] += 1
                    logger.error(f"Task {task_id} permanently failed after 3 attempts")
        
        with self.client_lock:
            if client_id in self.clients:
                client = self.clients[client_id]
                client.tasks_failed += 1
        
        logger.error(f"Task {task_id} failed on client {client_id}: {error_msg}")
    
    def get_statistics(self) -> Dict:
        """Get scheduler statistics"""
        return {
            'tasks_planned': self.stats['tasks_planned'],
            'tasks_assigned': self.stats['tasks_assigned'],
            'tasks_completed': self.stats['tasks_completed'],
            'tasks_failed': self.stats['tasks_failed'],
            'clients_connected': len(self.clients),
            'total_bytes_distributed': self.stats['total_bytes_distributed'],
            'queue_length': len(self.task_queue)
        }
    
    def print_status(self):
        """Print current status"""
        stats = self.get_statistics()
        
        print("\n" + "=" * 60)
        print(f"Enhanced Task Scheduler Status - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        print(f"\nConnected Clients: {stats['clients_connected']}")
        for client_id, client in self.clients.items():
            status_icon = "🟢" if client.status == 'active' else "🔴"
            busy_icon = "⏳" if client.busy else "✅"
            print(f"  {status_icon} {client_id}: {busy_icon} | "
                  f"Free: {client.free_memory}MB | "
                  f"Completed: {client.tasks_completed} | "
                  f"Failed: {client.tasks_failed}")
        
        print(f"\nTask Statistics:")
        print(f"  Planned: {stats['tasks_planned']}")
        print(f"  Assigned: {stats['tasks_assigned']}")
        print(f"  Completed: {stats['tasks_completed']}")
        print(f"  Failed: {stats['tasks_failed']}")
        print(f"  Queue: {stats['queue_length']}")
        print(f"  Total Bytes: {stats['total_bytes_distributed'] / (1024*1024):.2f} MB")
        print("=" * 60)

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Task Scheduler')
    parser.add_argument('--host', default='0.0.0.0', help='Scheduler host')
    parser.add_argument('--port', type=int, default=9000, help='Scheduler port')
    parser.add_argument('--data-dir', help='Data directory to process')
    parser.add_argument('--block-size', type=int, default=BLOCK_SIZE, help='Block size in bytes')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create scheduler
    scheduler = EnhancedTaskScheduler(
        scheduler_host=args.host,
        scheduler_port=args.port,
        block_size=args.block_size
    )
    
    try:
        # Plan tasks if data directory provided
        if args.data_dir:
            scheduler.plan_tasks_from_directory(args.data_dir)
        
        # Start scheduler
        scheduler.start_scheduling()
        scheduler.start_tcp_server()
        
        print("=" * 60)
        print("Enhanced Task Scheduler for Distributed Data Analytics")
        print("=" * 60)
        print(f"TCP server listening on {args.host}:{args.port}")
        print("Enhanced task scheduler started")
        
        # Main loop
        while True:
            time.sleep(30)
            scheduler.print_status()
            
    except KeyboardInterrupt:
        print("\nShutting down enhanced task scheduler...")
    finally:
        scheduler.close()

if __name__ == "__main__":
    main() 