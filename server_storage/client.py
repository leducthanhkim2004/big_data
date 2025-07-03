import socket
import json
import time
import threading
import os
import signal
import sys
from datetime import datetime
from typing import List, Dict, Any

try:
    import psutil
except ImportError:
    print("❌ psutil not installed. Install with: pip install psutil")
    sys.exit(1)

class DistributedStorageClient:
    def __init__(self, server_host='localhost', server_port=1234, node_id=None):
        self.server_host = server_host
        self.server_port = server_port
        self.node_id = node_id or f"node_{os.getpid()}"
        self.running = False
        self.socket = None
        self.start_time = time.time()
        
        # Storage configuration
        self.storage_path = f"./storage_{self.node_id}"
        os.makedirs(self.storage_path, exist_ok=True)
        
        print(f"🔧 Initialized client {self.node_id}")
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"\n🛑 Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def connect_to_server(self):
        """Establish connection to the server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.server_host, self.server_port))
            print(f"✅ Connected to server at {self.server_host}:{self.server_port}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to server: {e}")
            return False
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get real system status using psutil"""
        try:
            # Get real CPU usage
            cpu_usage = psutil.cpu_percent(interval=1)
            
            # Get real memory usage
            memory = psutil.virtual_memory()
            memory_usage = memory.percent
            
            # Get disk usage for storage path
            disk_usage = psutil.disk_usage(self.storage_path)
            storage_used_gb = (disk_usage.used) / (1024 ** 3)
            storage_total_gb = (disk_usage.total) / (1024 ** 3)
            storage_free_gb = (disk_usage.free) / (1024 ** 3)
            
            # Get network connections
            connections = len(psutil.net_connections())
            
            # Calculate uptime
            uptime_seconds = time.time() - self.start_time
            
            # Get process count
            process_count = len(psutil.pids())
            
            # Get load average (Unix-like systems)
            try:
                load_avg = os.getloadavg()
            except (OSError, AttributeError):
                load_avg = [0.0, 0.0, 0.0]  # Windows doesn't have load average
            
            status_data = {
                'type': 'status',
                'node_id': self.node_id,
                'client_name': f"StorageNode_{self.node_id}",
                'status': self.determine_status(cpu_usage, memory_usage, storage_used_gb, storage_total_gb),
                'timestamp': datetime.now().isoformat(),
                'cpu_usage': round(cpu_usage, 2),
                'memory_usage': round(memory_usage, 2),
                'disk_usage': round((storage_used_gb / storage_total_gb) * 100, 2),
                'storage_used_gb': round(storage_used_gb, 2),
                'storage_total_gb': round(storage_total_gb, 2),
                'storage_free_gb': round(storage_free_gb, 2),
                'active_connections': connections,
                'process_count': process_count,
                'uptime_seconds': round(uptime_seconds, 2),
                'load_average': load_avg,
                'data': {
                    'storage_path': self.storage_path,
                    'pid': os.getpid(),
                    'hostname': os.uname().nodename if hasattr(os, 'uname') else 'windows'
                }
            }
            
            return status_data
            
        except Exception as e:
            print(f"❌ Error getting system status: {e}")
            return {
                'type': 'status',
                'node_id': self.node_id,
                'client_name': f"StorageNode_{self.node_id}",
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def determine_status(self, cpu_usage: float, memory_usage: float, storage_used_gb: float, storage_total_gb: float) -> str:
        """Determine current client status based on real metrics"""
        storage_usage_percent = (storage_used_gb / storage_total_gb) * 100 if storage_total_gb > 0 else 0
        
        if cpu_usage > 90 or memory_usage > 95 or storage_usage_percent > 95:
            return 'critical'
        elif cpu_usage > 75 or memory_usage > 85 or storage_usage_percent > 85:
            return 'warning'
        elif cpu_usage < 10 and memory_usage < 30:
            return 'idle'
        else:
            return 'healthy'
    
    def send_status(self):
        """Send real status to server"""
        if not self.socket:
            return False
            
        try:
            status_data = self.get_system_status()
            message = json.dumps(status_data)
            
            self.socket.send(message.encode('utf-8'))
            
            # Wait for server acknowledgment
            response = self.socket.recv(1024).decode('utf-8')
            ack_data = json.loads(response)
            
            print(f"📡 Status sent - {status_data['status']} | "
                  f"CPU: {status_data['cpu_usage']}% | "
                  f"Memory: {status_data['memory_usage']}% | "
                  f"Disk: {status_data['disk_usage']}%")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to send status: {e}")
            return False
    
    def start_status_updates(self, interval=15):
        """Start periodic status updates"""
        if not self.connect_to_server():
            return
            
        self.running = True
        print(f"🔄 Starting status updates every {interval} seconds")
        
        try:
            while self.running:
                if not self.send_status():
                    print("🔄 Reconnecting to server...")
                    if not self.connect_to_server():
                        print("❌ Failed to reconnect. Waiting before retry...")
                        time.sleep(5)
                        continue
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print(f"\n🛑 Stopping client {self.node_id}...")
        except Exception as e:
            print(f"❌ Client error: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the client"""
        self.running = False
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
        print(f"✅ Client {self.node_id} stopped")

def main():
    import sys
    
    # Allow custom node ID from command line
    node_id = sys.argv[1] if len(sys.argv) > 1 else None
    
    client = DistributedStorageClient(node_id=node_id)
    
    # Start status updates
    client.start_status_updates(interval=10)  # Send status every 10 seconds

if __name__ == "__main__":
    main()