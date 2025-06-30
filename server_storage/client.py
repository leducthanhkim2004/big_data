import socket
import json
import time
import random
import threading
import os
import hashlib
from datetime import datetime
from typing import List, Dict, Any

class DistributedStorageClient:
    def __init__(self, server_host='localhost', server_port=8888, node_id=None):
        self.server_host = server_host
        self.server_port = server_port
        self.node_id = node_id or f"node_{random.randint(1000, 9999)}"
        self.running = False
        self.socket = None
        
        # Storage simulation
        self.storage_path = f"./storage_{self.node_id}"
        self.data_blocks = {}  # Dictionary of data blocks this client stores {block_id: block_info}
        self.max_storage_gb = random.uniform(50, 200)  # Random storage capacity
        self.block_operations_count = 0
        
        # Create storage directory
        os.makedirs(self.storage_path, exist_ok=True)
        
        print(f"🔧 Initialized client {self.node_id} with {self.max_storage_gb:.1f}GB capacity")
    
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
        """Get current system status"""
        try:
            # Simulate CPU and memory usage (replace with real psutil if needed)
            cpu_usage = random.uniform(10, 80)  # Simulate CPU usage between 10-80%
            memory_usage = random.uniform(30, 90)  # Simulate memory usage between 30-90%
            
            # Calculate storage usage
            storage_used = self.calculate_storage_used()
            
            # Simulate some data blocks
            if random.random() < 0.1:  # 10% chance to add/remove blocks
                self.simulate_data_operations()
            
            status_data = {
                'node_id': self.node_id,
                'status': self.determine_status(),
                'timestamp': datetime.now().isoformat(),
                'storage_used': storage_used,
                'storage_total': self.max_storage_gb,
                'cpu_usage': cpu_usage,
                'memory_usage': memory_usage,
                'active_connections': random.randint(0, 10),
                'data_blocks': self.data_blocks,
                'uptime': time.time()
            }
            
            return status_data
            
        except Exception as e:
            print(f"❌ Error getting system status: {e}")
            return {
                'node_id': self.node_id,
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def calculate_storage_used(self) -> float:
        """Calculate storage used by this client"""
        try:
            total_size = 0
            for root, dirs, files in os.walk(self.storage_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.path.exists(file_path):
                        total_size += os.path.getsize(file_path)
            
            # Convert bytes to GB
            return total_size / (1024 ** 3)
        except:
            return len(self.data_blocks) * 0.1  # Simulate 100MB per block
    
    def simulate_data_operations(self):
        """Simulate adding/removing data blocks"""
        if random.random() < 0.7 and len(self.data_blocks) < 100:
            # Add a new block
            block_id = f"block_{random.randint(10000, 99999)}"
            self.data_blocks.append({
                'id': block_id,
                'size': random.uniform(0.05, 0.5),  # 50MB to 500MB
                'created': datetime.now().isoformat(),
                'replicas': random.randint(1, 3)
            })
        elif len(self.data_blocks) > 5 and random.random() < 0.3:
            # Remove a block
            self.data_blocks.pop(random.randint(0, len(self.data_blocks) - 1))
    
    def determine_status(self) -> str:
        """Determine current client status"""
        storage_usage_percent = (self.calculate_storage_used() / self.max_storage_gb) * 100
        
        if storage_usage_percent > 90:
            return 'critical'
        elif storage_usage_percent > 75:
            return 'warning'
        elif len(self.data_blocks) == 0:
            return 'idle'
        else:
            return 'healthy'
    
    def announce_status(self):
        """Send status announcement to server"""
        if not self.socket:
            return False
            
        try:
            status_data = self.get_system_status()
            message = json.dumps(status_data)
            
            self.socket.send(message.encode('utf-8'))
            
            # Wait for server acknowledgment
            response = self.socket.recv(1024).decode('utf-8')
            ack_data = json.loads(response)
            
            print(f"📡 Status announced - {status_data['status']} | "
                  f"Storage: {status_data['storage_used']:.1f}GB/{status_data['storage_total']:.1f}GB | "
                  f"Blocks: {len(self.data_blocks)}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to announce status: {e}")
            return False
    
    def start_announcements(self, interval=15):
        """Start periodic status announcements"""
        if not self.connect_to_server():
            return
            
        self.running = True
        print(f"🔄 Starting status announcements every {interval} seconds")
        
        try:
            while self.running:
                if not self.announce_status():
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
    
    def simulate_workload(self):
        """Simulate some storage workload in background"""
        def workload():
            while self.running:
                try:
                    # Simulate file operations
                    if random.random() < 0.2:  # 20% chance
                        # Create a small test file
                        filename = f"{self.storage_path}/test_{random.randint(1000, 9999)}.dat"
                        with open(filename, 'w') as f:
                            f.write("x" * random.randint(1000, 10000))
                    
                    time.sleep(random.uniform(5, 15))
                except:
                    pass
        
        workload_thread = threading.Thread(target=workload, daemon=True)
        workload_thread.start()

def main():
    import sys
    
    # Allow custom node ID from command line
    node_id = sys.argv[1] if len(sys.argv) > 1 else None
    
    client = DistributedStorageClient(node_id=node_id)
    
    # Start background workload simulation
    client.simulate_workload()
    
    # Start status announcements
    client.start_announcements(interval=10)  # Announce every 10 seconds

if __name__ == "__main__":
    main()