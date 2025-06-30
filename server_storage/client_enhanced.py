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
    
    def send_message(self, message: Dict) -> Dict:
        """Send a message to the server and wait for response"""
        if not self.socket:
            if not self.connect_to_server():
                return {"status": "error", "message": "Cannot connect to server"}
        
        try:
            # Send message
            json_message = json.dumps(message)
            self.socket.send(json_message.encode('utf-8'))
            
            # Wait for response
            response_data = self.socket.recv(4096).decode('utf-8')
            response = json.loads(response_data)
            
            return response
            
        except Exception as e:
            print(f"❌ Failed to send message: {e}")
            self.socket = None  # Reset connection
            return {"status": "error", "message": str(e)}
    
    def create_block(self, data: str = None) -> str:
        """Create a new data block"""
        block_id = f"block_{self.node_id}_{int(time.time())}_{random.randint(1000, 9999)}"
        
        # Generate some dummy data if not provided
        if data is None:
            data = "x" * random.randint(1000, 100000)  # 1KB to 100KB of data
        
        # Calculate checksum
        checksum = hashlib.md5(data.encode()).hexdigest()
        
        # Save block to file
        block_file = os.path.join(self.storage_path, f"{block_id}.dat")
        with open(block_file, 'w') as f:
            f.write(data)
        
        # Store block info
        block_info = {
            'id': block_id,
            'size': len(data) / (1024 * 1024),  # Size in MB
            'checksum': checksum,
            'created': datetime.now().isoformat(),
            'file_path': block_file,
            'access_count': 0,
            'metadata': {
                'node_created': self.node_id,
                'data_type': 'simulated'
            }
        }
        
        self.data_blocks[block_id] = block_info
        self.block_operations_count += 1
        
        print(f"📦 Created block {block_id} ({block_info['size']:.2f}MB)")
        return block_id
    
    def delete_block(self, block_id: str) -> bool:
        """Delete a data block"""
        if block_id not in self.data_blocks:
            return False
        
        try:
            # Remove file
            block_info = self.data_blocks[block_id]
            if os.path.exists(block_info['file_path']):
                os.remove(block_info['file_path'])
            
            # Remove from memory
            del self.data_blocks[block_id]
            self.block_operations_count += 1
            
            # Notify server
            message = {
                'type': 'block_delete',
                'node_id': self.node_id,
                'block_id': block_id,
                'timestamp': datetime.now().isoformat()
            }
            
            response = self.send_message(message)
            print(f"🗑️ Deleted block {block_id}")
            
            return response.get('status') == 'success'
            
        except Exception as e:
            print(f"❌ Error deleting block {block_id}: {e}")
            return False
    
    def register_blocks_with_server(self):
        """Register all local blocks with the server"""
        if not self.data_blocks:
            return
        
        blocks_to_register = []
        for block_id, block_info in self.data_blocks.items():
            blocks_to_register.append({
                'id': block_id,
                'size': block_info['size'],
                'checksum': block_info['checksum'],
                'created': block_info['created'],
                'metadata': block_info['metadata']
            })
        
        message = {
            'type': 'block_register',
            'node_id': self.node_id,
            'blocks': blocks_to_register,
            'timestamp': datetime.now().isoformat()
        }
        
        response = self.send_message(message)
        if response.get('status') == 'success':
            print(f"📋 Registered {len(blocks_to_register)} blocks with server")
            
            # Handle replication tasks if any
            replication_tasks = response.get('replication_tasks', [])
            if replication_tasks:
                print(f"🔄 Received {len(replication_tasks)} replication tasks")
        
        return response
    
    def query_block_location(self, block_id: str) -> Dict:
        """Query server for block locations"""
        message = {
            'type': 'block_query',
            'node_id': self.node_id,
            'block_id': block_id,
            'timestamp': datetime.now().isoformat()
        }
        
        response = self.send_message(message)
        return response
    
    def sync_with_server(self) -> Dict:
        """Synchronize node state with server"""
        message = {
            'type': 'node_sync',
            'node_id': self.node_id,
            'local_blocks': list(self.data_blocks.keys()),
            'timestamp': datetime.now().isoformat()
        }
        
        response = self.send_message(message)
        return response
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get current system status"""
        try:
            # Simulate CPU and memory usage
            cpu_usage = random.uniform(10, 80)
            memory_usage = random.uniform(20, 70)
            
            # Calculate storage usage
            storage_used = self.calculate_storage_used()
            
            status_data = {
                'type': 'status',
                'node_id': self.node_id,
                'status': self.determine_status(),
                'timestamp': datetime.now().isoformat(),
                'storage_used': storage_used,
                'storage_total': self.max_storage_gb,
                'cpu_usage': cpu_usage,
                'memory_usage': memory_usage,
                'active_connections': random.randint(0, 10),
                'data_blocks': list(self.data_blocks.keys()),  # Just send block IDs
                'block_count': len(self.data_blocks),
                'operations_count': self.block_operations_count,
                'uptime': time.time()
            }
            
            return status_data
            
        except Exception as e:
            print(f"❌ Error getting system status: {e}")
            return {
                'type': 'status',
                'node_id': self.node_id,
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def calculate_storage_used(self) -> float:
        """Calculate storage used by this client"""
        try:
            total_size = 0
            for block_info in self.data_blocks.values():
                total_size += block_info['size']  # Size already in MB
            
            return total_size / 1024  # Convert MB to GB
        except:
            return len(self.data_blocks) * 0.1  # Simulate 100MB per block
    
    def simulate_data_operations(self):
        """Simulate adding/removing data blocks"""
        operation = random.choice(['create', 'delete', 'access'])
        
        if operation == 'create' and len(self.data_blocks) < 50:
            # Create a new block
            self.create_block()
            
        elif operation == 'delete' and len(self.data_blocks) > 2:
            # Delete a random block
            block_id = random.choice(list(self.data_blocks.keys()))
            self.delete_block(block_id)
            
        elif operation == 'access' and self.data_blocks:
            # Simulate accessing a block
            block_id = random.choice(list(self.data_blocks.keys()))
            self.data_blocks[block_id]['access_count'] += 1
            print(f"👁️ Accessed block {block_id}")
    
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
        try:
            status_data = self.get_system_status()
            response = self.send_message(status_data)
            
            if response.get('status') == 'received':
                storage_used = status_data['storage_used']
                storage_total = status_data['storage_total']
                block_count = status_data['block_count']
                
                print(f"📡 Status: {status_data['status']} | "
                      f"Storage: {storage_used:.1f}GB/{storage_total:.1f}GB | "
                      f"Blocks: {block_count} | Ops: {self.block_operations_count}")
            
            return response.get('status') == 'received'
            
        except Exception as e:
            print(f"❌ Failed to announce status: {e}")
            return False
    
    def start_operations(self, status_interval=10, block_interval=30):
        """Start client operations with periodic status updates and block operations"""
        if not self.connect_to_server():
            return
            
        self.running = True
        print(f"🔄 Starting operations - Status every {status_interval}s, Block ops every {block_interval}s")
        
        # Create some initial blocks
        for _ in range(random.randint(3, 8)):
            self.create_block()
        
        # Register initial blocks
        self.register_blocks_with_server()
        
        last_status_time = 0
        last_block_op_time = 0
        last_sync_time = 0
        
        try:
            while self.running:
                current_time = time.time()
                
                # Send status updates
                if current_time - last_status_time >= status_interval:
                    if self.announce_status():
                        last_status_time = current_time
                    else:
                        print("🔄 Reconnecting to server...")
                        if not self.connect_to_server():
                            time.sleep(5)
                            continue
                
                # Perform block operations
                if current_time - last_block_op_time >= block_interval:
                    if random.random() < 0.7:  # 70% chance of block operation
                        self.simulate_data_operations()
                        # Re-register blocks after operations
                        if random.random() < 0.3:  # 30% chance to re-register
                            self.register_blocks_with_server()
                    last_block_op_time = current_time
                
                # Periodic sync with server
                if current_time - last_sync_time >= 120:  # Every 2 minutes
                    sync_response = self.sync_with_server()
                    if sync_response.get('status') == 'success':
                        print(f"🔄 Synced with server - Total system blocks: {sync_response.get('total_blocks_in_system', 0)}")
                    last_sync_time = current_time
                
                time.sleep(1)  # Check every second
                
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
                    # Simulate various operations
                    operations = ['read', 'write', 'query']
                    operation = random.choice(operations)
                    
                    if operation == 'write' and random.random() < 0.2:
                        # Create new block occasionally
                        self.create_block()
                        
                    elif operation == 'query' and random.random() < 0.1:
                        # Query random block location
                        if self.data_blocks:
                            block_id = random.choice(list(self.data_blocks.keys()))
                            result = self.query_block_location(block_id)
                            if result.get('status') == 'success':
                                locations = result.get('locations', [])
                                print(f"🔍 Block {block_id} found on {len(locations)} nodes")
                    
                    time.sleep(random.uniform(10, 30))
                except Exception as e:
                    print(f"❌ Background workload error: {e}")
        
        workload_thread = threading.Thread(target=workload, daemon=True)
        workload_thread.start()

def main():
    import sys
    
    # Allow custom node ID from command line
    node_id = sys.argv[1] if len(sys.argv) > 1 else None
    
    client = DistributedStorageClient(node_id=node_id)
    
    # Start background workload simulation
    client.simulate_workload()
    
    # Start main operations
    client.start_operations(status_interval=8, block_interval=20)

if __name__ == "__main__":
    main()
