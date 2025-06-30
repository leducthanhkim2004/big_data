"""
Script to simulate various node errors in the distributed storage system
"""

import socket
import json
import time
import random
import threading
import os
from datetime import datetime
from client_enhanced import DistributedStorageClient

class ErrorSimulationClient(DistributedStorageClient):
    """Enhanced client that simulates various error scenarios"""
    
    def __init__(self, server_host='localhost', server_port=8888, node_id=None, error_type=None):
        super().__init__(server_host, server_port, node_id)
        self.error_type = error_type
        self.error_active = False
        self.error_start_time = None
        self.original_socket = None
        
    def simulate_error_scenarios(self):
        """Simulate different types of errors"""
        if not self.error_type:
            return
            
        print(f"🔥 {self.node_id}: Preparing to simulate {self.error_type} error...")
        
        # Wait some time before triggering error
        time.sleep(random.uniform(10, 30))
        
        self.error_active = True
        self.error_start_time = time.time()
        
        if self.error_type == "network_disconnect":
            self.simulate_network_disconnect()
        elif self.error_type == "intermittent_connection":
            self.simulate_intermittent_connection()
        elif self.error_type == "slow_response":
            self.simulate_slow_response()
        elif self.error_type == "corrupted_data":
            self.simulate_corrupted_data()
        elif self.error_type == "storage_full":
            self.simulate_storage_full()
        elif self.error_type == "high_cpu":
            self.simulate_high_cpu()
        elif self.error_type == "memory_leak":
            self.simulate_memory_leak()
        elif self.error_type == "block_corruption":
            self.simulate_block_corruption()
        elif self.error_type == "random_crash":
            self.simulate_random_crash()
    
    def simulate_network_disconnect(self):
        """Simulate network disconnection"""
        print(f"🔥 {self.node_id}: NETWORK DISCONNECT - Closing connection for 60 seconds")
        if self.socket:
            self.socket.close()
            self.socket = None
        
        time.sleep(60)  # Stay disconnected for 1 minute
        
        print(f"🔄 {self.node_id}: Network recovered, attempting reconnection...")
        self.error_active = False
    
    def simulate_intermittent_connection(self):
        """Simulate intermittent connection issues"""
        print(f"🔥 {self.node_id}: INTERMITTENT CONNECTION - Random disconnects")
        
        for _ in range(10):  # 10 cycles of connect/disconnect
            if random.random() < 0.7:  # 70% chance to disconnect
                print(f"⚡ {self.node_id}: Connection lost temporarily...")
                if self.socket:
                    self.socket.close()
                    self.socket = None
                time.sleep(random.uniform(5, 15))
            else:
                time.sleep(random.uniform(3, 8))
        
        self.error_active = False
        print(f"🔄 {self.node_id}: Intermittent connection issues resolved")
    
    def simulate_slow_response(self):
        """Simulate slow response times"""
        print(f"🔥 {self.node_id}: SLOW RESPONSE - Adding delays to operations")
        # This will be handled in send_message method
        time.sleep(120)  # Keep slow for 2 minutes
        self.error_active = False
        print(f"🔄 {self.node_id}: Response time back to normal")
    
    def simulate_corrupted_data(self):
        """Simulate sending corrupted data"""
        print(f"🔥 {self.node_id}: CORRUPTED DATA - Sending malformed messages")
        time.sleep(90)  # Send corrupted data for 90 seconds
        self.error_active = False
        print(f"🔄 {self.node_id}: Data corruption resolved")
    
    def simulate_storage_full(self):
        """Simulate storage full condition"""
        print(f"🔥 {self.node_id}: STORAGE FULL - Reporting 100% storage usage")
        time.sleep(120)  # Report full storage for 2 minutes
        self.error_active = False
        print(f"🔄 {self.node_id}: Storage space recovered")
    
    def simulate_high_cpu(self):
        """Simulate high CPU usage"""
        print(f"🔥 {self.node_id}: HIGH CPU - Reporting 95%+ CPU usage")
        time.sleep(180)  # High CPU for 3 minutes
        self.error_active = False
        print(f"🔄 {self.node_id}: CPU usage normalized")
    
    def simulate_memory_leak(self):
        """Simulate memory leak"""
        print(f"🔥 {self.node_id}: MEMORY LEAK - Gradually increasing memory usage")
        time.sleep(150)  # Memory leak for 2.5 minutes
        self.error_active = False
        print(f"🔄 {self.node_id}: Memory leak fixed")
    
    def simulate_block_corruption(self):
        """Simulate block corruption"""
        print(f"🔥 {self.node_id}: BLOCK CORRUPTION - Corrupting random blocks")
        
        # Corrupt some blocks
        corrupted_blocks = []
        for block_id in list(self.data_blocks.keys())[:3]:  # Corrupt first 3 blocks
            if block_id in self.data_blocks:
                self.data_blocks[block_id]['checksum'] = 'CORRUPTED_' + self.data_blocks[block_id]['checksum']
                corrupted_blocks.append(block_id)
                print(f"💥 {self.node_id}: Corrupted block {block_id}")
        
        time.sleep(60)
        
        # Restore blocks
        for block_id in corrupted_blocks:
            if block_id in self.data_blocks:
                self.data_blocks[block_id]['checksum'] = self.data_blocks[block_id]['checksum'].replace('CORRUPTED_', '')
                print(f"🔧 {self.node_id}: Restored block {block_id}")
        
        self.error_active = False
        print(f"🔄 {self.node_id}: Block corruption resolved")
    
    def simulate_random_crash(self):
        """Simulate random crashes"""
        print(f"🔥 {self.node_id}: RANDOM CRASH - Node will crash randomly")
        
        crash_time = random.uniform(30, 120)  # Crash between 30s and 2min
        time.sleep(crash_time)
        
        print(f"💥 {self.node_id}: CRASHING NOW!")
        self.running = False
        if self.socket:
            self.socket.close()
        
        # Wait before "restart"
        time.sleep(30)
        
        print(f"🔄 {self.node_id}: Restarting after crash...")
        self.running = True
        self.error_active = False
    
    def send_message(self, message):
        """Override send_message to inject errors"""
        if self.error_active and self.error_type == "slow_response":
            # Add random delays for slow response simulation
            delay = random.uniform(2, 10)
            print(f"🐌 {self.node_id}: Adding {delay:.1f}s delay...")
            time.sleep(delay)
        
        if self.error_active and self.error_type == "corrupted_data":
            # Corrupt the message randomly
            if random.random() < 0.3:  # 30% chance to corrupt
                print(f"💥 {self.node_id}: Sending corrupted data...")
                corrupted_message = message.copy()
                corrupted_message['corrupted_field'] = "INVALID_DATA_" + str(random.randint(1000, 9999))
                corrupted_message['timestamp'] = "INVALID_TIMESTAMP"
                return super().send_message(corrupted_message)
        
        return super().send_message(message)
    
    def get_system_status(self):
        """Override to inject status errors"""
        status = super().get_system_status()
        
        if self.error_active:
            if self.error_type == "storage_full":
                status['storage_used'] = status['storage_total'] * 0.99  # 99% full
                status['status'] = 'critical'
                
            elif self.error_type == "high_cpu":
                status['cpu_usage'] = random.uniform(95, 99)
                status['status'] = 'critical'
                
            elif self.error_type == "memory_leak":
                # Gradually increase memory usage
                elapsed = time.time() - self.error_start_time
                leak_factor = min(elapsed / 100, 0.4)  # Max 40% increase
                status['memory_usage'] = min(status['memory_usage'] + (leak_factor * 50), 95)
                if status['memory_usage'] > 85:
                    status['status'] = 'warning'
        
        return status
    
    def start_error_simulation(self):
        """Start the error simulation in a separate thread"""
        if self.error_type:
            error_thread = threading.Thread(target=self.simulate_error_scenarios, daemon=True)
            error_thread.start()

def create_error_node(error_type, node_suffix=""):
    """Create a node with specific error simulation"""
    node_id = f"error_node_{error_type}{node_suffix}"
    client = ErrorSimulationClient(node_id=node_id, error_type=error_type)
    
    # Start error simulation
    client.start_error_simulation()
    
    # Start normal operations
    client.simulate_workload()
    client.start_operations(status_interval=5, block_interval=15)

def main():
    print("🚨 NODE ERROR SIMULATION")
    print("=" * 50)
    print("This script demonstrates various node error scenarios:")
    print("1. Network disconnection")
    print("2. Intermittent connection issues")
    print("3. Slow response times")
    print("4. Corrupted data transmission")
    print("5. Storage full conditions")
    print("6. High CPU usage")
    print("7. Memory leaks")
    print("8. Block corruption")
    print("9. Random crashes")
    print()
    
    # Ask user which error to simulate
    error_types = [
        "network_disconnect",
        "intermittent_connection", 
        "slow_response",
        "corrupted_data",
        "storage_full",
        "high_cpu",
        "memory_leak",
        "block_corruption",
        "random_crash"
    ]
    
    print("Available error types:")
    for i, error_type in enumerate(error_types, 1):
        print(f"{i}. {error_type}")
    
    try:
        choice = input("\nEnter error type number (1-9) or 'all' for all errors: ").strip()
        
        if choice.lower() == 'all':
            print("🔥 Starting multiple error simulations...")
            threads = []
            
            for i, error_type in enumerate(error_types):
                thread = threading.Thread(
                    target=create_error_node, 
                    args=(error_type, f"_{i+1}"),
                    daemon=True
                )
                threads.append(thread)
                thread.start()
                time.sleep(2)  # Stagger starts
            
            print("✅ All error simulations started!")
            
            # Keep main thread alive
            for thread in threads:
                thread.join()
                
        else:
            try:
                error_index = int(choice) - 1
                if 0 <= error_index < len(error_types):
                    error_type = error_types[error_index]
                    print(f"🔥 Starting {error_type} simulation...")
                    create_error_node(error_type)
                else:
                    print("❌ Invalid choice")
            except ValueError:
                print("❌ Invalid input")
                
    except KeyboardInterrupt:
        print("\n🛑 Stopping error simulations...")

if __name__ == "__main__":
    main()
