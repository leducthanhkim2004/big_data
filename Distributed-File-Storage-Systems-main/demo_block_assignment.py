#!/usr/bin/env python3
"""
Demo: Block Assignment System
Demonstrates how the block assignment system works for nodes
"""

import os
import time
import json
from datetime import datetime
from hdfs_features import HDFSFeatures
from block_manager import BlockManager

class MockActiveClientManager:
    """Mock client manager for demo"""
    
    def __init__(self):
        # Only 1 client free, 3 clients busy
        self.clients = {
            'node1': {'status': 'free', 'cpu_usage': 10, 'memory_usage': 20},
            'node2': {'status': 'busy', 'cpu_usage': 80, 'memory_usage': 70},
            'node3': {'status': 'busy', 'cpu_usage': 75, 'memory_usage': 65},
            'node4': {'status': 'busy', 'cpu_usage': 85, 'memory_usage': 80}
        }
    
    def get_active_clients(self):
        return self.clients
    
    def set_client_status(self, client_id, status):
        if client_id in self.clients:
            self.clients[client_id]['status'] = status
            print(f"📊 Client {client_id} status changed to: {status}")
    
    def print_client_status(self):
        """Print current client status"""
        print("\n📋 Current Client Status:")
        for client_id, info in self.clients.items():
            status_icon = "✅" if info['status'] == 'free' else "⏳"
            print(f"  {status_icon} {client_id}: {info['status']} (CPU: {info['cpu_usage']}%, Memory: {info['memory_usage']}%)")

def create_test_file(filename="test_data.txt", size_mb=10):
    """Create test file with specified size"""
    filepath = os.path.join("uploads", filename)
    os.makedirs("uploads", exist_ok=True)
    
    # Create test content
    content = "This is test data for block assignment demo. " * 1000
    with open(filepath, 'w') as f:
        for i in range(size_mb * 100):  # Create file approximately size_mb MB
            f.write(f"Block {i}: {content}\n")
    
    print(f"📁 Created test file: {filepath} ({os.path.getsize(filepath)} bytes)")
    return filepath

def demo_block_assignment():
    """Demo complete block assignment process"""
    print("🚀 Starting Block Assignment Demo")
    print("=" * 60)
    
    # Initialize components
    client_manager = MockActiveClientManager()
    hdfs = HDFSFeatures(active_client_manager=client_manager)
    block_manager = BlockManager(active_client_manager=client_manager, hdfs_features=hdfs)
    
    print("\n📋 Initial Status:")
    client_manager.print_client_status()
    print(f"Free nodes: {block_manager.get_free_nodes()}")
    
    # Step 1: Create test file
    print("\n" + "=" * 60)
    print("STEP 1: Creating test file")
    test_file = create_test_file("demo_file.txt", size_mb=5)
    
    # Step 2: Split file into blocks (without assigning to nodes)
    print("\n" + "=" * 60)
    print("STEP 2: Splitting file into blocks")
    blocks = hdfs.split_file_into_blocks(test_file)
    
    print(f"\n📊 After splitting:")
    print(f"Total blocks created: {len(blocks)}")
    print(f"Unassigned blocks: {hdfs.get_unassigned_blocks()}")
    
    # Step 3: Check status before assignment
    print("\n" + "=" * 60)
    print("STEP 3: Checking assignment status")
    status = block_manager.get_assignment_status()
    print(f"Assignment status: {json.dumps(status, indent=2)}")
    
    # Step 4: Assign blocks to nodes (only need 1 free client)
    print("\n" + "=" * 60)
    print("STEP 4: Assigning blocks to nodes")
    print("💡 Logic: Only need 1 free client, others can be busy")
    block_manager.assign_pending_blocks()
    
    # Step 5: Check final result
    print("\n" + "=" * 60)
    print("STEP 5: Checking final status")
    final_status = block_manager.get_assignment_status()
    print(f"Final assignment status: {json.dumps(final_status, indent=2)}")
    
    # Step 6: Demo with all nodes busy
    print("\n" + "=" * 60)
    print("STEP 6: Demo - All nodes become busy")
    
    # Create another test file
    test_file2 = create_test_file("demo_file2.txt", size_mb=3)
    blocks2 = hdfs.split_file_into_blocks(test_file2)
    
    print(f"\n📊 After creating second file:")
    print(f"Unassigned blocks: {hdfs.get_unassigned_blocks()}")
    
    # Simulate all nodes becoming busy
    print("\n🔄 Simulating all nodes becoming busy...")
    client_manager.set_client_status('node1', 'busy')
    client_manager.print_client_status()
    
    # Try to assign again - will fail because no free clients
    print("\n🔄 Trying to assign remaining blocks...")
    block_manager.assign_pending_blocks()
    
    # Step 7: Demo with node becoming free
    print("\n" + "=" * 60)
    print("STEP 7: Demo - Node becomes free again")
    
    # Simulate node1 becoming free
    print("\n🔄 Simulating node1 becoming free again...")
    client_manager.set_client_status('node1', 'free')
    client_manager.print_client_status()
    
    # Try to assign again - will succeed
    print("\n🔄 Trying to assign remaining blocks...")
    block_manager.assign_pending_blocks()
    
    # Check final result
    print("\n" + "=" * 60)
    print("FINAL STATUS:")
    final_status = block_manager.get_assignment_status()
    print(f"Assignment status: {json.dumps(final_status, indent=2)}")
    
    # Display metadata
    print("\n📋 Block Metadata:")
    for block_name, block_info in hdfs.metadata['blocks'].items():
        print(f"  {block_name}:")
        print(f"    Status: {block_info.get('status', 'unknown')}")
        print(f"    Assigned to: {block_info.get('assigned_clients', [])}")
        print(f"    Size: {block_info.get('size', 0)} bytes")
    
    print("\n✅ Demo completed!")
    print("\n💡 Key Points:")
    print("  - Only 1 free client is required for block assignment")
    print("  - Other 2 clients can be busy")
    print("  - If no free clients, blocks remain unassigned")
    print("  - When a client becomes free, pending blocks can be assigned")

if __name__ == "__main__":
    demo_block_assignment() 