"""
Test script to demonstrate handling multiple messages from different machines for each block
"""

import socket
import json
import time
import threading
from datetime import datetime

def create_test_client(node_id, server_host='localhost', server_port=8888):
    """Create a test client that sends various block-related messages"""
    
    def send_message(sock, message):
        """Send a message and get response"""
        try:
            sock.send(json.dumps(message).encode('utf-8'))
            response = sock.recv(4096).decode('utf-8')
            return json.loads(response)
        except Exception as e:
            print(f"❌ Error sending message: {e}")
            return None
    
    def run_client():
        try:
            # Connect to server
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((server_host, server_port))
            print(f"✅ {node_id} connected to server")
            
            # 1. Send status update
            status_msg = {
                'type': 'status',
                'node_id': node_id,
                'status': 'healthy',
                'storage_used': 25.5,
                'storage_total': 100.0,
                'cpu_usage': 45.2,
                'memory_usage': 62.1,
                'block_count': 15,
                'timestamp': datetime.now().isoformat()
            }
            print(f"📡 {node_id}: Sending status update")
            response = send_message(sock, status_msg)
            print(f"📡 {node_id}: Status response: {response.get('status', 'unknown')}")
            
            time.sleep(1)
            
            # 2. Register blocks
            blocks = []
            for i in range(5):
                block_id = f"block_{node_id}_{i+1}"
                blocks.append({
                    'id': block_id,
                    'size': 0.5 + (i * 0.1),  # 0.5MB to 0.9MB
                    'checksum': f"checksum_{block_id}",
                    'created': datetime.now().isoformat(),
                    'metadata': {'node': node_id, 'type': 'test_data'}
                })
            
            register_msg = {
                'type': 'block_register',
                'node_id': node_id,
                'blocks': blocks,
                'timestamp': datetime.now().isoformat()
            }
            print(f"📦 {node_id}: Registering {len(blocks)} blocks")
            response = send_message(sock, register_msg)
            if response:
                print(f"📦 {node_id}: Registered {response.get('registered_blocks', 0)} blocks")
                replication_tasks = response.get('replication_tasks', [])
                if replication_tasks:
                    print(f"🔄 {node_id}: Got {len(replication_tasks)} replication tasks")
            
            time.sleep(2)
            
            # 3. Query block locations
            for block in blocks[:2]:  # Query first 2 blocks
                query_msg = {
                    'type': 'block_query',
                    'node_id': node_id,
                    'block_id': block['id'],
                    'timestamp': datetime.now().isoformat()
                }
                print(f"🔍 {node_id}: Querying block {block['id']}")
                response = send_message(sock, query_msg)
                if response and response.get('status') == 'success':
                    locations = response.get('locations', [])
                    print(f"🔍 {node_id}: Block {block['id']} found on {len(locations)} nodes: {locations}")
            
            time.sleep(1)
            
            # 4. Simulate block replication request
            if blocks:
                replicate_msg = {
                    'type': 'block_replicate',
                    'node_id': node_id,
                    'block_id': blocks[0]['id'],
                    'source_node': node_id,
                    'target_nodes': [f"node_{i}" for i in range(1, 4) if f"node_{i}" != node_id],
                    'timestamp': datetime.now().isoformat()
                }
                print(f"🔄 {node_id}: Requesting replication for {blocks[0]['id']}")
                response = send_message(sock, replicate_msg)
                if response:
                    print(f"🔄 {node_id}: Replication response: {response.get('message', 'unknown')}")
            
            time.sleep(1)
            
            # 5. Node synchronization
            sync_msg = {
                'type': 'node_sync',
                'node_id': node_id,
                'local_blocks': [block['id'] for block in blocks],
                'timestamp': datetime.now().isoformat()
            }
            print(f"🔄 {node_id}: Synchronizing with server")
            response = send_message(sock, sync_msg)
            if response and response.get('status') == 'success':
                total_blocks = response.get('total_blocks_in_system', 0)
                print(f"🔄 {node_id}: Sync complete. Total system blocks: {total_blocks}")
            
            time.sleep(2)
            
            # 6. Simulate block deletion
            if blocks:
                delete_msg = {
                    'type': 'block_delete',
                    'node_id': node_id,
                    'block_id': blocks[-1]['id'],  # Delete last block
                    'timestamp': datetime.now().isoformat()
                }
                print(f"🗑️ {node_id}: Deleting block {blocks[-1]['id']}")
                response = send_message(sock, delete_msg)
                if response:
                    print(f"🗑️ {node_id}: Delete response: {response.get('message', 'unknown')}")
            
            print(f"✅ {node_id}: Test sequence completed")
            
        except Exception as e:
            print(f"❌ {node_id}: Error: {e}")
        finally:
            try:
                sock.close()
            except:
                pass
    
    return run_client

def main():
    print("🧪 Testing Multi-Machine Block Message Handling")
    print("=" * 50)
    print("This test demonstrates how the server handles multiple")
    print("messages from different machines for block operations:")
    print("• Status updates")
    print("• Block registration")
    print("• Block location queries")
    print("• Replication requests")
    print("• Node synchronization")
    print("• Block deletion")
    print()
    
    # Wait for server to be ready
    print("⏳ Waiting for server to be ready...")
    time.sleep(2)
    
    # Create multiple test clients (simulating different machines)
    nodes = ['test_node_1', 'test_node_2', 'test_node_3', 'test_node_4']
    threads = []
    
    # Start all clients concurrently
    for node_id in nodes:
        client_func = create_test_client(node_id)
        thread = threading.Thread(target=client_func, daemon=True)
        threads.append(thread)
        thread.start()
        time.sleep(0.5)  # Slight delay between starts
    
    # Wait for all tests to complete
    for thread in threads:
        thread.join()
    
    print("\n🎉 All tests completed!")
    print("Check the server output to see how it handled all the messages")

if __name__ == "__main__":
    main()
