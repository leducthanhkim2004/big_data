import time
import socket
import threading
import json
import hashlib
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

class BlockManager:
    """Manages data blocks across multiple storage nodes"""
    def __init__(self):
        self.blocks = {}  # block_id -> block_info
        self.node_blocks = {}  # node_id -> list of block_ids
        self.block_replicas = {}  # block_id -> list of node_ids that have this block
        self.lock = threading.Lock()
        
    def register_block(self, block_id: str, node_id: str, block_info: Dict):
        """Register a block on a specific node"""
        with self.lock:
            if block_id not in self.blocks:
                self.blocks[block_id] = {
                    'id': block_id,
                    'size': block_info.get('size', 0),
                    'checksum': block_info.get('checksum', ''),
                    'created': block_info.get('created', datetime.now().isoformat()),
                    'last_updated': datetime.now().isoformat(),
                    'access_count': 0,
                    'metadata': block_info.get('metadata', {})
                }
                self.block_replicas[block_id] = []
            
            # Add node to block replicas if not already present
            if node_id not in self.block_replicas[block_id]:
                self.block_replicas[block_id].append(node_id)
            
            # Update node's block list
            if node_id not in self.node_blocks:
                self.node_blocks[node_id] = []
            if block_id not in self.node_blocks[node_id]:
                self.node_blocks[node_id].append(block_id)
                
            self.blocks[block_id]['last_updated'] = datetime.now().isoformat()
            
    def remove_block_from_node(self, block_id: str, node_id: str):
        """Remove a block from a specific node"""
        with self.lock:
            if block_id in self.block_replicas and node_id in self.block_replicas[block_id]:
                self.block_replicas[block_id].remove(node_id)
                
            if node_id in self.node_blocks and block_id in self.node_blocks[node_id]:
                self.node_blocks[node_id].remove(block_id)
                
            # If no replicas left, remove block entirely
            if block_id in self.block_replicas and len(self.block_replicas[block_id]) == 0:
                del self.blocks[block_id]
                del self.block_replicas[block_id]
                
    def get_block_locations(self, block_id: str) -> List[str]:
        """Get all nodes that have a specific block"""
        with self.lock:
            return self.block_replicas.get(block_id, []).copy()
            
    def get_node_blocks(self, node_id: str) -> List[str]:
        """Get all blocks stored on a specific node"""
        with self.lock:
            return self.node_blocks.get(node_id, []).copy()
            
    def find_best_nodes_for_replication(self, block_id: str, target_replicas: int = 3) -> List[str]:
        """Find the best nodes to replicate a block to"""
        with self.lock:
            current_replicas = self.block_replicas.get(block_id, [])
            if len(current_replicas) >= target_replicas:
                return []
                
            # Simple strategy: find nodes with least blocks
            node_load = [(node_id, len(blocks)) for node_id, blocks in self.node_blocks.items() 
                        if node_id not in current_replicas]
            node_load.sort(key=lambda x: x[1])
            
            needed_replicas = target_replicas - len(current_replicas)
            return [node_id for node_id, _ in node_load[:needed_replicas]]

class DistributedStorageServer:
    def __init__(self, host='localhost', port=8888):
        self.host = host
        self.port = port
        self.clients = {}  # Store client information
        self.lock = threading.Lock()
        self.running = False
        self.block_manager = BlockManager()
        self.message_queue = []  # Queue for inter-node messages
        self.queue_lock = threading.Lock()
        
    def start(self):
        """Start the server to listen for client announcements"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(20)  # Increased for more concurrent connections
            self.running = True
            
            print(f"🚀 Distributed Storage Server started on {self.host}:{self.port}")
            print("Waiting for client status announcements and block operations...")
            
            # Start background threads
            cleanup_thread = threading.Thread(target=self.cleanup_inactive_clients, daemon=True)
            cleanup_thread.start()
            
            message_processor = threading.Thread(target=self.process_message_queue, daemon=True)
            message_processor.start()
            
            replication_monitor = threading.Thread(target=self.monitor_replication, daemon=True)
            replication_monitor.start()
            
            while self.running:
                try:
                    client_socket, client_address = self.server_socket.accept()
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, client_address),
                        daemon=True
                    )
                    client_thread.start()
                except socket.error:
                    if self.running:
                        print("❌ Error accepting client connection")
                        
        except Exception as e:
            print(f"❌ Server error: {e}")
        finally:
            self.server_socket.close()
    
    def handle_client(self, client_socket, client_address):
        """Handle individual client messages and block operations with error handling"""
        print(f"📱 New client connected from {client_address}")
        connection_errors = 0
        max_connection_errors = 5
        
        try:
            while self.running:
                try:
                    # Set timeout for socket operations
                    client_socket.settimeout(30)  # 30 second timeout
                    
                    data = client_socket.recv(4096).decode('utf-8')
                    if not data:
                        print(f"📱 Client {client_address} sent empty data, disconnecting")
                        break
                    
                    # Reset error counter on successful data receive
                    connection_errors = 0
                    
                    try:
                        message = json.loads(data)
                        response = self.process_message(client_address, message)
                        
                        # Send response back to client
                        response_json = json.dumps(response)
                        client_socket.send(response_json.encode('utf-8'))
                        
                    except json.JSONDecodeError as e:
                        print(f"❌ Invalid JSON from {client_address}: {e}")
                        error_response = {
                            "status": "error", 
                            "message": f"Invalid JSON format: {str(e)}",
                            "timestamp": datetime.now().isoformat()
                        }
                        try:
                            client_socket.send(json.dumps(error_response).encode('utf-8'))
                        except:
                            break  # Cannot send error response, disconnect
                    
                    except Exception as json_error:
                        print(f"❌ Error processing message from {client_address}: {json_error}")
                        error_response = {
                            "status": "error", 
                            "message": f"Processing error: {str(json_error)}",
                            "timestamp": datetime.now().isoformat()
                        }
                        try:
                            client_socket.send(json.dumps(error_response).encode('utf-8'))
                        except:
                            break
                
                except socket.timeout:
                    print(f"⏰ Timeout waiting for data from {client_address}")
                    connection_errors += 1
                    if connection_errors >= max_connection_errors:
                        print(f"❌ Too many timeouts from {client_address}, disconnecting")
                        break
                
                except socket.error as e:
                    print(f"❌ Socket error with {client_address}: {e}")
                    connection_errors += 1
                    if connection_errors >= max_connection_errors:
                        print(f"❌ Too many socket errors from {client_address}, disconnecting")
                        break
                
                except Exception as e:
                    print(f"❌ Unexpected error handling client {client_address}: {e}")
                    break
                    
        except KeyboardInterrupt:
            print(f"🛑 Server shutdown requested while handling {client_address}")
        except Exception as e:
            print(f"❌ Fatal error handling client {client_address}: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass
            print(f"📱 Client {client_address} disconnected (errors: {connection_errors})")
    
    def process_message(self, client_address, message: Dict) -> Dict:
        """Process different types of messages from clients"""
        message_type = message.get('type', 'status')
        
        if message_type == 'status':
            return self.handle_status_update(client_address, message)
        elif message_type == 'block_register':
            return self.handle_block_register(client_address, message)
        elif message_type == 'block_query':
            return self.handle_block_query(client_address, message)
        elif message_type == 'block_replicate':
            return self.handle_block_replicate(client_address, message)
        elif message_type == 'block_delete':
            return self.handle_block_delete(client_address, message)
        elif message_type == 'node_sync':
            return self.handle_node_sync(client_address, message)
        else:
            return {"status": "error", "message": f"Unknown message type: {message_type}"}
    
    def handle_status_update(self, client_address, message: Dict) -> Dict:
        """Handle regular status updates from clients"""
        self.update_client_status(client_address, message)
        return {"status": "received", "timestamp": datetime.now().isoformat()}
    
    def handle_block_register(self, client_address, message: Dict) -> Dict:
        """Handle block registration from a node"""
        try:
            node_id = message.get('node_id', f"{client_address[0]}:{client_address[1]}")
            blocks = message.get('blocks', [])
            
            registered_blocks = []
            for block_info in blocks:
                block_id = block_info.get('id')
                if block_id:
                    self.block_manager.register_block(block_id, node_id, block_info)
                    registered_blocks.append(block_id)
            
            print(f"📦 Registered {len(registered_blocks)} blocks from node {node_id}")
            
            # Check if replication is needed
            replication_tasks = []
            for block_id in registered_blocks:
                needed_nodes = self.block_manager.find_best_nodes_for_replication(block_id)
                if needed_nodes:
                    replication_tasks.append({
                        'block_id': block_id,
                        'target_nodes': needed_nodes
                    })
            
            return {
                "status": "success",
                "registered_blocks": len(registered_blocks),
                "replication_tasks": replication_tasks,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def handle_block_query(self, client_address, message: Dict) -> Dict:
        """Handle queries for block locations"""
        try:
            block_id = message.get('block_id')
            if not block_id:
                return {"status": "error", "message": "block_id required"}
            
            locations = self.block_manager.get_block_locations(block_id)
            block_info = self.block_manager.blocks.get(block_id, {})
            
            return {
                "status": "success",
                "block_id": block_id,
                "locations": locations,
                "block_info": block_info,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def handle_block_replicate(self, client_address, message: Dict) -> Dict:
        """Handle block replication requests"""
        try:
            block_id = message.get('block_id')
            source_node = message.get('source_node')
            target_nodes = message.get('target_nodes', [])
            
            # Add replication tasks to queue
            for target_node in target_nodes:
                replication_task = {
                    'type': 'replicate',
                    'block_id': block_id,
                    'source_node': source_node,
                    'target_node': target_node,
                    'timestamp': datetime.now().isoformat()
                }
                self.add_to_message_queue(replication_task)
            
            return {
                "status": "success",
                "message": f"Replication tasks queued for block {block_id}",
                "tasks": len(target_nodes)
            }
            
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def handle_block_delete(self, client_address, message: Dict) -> Dict:
        """Handle block deletion notifications"""
        try:
            node_id = message.get('node_id', f"{client_address[0]}:{client_address[1]}")
            block_id = message.get('block_id')
            
            self.block_manager.remove_block_from_node(block_id, node_id)
            
            print(f"🗑️ Block {block_id} removed from node {node_id}")
            
            return {
                "status": "success",
                "message": f"Block {block_id} removed from node {node_id}"
            }
            
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def handle_node_sync(self, client_address, message: Dict) -> Dict:
        """Handle node synchronization requests"""
        try:
            node_id = message.get('node_id', f"{client_address[0]}:{client_address[1]}")
            
            # Get all blocks for this node
            node_blocks = self.block_manager.get_node_blocks(node_id)
            
            # Get pending messages for this node
            pending_messages = self.get_pending_messages_for_node(node_id)
            
            return {
                "status": "success",
                "node_blocks": node_blocks,
                "pending_messages": pending_messages,
                "total_blocks_in_system": len(self.block_manager.blocks),
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def add_to_message_queue(self, message: Dict):
        """Add a message to the processing queue"""
        with self.queue_lock:
            self.message_queue.append(message)
    
    def get_pending_messages_for_node(self, node_id: str) -> List[Dict]:
        """Get pending messages for a specific node"""
        with self.queue_lock:
            return [msg for msg in self.message_queue if msg.get('target_node') == node_id]
    
    def process_message_queue(self):
        """Process queued messages for inter-node communication"""
        while self.running:
            try:
                with self.queue_lock:
                    if self.message_queue:
                        message = self.message_queue.pop(0)
                        print(f"📨 Processing queued message: {message.get('type', 'unknown')}")
                        
                        # Here you would implement the actual message delivery
                        # For now, we just log it
                        
                time.sleep(1)  # Process messages every second
                
            except Exception as e:
                print(f"❌ Message queue processing error: {e}")
    
    def monitor_replication(self):
        """Monitor and ensure proper block replication"""
        while self.running:
            try:
                with self.block_manager.lock:
                    for block_id, replicas in self.block_manager.block_replicas.items():
                        if len(replicas) < 2:  # Minimum 2 replicas
                            needed_nodes = self.block_manager.find_best_nodes_for_replication(block_id, 3)
                            if needed_nodes and replicas:  # Need source node
                                replication_task = {
                                    'type': 'auto_replicate',
                                    'block_id': block_id,
                                    'source_node': replicas[0],
                                    'target_nodes': needed_nodes,
                                    'timestamp': datetime.now().isoformat()
                                }
                                self.add_to_message_queue(replication_task)
                                print(f"🔄 Auto-replication queued for block {block_id}")
                
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                print(f"❌ Replication monitoring error: {e}")
    
    def update_client_status(self, client_address, status_data):
        """Update client status information with error detection"""
        with self.lock:
            client_id = f"{client_address[0]}:{client_address[1]}"
            node_id = status_data.get('node_id', client_id)
            
            # Detect various error conditions
            errors_detected = []
            warnings_detected = []
            
            # Storage errors
            storage_used = status_data.get('storage_used', 0)
            storage_total = status_data.get('storage_total', 1)
            storage_percent = (storage_used / storage_total) * 100 if storage_total > 0 else 0
            
            if storage_percent >= 95:
                errors_detected.append(f"CRITICAL: Storage {storage_percent:.1f}% full")
            elif storage_percent >= 85:
                warnings_detected.append(f"WARNING: Storage {storage_percent:.1f}% full")
            
            # CPU errors
            cpu_usage = status_data.get('cpu_usage', 0)
            if cpu_usage >= 90:
                errors_detected.append(f"CRITICAL: CPU usage {cpu_usage:.1f}%")
            elif cpu_usage >= 75:
                warnings_detected.append(f"WARNING: High CPU usage {cpu_usage:.1f}%")
            
            # Memory errors
            memory_usage = status_data.get('memory_usage', 0)
            if memory_usage >= 90:
                errors_detected.append(f"CRITICAL: Memory usage {memory_usage:.1f}%")
            elif memory_usage >= 80:
                warnings_detected.append(f"WARNING: High memory usage {memory_usage:.1f}%")
            
            # Check for corrupted data
            if 'corrupted_field' in status_data:
                errors_detected.append("CRITICAL: Corrupted data detected in message")
            
            # Check timestamp validity
            try:
                timestamp = status_data.get('timestamp', '')
                datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                errors_detected.append("ERROR: Invalid timestamp format")
            
            # Determine overall status
            if errors_detected:
                computed_status = 'error'
            elif warnings_detected:
                computed_status = 'warning'
            else:
                computed_status = status_data.get('status', 'unknown')
            
            # Store client information
            self.clients[client_id] = {
                'address': client_address,
                'last_seen': datetime.now(),
                'status': computed_status,
                'reported_status': status_data.get('status', 'unknown'),
                'storage_used': storage_used,
                'storage_total': storage_total,
                'cpu_usage': cpu_usage,
                'memory_usage': memory_usage,
                'active_connections': status_data.get('active_connections', 0),
                'data_blocks': status_data.get('data_blocks', []),
                'node_id': node_id,
                'errors': errors_detected,
                'warnings': warnings_detected,
                'block_count': status_data.get('block_count', 0),
                'operations_count': status_data.get('operations_count', 0)
            }
            
            # Log status with error information
            status_msg = f"📊 Updated status for {node_id}: {computed_status}"
            if errors_detected:
                status_msg += f" | ERRORS: {'; '.join(errors_detected)}"
            elif warnings_detected:
                status_msg += f" | WARNINGS: {'; '.join(warnings_detected)}"
            
            print(status_msg)
    
    def cleanup_inactive_clients(self):
        """Remove clients that haven't sent status updates recently"""
        while self.running:
            try:
                current_time = datetime.now()
                inactive_threshold = timedelta(minutes=2)  # Consider client inactive after 2 minutes
                
                with self.lock:
                    inactive_clients = []
                    for client_id, client_info in self.clients.items():
                        if current_time - client_info['last_seen'] > inactive_threshold:
                            inactive_clients.append(client_id)
                    
                    for client_id in inactive_clients:
                        print(f"🗑️ Removing inactive client: {client_id}")
                        # Also remove blocks associated with this node
                        node_blocks = self.block_manager.get_node_blocks(client_id)
                        for block_id in node_blocks:
                            self.block_manager.remove_block_from_node(block_id, client_id)
                        del self.clients[client_id]
                
                time.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                print(f"❌ Cleanup error: {e}")
    
    def get_active_clients(self):
        """Get list of currently active clients"""
        with self.lock:
            return dict(self.clients)
    
    def print_status(self):
        """Print current server status with block information and error detection"""
        clients = self.get_active_clients()
        total_blocks = len(self.block_manager.blocks)
        
        # Count nodes by status
        healthy_nodes = sum(1 for c in clients.values() if c['status'] == 'healthy')
        warning_nodes = sum(1 for c in clients.values() if c['status'] in ['warning', 'critical'])
        error_nodes = sum(1 for c in clients.values() if c['status'] == 'error')
        
        print(f"\n📈 Distributed Storage Server Status")
        print(f"Active Clients: {len(clients)} | Total Blocks: {total_blocks}")
        print(f"Health Status: 🟢 {healthy_nodes} healthy | 🟡 {warning_nodes} warning | 🔴 {error_nodes} error")
        print("-" * 90)
        
        for client_id, info in clients.items():
            node_blocks = self.block_manager.get_node_blocks(info['node_id'])
            storage_percent = (info['storage_used'] / info['storage_total'] * 100) if info['storage_total'] > 0 else 0
            
            # Status indicator
            status_icon = "🟢" if info['status'] == 'healthy' else "🟡" if info['status'] in ['warning', 'critical'] else "🔴"
            
            print(f"Node: {info['node_id']} {status_icon}")
            print(f"  Status: {info['status']} (reported: {info.get('reported_status', 'unknown')}) | Blocks: {len(node_blocks)}")
            print(f"  Storage: {info['storage_used']:.1f}GB / {info['storage_total']:.1f}GB ({storage_percent:.1f}%)")
            print(f"  Resources: CPU {info['cpu_usage']:.1f}% | Memory {info['memory_usage']:.1f}%")
            print(f"  Operations: {info.get('operations_count', 0)} | Last Seen: {info['last_seen'].strftime('%H:%M:%S')}")
            
            # Show errors and warnings
            if info.get('errors'):
                print(f"  🔴 ERRORS: {'; '.join(info['errors'])}")
            if info.get('warnings'):
                print(f"  🟡 WARNINGS: {'; '.join(info['warnings'])}")
            print()
        
        # Show block distribution summary
        if total_blocks > 0:
            print("📦 Block Distribution Summary:")
            replication_stats = {}
            for block_id, replicas in self.block_manager.block_replicas.items():
                replica_count = len(replicas)
                replication_stats[replica_count] = replication_stats.get(replica_count, 0) + 1
            
            for replica_count, block_count in sorted(replication_stats.items()):
                status_emoji = "🔴" if replica_count == 1 else "🟡" if replica_count == 2 else "🟢"
                print(f"  {status_emoji} {block_count} blocks with {replica_count} replica(s)")
            
            # Show critical blocks (single replica)
            critical_blocks = [block_id for block_id, replicas in self.block_manager.block_replicas.items() if len(replicas) == 1]
            if critical_blocks:
                print(f"  ⚠️ CRITICAL: {len(critical_blocks)} blocks have only 1 replica!")
            print()
    
    def stop(self):
        """Stop the server"""
        self.running = False
        if hasattr(self, 'server_socket'):
            self.server_socket.close()

def main():
    server = DistributedStorageServer()
    
    try:
        server_thread = threading.Thread(target=server.start, daemon=True)
        server_thread.start()
        
        while True:
            time.sleep(15)  # Print status every 15 seconds
            server.print_status()
            
    except KeyboardInterrupt:
        print("\n🛑 Shutting down server...")
        server.stop()

if __name__ == "__main__":
    main()