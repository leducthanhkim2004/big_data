import time
import socket
import threading
import json
import signal
import sys
from datetime import datetime, timedelta

class SimpleServer:
    """Simple server to receive real client status information"""
    
    def __init__(self, host='localhost', port=1234):
        self.host = host
        self.port = port
        self.clients = {}  # Store client information
        self.lock = threading.Lock()
        self.running = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"\n🛑 Received signal {signum}, shutting down server...")
        self.stop()
        sys.exit(0)
        
    def start(self):
        """Start the server to listen for client messages"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(10)
            self.running = True
            
            print(f"🚀 Storage Server started on {self.host}:{self.port}")
            print("Waiting for storage node connections...")
            
            # Start cleanup thread
            cleanup_thread = threading.Thread(target=self.cleanup_inactive_clients, daemon=True)
            cleanup_thread.start()
            
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
        """Handle individual client messages"""
        print(f"📱 New client connected from {client_address}")
        
        try:
            while self.running:
                try:
                    # Set timeout for socket operations
                    client_socket.settimeout(30)
                    
                    data = client_socket.recv(4096).decode('utf-8')
                    if not data:
                        print(f"📱 Client {client_address} disconnected")
                        break
                    
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
                        client_socket.send(json.dumps(error_response).encode('utf-8'))
                    
                except socket.timeout:
                    print(f"⏰ Timeout from {client_address}")
                    break
                
                except socket.error as e:
                    print(f"❌ Socket error with {client_address}: {e}")
                    break
                    
        except Exception as e:
            print(f"❌ Error handling client {client_address}: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass
            print(f"📱 Client {client_address} disconnected")
    
    def process_message(self, client_address, message):
        """Process messages from clients"""
        message_type = message.get('type', 'info')
        
        if message_type == 'info':
            return self.handle_client_info(client_address, message)
        elif message_type == 'status':
            return self.handle_status_update(client_address, message)
        else:
            return {"status": "received", "message": f"Received {message_type}", "timestamp": datetime.now().isoformat()}
    
    def handle_client_info(self, client_address, message):
        """Handle client information updates"""
        self.update_client_info(client_address, message)
        return {
            "status": "received",
            "message": "Client information updated",
            "timestamp": datetime.now().isoformat()
        }
    
    def handle_status_update(self, client_address, message):
        """Handle status updates from clients"""
        self.update_client_info(client_address, message)
        return {
            "status": "received",
            "message": "Status updated",
            "timestamp": datetime.now().isoformat()
        }
    
    def update_client_info(self, client_address, info_data):
        """Update client information with real status data"""
        with self.lock:
            node_id = info_data.get('node_id', f"{client_address[0]}:{client_address[1]}")
            
            # Store comprehensive client information
            self.clients[node_id] = {
                'address': client_address,
                'last_seen': datetime.now(),
                'node_id': node_id,
                'client_name': info_data.get('client_name', 'Unknown'),
                'status': info_data.get('status', 'unknown'),
                'cpu_usage': info_data.get('cpu_usage', 0),
                'memory_usage': info_data.get('memory_usage', 0),
                'disk_usage': info_data.get('disk_usage', 0),
                'storage_used_gb': info_data.get('storage_used_gb', 0),
                'storage_total_gb': info_data.get('storage_total_gb', 0),
                'storage_free_gb': info_data.get('storage_free_gb', 0),
                'active_connections': info_data.get('active_connections', 0),
                'process_count': info_data.get('process_count', 0),
                'uptime_seconds': info_data.get('uptime_seconds', 0),
                'load_average': info_data.get('load_average', [0, 0, 0]),
                'timestamp': info_data.get('timestamp', datetime.now().isoformat()),
                'data': info_data.get('data', {})
            }
            
            status_icon = {
                'healthy': '✅',
                'warning': '⚠️',
                'critical': '🚨',
                'idle': '😴',
                'error': '❌'
            }.get(info_data.get('status', 'unknown'), '❓')
            
            print(f"📊 {status_icon} {node_id}: {info_data.get('status', 'unknown')} | "
                  f"CPU: {info_data.get('cpu_usage', 0)}% | "
                  f"Memory: {info_data.get('memory_usage', 0)}% | "
                  f"Disk: {info_data.get('disk_usage', 0)}%")
    
    def cleanup_inactive_clients(self):
        """Remove clients that haven't sent updates recently"""
        while self.running:
            try:
                current_time = datetime.now()
                inactive_threshold = timedelta(minutes=2)
                
                with self.lock:
                    inactive_clients = []
                    for client_id, client_info in self.clients.items():
                        if current_time - client_info['last_seen'] > inactive_threshold:
                            inactive_clients.append(client_id)
                    
                    for client_id in inactive_clients:
                        print(f"🗑️ Removing inactive client: {client_id}")
                        del self.clients[client_id]
                
                time.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                print(f"❌ Cleanup error: {e}")
    
    def get_active_clients(self):
        """Get list of currently active clients"""
        with self.lock:
            return dict(self.clients)
    
    def print_status(self):
        """Print detailed server status with real metrics"""
        clients = self.get_active_clients()
        
        print(f"\n📈 Storage Server Status - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Active Storage Nodes: {len(clients)}")
        print("=" * 80)
        
        if not clients:
            print("No active storage nodes")
            return
        
        # Calculate totals
        total_storage_gb = sum(client.get('storage_total_gb', 0) for client in clients.values())
        total_used_gb = sum(client.get('storage_used_gb', 0) for client in clients.values())
        avg_cpu = sum(client.get('cpu_usage', 0) for client in clients.values()) / len(clients)
        avg_memory = sum(client.get('memory_usage', 0) for client in clients.values()) / len(clients)
        
        print(f"Cluster Summary:")
        print(f"  Total Storage: {total_used_gb:.1f}GB / {total_storage_gb:.1f}GB ({(total_used_gb/total_storage_gb)*100 if total_storage_gb > 0 else 0:.1f}%)")
        print(f"  Average CPU: {avg_cpu:.1f}% | Average Memory: {avg_memory:.1f}%")
        print("-" * 80)
            
        for node_id, info in clients.items():
            status_icon = {
                'healthy': '✅',
                'warning': '⚠️', 
                'critical': '🚨',
                'idle': '😴',
                'error': '❌'
            }.get(info.get('status', 'unknown'), '❓')
            
            print(f"{status_icon} Node: {node_id}")
            print(f"  Status: {info.get('status', 'unknown').upper()}")
            print(f"  Resources: CPU {info.get('cpu_usage', 0):.1f}% | Memory {info.get('memory_usage', 0):.1f}% | Disk {info.get('disk_usage', 0):.1f}%")
            print(f"  Storage: {info.get('storage_used_gb', 0):.1f}GB / {info.get('storage_total_gb', 0):.1f}GB")
            print(f"  Connections: {info.get('active_connections', 0)} | Processes: {info.get('process_count', 0)}")
            print(f"  Uptime: {info.get('uptime_seconds', 0):.0f}s | Last Seen: {info['last_seen'].strftime('%H:%M:%S')}")
            
            # Show additional system data
            data = info.get('data', {})
            if data:
                print(f"  System: PID {data.get('pid', 'N/A')} | Host: {data.get('hostname', 'N/A')}")
            print()
    
    def stop(self):
        """Stop the server"""
        self.running = False
        if hasattr(self, 'server_socket'):
            self.server_socket.close()

def main():
    server = SimpleServer()
    
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