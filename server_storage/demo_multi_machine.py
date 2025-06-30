"""
Demo script for testing distributed storage with multiple clients handling block operations
"""

import subprocess
import time
import sys
import os

def start_server():
    """Start the server in a separate process"""
    print("🚀 Starting distributed storage server...")
    server_process = subprocess.Popen([
        sys.executable, "server.py"
    ], cwd=os.path.dirname(__file__))
    
    # Give server time to start
    time.sleep(3)
    return server_process

def start_client(node_id):
    """Start a client in a separate process"""
    print(f"📱 Starting client {node_id}...")
    client_process = subprocess.Popen([
        sys.executable, "client_enhanced.py", node_id
    ], cwd=os.path.dirname(__file__))
    return client_process

def main():
    print("🎯 Distributed Storage Demo - Multi Machine Block Handling")
    print("=" * 60)
    
    processes = []
    
    try:
        # Start server
        server_proc = start_server()
        processes.append(server_proc)
        
        # Start multiple clients to simulate different machines
        client_nodes = ["node_alpha", "node_beta", "node_gamma", "node_delta"]
        
        for node_id in client_nodes:
            client_proc = start_client(node_id)
            processes.append(client_proc)
            time.sleep(2)  # Stagger client startup
        
        print("\n✅ All processes started!")
        print("📊 The system will now:")
        print("   • Each client creates and manages data blocks")
        print("   • Clients announce their status every 8 seconds")
        print("   • Block operations (create/delete/access) happen every 20 seconds")
        print("   • Server coordinates block replication across nodes")
        print("   • Server monitors and displays system status")
        print("\n🔄 Running... Press Ctrl+C to stop all processes")
        
        # Wait for user to stop
        while True:
            time.sleep(1)
            
            # Check if any process died
            for i, proc in enumerate(processes):
                if proc.poll() is not None:
                    print(f"❌ Process {i} died, restarting might be needed")
    
    except KeyboardInterrupt:
        print("\n🛑 Stopping all processes...")
        
        # Terminate all processes
        for proc in processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except:
                try:
                    proc.kill()
                except:
                    pass
        
        print("✅ All processes stopped")

if __name__ == "__main__":
    main()
