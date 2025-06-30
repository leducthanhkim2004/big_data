"""
Simple script to demonstrate specific node error scenarios
"""

import subprocess
import sys
import time
import os

def run_server():
    """Run the server"""
    print("🚀 Starting server...")
    server = subprocess.Popen([sys.executable, "server.py"])
    time.sleep(3)  # Give server time to start
    return server

def run_normal_clients(count=2):
    """Run normal healthy clients"""
    clients = []
    for i in range(count):
        print(f"📱 Starting normal client {i+1}...")
        client = subprocess.Popen([sys.executable, "client_enhanced.py", f"healthy_node_{i+1}"])
        clients.append(client)
        time.sleep(1)
    return clients

def run_error_client(error_type):
    """Run a client with specific error"""
    print(f"🔥 Starting error client with {error_type}...")
    error_client = subprocess.Popen([sys.executable, "error_simulation.py"])
    return error_client

def demo_network_errors():
    """Demonstrate network-related errors"""
    print("\n🌐 NETWORK ERROR DEMO")
    print("=" * 40)
    
    processes = []
    
    try:
        # Start server
        server = run_server()
        processes.append(server)
        
        # Start 2 healthy clients
        healthy_clients = run_normal_clients(2)
        processes.extend(healthy_clients)
        
        print("✅ System running normally...")
        time.sleep(10)
        
        # Now demonstrate network disconnect
        print("\n🔥 Simulating network disconnection...")
        from error_simulation import ErrorSimulationClient
        
        error_client = ErrorSimulationClient(node_id="network_error_node", error_type="network_disconnect")
        error_client.start_error_simulation()
        error_client.simulate_workload()
        
        # Run the error client in a thread
        import threading
        error_thread = threading.Thread(
            target=error_client.start_operations, 
            kwargs={'status_interval': 5, 'block_interval': 15},
            daemon=True
        )
        error_thread.start()
        
        print("🔄 Watch the server output to see how it handles the disconnection...")
        print("💡 You should see:")
        print("   • Client connects and registers blocks")
        print("   • Network disconnect occurs after 10-30 seconds")
        print("   • Server detects client timeout/disconnection")
        print("   • Client attempts to reconnect after 60 seconds")
        print("   • Server handles reconnection and updates status")
        
        # Wait for demo
        time.sleep(180)  # 3 minutes demo
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping demo...")
    finally:
        for proc in processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except:
                try:
                    proc.kill()
                except:
                    pass

def demo_resource_errors():
    """Demonstrate resource-related errors"""
    print("\n💾 RESOURCE ERROR DEMO")
    print("=" * 40)
    
    processes = []
    
    try:
        # Start server
        server = run_server()
        processes.append(server)
        
        # Start 1 healthy client
        healthy_clients = run_normal_clients(1)
        processes.extend(healthy_clients)
        
        print("✅ System running normally...")
        time.sleep(5)
        
        # Demonstrate high CPU
        print("\n🔥 Simulating high CPU usage...")
        from error_simulation import ErrorSimulationClient
        
        cpu_error_client = ErrorSimulationClient(node_id="high_cpu_node", error_type="high_cpu")
        cpu_error_client.start_error_simulation()
        cpu_error_client.simulate_workload()
        
        import threading
        cpu_thread = threading.Thread(
            target=cpu_error_client.start_operations,
            kwargs={'status_interval': 3, 'block_interval': 10},
            daemon=True
        )
        cpu_thread.start()
        
        time.sleep(30)
        
        # Demonstrate storage full
        print("\n🔥 Simulating storage full...")
        storage_error_client = ErrorSimulationClient(node_id="storage_full_node", error_type="storage_full")
        storage_error_client.start_error_simulation()
        storage_error_client.simulate_workload()
        
        storage_thread = threading.Thread(
            target=storage_error_client.start_operations,
            kwargs={'status_interval': 3, 'block_interval': 10},
            daemon=True
        )
        storage_thread.start()
        
        print("🔄 Watch the server output to see resource error detection...")
        print("💡 You should see:")
        print("   • Normal client running healthy")
        print("   • High CPU node reporting 95%+ CPU usage")
        print("   • Storage full node reporting 99% storage")
        print("   • Server detecting and flagging these as CRITICAL errors")
        print("   • Status display showing error indicators")
        
        # Wait for demo
        time.sleep(120)  # 2 minutes demo
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping demo...")
    finally:
        for proc in processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except:
                try:
                    proc.kill()
                except:
                    pass

def demo_data_corruption():
    """Demonstrate data corruption errors"""
    print("\n💥 DATA CORRUPTION DEMO")
    print("=" * 40)
    
    processes = []
    
    try:
        # Start server
        server = run_server()
        processes.append(server)
        
        print("✅ Server started...")
        time.sleep(3)
        
        # Demonstrate corrupted data
        print("\n🔥 Simulating corrupted data transmission...")
        from error_simulation import ErrorSimulationClient
        
        corrupt_client = ErrorSimulationClient(node_id="corrupt_data_node", error_type="corrupted_data")
        corrupt_client.start_error_simulation()
        corrupt_client.simulate_workload()
        
        import threading
        corrupt_thread = threading.Thread(
            target=corrupt_client.start_operations,
            kwargs={'status_interval': 5, 'block_interval': 15},
            daemon=True
        )
        corrupt_thread.start()
        
        print("🔄 Watch the server output to see data corruption handling...")
        print("💡 You should see:")
        print("   • Client connects normally")
        print("   • After 10-30 seconds, corrupted data starts")
        print("   • Server detects invalid JSON or corrupted fields")
        print("   • Server logs error messages about corruption")
        print("   • After 90 seconds, corruption stops and node recovers")
        
        # Wait for demo
        time.sleep(150)  # 2.5 minutes demo
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping demo...")
    finally:
        for proc in processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except:
                try:
                    proc.kill()
                except:
                    pass

def main():
    print("🚨 NODE ERROR DEMONSTRATION")
    print("=" * 50)
    print("Choose an error scenario to demonstrate:")
    print("1. Network errors (disconnection, intermittent connection)")
    print("2. Resource errors (high CPU, storage full)")
    print("3. Data corruption errors")
    print("4. All error types (comprehensive demo)")
    
    try:
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == "1":
            demo_network_errors()
        elif choice == "2":
            demo_resource_errors()
        elif choice == "3":
            demo_data_corruption()
        elif choice == "4":
            print("🎯 Running comprehensive error demo...")
            demo_network_errors()
            time.sleep(5)
            demo_resource_errors()
            time.sleep(5)
            demo_data_corruption()
        else:
            print("❌ Invalid choice")
            
    except KeyboardInterrupt:
        print("\n🛑 Demo interrupted by user")

if __name__ == "__main__":
    main()
