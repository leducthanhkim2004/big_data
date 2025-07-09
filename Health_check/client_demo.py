import grpc
import task_pb2
import task_pb2_grpc
import time
import threading
from datetime import datetime

class DemoClient:
    def __init__(self, client_id, server_address='localhost:50051'):
        self.client_id = client_id
        self.server_address = server_address
        self.channel = grpc.insecure_channel(server_address)
        self.stub = task_pb2_grpc.TaskServiceStub(self.channel)
        self.status = "free"
        self.running = True
        
    def start(self):
        """Start the client - request tasks and send heartbeats"""
        print(f"🚀 Client {self.client_id} starting...")
        
        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        
        # Main task request loop
        self._task_request_loop()
    
    def _task_request_loop(self):
        """Main loop to request tasks from server"""
        while self.running:
            try:
                # Request task
                request = task_pb2.TaskRequest(
                    client_id=self.client_id,
                    status=self.status
                )
                
                response = self.stub.RequestTask(request)
                
                if response.task:
                    print(f"📦 Client {self.client_id} received: {response.task}")
                    
                    # Process the task
                    self._process_task(response.task)
                else:
                    print(f"⏳ Client {self.client_id} - no tasks available")
                    time.sleep(2)
                    
            except grpc.RpcError as e:
                print(f"❌ Client {self.client_id} RPC error: {e}")
                time.sleep(5)
            except Exception as e:
                print(f"❌ Client {self.client_id} error: {e}")
                time.sleep(5)
    
    def _process_task(self, task):
        """Process received task"""
        self.status = "busy"
        
        if task.startswith("BLOCK:"):
            # Process block
            block_name = task[6:]  # Remove "BLOCK:" prefix
            print(f"🔧 Client {self.client_id} processing block: {block_name}")
            
            # Simulate block processing
            time.sleep(3)  # Simulate processing time
            
            # Send block result
            result = f"BLOCK_RESULT:{block_name}:processed_successfully"
            self._send_result(result)
            
        else:
            # Process regular task
            print(f"🔧 Client {self.client_id} processing task: {task}")
            
            # Simulate task processing
            time.sleep(2)  # Simulate processing time
            
            # Send task result
            result = f"Task {task} completed successfully"
            self._send_result(result)
        
        self.status = "free"
    
    def _send_result(self, result):
        """Send result back to server"""
        try:
            request = task_pb2.TaskResult(
                client_id=self.client_id,
                result=result
            )
            
            response = self.stub.SendResult(request)
            if response.success:
                print(f"✅ Client {self.client_id} sent result: {result}")
            else:
                print(f"❌ Client {self.client_id} failed to send result")
                
        except Exception as e:
            print(f"❌ Client {self.client_id} error sending result: {e}")
    
    def _heartbeat_loop(self):
        """Send periodic heartbeats to server"""
        while self.running:
            try:
                def generate_heartbeats():
                    while self.running:
                        heartbeat = task_pb2.HeartbeatMessage(
                            client_id=self.client_id,
                            status=self.status,
                            timestamp=int(time.time())
                        )
                        yield heartbeat
                        time.sleep(5)  # Send heartbeat every 5 seconds
                
                # Start streaming heartbeat
                responses = self.stub.Heartbeat(generate_heartbeats())
                
                for response in responses:
                    if response.alive:
                        print(f"💓 Client {self.client_id} heartbeat: {response.message}")
                    else:
                        print(f"⚠️ Client {self.client_id} heartbeat failed")
                        
            except grpc.RpcError as e:
                print(f"❌ Client {self.client_id} heartbeat error: {e}")
                time.sleep(5)
            except Exception as e:
                print(f"❌ Client {self.client_id} heartbeat error: {e}")
                time.sleep(5)
    
    def stop(self):
        """Stop the client"""
        self.running = False
        if self.channel:
            self.channel.close()

def main():
    """Demo with multiple clients"""
    print("🚀 Starting Demo Clients...")
    
    # Create multiple clients
    clients = []
    for i in range(3):
        client = DemoClient(f"client_{i+1}")
        clients.append(client)
    
    # Start all clients in separate threads
    threads = []
    for client in clients:
        thread = threading.Thread(target=client.start, daemon=True)
        thread.start()
        threads.append(thread)
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopping clients...")
        for client in clients:
            client.stop()
        print("✅ All clients stopped")

if __name__ == "__main__":
    main() 