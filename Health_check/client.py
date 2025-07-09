import grpc
import task_pb2
import task_pb2_grpc
import time
import threading
from datetime import datetime

class Client:
    def __init__(self, client_id, server_address='localhost:50051'):
        self.client_id = client_id
        self.server_address = server_address
        self.stub = None
        self.heartbeat_thread = None
        self.task_thread = None
        self.running = False

    def connect(self):
        """Connect to server and start heartbeat"""
        channel = grpc.insecure_channel(self.server_address)
        self.stub = task_pb2_grpc.TaskServiceStub(channel)
        
        # Start heartbeat thread
        self.running = True
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeat, daemon=True)
        self.heartbeat_thread.start()
        
        # Start task processing thread
        self.task_thread = threading.Thread(target=self._process_tasks, daemon=True)
        self.task_thread.start()
        
        print(f"Client {self.client_id} connected to server")

    def _send_heartbeat(self):
        """Send periodic heartbeat to server"""
        while self.running:
            try:
                # Create heartbeat message
                heartbeat_msg = task_pb2.HeartbeatMessage(
                    client_id=self.client_id,
                    status="active",
                    timestamp=int(time.time())
                )
                
                # Send heartbeat
                response = self.stub.Heartbeat(iter([heartbeat_msg]))
                for resp in response:
                    print(f"Heartbeat response: {resp.message}")
                
                time.sleep(5)  # Send heartbeat every 5 seconds
                
            except grpc.RpcError as e:
                print(f"Heartbeat failed: {e}")
                break

    def _process_tasks(self):
        """Process tasks automatically (original functionality)"""
        status = "free"
        while self.running:
            try:
                # Request a task
                response = self.stub.RequestTask(task_pb2.TaskRequest(client_id=self.client_id, status=status))
                
                if response.task:
                    print(f"{self.client_id} received task: {response.task}")
                    status = "busy"
                    
                    # Simulate doing the task
                    time.sleep(2)
                    
                    result = f"{self.client_id} completed {response.task}"
                    self.stub.SendResult(task_pb2.TaskResult(client_id=self.client_id, result=result))
                    print(f"{self.client_id} sent result: {result}")
                    status = "free"
                else:
                    print(f"{self.client_id} has no task, waiting...")
                    time.sleep(1)  # Wait before requesting again
                    
            except grpc.RpcError as e:
                print(f"Task processing failed: {e}")
                time.sleep(5)  # Wait before retrying

    def disconnect(self):
        """Disconnect from server"""
        self.running = False
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=1)
        if self.task_thread:
            self.task_thread.join(timeout=1)
        print(f"Client {self.client_id} disconnected")

    def request_task(self):
        """Request task from server (manual method)"""
        try:
            request = task_pb2.TaskRequest(
                client_id=self.client_id,
                status="free"
            )
            response = self.stub.RequestTask(request)
            return response.task
        except grpc.RpcError as e:
            print(f"Request task failed: {e}")
            return None

    def send_result(self, result):
        """Send result to server (manual method)"""
        try:
            result_msg = task_pb2.TaskResult(
                client_id=self.client_id,
                result=result
            )
            response = self.stub.SendResult(result_msg)
            return response.success
        except grpc.RpcError as e:
            print(f"Send result failed: {e}")
            return False

def run(client_id):
    """Original function for backward compatibility"""
    client = Client(client_id)
    client.connect()
    
    try:
        # Keep running until interrupted
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down client...")
    finally:
        client.disconnect()

# Usage example
if __name__ == "__main__":
    import sys
    client_id = sys.argv[1] if len(sys.argv) > 1 else "clientA"
    run(client_id)
    