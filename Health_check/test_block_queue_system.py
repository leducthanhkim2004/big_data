import grpc
import task_pb2
import task_pb2_grpc
import time
import threading
from datetime import datetime

def test_block_queue_system():
    """Test the block queue system"""
    print("🧪 Testing Block Queue System")
    print("=" * 50)
    
    # Connect to server
    channel = grpc.insecure_channel('localhost:50051')
    stub = task_pb2_grpc.TaskServiceStub(channel)
    
    try:
        # Test 1: Get initial queue status
        print("\n📊 Test 1: Get initial queue status")
        request = task_pb2.QueueRequest()
        response = stub.GetQueueStatus(request)
        print(f"Task queue length: {response.task_queue_length}")
        print(f"Block queue length: {response.block_queue_length}")
        print(f"Active clients: {response.active_clients}")
        print(f"Task queue: {response.task_queue}")
        print(f"Block queue: {response.block_queue}")
        
        # Test 2: Add more blocks to queue
        print("\n📋 Test 2: Add more blocks to queue")
        new_blocks = ['file3_block_0', 'file3_block_1', 'file4_block_0']
        for block in new_blocks:
            request = task_pb2.BlockRequest(block_name=block)
            response = stub.AddBlockToQueue(request)
            print(f"Added block {block}: {response.success}")
        
        # Test 3: Get updated queue status
        print("\n📊 Test 3: Get updated queue status")
        request = task_pb2.QueueRequest()
        response = stub.GetQueueStatus(request)
        print(f"Task queue length: {response.task_queue_length}")
        print(f"Block queue length: {response.block_queue_length}")
        print(f"Block queue: {response.block_queue}")
        
        # Test 4: Simulate client requesting tasks
        print("\n👤 Test 4: Simulate client requesting tasks")
        client_id = "test_client_1"
        
        # Request multiple tasks
        for i in range(5):
            request = task_pb2.TaskRequest(
                client_id=client_id,
                status="free"
            )
            response = stub.RequestTask(request)
            
            if response.task:
                print(f"Client {client_id} received: {response.task}")
                
                # Simulate processing
                time.sleep(1)
                
                # Send result
                if response.task.startswith("BLOCK:"):
                    block_name = response.task[6:]
                    result = f"BLOCK_RESULT:{block_name}:processed_by_{client_id}"
                else:
                    result = f"Task {response.task} completed by {client_id}"
                
                result_request = task_pb2.TaskResult(
                    client_id=client_id,
                    result=result
                )
                result_response = stub.SendResult(result_request)
                print(f"Result sent: {result_response.success}")
            else:
                print(f"No tasks available for {client_id}")
                break
        
        # Test 5: Final queue status
        print("\n📊 Test 5: Final queue status")
        request = task_pb2.QueueRequest()
        response = stub.GetQueueStatus(request)
        print(f"Task queue length: {response.task_queue_length}")
        print(f"Block queue length: {response.block_queue_length}")
        print(f"Task queue: {response.task_queue}")
        print(f"Block queue: {response.block_queue}")
        
        print("\n✅ Block queue system test completed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
    finally:
        channel.close()

def test_multiple_clients():
    """Test with multiple clients"""
    print("\n👥 Testing Multiple Clients")
    print("=" * 50)
    
    # Create multiple client threads
    def client_worker(client_id):
        channel = grpc.insecure_channel('localhost:50051')
        stub = task_pb2_grpc.TaskServiceStub(channel)
        
        try:
            for i in range(3):  # Each client requests 3 tasks
                request = task_pb2.TaskRequest(
                    client_id=client_id,
                    status="free"
                )
                response = stub.RequestTask(request)
                
                if response.task:
                    print(f"Client {client_id} received: {response.task}")
                    
                    # Simulate processing
                    time.sleep(2)
                    
                    # Send result
                    if response.task.startswith("BLOCK:"):
                        block_name = response.task[6:]
                        result = f"BLOCK_RESULT:{block_name}:processed_by_{client_id}"
                    else:
                        result = f"Task {response.task} completed by {client_id}"
                    
                    result_request = task_pb2.TaskResult(
                        client_id=client_id,
                        result=result
                    )
                    stub.SendResult(result_request)
                    print(f"Client {client_id} completed: {response.task}")
                else:
                    print(f"Client {client_id} - no tasks available")
                    break
                    
        except Exception as e:
            print(f"Client {client_id} error: {e}")
        finally:
            channel.close()
    
    # Start multiple clients
    threads = []
    for i in range(3):
        thread = threading.Thread(target=client_worker, args=(f"multi_client_{i+1}",))
        thread.start()
        threads.append(thread)
    
    # Wait for all clients to finish
    for thread in threads:
        thread.join()
    
    print("✅ Multiple clients test completed!")

if __name__ == "__main__":
    print("🚀 Starting Block Queue System Tests")
    print("Make sure the gRPC server is running on localhost:50051")
    print("=" * 60)
    
    # Wait a bit for server to be ready
    time.sleep(2)
    
    # Run tests
    test_block_queue_system()
    test_multiple_clients()
    
    print("\n🎉 All tests completed!") 