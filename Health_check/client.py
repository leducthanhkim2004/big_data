import grpc
import time
import task_pb2
import task_pb2_grpc

def run(client_id):
    channel = grpc.insecure_channel('localhost:50051')
    stub = task_pb2_grpc.TaskServiceStub(channel)
    status = "free"
    while True:
        # Request a task
        response = stub.RequestTask(task_pb2.ClientStatus(client_id=client_id, status=status))
        if response.task:
            print(f"{client_id} received task: {response.task}")
            # Simulate doing the task
            time.sleep(2)
            result = f"{client_id} completed {response.task}"
            stub.SendResult(task_pb2.TaskResult(client_id=client_id, result=result))
            print(f"{client_id} sent result: {result}")
            status = "free"
        else:
            print(f"{client_id} has no task, waiting...")
            time.sleep(2)

if __name__ == '__main__':
    import sys
    client_id = sys.argv[1] if len(sys.argv) > 1 else "clientA"
    run(client_id)
    