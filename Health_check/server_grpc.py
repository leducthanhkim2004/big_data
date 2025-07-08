import grpc
from concurrent import futures
import task_pb2
import task_pb2_grpc

class TaskServiceServicer(task_pb2_grpc.TaskServiceServicer):
    def __init__(self):
        self.task_queue = []
        self.clients = {}
        # Add some example tasks
        self.task_queue.append('Task 1')
        self.task_queue.append('Task 2')
        self.task_queue.append('Task 3')

    def RequestTask(self, request, context):
        client_id = request.client_id
        status = request.status
        self.clients[client_id] = status
        print(f"Status from {client_id}: {status}")
        if status == "free" and self.task_queue:
            task = self.task_queue.pop(0)
            print(f"Assigned task to {client_id}: {task}")
            return task_pb2.TaskAssignment(task=task)
        else:
            return task_pb2.TaskAssignment(task="")

    def SendResult(self, request, context):
        print(f"Result from {request.client_id}: {request.result}")
        self.clients[request.client_id] = "free"
        return task_pb2.Ack(success=True)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    task_pb2_grpc.add_TaskServiceServicer_to_server(TaskServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("gRPC server running on port 50051")
    server.wait_for_termination()

if __name__ == '__main__':
    serve() 