import grpc
from concurrent import futures
import task_pb2
import task_pb2_grpc
import threading
import time
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path to import HDFS modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from Distributed_File_Storage_Systems_main.block_manager import BlockManager
    from Distributed_File_Storage_Systems_main.hdfs_features import HDFSFeatures
    print("✅ Successfully imported HDFS modules")
except ImportError as e:
    print(f"⚠️ Warning: Could not import HDFS modules: {e}")
    # Create dummy classes for fallback
    class BlockManager:
        def __init__(self):
            self.block_queue = []
            self.block_assignments = {}
            self.leader_assignments = {}
            self.block_status = {}
        
        def add_block_to_queue(self, block_name):
            if block_name not in self.block_queue:
                self.block_queue.append(block_name)
                self.block_status[block_name] = "pending"
                print(f"📋 Added block {block_name} to queue (status: pending)")
        
        def request_block_assignment(self, client_id, client_status="free"):
            if client_status != "free" or not self.block_queue:
                return ""
            block_name = self.block_queue.pop(0)
            self.block_assignments[block_name] = [client_id]
            self.leader_assignments[block_name] = client_id
            self.block_status[block_name] = "processing"
            print(f"✅ Assigned block {block_name} to leader {client_id} (status: processing)")
            return block_name
        
        def send_block_result(self, client_id, block_name, result):
            if block_name in self.block_assignments:
                print(f"📤 Result from {client_id} for block {block_name}: {result}")
                self.block_status[block_name] = "done"
                print(f"✅ Block {block_name} marked as DONE")
                # Cleanup
                if block_name in self.block_assignments:
                    del self.block_assignments[block_name]
                if block_name in self.leader_assignments:
                    del self.leader_assignments[block_name]
                if block_name in self.block_status:
                    del self.block_status[block_name]
                return True
            return False
        
        def handle_leader_disconnect(self, client_id):
            blocks_to_return = []
            for block_name, leader in list(self.leader_assignments.items()):
                if leader == client_id:
                    blocks_to_return.append(block_name)
                    del self.leader_assignments[block_name]
                    if block_name in self.block_assignments:
                        del self.block_assignments[block_name]
            
            for block_name in blocks_to_return:
                self.add_block_to_queue(block_name)
                print(f"🔄 Block {block_name} returned to queue due to leader {client_id} disconnect (status: pending)")
            
            return blocks_to_return
        
        def get_status_summary(self):
            status_counts = {}
            for status in self.block_status.values():
                status_counts[status] = status_counts.get(status, 0) + 1
            
            return {
                "pending": status_counts.get("pending", 0),
                "processing": status_counts.get("processing", 0),
                "done": status_counts.get("done", 0),
                "total": len(self.block_status)
            }
    
    class HDFSFeatures:
        def __init__(self):
            pass

class TaskServiceServicer(task_pb2_grpc.TaskServiceServicer):
    def __init__(self):
        self.task_queue = []
        self.clients = {}
        self.active_connections = {}  # Track active client connections
        self.heartbeat_timeout = 30  # seconds
        self.lock = threading.Lock()  # Thread safety
        
        # Initialize Block Manager for block assignment
        self.block_manager = BlockManager()
        self.hdfs_features = HDFSFeatures()
        
        # Add some example tasks
        self.task_queue.append('Task 1')
        self.task_queue.append('Task 2')
        self.task_queue.append('Task 3')
        
        # Add some example blocks to queue
        self.block_manager.add_block_to_queue('file1_block_0')
        self.block_manager.add_block_to_queue('file1_block_1')
        self.block_manager.add_block_to_queue('file2_block_0')
        
        # Start cleanup thread
        self.cleanup_thread = threading.Thread(target=self._cleanup_inactive_clients, daemon=True)
        self.cleanup_thread.start()
        
        print("🚀 gRPC Server initialized with Block Manager")
        print(f"📋 Initial task queue: {self.task_queue}")
        print(f"📦 Initial block queue: {self.block_manager.block_queue}")
        print("👑 Leader-Follower system enabled")
        print("📊 Status management: pending → processing → done → cleanup")

    def RequestTask(self, request, context):
        client_id = request.client_id
        status = request.status
        
        with self.lock:
            self.clients[client_id] = status
            # Mark client as active
            self.active_connections[client_id] = {
                'last_seen': datetime.now(),
                'status': status,
                'context': context
            }
        
        print(f"📡 RequestTask from {client_id}: {status}")
        print(f"👥 Active clients: {list(self.active_connections.keys())}")
        
        # First try to assign a block (higher priority)
        if status == "free":
            assigned_block = self.block_manager.request_block_assignment(client_id, status)
            if assigned_block:
                print(f"👑 Assigned block to LEADER {client_id}: {assigned_block} (status: processing)")
                return task_pb2.TaskAssignment(task=f"BLOCK:{assigned_block}")
        
        # If no block available, try to assign a regular task
        if status == "free" and self.task_queue:
            task = self.task_queue.pop(0)
            print(f"📋 Assigned task to {client_id}: {task}")
            return task_pb2.TaskAssignment(task=task)
        else:
            return task_pb2.TaskAssignment(task="")

    def SendResult(self, request, context):
        client_id = request.client_id
        result = request.result
        
        with self.lock:
            print(f"📤 Result from {client_id}: {result}")
            self.clients[client_id] = "free"
            # Update last seen time
            if client_id in self.active_connections:
                self.active_connections[client_id]['last_seen'] = datetime.now()
                self.active_connections[client_id]['status'] = "free"
        
        # Check if this is a block result
        if result.startswith("BLOCK_RESULT:"):
            # Format: "BLOCK_RESULT:block_name:processing_result"
            parts = result.split(":", 2)
            if len(parts) == 3:
                block_name = parts[1]
                processing_result = parts[2]
                success = self.block_manager.send_block_result(client_id, block_name, processing_result)
                if success:
                    print(f"✅ Block {block_name} completed and cleaned up")
                else:
                    print(f"❌ Failed to process block result for {block_name}")
        
        return task_pb2.Ack(success=True)

    def AddBlockToQueue(self, request, context):
        """New method to add blocks to the queue"""
        block_name = request.block_name
        with self.lock:
            self.block_manager.add_block_to_queue(block_name)
            print(f"📋 Added block {block_name} to queue via gRPC (status: pending)")
        return task_pb2.Ack(success=True)

    def GetQueueStatus(self, request, context):
        """Get current queue status"""
        with self.lock:
            status_summary = self.block_manager.get_status_summary()
            return task_pb2.QueueStatus(
                task_queue_length=len(self.task_queue),
                block_queue_length=len(self.block_manager.block_queue),
                active_clients=len(self.active_connections),
                task_queue=list(self.task_queue),
                block_queue=list(self.block_manager.block_queue)
            )

    def GetBlockStatus(self, request, context):
        """Get detailed block status information"""
        with self.lock:
            status_summary = self.block_manager.get_status_summary()
            return task_pb2.BlockStatus(
                pending_blocks=status_summary.get("pending", 0),
                processing_blocks=status_summary.get("processing", 0),
                completed_blocks=status_summary.get("done", 0),
                total_blocks=status_summary.get("total", 0),
                block_status=dict(self.block_manager.block_status),
                leader_assignments=dict(self.block_manager.leader_assignments)
            )

    def Heartbeat(self, request_iterator, context):
        """Streaming heartbeat to track active connections"""
        try:
            for heartbeat_msg in request_iterator:
                client_id = heartbeat_msg.client_id
                status = heartbeat_msg.status
                timestamp = heartbeat_msg.timestamp
                
                with self.lock:
                    # Update client status and last seen time
                    self.clients[client_id] = status
                    self.active_connections[client_id] = {
                        'last_seen': datetime.now(),
                        'status': status,
                        'context': context
                    }
                
                print(f"💓 Heartbeat from {client_id}: {status}")
                
                # Send response back to client
                response = task_pb2.HeartbeatResponse(
                    alive=True,
                    message=f"Server received heartbeat from {client_id}"
                )
                yield response
                
        except grpc.RpcError as e:
            # Client disconnected
            client_id = self._get_client_id_from_context(context)
            if client_id:
                self._handle_client_disconnect(client_id)
                print(f"❌ Client {client_id} disconnected due to RPC error")

    def _handle_client_disconnect(self, client_id):
        """Handle client disconnection with leader-follower logic"""
        with self.lock:
            # Check if this client was a leader for any blocks
            blocks_returned = self.block_manager.handle_leader_disconnect(client_id)
            
            if blocks_returned:
                print(f"👑 Leader {client_id} disconnected, returned {len(blocks_returned)} blocks to queue")
                print(f"🔄 Returned blocks: {blocks_returned}")
            
            # Remove client from active connections
            self._remove_client(client_id)

    def _get_client_id_from_context(self, context):
        """Extract client ID from context metadata"""
        try:
            # You can add client_id in metadata when client connects
            metadata = context.invocation_metadata()
            for key, value in metadata:
                if key == 'client_id':
                    return value
        except:
            pass
        return None

    def _remove_client(self, client_id):
        """Remove client from active connections"""
        with self.lock:
            if client_id in self.active_connections:
                del self.active_connections[client_id]
                print(f"🗑️ Removed client {client_id} from active connections")
            
            if client_id in self.clients:
                del self.clients[client_id]
                print(f"🗑️ Removed client {client_id} from clients list")

    def _cleanup_inactive_clients(self):
        """Background thread to cleanup inactive clients"""
        while True:
            time.sleep(10)  # Check every 10 seconds
            current_time = datetime.now()
            
            with self.lock:
                inactive_clients = []
                for client_id, info in self.active_connections.items():
                    time_diff = (current_time - info['last_seen']).total_seconds()
                    if time_diff > self.heartbeat_timeout:
                        inactive_clients.append(client_id)
                
                # Remove inactive clients
                for client_id in inactive_clients:
                    self._handle_client_disconnect(client_id)
                
                if inactive_clients:
                    print(f"🧹 Cleaned up inactive clients: {inactive_clients}")

    def get_active_clients(self):
        """Get list of currently active clients"""
        with self.lock:
            return {
                client_id: {
                    'status': info['status'],
                    'last_seen': info['last_seen'].isoformat(),
                    'connected_for': (datetime.now() - info['last_seen']).total_seconds()
                }
                for client_id, info in self.active_connections.items()
            }

    def get_connection_stats(self):
        """Get connection statistics"""
        with self.lock:
            status_summary = self.block_manager.get_status_summary()
            return {
                'total_active_clients': len(self.active_connections),
                'total_registered_clients': len(self.clients),
                'active_clients': list(self.active_connections.keys()),
                'client_statuses': dict(self.clients),
                'task_queue_length': len(self.task_queue),
                'block_queue_length': len(self.block_manager.block_queue),
                'assigned_blocks': len(self.block_manager.block_assignments),
                'leader_assignments': dict(self.block_manager.leader_assignments),
                'block_status_summary': status_summary
            }

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    task_pb2_grpc.add_TaskServiceServicer_to_server(TaskServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("🚀 gRPC server running on port 50051")
    print("📦 Server now manages block assignments and task distribution")
    print("👑 Leader-Follower system: First client (free) becomes leader")
    print("📊 Status management: pending → processing → done → cleanup")
    print("📋 Available methods:")
    print("   - RequestTask: Clients request tasks/blocks")
    print("   - SendResult: Clients send processing results")
    print("   - AddBlockToQueue: Add blocks to assignment queue")
    print("   - GetQueueStatus: Get current queue status")
    print("   - GetBlockStatus: Get detailed block status")
    print("   - Heartbeat: Track client connections")
    server.wait_for_termination()

if __name__ == '__main__':
    serve() 