from typing import List

class BlockManager:
    def __init__(self, active_client_manager=None, hdfs_features=None):
        # Dictionary to store: {block_name: [list_of_nodes]}
        self.block_assignments = {}
        # Dictionary to store node status: {node_name: "active/inactive"}
        self.node_status = {}
        # Track which client is leader for each block
        self.leader_assignments = {}
        # Track block processing status: {block_name: "pending/processing/done"}
        self.block_status = {}
        # Default replication factor for each block
        self.replication_factor = 3
        # Reference to active client manager
        self.active_client_manager = active_client_manager
        # Reference to HDFS features for metadata updates
        self.hdfs_features = hdfs_features
        # Block queue - blocks waiting to be assigned
        self.block_queue = []
        # Thread safety
        self.lock = None
        try:
            import threading
            self.lock = threading.Lock()
        except ImportError:
            pass
    
    def add_block_to_queue(self, block_name: str):
        """Add block to assignment queue with pending status"""
        if self.lock:
            with self.lock:
                if block_name not in self.block_queue:
                    self.block_queue.append(block_name)
                    self.block_status[block_name] = "pending"
                    print(f"📋 Added block {block_name} to assignment queue (status: pending)")
        else:
            if block_name not in self.block_queue:
                self.block_queue.append(block_name)
                self.block_status[block_name] = "pending"
                print(f"📋 Added block {block_name} to assignment queue (status: pending)")
    
    def request_block_assignment(self, client_id: str, client_status: str = "free"):
        """
        Client requests block assignment (similar to RequestTask in gRPC)
        Returns block name if assigned, empty string if no blocks available
        """
        if not self.active_client_manager:
            print(f"Cannot assign block to {client_id}: No active client manager")
            return ""
        
        if client_status != "free":
            print(f"Client {client_id} is not free (status: {client_status}), cannot assign block")
            return ""
        
        if not self.block_queue:
            print(f"No blocks in queue for client {client_id}")
            return ""
        
        # Get all active clients to check availability
        active_clients = self.active_client_manager.get_active_clients()
        if not active_clients:
            print(f"No active clients available for block assignment")
            return ""
        
        # Filter free clients
        free_clients = [cid for cid, info in active_clients.items() if info.get('status') == 'free']
        
        if not free_clients:
            print(f"No free clients available for block assignment")
            return ""
        
        # Remove block from queue
        if self.lock:
            with self.lock:
                if self.block_queue:
                    block_name = self.block_queue.pop(0)
                else:
                    return ""
        else:
            if self.block_queue:
                block_name = self.block_queue.pop(0)
            else:
                return ""
        
        # Assign block to clients - first client becomes leader
        selected_clients = self._select_clients_for_block(free_clients, active_clients)
        
        if selected_clients:
            # Update block manager
            self.add_block(block_name, selected_clients)
            
            # Set leader (first client)
            self.leader_assignments[block_name] = selected_clients[0]
            
            # Update block status to processing
            self.block_status[block_name] = "processing"
            
            # Update HDFS metadata
            if self.hdfs_features:
                self.hdfs_features.update_block_assignment(block_name, selected_clients)
                self.hdfs_features.update_block_status(block_name, "processing")
            
            print(f"👑 Block {block_name} assigned to LEADER {selected_clients[0]} and followers: {selected_clients[1:]} (status: processing)")
            return block_name
        else:
            # Put block back in queue if assignment failed
            self.add_block_to_queue(block_name)
            print(f"❌ Failed to assign block {block_name}, returned to queue (status: pending)")
            return ""
    
    def send_block_result(self, client_id: str, block_name: str, result: str):
        """
        Client sends result after processing block (similar to SendResult in gRPC)
        """
        if block_name in self.block_assignments:
            print(f"📤 Result from {client_id} for block {block_name}: {result}")
            
            # Check if this client was the leader
            if block_name in self.leader_assignments and self.leader_assignments[block_name] == client_id:
                print(f"👑 Leader {client_id} completed block {block_name}")
            
            # Update block status to done
            self.block_status[block_name] = "done"
            
            # Update HDFS metadata
            if self.hdfs_features:
                self.hdfs_features.update_block_status(block_name, "done")
            
            print(f"✅ Block {block_name} marked as DONE")
            
            # Clean up completed block
            self._cleanup_completed_block(block_name)
            
            # Update client status to free
            if self.active_client_manager:
                active_clients = self.active_client_manager.get_active_clients()
                if client_id in active_clients:
                    # Note: In real implementation, you'd update the client status here
                    print(f"🔄 Client {client_id} completed block {block_name}, status set to free")
            
            return True
        else:
            print(f"❌ Block {block_name} not found in assignments")
            return False
    
    def _cleanup_completed_block(self, block_name: str):
        """
        Clean up completed block - remove from storage and assignments
        """
        print(f"🧹 Cleaning up completed block: {block_name}")
        
        # Remove from block assignments
        if block_name in self.block_assignments:
            del self.block_assignments[block_name]
        
        # Remove from leader assignments
        if block_name in self.leader_assignments:
            del self.leader_assignments[block_name]
        
        # Remove from block status
        if block_name in self.block_status:
            del self.block_status[block_name]
        
        # Remove physical block file
        if self.hdfs_features:
            self.hdfs_features.delete_block_file(block_name)
        
        print(f"🗑️ Block {block_name} cleaned up and removed from system")
    
    def handle_leader_disconnect(self, client_id: str):
        """
        Handle leader disconnection - return blocks to queue for reassignment
        """
        blocks_to_return = []
        
        if self.lock:
            with self.lock:
                for block_name, leader in list(self.leader_assignments.items()):
                    if leader == client_id:
                        blocks_to_return.append(block_name)
                        del self.leader_assignments[block_name]
                        if block_name in self.block_assignments:
                            del self.block_assignments[block_name]
        else:
            for block_name, leader in list(self.leader_assignments.items()):
                if leader == client_id:
                    blocks_to_return.append(block_name)
                    del self.leader_assignments[block_name]
                    if block_name in self.block_assignments:
                        del self.block_assignments[block_name]
        
        # Return blocks to queue with pending status
        for block_name in blocks_to_return:
            self.add_block_to_queue(block_name)
            print(f"🔄 Block {block_name} returned to queue due to leader {client_id} disconnect (status: pending)")
        
        return blocks_to_return
    
    def get_queue_status(self):
        """Get current queue status"""
        return {
            "queue_length": len(self.block_queue),
            "queued_blocks": self.block_queue.copy(),
            "assigned_blocks": len(self.block_assignments),
            "total_blocks": len(self.block_assignments) + len(self.block_queue),
            "leader_assignments": dict(self.leader_assignments),
            "block_status": dict(self.block_status)
        }
    
    def get_block_status(self, block_name: str):
        """Get status of specific block"""
        return self.block_status.get(block_name, "unknown")
    
    def get_blocks_by_status(self, status: str):
        """Get all blocks with specific status"""
        return [block for block, block_status in self.block_status.items() if block_status == status]
    
    def get_status_summary(self):
        """Get summary of all block statuses"""
        status_counts = {}
        for status in self.block_status.values():
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            "pending": status_counts.get("pending", 0),
            "processing": status_counts.get("processing", 0),
            "done": status_counts.get("done", 0),
            "total": len(self.block_status)
        }
    
    def assign_pending_blocks(self):
        """Legacy method - now blocks are assigned only on request"""
        print("⚠️ assign_pending_blocks() is deprecated. Use request_block_assignment() instead.")
        print("Blocks are now assigned only when clients request them.")
        return []
    
    def _try_assign_block(self, block_name: str):
        """Legacy method - now blocks are assigned only on request"""
        print(f"⚠️ _try_assign_block() is deprecated. Block {block_name} should be requested by client.")
        return
    
    def _select_clients_for_block(self, free_clients: List[str], all_clients: dict) -> List[str]:
        """Select clients for block assignment - first client becomes leader"""
        selected_clients = []
        
        # First client MUST be free (will become leader)
        if free_clients:
            selected_clients.append(free_clients[0])
            free_clients = free_clients[1:]  # Remove used client
        else:
            # If no free clients, cannot assign block
            return []
        
        # Fill remaining slots with ANY available clients (can be busy) - these become followers
        remaining_slots = self.replication_factor - 1
        available_clients = [cid for cid in all_clients.keys() if cid not in selected_clients]
        
        # Select remaining clients (can be busy) - these become followers
        for i in range(min(remaining_slots, len(available_clients))):
            selected_clients.append(available_clients[i])
        
        return selected_clients
    
    def check_and_assign_blocks(self):
        """Legacy method - now blocks are assigned only on request"""
        print("⚠️ check_and_assign_blocks() is deprecated. Use request_block_assignment() instead.")
        return
    
    def get_free_nodes(self) -> List[str]:
        """Get list of currently free nodes"""
        if not self.active_client_manager:
            return []
        
        active_clients = self.active_client_manager.get_active_clients()
        free_nodes = [cid for cid, info in active_clients.items() if info.get('status') == 'free']
        return free_nodes
    
    def get_assignment_status(self) -> dict:
        """Get detailed assignment status"""
        if not self.hdfs_features:
            return {"error": "HDFS features not available"}
        
        unassigned_blocks = self.hdfs_features.get_unassigned_blocks()
        free_nodes = self.get_free_nodes()
        queue_status = self.get_queue_status()
        status_summary = self.get_status_summary()
        
        return {
            "unassigned_blocks": unassigned_blocks,
            "free_nodes": free_nodes,
            "total_blocks": len(self.block_assignments),
            "total_nodes": len(self.node_status),
            "can_assign": len(self.block_queue) > 0 and len(free_nodes) > 0,
            "queue_status": queue_status,
            "leader_assignments": dict(self.leader_assignments),
            "status_summary": status_summary
        }
    
    def assign_block_to_active_clients(self, block_name, active_clients_info):
        """
        Legacy method - kept for backward compatibility
        """
        if not active_clients_info:
            print(f"No active clients available for block {block_name}")
            return []
        
        # Filter active clients and separate free vs busy
        free_clients = []
        busy_clients = []
        
        for client_id, info in active_clients_info.items():
            if info['status'] == 'free':
                free_clients.append(client_id)
            else:
                busy_clients.append(client_id)
        
        # Ensure we have at least one free client for first assignment (leader)
        if not free_clients:
            print(f"No free clients available for block {block_name}")
            return []
        
        # Select clients for assignment
        selected_clients = []
        
        # First client must be free (becomes leader)
        selected_clients.append(free_clients[0])
        free_clients = free_clients[1:]  # Remove used client
        
        # Fill remaining slots with any available clients (followers)
        remaining_slots = self.replication_factor - 1
        available_clients = free_clients + busy_clients
        
        # Select remaining clients
        for i in range(min(remaining_slots, len(available_clients))):
            selected_clients.append(available_clients[i])
        
        # Assign block to selected clients
        self.add_block(block_name, selected_clients)
        
        # Set leader
        self.leader_assignments[block_name] = selected_clients[0]
        
        # Update block status to processing
        self.block_status[block_name] = "processing"
        
        print(f"Block {block_name} assigned to: {selected_clients}")
        print(f"  - Leader (free): {selected_clients[0]}")
        print(f"  - Followers: {selected_clients[1:]}")
        print(f"  - Status: processing")
        
        return selected_clients
    
    def add_block(self, block_name, assigned_nodes):
        """
        Add block and list of assigned nodes
        Example: add_block("file1_block_0", ["client1", "client2", "client3"])
        """
        self.block_assignments[block_name] = assigned_nodes
        # Update node status
        for node in assigned_nodes:
            if node not in self.node_status:
                self.node_status[node] = "active"
    
    def get_nodes_for_block(self, block_name):
        """Get list of nodes assigned to a block"""
        return self.block_assignments.get(block_name, [])
    
    def get_blocks_for_node(self, node_name):
        """Get list of blocks assigned to a node"""
        blocks = []
        for block, nodes in self.block_assignments.items():
            if node_name in nodes:
                blocks.append(block)
        return blocks
    
    def get_leader_for_block(self, block_name):
        """Get the leader client for a specific block"""
        return self.leader_assignments.get(block_name, None)
    
    def get_leader_blocks(self, client_id):
        """Get all blocks where this client is the leader"""
        return [block for block, leader in self.leader_assignments.items() if leader == client_id]
    
    def assign_block_to_nodes(self, block_name, available_nodes):
        """
        Automatically assign block to available nodes
        """
        # Randomly select nodes (can be changed to different algorithm)
        import random
        selected_nodes = random.sample(available_nodes, 
                                     min(self.replication_factor, len(available_nodes)))
        self.add_block(block_name, selected_nodes)
        return selected_nodes
    
    def remove_node(self, node_name):
        """Remove node from system"""
        self.node_status[node_name] = "inactive"
        # Handle leader disconnection
        self.handle_leader_disconnect(node_name)
    
    def get_system_info(self):
        """Get system overview information"""
        status_summary = self.get_status_summary()
        return {
            "total_blocks": len(self.block_assignments),
            "total_nodes": len(self.node_status),
            "active_nodes": len([n for n, s in self.node_status.items() if s == "active"]),
            "block_assignments": self.block_assignments,
            "node_status": self.node_status,
            "queue_length": len(self.block_queue),
            "leader_assignments": dict(self.leader_assignments),
            "block_status": dict(self.block_status),
            "status_summary": status_summary
        }
    
    def print_assignments(self):
        """Print block distribution information"""
        print("=== BLOCK ASSIGNMENTS ===")
        for block, nodes in self.block_assignments.items():
            leader = self.leader_assignments.get(block, "Unknown")
            followers = [n for n in nodes if n != leader]
            status = self.block_status.get(block, "unknown")
            print(f"{block}:")
            print(f"  👑 Leader: {leader}")
            print(f"  👥 Followers: {', '.join(followers)}")
            print(f"  📊 Status: {status}")
        print("========================")
        print("=== BLOCK QUEUE ===")
        for i, block in enumerate(self.block_queue):
            status = self.block_status.get(block, "unknown")
            print(f"{i+1}. {block} (status: {status})")
        print("==================")
        print("=== STATUS SUMMARY ===")
        summary = self.get_status_summary()
        print(f"Pending: {summary['pending']}")
        print(f"Processing: {summary['processing']}")
        print(f"Done: {summary['done']}")
        print(f"Total: {summary['total']}")
        print("=====================") 