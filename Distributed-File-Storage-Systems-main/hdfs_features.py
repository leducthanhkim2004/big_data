import os
import json
import hashlib
import shutil
from datetime import datetime
from typing import Dict, List, Optional
import block_manager

class HDFSFeatures:
    """HDFS-like features for distributed file storage"""
    
    def __init__(self, base_dir="uploads", active_client_manager=None):
        self.base_dir = base_dir
        self.metadata_file = os.path.join(base_dir, "metadata.json")
        self.blocks_dir = os.path.join(base_dir, "blocks")
        self.metadata_dir = os.path.join(base_dir, "metadata")
        self.replication_factor = 3
        self.block_size = 64 * 1024 * 1024  # 64MB blocks
        self.active_client_manager = active_client_manager
        
        # Create directories if they don't exist
        os.makedirs(self.blocks_dir, exist_ok=True)
        os.makedirs(self.metadata_dir, exist_ok=True)
        
        self.load_metadata()
    
    def load_metadata(self):
        """Load metadata from file"""
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r') as f:
                self.metadata = json.load(f)
        else:
            self.metadata = {
                'files': {},
                'blocks': {},
                'servers': [],
                'created_at': datetime.now().isoformat()
            }
            self.save_metadata()
    
    def save_metadata(self):
        """Save metadata to file"""
        with open(self.metadata_file, 'w') as f:
            json.dump(self.metadata, f, indent=2)
    
    def calculate_file_hash(self, filepath: str) -> str:
        """Calculate SHA256 hash of file"""
        hash_sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    def split_file_into_blocks(self, file_path: str) -> List[str]:
        """
        Split file into blocks and return list of block paths
        """
        if not os.path.exists(file_path):
            print(f"File {file_path} not found")
            return []
        
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        # Create blocks
        blocks = []
        block_number = 0
        
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(self.block_size)
                if not chunk:
                    break
                
                block_name = f"{file_name}_block_{block_number}"
                block_path = os.path.join(self.blocks_dir, block_name)
                
                # Save block to disk
                with open(block_path, 'wb') as block_file:
                    block_file.write(chunk)
                
                blocks.append(block_path)
                
                # Store block metadata with pending status
                self.metadata['blocks'][block_name] = {
                    'assigned_clients': [],  # Empty - will be filled by BlockManager
                    'size': len(chunk),
                    'file': file_name,
                    'created_at': datetime.now().isoformat(),
                    'status': 'pending'  # Initial status
                }
                
                block_number += 1
        
        # Save updated metadata
        self.save_metadata()
        
        print(f"✅ File {file_name} split into {len(blocks)} blocks")
        print(f"📁 Blocks saved to {self.blocks_dir}")
        print(f"📋 Block status: pending")
        
        return blocks
    
    def split_file(self, file_path, chunk_size=1024*1024):
        """
        Split file into blocks and store metadata
        """
        if not os.path.exists(file_path):
            print(f"File {file_path} not found")
            return False
        
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        # Create blocks
        blocks = []
        block_number = 0
        
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                
                block_name = f"{file_name}_block_{block_number}"
                block_path = os.path.join(self.blocks_dir, block_name)
                
                # Save block to disk
                with open(block_path, 'wb') as block_file:
                    block_file.write(chunk)
                
                blocks.append({
                    'name': block_name,
                    'size': len(chunk),
                    'path': block_path,
                    'assigned_clients': []  # Initially empty - will be assigned when clients request
                })
                
                block_number += 1
        
        # Store metadata
        metadata = {
            'file_name': file_name,
            'file_size': file_size,
            'chunk_size': chunk_size,
            'blocks': blocks,
            'created_at': datetime.now().isoformat()
        }
        
        metadata_path = os.path.join(self.metadata_dir, f"{file_name}_metadata.json")
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"✅ File {file_name} split into {len(blocks)} blocks")
        print(f"📁 Metadata saved to {metadata_path}")
        
        # Add blocks to assignment queue instead of automatic assignment
        if hasattr(self, 'block_manager') and self.block_manager:
            for block in blocks:
                self.block_manager.add_block_to_queue(block['name'])
            print(f"📋 Added {len(blocks)} blocks to assignment queue")
        
        return True
    
    def _store_block_metadata(self, blocks: List[str], filename: str):
        """Store block metadata without node assignment"""
        for block_path in blocks:
            block_name = os.path.basename(block_path)
            
            # Store block metadata with empty assigned_clients
            self.metadata['blocks'][block_name] = {
                'assigned_clients': [],  # Empty - will be filled by BlockManager
                'size': os.path.getsize(block_path),
                'file': filename,
                'created_at': datetime.now().isoformat(),
                'status': 'pending'  # Initial status
            }
        
        # Save updated metadata
        self.save_metadata()
    
    def get_unassigned_blocks(self) -> List[str]:
        """Get list of blocks that haven't been assigned to any nodes"""
        unassigned_blocks = []
        for block_name, block_info in self.metadata['blocks'].items():
            if not block_info.get('assigned_clients') or len(block_info['assigned_clients']) == 0:
                unassigned_blocks.append(block_name)
        return unassigned_blocks
    
    def update_block_assignment(self, block_name: str, assigned_clients: List[str]):
        """Update block assignment in metadata"""
        if block_name in self.metadata['blocks']:
            self.metadata['blocks'][block_name]['assigned_clients'] = assigned_clients
            self.metadata['blocks'][block_name]['status'] = 'processing'
            self.save_metadata()
            print(f"Updated block {block_name} assignment: {assigned_clients} (status: processing)")
        else:
            print(f"Block {block_name} not found in metadata")
    
    def update_block_status(self, block_name: str, status: str):
        """Update block status in metadata"""
        if block_name in self.metadata['blocks']:
            self.metadata['blocks'][block_name]['status'] = status
            self.save_metadata()
            print(f"Updated block {block_name} status: {status}")
        else:
            print(f"Block {block_name} not found in metadata")
    
    def delete_block_file(self, block_name: str):
        """Delete physical block file from storage"""
        block_path = os.path.join(self.blocks_dir, block_name)
        if os.path.exists(block_path):
            try:
                os.remove(block_path)
                print(f"🗑️ Deleted physical block file: {block_path}")
            except Exception as e:
                print(f"❌ Error deleting block file {block_path}: {e}")
        else:
            print(f"⚠️ Block file not found: {block_path}")
    
    def cleanup_completed_block(self, block_name: str):
        """Clean up completed block from metadata and storage"""
        print(f"🧹 Cleaning up completed block: {block_name}")
        
        # Remove from metadata
        if block_name in self.metadata['blocks']:
            del self.metadata['blocks'][block_name]
            self.save_metadata()
            print(f"🗑️ Removed {block_name} from metadata")
        
        # Delete physical file
        self.delete_block_file(block_name)
        
        print(f"✅ Block {block_name} cleanup completed")
    
    def get_blocks_by_status(self, status: str) -> List[str]:
        """Get all blocks with specific status"""
        blocks = []
        for block_name, block_info in self.metadata['blocks'].items():
            if block_info.get('status') == status:
                blocks.append(block_name)
        return blocks
    
    def get_status_summary(self) -> Dict:
        """Get summary of all block statuses"""
        status_counts = {}
        for block_info in self.metadata['blocks'].values():
            status = block_info.get('status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            "pending": status_counts.get("pending", 0),
            "processing": status_counts.get("processing", 0),
            "done": status_counts.get("done", 0),
            "total": len(self.metadata['blocks'])
        }
    
    def replicate_blocks(self, blocks: List[str]) -> Dict[str, List[str]]:
        """Replicate blocks across servers"""
        replication_map = {}
        
        for block in blocks:
            # For now, just replicate locally (in real HDFS, this would be across nodes)
            replicas = [block]
            for i in range(1, self.replication_factor):
                replica_path = f"{block}_replica_{i}"
                shutil.copy2(block, replica_path)
                replicas.append(replica_path)
            
            replication_map[block] = replicas
        
        return replication_map
    
    def store_file_metadata(self, original_filename: str, filepath: str, blocks: List[str]):
        """Store file metadata"""
        file_hash = self.calculate_file_hash(filepath)
        
        self.metadata['files'][original_filename] = {
            'path': filepath,
            'hash': file_hash,
            'size': os.path.getsize(filepath),
            'blocks': blocks,
            'created_at': datetime.now().isoformat(),
            'replication_factor': self.replication_factor
        }
        
        # Store block metadata with pending status
        for block in blocks:
            block_name = os.path.basename(block)
            self.metadata['blocks'][block_name] = {
                'size': os.path.getsize(block),
                'file': original_filename,
                'created_at': datetime.now().isoformat(),
                'status': 'pending'  # Initial status
            }
        
        self.save_metadata()
    
    def get_file_info(self, filename: str) -> Optional[Dict]:
        """Get file information"""
        return self.metadata['files'].get(filename)
    
    def list_files(self) -> List[str]:
        """List all files in the system"""
        return list(self.metadata['files'].keys())
    
    def delete_file(self, filename: str) -> bool:
        """Delete file and its blocks"""
        if filename not in self.metadata['files']:
            return False
        
        file_info = self.metadata['files'][filename]
        
        # Delete original file
        if os.path.exists(file_info['path']):
            os.remove(file_info['path'])
        
        # Delete blocks
        for block in file_info['blocks']:
            if os.path.exists(block):
                os.remove(block)
        
        # Remove from metadata
        del self.metadata['files'][filename]
        self.save_metadata()
        
        return True
    
    def get_system_stats(self) -> Dict:
        """Get system statistics"""
        total_files = len(self.metadata['files'])
        total_blocks = len(self.metadata['blocks'])
        total_size = sum(file_info['size'] for file_info in self.metadata['files'].values())
        status_summary = self.get_status_summary()
        
        return {
            'total_files': total_files,
            'total_blocks': total_blocks,
            'total_size_bytes': total_size,
            'total_size_mb': total_size / (1024 * 1024),
            'replication_factor': self.replication_factor,
            'block_size_mb': self.block_size / (1024 * 1024),
            'status_summary': status_summary
        }

# Usage example
if __name__ == "__main__":
    hdfs = HDFSFeatures()
    print("HDFS-like features initialized")
    print(f"System stats: {hdfs.get_system_stats()}") 