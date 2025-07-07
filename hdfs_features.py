import os
import json
import hashlib
import shutil
from datetime import datetime
from typing import Dict, List, Optional

class HDFSFeatures:
    """HDFS-like features for distributed file storage"""
    
    def __init__(self, base_dir="uploads"):
        self.base_dir = base_dir
        self.metadata_file = os.path.join(base_dir, "metadata.json")
        self.replication_factor = 3
        self.block_size = 64 * 1024 * 1024  # 64MB blocks
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
    
    def split_file_into_blocks(self, filepath: str) -> List[str]:
        """Split file into blocks"""
        blocks = []
        filename = os.path.basename(filepath)
        
        with open(filepath, 'rb') as f:
            block_num = 0
            while True:
                chunk = f.read(self.block_size)
                if not chunk:
                    break
                
                block_filename = f"{filename}_block_{block_num}"
                block_path = os.path.join(self.base_dir, "blocks", block_filename)
                
                # Ensure blocks directory exists
                os.makedirs(os.path.dirname(block_path), exist_ok=True)
                
                with open(block_path, 'wb') as block_file:
                    block_file.write(chunk)
                
                blocks.append(block_path)
                block_num += 1
        
        return blocks
    
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
        
        # Store block metadata
        for block in blocks:
            self.metadata['blocks'][block] = {
                'size': os.path.getsize(block),
                'file': original_filename,
                'created_at': datetime.now().isoformat()
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
        
        return {
            'total_files': total_files,
            'total_blocks': total_blocks,
            'total_size_bytes': total_size,
            'total_size_mb': total_size / (1024 * 1024),
            'replication_factor': self.replication_factor,
            'block_size_mb': self.block_size / (1024 * 1024)
        }

# Usage example
if __name__ == "__main__":
    hdfs = HDFSFeatures()
    print("HDFS-like features initialized")
    print(f"System stats: {hdfs.get_system_stats()}") 