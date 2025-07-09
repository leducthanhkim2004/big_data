# Block Status Management System

## Overview

Hệ thống quản lý status cho blocks với 3 trạng thái chính: `pending`, `processing`, `done` và tự động cleanup khi hoàn thành. Hệ thống này đảm bảo tracking chính xác trạng thái của từng block và tự động dọn dẹp sau khi xử lý xong.

## Block Status Flow

```
📋 PENDING → 🔧 PROCESSING → ✅ DONE → 🗑️ CLEANUP
```

### Status Descriptions

| Status | Description | Trigger |
|--------|-------------|---------|
| **pending** | Block đang chờ được assign cho client | Block được thêm vào queue |
| **processing** | Block đang được xử lý bởi client | Client request và được assign |
| **done** | Block đã được xử lý xong | Client gửi result thành công |
| **cleanup** | Block được xóa khỏi hệ thống | Tự động sau khi done |

## Architecture

```
┌─────────────────┐    gRPC     ┌─────────────────┐
│   Client        │ ──────────► │  gRPC Server    │
│   (free/busy)   │             │                 │
└─────────────────┘             │ ┌─────────────┐ │
                                │ │Block Manager│ │
                                │ │             │ │
                                │ │ - Status    │ │
                                │ │   Tracking  │ │
                                │ │ - Cleanup   │ │
                                │ │ - Metadata  │ │
                                │ └─────────────┘ │
                                └─────────────────┘
```

## Key Features

### 1. Status Tracking
- **Real-time tracking**: Theo dõi status của từng block
- **Status transitions**: Tự động chuyển đổi status
- **Metadata persistence**: Lưu status vào metadata file

### 2. Automatic Cleanup
- **Physical cleanup**: Xóa block file khỏi storage
- **Metadata cleanup**: Xóa thông tin khỏi metadata
- **Assignment cleanup**: Xóa khỏi block assignments

### 3. Status Monitoring
- **Status summary**: Tổng hợp số lượng blocks theo status
- **Real-time updates**: Cập nhật status real-time
- **gRPC monitoring**: API để monitor status

## API Methods

### RequestTask (pending → processing)
```protobuf
rpc RequestTask (TaskRequest) returns (TaskAssignment)
```
- Client request block với status "free"
- Server gán block và set status thành "processing"
- Trả về: `BLOCK:block_name`

### SendResult (processing → done → cleanup)
```protobuf
rpc SendResult (TaskResult) returns (Ack)
```
- Client gửi kết quả xử lý
- Server set status thành "done"
- Tự động cleanup block

### GetBlockStatus (Monitoring)
```protobuf
rpc GetBlockStatus (BlockStatusRequest) returns (BlockStatus)
```
- Lấy thông tin chi tiết về block status
- Trả về số lượng blocks theo từng status
- Trả về mapping block_name → status

## Block Manager Methods

### Status Management
```python
def get_block_status(self, block_name: str):
    """Get status of specific block"""
    return self.block_status.get(block_name, "unknown")

def get_blocks_by_status(self, status: str):
    """Get all blocks with specific status"""
    return [block for block, block_status in self.block_status.items() if block_status == status]

def get_status_summary(self):
    """Get summary of all block statuses"""
    return {
        "pending": count,
        "processing": count,
        "done": count,
        "total": total
    }
```

### Cleanup Methods
```python
def _cleanup_completed_block(self, block_name: str):
    """Clean up completed block - remove from storage and assignments"""
    # Remove from block assignments
    # Remove from leader assignments
    # Remove from block status
    # Remove physical block file
```

## HDFS Features Integration

### Status Updates
```python
def update_block_status(self, block_name: str, status: str):
    """Update block status in metadata"""
    self.metadata['blocks'][block_name]['status'] = status
    self.save_metadata()

def delete_block_file(self, block_name: str):
    """Delete physical block file from storage"""
    block_path = os.path.join(self.blocks_dir, block_name)
    os.remove(block_path)
```

### Metadata Structure
```json
{
  "blocks": {
    "file1_block_0": {
      "assigned_clients": ["node1", "node2", "node3"],
      "size": 67108864,
      "file": "file1.txt",
      "created_at": "2024-01-01T12:00:00",
      "status": "processing"
    }
  }
}
```

## Workflow Examples

### Example 1: Normal Processing
```
1. Block added to queue → status: "pending"
2. Client requests block → status: "processing"
3. Client processes block → status: "processing"
4. Client sends result → status: "done"
5. System cleanup → block removed
```

### Example 2: Leader Disconnect
```
1. Block assigned → status: "processing"
2. Leader disconnects → status: "pending" (returned to queue)
3. New client requests → status: "processing"
4. Client completes → status: "done"
5. System cleanup → block removed
```

### Example 3: Multiple Blocks
```
1. Block1: pending → processing → done → cleanup
2. Block2: pending → processing → done → cleanup
3. Block3: pending → processing → done → cleanup
```

## Usage Examples

### 1. Start Server with Status Management
```bash
cd Health_check
python server_grpc.py
```

### 2. Test Status Management
```bash
python test_status_management.py
```

### 3. Monitor Status via gRPC
```python
import grpc
import task_pb2
import task_pb2_grpc

channel = grpc.insecure_channel('localhost:50051')
stub = task_pb2_grpc.TaskServiceStub(channel)

# Get block status
request = task_pb2.BlockStatusRequest()
response = stub.GetBlockStatus(request)
print(f"Pending: {response.pending_blocks}")
print(f"Processing: {response.processing_blocks}")
print(f"Completed: {response.completed_blocks}")
```

## Configuration

### Server Settings
- **Status tracking**: Enabled by default
- **Auto cleanup**: Enabled by default
- **Metadata persistence**: Automatic save on status change

### Block Settings
- **Initial status**: "pending"
- **Processing status**: "processing"
- **Completion status**: "done"
- **Cleanup delay**: Immediate after completion

## Monitoring

### Status Summary
```python
# Get status summary
summary = block_manager.get_status_summary()
print(f"Pending: {summary['pending']}")
print(f"Processing: {summary['processing']}")
print(f"Done: {summary['done']}")
print(f"Total: {summary['total']}")
```

### Real-time Monitoring
```python
# Monitor specific block
status = block_manager.get_block_status("file1_block_0")
print(f"Block status: {status}")

# Get blocks by status
processing_blocks = block_manager.get_blocks_by_status("processing")
print(f"Processing blocks: {processing_blocks}")
```

### gRPC Monitoring
```python
# Get detailed status via gRPC
request = task_pb2.BlockStatusRequest()
response = stub.GetBlockStatus(request)
print(f"Block status mapping: {response.block_status}")
```

## Benefits

1. **Clear Status Tracking**: Biết chính xác trạng thái của từng block
2. **Automatic Cleanup**: Tự động dọn dẹp sau khi hoàn thành
3. **Resource Management**: Tiết kiệm storage space
4. **Fault Tolerance**: Xử lý leader disconnect với status reset
5. **Real-time Monitoring**: Theo dõi real-time qua gRPC
6. **Metadata Consistency**: Đảm bảo metadata luôn accurate

## Error Handling

### Status Errors
- **Invalid status transition**: Log warning và giữ nguyên status
- **Missing block**: Return "unknown" status
- **Cleanup failure**: Log error và retry

### Recovery Scenarios
- **Server restart**: Load status từ metadata
- **Client disconnect**: Reset status về "pending"
- **Processing failure**: Reset status về "pending"

## Testing

### Test Commands
```bash
# Test status management
python test_status_management.py

# Test status transitions
python test_leader_follower.py

# Test block queue system
python test_block_queue_system.py
```

### Expected Output
```
📊 Status after block assignment:
Pending blocks: 2
Processing blocks: 1
Completed blocks: 0

✅ Block file1_block_0 marked as DONE
🧹 Cleaning up completed block: file1_block_0
🗑️ Block file1_block_0 cleaned up and removed from system
```

## Integration Points

### With Leader-Follower System
- Status tracking cho Leader và Followers
- Automatic failover với status reset
- Cleanup khi Leader disconnect

### With Block Queue System
- Queue management với status tracking
- Status-based assignment logic
- Queue cleanup sau completion

### With HDFS Features
- Metadata persistence
- Physical file management
- Storage optimization

## Troubleshooting

### Common Issues
1. **Status not updating**: Check gRPC connection
2. **Cleanup not working**: Check file permissions
3. **Metadata inconsistency**: Restart server
4. **Memory leaks**: Check cleanup methods

### Debug Commands
```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Check status manually
print(block_manager.block_status)
print(hdfs_features.metadata['blocks'])
``` 