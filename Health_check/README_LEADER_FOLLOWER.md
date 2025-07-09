# Leader-Follower System with Automatic Failover

## Overview

Hệ thống Leader-Follower được tích hợp vào Block Manager để đảm bảo tính ổn định và khả năng phục hồi khi xử lý block. Khi một block được gán cho 3 client, client đầu tiên (phải có status "free") sẽ trở thành **Leader** và thực hiện xử lý block, trong khi 2 client còn lại đóng vai trò **Followers**.

## Architecture

```
┌─────────────────┐    gRPC     ┌─────────────────┐
│   Client 1      │ ──────────► │  gRPC Server    │
│   (LEADER)      │             │                 │
│   (free)        │             │ ┌─────────────┐ │
└─────────────────┘             │ │Block Manager│ │
                                │ │             │ │
┌─────────────────┐             │ │ - Leader    │ │
│   Client 2      │ ──────────► │ │   Tracking  │ │
│   (FOLLOWER)    │             │ │ - Failover  │ │
│   (busy/free)   │             │ │ - Queue     │ │
└─────────────────┘             │ └─────────────┘ │
                                └─────────────────┘
┌─────────────────┐
│   Client 3      │ ──────────►
│   (FOLLOWER)    │
│   (busy/free)   │
└─────────────────┘
```

## Key Features

### 1. Leader Selection
- **Client đầu tiên** có status "free" sẽ trở thành Leader
- Leader được track trong `leader_assignments`
- Chỉ Leader mới thực hiện xử lý block

### 2. Follower Role
- 2 client còn lại trở thành Followers
- Followers có thể có status "busy" hoặc "free"
- Followers không thực hiện xử lý, chỉ backup

### 3. Automatic Failover
- Khi Leader disconnect, server tự động phát hiện
- Block được return về queue để reassignment
- Một trong các Follower có thể trở thành Leader mới

## Workflow

### 1. Block Assignment
```
1. Client gửi RequestTask với status "free"
2. Server kiểm tra block queue
3. Nếu có block, gán cho client làm Leader
4. Chọn thêm 2 client làm Followers
5. Lưu thông tin Leader trong leader_assignments
```

### 2. Leader Processing
```
1. Leader nhận block assignment
2. Leader thực hiện xử lý block
3. Leader gửi result về server
4. Block được remove khỏi assignments
5. Leader status chuyển về "free"
```

### 3. Leader Disconnection
```
1. Server phát hiện Leader disconnect (heartbeat timeout)
2. Server gọi handle_leader_disconnect()
3. Block được return về queue
4. Server remove Leader khỏi active connections
5. Block chờ client mới request
```

## API Methods

### RequestTask (Leader Assignment)
```protobuf
rpc RequestTask (TaskRequest) returns (TaskAssignment)
```
- Client request block với status "free"
- Server gán block và set client làm Leader
- Trả về: `BLOCK:block_name`

### SendResult (Leader Completion)
```protobuf
rpc SendResult (TaskResult) returns (Ack)
```
- Leader gửi kết quả xử lý
- Server remove block khỏi assignments
- Set Leader status về "free"

### Heartbeat (Connection Monitoring)
```protobuf
rpc Heartbeat (stream HeartbeatMessage) returns (stream HeartbeatResponse)
```
- Monitor connection của tất cả clients
- Phát hiện Leader disconnect
- Trigger failover process

## Block Manager Methods

### request_block_assignment()
```python
def request_block_assignment(self, client_id: str, client_status: str = "free"):
    # Gán block cho Leader (client đầu tiên)
    # Chọn Followers từ các client còn lại
    # Lưu Leader trong leader_assignments
```

### handle_leader_disconnect()
```python
def handle_leader_disconnect(self, client_id: str):
    # Tìm tất cả blocks mà client_id là Leader
    # Return blocks về queue
    # Remove khỏi leader_assignments
```

### get_leader_for_block()
```python
def get_leader_for_block(self, block_name):
    # Trả về client_id của Leader cho block
```

## Usage Examples

### 1. Start Server with Leader-Follower
```bash
cd Health_check
python server_grpc.py
```

### 2. Test Leader-Follower System
```bash
python test_leader_follower.py
```

### 3. Simulate Leader Disconnection
```python
# Server sẽ tự động phát hiện khi Leader disconnect
# và return block về queue
```

## Configuration

### Server Settings
- **Heartbeat timeout**: 30 seconds
- **Cleanup interval**: 10 seconds
- **Replication factor**: 3 (1 Leader + 2 Followers)

### Leader Requirements
- Status phải là "free"
- Phải có khả năng xử lý block
- Phải maintain heartbeat connection

### Follower Requirements
- Có thể có status "busy" hoặc "free"
- Không thực hiện xử lý
- Backup cho Leader

## Monitoring

### Leader Status
```python
# Get leader for specific block
leader = block_manager.get_leader_for_block("block_name")

# Get all blocks where client is leader
leader_blocks = block_manager.get_leader_blocks("client_id")
```

### Assignment Status
```python
status = block_manager.get_assignment_status()
print(f"Leader assignments: {status['leader_assignments']}")
```

### Queue Status
```python
queue_status = block_manager.get_queue_status()
print(f"Blocks in queue: {queue_status['queued_blocks']}")
```

## Benefits

1. **Fault Tolerance**: Tự động failover khi Leader disconnect
2. **Load Distribution**: Chỉ Leader xử lý, Followers backup
3. **Resource Efficiency**: Followers có thể busy với task khác
4. **Automatic Recovery**: Block tự động return queue để reassignment
5. **Clear Responsibility**: Leader rõ ràng, Followers backup

## Failover Scenarios

### Scenario 1: Leader Disconnects During Processing
```
1. Leader đang xử lý block
2. Leader disconnect (network issue, crash, etc.)
3. Server phát hiện qua heartbeat timeout
4. Block được return về queue
5. Client khác request và trở thành Leader mới
```

### Scenario 2: Leader Completes Successfully
```
1. Leader xử lý block thành công
2. Leader gửi result về server
3. Block được remove khỏi assignments
4. Leader status chuyển về "free"
5. Leader có thể request block mới
```

### Scenario 3: Multiple Leaders
```
1. Nhiều client free cùng lúc
2. Client đầu tiên request trở thành Leader
3. Client khác request sau sẽ nhận block khác
4. Mỗi block có 1 Leader riêng
```

## Testing

### Test Commands
```bash
# Test basic leader-follower
python test_leader_follower.py

# Test multiple clients
python client_demo.py

# Test block queue system
python test_block_queue_system.py
```

### Expected Output
```
👑 Leader client_1 received block: file1_block_0
🔧 Leader client_1 processing block file1_block_0...
✅ Leader client_1 completed block: file1_block_0
🔄 Block file1_block_0 returned to queue due to leader client_1 disconnect
```

## Troubleshooting

### Common Issues
1. **No Free Clients**: Không có client nào free để làm Leader
2. **Leader Disconnect**: Leader disconnect trước khi hoàn thành
3. **Heartbeat Timeout**: Server không nhận heartbeat từ Leader

### Debug Mode
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Integration with HDFS

Hệ thống Leader-Follower tích hợp với:
- **HDFSFeatures**: File splitting và metadata
- **BlockManager**: Block assignment và tracking
- **gRPC Server**: Communication và failover

Khi file được upload qua Flask server:
1. File được split thành blocks
2. Blocks được thêm vào gRPC queue
3. Client request và trở thành Leader
4. Leader xử lý block và gửi result
5. Nếu Leader disconnect, block return queue 