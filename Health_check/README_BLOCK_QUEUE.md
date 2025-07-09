# Block Queue System with gRPC Server

## Overview

Hệ thống này tích hợp Block Manager vào gRPC server để quản lý việc phân phối block và task cho các client. Server gRPC sẽ là trung tâm quản lý, nơi các block chỉ được gán khi client gửi request và có status "free".

## Architecture

```
┌─────────────────┐    gRPC     ┌─────────────────┐
│   Client 1      │ ──────────► │  gRPC Server    │
│   (free/busy)   │             │                 │
└─────────────────┘             │ ┌─────────────┐ │
                                │ │Block Manager│ │
┌─────────────────┐             │ │             │ │
│   Client 2      │ ──────────► │ │ - Block     │ │
│   (free/busy)   │             │ │   Queue     │ │
└─────────────────┘             │ │ - Task      │ │
                                │ │   Queue     │ │
┌─────────────────┐             │ │ - Client    │ │
│   Client 3      │ ──────────► │ │   Status    │ │
│   (free/busy)   │             │ └─────────────┘ │
└─────────────────┘             └─────────────────┘
```

## Key Features

### 1. Block Queue Management
- Blocks được thêm vào queue khi file được split
- Blocks chỉ được gán khi client request và status "free"
- Ưu tiên gán block trước task thường

### 2. Client Status Tracking
- Server theo dõi status của tất cả client (free/busy)
- Chỉ gán task/block cho client có status "free"
- Heartbeat để duy trì kết nối

### 3. Task Distribution
- Hỗ trợ cả block processing và task thường
- Block được format: `BLOCK:block_name`
- Result format: `BLOCK_RESULT:block_name:result`

## API Methods

### RequestTask
```protobuf
rpc RequestTask (TaskRequest) returns (TaskAssignment)
```
- Client request task/block
- Server trả về block (ưu tiên) hoặc task thường
- Chỉ gán cho client có status "free"

### SendResult
```protobuf
rpc SendResult (TaskResult) returns (Ack)
```
- Client gửi kết quả xử lý
- Tự động set client status về "free"
- Hỗ trợ block result format

### AddBlockToQueue
```protobuf
rpc AddBlockToQueue (BlockRequest) returns (Ack)
```
- Thêm block vào queue để gán
- Có thể gọi từ Flask server hoặc external system

### GetQueueStatus
```protobuf
rpc GetQueueStatus (QueueRequest) returns (QueueStatus)
```
- Lấy thông tin queue hiện tại
- Số lượng task/block trong queue
- Danh sách active clients

### Heartbeat
```protobuf
rpc Heartbeat (stream HeartbeatMessage) returns (stream HeartbeatResponse)
```
- Streaming heartbeat để track client connection
- Tự động cleanup inactive clients

## Usage Examples

### 1. Start gRPC Server
```bash
cd Health_check
python server_grpc.py
```

### 2. Run Demo Clients
```bash
python client_demo.py
```

### 3. Test Block Queue System
```bash
python test_block_queue_system.py
```

### 4. Add Blocks from Flask Server
```python
# Trong Flask server
import grpc
import task_pb2
import task_pb2_grpc

channel = grpc.insecure_channel('localhost:50051')
stub = task_pb2_grpc.TaskServiceStub(channel)

# Add blocks to queue
for block in blocks:
    request = task_pb2.BlockRequest(block_name=block)
    response = stub.AddBlockToQueue(request)
```

## Workflow

### 1. File Upload (Flask Server)
1. File được upload qua Flask server
2. File được split thành blocks
3. Blocks được thêm vào gRPC server queue
4. Blocks chờ client request

### 2. Block Assignment (gRPC Server)
1. Client gửi RequestTask với status "free"
2. Server kiểm tra block queue
3. Nếu có block, gán cho client
4. Client status chuyển thành "busy"

### 3. Block Processing (Client)
1. Client nhận block assignment
2. Xử lý block (simulate processing)
3. Gửi result về server
4. Client status chuyển về "free"

### 4. Result Handling (gRPC Server)
1. Server nhận result từ client
2. Cập nhật block assignment status
3. Set client status về "free"
4. Client có thể request task mới

## Configuration

### Server Settings
- Port: 50051
- Heartbeat timeout: 30 seconds
- Cleanup interval: 10 seconds
- Max workers: 10

### Client Settings
- Heartbeat interval: 5 seconds
- Retry delay: 5 seconds
- Processing simulation time: 2-3 seconds

## Integration with HDFS Features

Server gRPC có thể import và sử dụng:
- `BlockManager`: Quản lý block assignment
- `HDFSFeatures`: File splitting và metadata

Nếu không import được, server sẽ sử dụng fallback implementation.

## Monitoring

### Queue Status
```python
request = task_pb2.QueueRequest()
response = stub.GetQueueStatus(request)
print(f"Task queue: {response.task_queue_length}")
print(f"Block queue: {response.block_queue_length}")
print(f"Active clients: {response.active_clients}")
```

### Connection Stats
```python
stats = server.get_connection_stats()
print(f"Active clients: {stats['total_active_clients']}")
print(f"Assigned blocks: {stats['assigned_blocks']}")
```

## Benefits

1. **Centralized Management**: Server gRPC quản lý tất cả block assignment
2. **Client-Driven**: Blocks chỉ được gán khi client request
3. **Status Tracking**: Theo dõi real-time client status
4. **Scalable**: Hỗ trợ nhiều client đồng thời
5. **Fault Tolerant**: Heartbeat và cleanup tự động
6. **Flexible**: Hỗ trợ cả block và task thường

## Troubleshooting

### Common Issues
1. **Import Error**: Server sẽ sử dụng fallback implementation
2. **Connection Lost**: Client tự động retry sau 5 giây
3. **No Tasks**: Client sẽ poll lại sau 2 giây
4. **Server Down**: Client sẽ retry connection

### Debug Mode
Enable debug logging trong server để xem chi tiết:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
``` 