# Block Assignment System

## Overview

The new Block Assignment system separates two functions:
1. **HDFS Features**: Split files into blocks
2. **Block Manager**: Manage block assignment to nodes

## How it works

### 1. Split Data (HDFS Features)
```python
# Split file into blocks without assigning to nodes
blocks = hdfs.split_file_into_blocks(filepath)
```

**Result:**
- File is split into blocks
- Each block is stored in the `uploads/blocks/` directory
- Metadata is stored with `assigned_clients: []` (empty)
- Status: `unassigned`

### 2. Block Assignment (Block Manager)
```python
# Assign blocks to available nodes
block_manager.assign_pending_blocks()
```

**Logic:**
- Check for unassigned blocks (`unassigned`)
- **Only need 1 node with `free` status** to start assignment
- First client **MUST** be free
- 2 other clients can be `busy`
- Update metadata with `assigned_clients` and `status: assigned`

## Block Assignment Rules

### Minimum Requirements:
- **At least 1 free client** to assign block
- First client in the list **MUST** be free
- 2 remaining clients can be busy

### Examples:
```
✅ Can assign: [free, busy, busy]  ✅
✅ Can assign: [free, free, busy]  ✅
✅ Can assign: [free, free, free]  ✅
❌ Cannot assign: [busy, busy, busy] ❌
❌ Cannot assign: [busy, free, free] ❌
```

## Main Components

### HDFSFeatures
- `split_file_into_blocks()`: Split file into blocks
- `get_unassigned_blocks()`: Get list of unassigned blocks
- `update_block_assignment()`: Update block assignment information

### BlockManager
- `assign_pending_blocks()`: Assign all unassigned blocks
- `_try_assign_block()`: Try to assign a specific block
- `get_free_nodes()`: Get list of free nodes
- `get_assignment_status()`: Get block assignment status

## New API Endpoints

### `/assign_blocks`
- **Method**: GET
- **Function**: Manually assign unassigned blocks
- **Response**: Status after assignment

### `/block_status`
- **Method**: GET
- **Function**: View current block assignment status
- **Response**: Detailed information about blocks and nodes

### `/stats` (updated)
- **Method**: GET
- **Function**: System statistics + block assignment information
- **Response**: Stats + block assignment status

## Demo

Run demo to see how it works:
```bash
cd Distributed-File-Storage-Systems-main
python demo_block_assignment.py
```

## Workflow

### When uploading file:
1. **Split**: File is split into blocks
2. **Store**: Metadata is stored with `assigned_clients: []`
3. **Try Assign**: Try to assign blocks (only need 1 free client)
4. **Report**: Report number of assigned and unassigned blocks

### When node becomes free:
1. **Check**: Check for unassigned blocks
2. **Assign**: Assign blocks to free node (and 2 other nodes)
3. **Update**: Update metadata

## Advantages

1. **Separation of concerns**: Split data and assignment are independent
2. **Flexibility**: Can assign blocks after splitting
3. **Efficiency**: Only need 1 free client to assign
4. **Realistic**: Suitable for production environment
5. **Monitoring**: Easy to track block status
6. **Extensible**: Easy to add complex assignment logic

## Metadata Structure

```json
{
  "blocks": {
    "file1_block_0": {
      "assigned_clients": ["node1", "node2", "node3"],
      "size": 67108864,
      "file": "file1.txt",
      "created_at": "2024-01-01T12:00:00",
      "status": "assigned"
    },
    "file1_block_1": {
      "assigned_clients": [],
      "size": 67108864,
      "file": "file1.txt", 
      "created_at": "2024-01-01T12:00:00",
      "status": "unassigned"
    }
  }
}
```

## Monitoring

### Check status:
```bash
# View block status
curl http://localhost:5000/block_status

# Manually assign blocks
curl http://localhost:5000/assign_blocks

# View comprehensive stats
curl http://localhost:5000/stats
```

### Log messages:
- `🔄 Splitting file...`: Splitting file
- `📊 Found X unassigned blocks`: Found unassigned blocks
- `✅ Block X assigned to: [nodes]`: Assignment successful
- `  - First client (free): node1`: First client (free)
- `  - Other clients (can be busy): [node2, node3]`: Other clients
- `❌ Cannot assign block X: No free clients available (need at least 1 free client)`: No free clients

## Scenarios

### Scenario 1: Has 1 free client
```
Clients: [free, busy, busy, busy]
Result: ✅ Can assign block
Assignment: [free, busy, busy]
```

### Scenario 2: No free clients
```
Clients: [busy, busy, busy, busy]
Result: ❌ Cannot assign block
Status: Block remains unassigned
```

### Scenario 3: Client becomes free
```
Before: [busy, busy, busy, busy] → Block unassigned
After:  [free, busy, busy, busy] → Block assigned to [free, busy, busy]
``` 