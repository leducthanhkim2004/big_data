# Distributed Storage System Demo

This system demonstrates how clients announce their status to a server in a distributed storage environment.

## Files:
- `server.py` - The main server that receives and tracks client status announcements
- `client.py` - Client nodes that periodically announce their status to the server

## How it works:

### Server (`server.py`):
- Listens on localhost:8888 for client connections
- Receives JSON status messages from clients
- Tracks client information (storage usage, CPU, memory, data blocks)
- Automatically removes inactive clients after 2 minutes
- Displays real-time status of all connected clients

### Client (`client.py`):
- Connects to the server and announces status every 10 seconds
- Reports: storage usage, CPU/memory usage, active data blocks
- Simulates storage operations and data block management
- Handles reconnection if server connection is lost

## Running the System:

### 1. Start the Server:
```powershell
python server.py
```

### 2. Start one or more clients (in separate terminals):
```powershell
# Default client
python client.py

# Named client
python client.py node_web1

# Multiple clients
python client.py node_db1
python client.py node_cache1
python client.py node_storage1
```

## What you'll see:

### Server Output:
- 🚀 Server startup message
- 📱 Client connection notifications
- 📊 Status updates from clients
- 📈 Periodic status summary of all clients
- 🗑️ Cleanup of inactive clients

### Client Output:
- 🔧 Client initialization with storage capacity
- ✅ Connection confirmation
- 📡 Periodic status announcements
- 🔄 Reconnection attempts if needed

## Status Information Tracked:
- **Node ID**: Unique identifier for each client
- **Status**: healthy, idle, warning, critical, error
- **Storage**: Used/Total storage in GB
- **CPU/Memory**: Resource utilization percentages
- **Data Blocks**: List of data blocks stored on the client
- **Last Seen**: Timestamp of last status update

## Features:
- **Automatic Reconnection**: Clients reconnect if server restarts
- **Health Monitoring**: Server tracks client health and removes inactive ones
- **Resource Simulation**: Simulates realistic storage and system metrics
- **Scalable**: Support for multiple concurrent clients
- **Real-time Updates**: Live status monitoring and reporting

## Extending the System:
- Add authentication for client connections
- Implement data replication coordination
- Add load balancing for client requests
- Include network bandwidth monitoring
- Add persistent storage for client metadata
