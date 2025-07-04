# Simple 128MB Block Distribution - Step by Step

This is a simple setup to distribute data from `D:\feb_to_apr` in 128MB blocks to multiple machines via Kafka.

## 🎯 What This Does

- Reads files from `D:\feb_to_apr` folder
- Splits each file into 128MB blocks
- Sends blocks to Kafka
- Multiple machines receive different blocks automatically
- Each machine saves received blocks locally

## 📋 Prerequisites

- Docker Desktop installed
- Python 3.8+ installed
- All machines on same network

## 🚀 Step-by-Step Setup

### Step 1: Main Machine (Your Laptop with Data)

1. **Start Kafka:**
   ```bash
   docker-compose up -d
   ```

2. **Install Python packages:**
   ```bash
   pip install kafka-python
   ```

3. **Start streaming data:**
   ```bash
   python simple_data_streamer.py --data-folder "D:\feb_to_apr"
   ```

### Step 2: Client Machines (Other Laptops)

On each laptop that will receive data:

1. **Install Python packages:**
   ```bash
   pip install kafka-python
   ```

2. **Get the main machine's IP address** (ask the main machine owner)

3. **Start receiving data:**
   ```bash
   python simple_data_consumer.py --machine-id machine1 --brokers MAIN_MACHINE_IP:29092 MAIN_MACHINE_IP:29093 MAIN_MACHINE_IP:29094
   ```

## 📁 File Structure

```
project/
├── docker-compose.yml              # Kafka cluster
├── simple_data_streamer.py         # Sends 128MB blocks
├── simple_data_consumer.py         # Receives blocks
├── requirements.txt                # Python packages
└── SIMPLE_SETUP.md                # This guide
```

## 🖥️ Example Commands

### Main Machine
```bash
# Start Kafka
docker-compose up -d

# Stream data
python simple_data_streamer.py --data-folder "D:\feb_to_apr"
```

### Client Machines
```bash
# Machine 1
python simple_data_consumer.py --machine-id machine1 --brokers 192.168.1.100:29092 192.168.1.100:29093 192.168.1.100:29094

# Machine 2
python simple_data_consumer.py --machine-id machine2 --brokers 192.168.1.100:29092 192.168.1.100:29093 192.168.1.100:29094

# Machine 3
python simple_data_consumer.py --machine-id machine3 --brokers 192.168.1.100:29092 192.168.1.100:29093 192.168.1.100:29094
```

## 📊 What Happens

1. **Main Machine:**
   - Reads files from `D:\feb_to_apr`
   - Splits each file into 128MB blocks
   - Sends blocks to Kafka topic `data-blocks`

2. **Client Machines:**
   - Each machine gets different blocks automatically
   - Saves blocks to `received_blocks_MACHINE_ID/` folder
   - Shows progress and statistics

3. **Result:**
   - Data is distributed across all machines
   - Each machine has different 128MB blocks
   - No duplication - each block goes to one machine

## 🔍 Testing

### Test with Small Data
```bash
# Main machine - limit to 5 blocks
python simple_data_streamer.py --data-folder "D:\feb_to_apr" --max-blocks 5

# Client machine - receive 3 blocks
python simple_data_consumer.py --machine-id test --max-blocks 3
```

### Check Results
- Main machine: Shows blocks sent
- Client machines: Shows blocks received in `received_blocks_MACHINE_ID/` folder
- Each machine gets different blocks

## 🛠️ Troubleshooting

### Connection Issues
```bash
# Check if Kafka is running
docker-compose ps

# Test connection
telnet MAIN_MACHINE_IP 29092
```

### No Data Received
- Make sure main machine is streaming data
- Check if client machines are connected
- Verify IP address is correct

### Windows Firewall
- Allow connections on port 29092
- Add Python to firewall exceptions

## 📈 Expected Output

### Main Machine
```
Simple Data Streamer - 128MB Blocks
========================================
Data folder: D:\feb_to_apr
Topic: data-blocks
Brokers: ['localhost:29092']

Found 5 files in D:\feb_to_apr
Starting to stream 5 files in 128MB blocks...
Topic: data-blocks
--------------------------------------------------

File 1/5: data1.csv
Processing file: data1.csv
File size: 256.50 MB
✓ Sent block 0 from data1.csv (134217728 bytes)
✓ Sent block 1 from data1.csv (134217728 bytes)
✓ Completed file: data1.csv (2 blocks)
```

### Client Machine
```
Simple Data Consumer - Machine machine1
==================================================
Topic: data-blocks
Group ID: data-consumers
Output Directory: received_blocks_machine1
Assigned partitions: ['data-blocks-0', 'data-blocks-2']

Waiting for data blocks...
Press Ctrl+C to stop
--------------------------------------------------
✓ Saved block 0 from data1.csv (134217728 bytes)
  Saved to: received_blocks_machine1\data1_csv\block_0000.bin
```

## 🎯 Success Criteria

✅ Kafka cluster running  
✅ Main machine streaming data  
✅ Client machines receiving blocks  
✅ Each machine gets different blocks  
✅ Blocks saved to local folders  
✅ No errors in console  

This simple setup distributes your `D:\feb_to_apr` data in 128MB blocks across multiple machines automatically! 