# Client Machine Setup Guide

This guide helps you set up client machines to receive 128MB data blocks from the main Kafka server.

## 🎯 What You Need

### Prerequisites
- Python 3.8+ installed
- Network access to main machine
- Same network (WiFi/LAN)

### Files Needed
Copy these files to each client machine:
- `simple_data_consumer.py`
- `requirements.txt`

## 🚀 Step-by-Step Setup

### Step 1: Get Main Machine IP

Ask the main machine owner for their IP address. You can find it by running on the main machine:
```bash
ipconfig
```
Look for the IPv4 address (usually starts with 192.168.x.x or 10.x.x.x)

### Step 2: Install Python Dependencies

On each client machine:
```bash
pip install kafka-python
```

### Step 3: Test Connection

Test if you can reach the main machine:
```bash
# Replace with actual IP
ping 192.168.1.100

# Test Kafka port
telnet 192.168.1.100 29092
```

### Step 4: Start Receiving Data

Run the consumer with your unique machine ID:
```bash
python simple_data_consumer.py --machine-id machine1 --brokers 192.168.1.100:29092 192.168.1.100:29093 192.168.1.100:29094
```

## 📋 Example Commands for Different Machines

### Machine 1:
```bash
python simple_data_consumer.py --machine-id machine1 --brokers 192.168.1.100:29092 192.168.1.100:29093 192.168.1.100:29094
```

### Machine 2:
```bash
python simple_data_consumer.py --machine-id machine2 --brokers 192.168.1.100:29092 192.168.1.100:29093 192.168.1.100:29094
```

### Machine 3:
```bash
python simple_data_consumer.py --machine-id machine3 --brokers 192.168.1.100:29092 192.168.1.100:29093 192.168.1.100:29094
```

## 📁 What Happens

1. **Connection**: Connects to Kafka cluster on main machine
2. **Partition Assignment**: Gets assigned specific partitions automatically
3. **Data Reception**: Receives 128MB blocks from assigned partitions
4. **Local Storage**: Saves blocks to `received_blocks_MACHINE_ID/` folder
5. **Progress Display**: Shows real-time progress and statistics

## 📊 Expected Output

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

## 🔍 Troubleshooting

### Connection Issues

**Error: Connection refused**
```bash
# Check if main machine IP is correct
ping MAIN_MACHINE_IP

# Check if Kafka is running on main machine
telnet MAIN_MACHINE_IP 29092
```

**Error: No data received**
- Make sure main machine is streaming data
- Check if topic exists: `data-blocks`
- Verify consumer group assignment

### Windows Firewall

If you get connection errors:
1. Open Windows Defender Firewall
2. Allow Python through firewall
3. Allow connections on ports 29092, 29093, 29094

### Network Issues

**Check network connectivity:**
```bash
# Test basic connectivity
ping MAIN_MACHINE_IP

# Test specific ports
telnet MAIN_MACHINE_IP 29092
telnet MAIN_MACHINE_IP 29093
telnet MAIN_MACHINE_IP 29094
```

## 📈 Monitoring

### Real-time Progress
- Shows blocks received
- Shows data transfer speed
- Shows files being processed

### Final Statistics
When you stop the consumer (Ctrl+C), you'll see:
```
FINAL STATISTICS - Machine machine1
==================================================
Blocks received: 25
Files received: 3
Total bytes: 3355.44 MB
Runtime: 45.23 seconds
Throughput: 74.18 MB/s
Output directory: received_blocks_machine1
```

## 🎯 Success Indicators

✅ **Connected to Kafka cluster**  
✅ **Assigned partitions shown**  
✅ **Receiving data blocks**  
✅ **Blocks saved to local folder**  
✅ **Progress updates showing**  
✅ **No error messages**  

## 📁 Output Structure

After receiving data, you'll have:
```
received_blocks_machine1/
├── data1_csv/
│   ├── block_0000.bin
│   ├── block_0001.bin
│   └── ...
├── data2_json/
│   ├── block_0000.bin
│   └── ...
└── ...
```

## 🔄 Restarting

If you need to restart the consumer:
```bash
# Stop with Ctrl+C, then restart
python simple_data_consumer.py --machine-id machine1 --brokers MAIN_IP:29092 MAIN_IP:29093 MAIN_IP:29094
```

The consumer will automatically resume from where it left off.

## 🆘 Getting Help

If you encounter issues:
1. Check the troubleshooting section above
2. Verify network connectivity
3. Ask the main machine owner to check Kafka status
4. Check if the main machine is streaming data

This setup allows each client machine to receive and process different parts of the data automatically! 