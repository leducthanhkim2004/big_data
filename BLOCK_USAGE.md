# Block Processing with Offset Information

This guide explains how to use the enhanced 128MB block system with start/end offset information for easier distribution, assignment, and processing.

## 🎯 **What's New with Offsets**

### **Enhanced Block Information:**
Each block now includes:
- **Start Offset**: Where this block starts in the original file
- **End Offset**: Where this block ends in the original file
- **File Size**: Total size of the original file
- **Total Blocks**: How many blocks the file is split into
- **Progress**: Current block number and percentage

### **Example Block Structure:**
```
received_blocks_machine1/
├── data1_csv/
│   ├── block_0000.bin              ← 128MB data block
│   ├── block_0000_metadata.json    ← Block metadata with offsets
│   ├── block_0001.bin              ← Next 128MB data block
│   ├── block_0001_metadata.json    ← Block metadata with offsets
│   └── ...
```

## 📊 **Block Metadata Example**

Each `block_XXXX_metadata.json` contains:
```json
{
  "file_name": "data1.csv",
  "block_number": 0,
  "start_offset": 0,
  "end_offset": 134217727,
  "block_size_bytes": 134217728,
  "file_size_bytes": 268435456,
  "total_blocks": 2,
  "progress_percent": 50.0,
  "received_at": 1703123456.789,
  "machine_id": "machine1"
}
```

## 🚀 **How to Use the Enhanced System**

### **1. Start Streaming (Main Machine)**
```bash
# Restart Kafka with new settings
docker-compose down
docker-compose up -d

# Start streaming with offset information
python simple_data_streamer.py --data-folder "D:\feb_to_apr"
```

### **2. Receive Blocks (Client Machines)**
```bash
# Each machine gets blocks with offset information
python simple_data_consumer.py --machine-id machine1 --brokers MAIN_IP:29092 MAIN_IP:29093 MAIN_IP:29094
```

### **3. Process Blocks (Any Machine)**
```bash
# List all files and their progress
python block_processor.py --action list

# Reconstruct a specific file
python block_processor.py --action reconstruct --file-name "data1.csv"

# Validate block integrity
python block_processor.py --action validate --file-name "data1.csv"

# Export detailed report
python block_processor.py --action report
```

## 📈 **Enhanced Output Examples**

### **Streamer Output:**
```
Processing file: data1.csv
File size: 256.50 MB
✓ Sent block 0 from data1.csv (134217728 bytes)
  Offsets: 0 - 134217727 (File: 268435456 bytes)
  Progress: 1/2 (50.0%)
✓ Sent block 1 from data1.csv (134217728 bytes)
  Offsets: 134217728 - 268435455 (File: 268435456 bytes)
  Progress: 2/2 (100.0%)
```

### **Consumer Output:**
```
✓ Saved block 0 from data1.csv (134217728 bytes)
  Offsets: 0 - 134217727 (File: 268435456 bytes)
  Progress: 1/2 (50.0%)
  Saved to: received_blocks_machine1\data1_csv\block_0000.bin
```

### **Block Processor Output:**
```
📁 File Progress Summary
============================================================
📄 data1.csv
   Progress: 2/2 blocks (100.0%)
   Size: 256.00 MB

🤖 Block Assignment Summary
============================================================
🖥️ Machine: machine1
   Files: 1
   Blocks: 2
   Data: 256.00 MB
```

## 🔧 **Block Processing Commands**

### **List All Files:**
```bash
python block_processor.py --action list
```

### **Reconstruct File:**
```bash
python block_processor.py --action reconstruct --file-name "data1.csv"
```

### **Validate Blocks:**
```bash
python block_processor.py --action validate --file-name "data1.csv"
```

### **Export Report:**
```bash
python block_processor.py --action report
```

## 🎯 **Benefits of Offset Information**

### **1. Easy Distribution:**
- Know exactly which part of the file each block represents
- Track progress across multiple machines
- Identify missing blocks easily

### **2. Simple Assignment:**
- Each machine knows its block range
- No overlap between machines
- Automatic load balancing

### **3. Efficient Processing:**
- Process blocks in parallel
- Reconstruct files in correct order
- Validate data integrity

### **4. Progress Tracking:**
- Real-time progress updates
- Know when all blocks are received
- Monitor processing speed

## 📁 **File Structure After Processing**

```
project/
├── received_blocks_machine1/
│   ├── data1_csv/
│   │   ├── block_0000.bin
│   │   ├── block_0000_metadata.json
│   │   ├── block_0001.bin
│   │   └── block_0001_metadata.json
│   └── data2_json/
│       ├── block_0000.bin
│       └── block_0000_metadata.json
├── reconstructed_files/
│   ├── data1.csv
│   └── data2.json
└── block_report.json
```

## 🔍 **Troubleshooting**

### **Missing Blocks:**
```bash
# Check which blocks are missing
python block_processor.py --action list
```

### **Offset Errors:**
```bash
# Validate block integrity
python block_processor.py --action validate --file-name "problematic_file.csv"
```

### **Reconstruction Issues:**
```bash
# Check if all blocks are present
python block_processor.py --action reconstruct --file-name "file.csv"
```

## ✅ **Success Indicators**

- ✅ Each block has proper start/end offsets
- ✅ No gaps in offset ranges
- ✅ Progress percentages are accurate
- ✅ Files can be reconstructed completely
- ✅ Block metadata is saved correctly

The offset information makes it much easier to manage, distribute, and process your 128MB data blocks! 