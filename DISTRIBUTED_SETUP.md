# Distributed Big Data Processing System

A robust distributed system for processing large files across multiple machines using Kafka for reliable data distribution and processing.

## Features

- **Reliable Data Distribution**: No data loss with proper acknowledgments
- **Data Integrity**: SHA-256 checksums for data verification
- **Distributed Processing**: Multiple machines can process different parts of large files
- **Progress Tracking**: Real-time monitoring of processing status
- **Fault Tolerance**: Handles failures and retries automatically
- **Scalable**: Add more machines to increase processing capacity

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Producer      │    │   Consumer      │    │   Monitor       │
│   Machine       │    │   Machine 1     │    │   (Optional)    │
│                 │    │                 │    │                 │
│ - Reads files   │    │ - Processes     │    │ - Tracks        │
│ - Sends blocks  │    │   blocks        │    │   progress      │
│ - Tracks status │    │ - Sends acks    │    │ - Shows stats   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   Kafka Cluster │
                    │                 │
                    │ - data-blocks   │
                    │ - processing-   │
                    │   status        │
                    │ - block-acks    │
                    └─────────────────┘
                                 │
                    ┌─────────────────┐
                    │   Consumer      │
                    │   Machine 2     │
                    │                 │
                    │ - Processes     │
                    │   blocks        │
                    │ - Sends acks    │
                    └─────────────────┘
```

## Setup Instructions

### 1. Start Kafka Cluster

First, ensure your Kafka cluster is running:

```bash
# Start Docker Compose
docker-compose up -d

# Verify services are running
docker-compose ps
```

### 2. Create Required Topics

The system uses these Kafka topics:
- `data-blocks`: Contains the actual data blocks
- `processing-status`: Status updates from machines
- `block-acknowledgments`: Acknowledgments for processed blocks

Topics are created automatically, but you can create them manually:

```bash
# Create topics with proper partitioning
kafka-topics --create --topic data-blocks --bootstrap-server 172.16.128.141:29092 --partitions 6 --replication-factor 3
kafka-topics --create --topic processing-status --bootstrap-server 172.16.128.141:29092 --partitions 3 --replication-factor 3
kafka-topics --create --topic block-acknowledgments --bootstrap-server 172.16.128.141:29092 --partitions 3 --replication-factor 3
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

## Usage

### Producer Mode (Data Distribution)

Run the producer to distribute large files across the cluster:

```bash
# Basic usage
python distributed_data_processor.py --mode producer --machine-id producer1 --data-folder "D:\feb_to_apr"

# With custom brokers
python distributed_data_processor.py --mode producer --machine-id producer1 --data-folder "D:\feb_to_apr" --brokers 172.16.128.141:29092 172.16.128.141:29093 172.16.128.141:29094
```

**Features:**
- Automatically splits files into 128MB blocks
- Calculates checksums for data integrity
- Sends blocks with metadata and offset information
- Tracks processing status
- Retries failed sends with exponential backoff

### Consumer Mode (Data Processing)

Run consumers on different machines to process the distributed data:

```bash
# Machine 1
python distributed_data_processor.py --mode consumer --machine-id machine1 --group-id distributed-processors

# Machine 2  
python distributed_data_processor.py --mode consumer --machine-id machine2 --group-id distributed-processors

# Machine 3
python distributed_data_processor.py --mode consumer --machine-id machine3 --group-id distributed-processors
```

**Features:**
- Automatically receives blocks based on partition assignment
- Verifies data integrity using checksums
- Sends acknowledgments for successful processing
- Saves processed blocks with metadata
- Manual offset commits for reliability

### Monitor Mode (Progress Tracking)

Run the monitor to track processing progress across all machines:

```bash
python distributed_monitor.py --brokers 172.16.128.141:29092 172.16.128.141:29093 172.16.128.141:29094
```

**Features:**
- Real-time monitoring of all machines
- Progress tracking for each file
- Statistics on throughput and processing rates
- Failure detection and reporting
- Detailed status information

## Configuration

### Producer Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--data-folder` | `D:\feb_to_apr` | Path to data folder |
| `--machine-id` | Required | Unique producer ID |
| `--brokers` | `172.16.128.141:29092,29093,29094` | Kafka broker addresses |

### Consumer Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--machine-id` | Required | Unique machine ID |
| `--group-id` | `distributed-processors` | Consumer group ID |
| `--max-blocks` | None | Maximum blocks to process |
| `--brokers` | `172.16.128.141:29092,29093,29094` | Kafka broker addresses |

### Monitor Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--brokers` | `172.16.128.141:29092,29093,29094` | Kafka broker addresses |

## Data Flow

1. **Producer** reads large files and splits them into 128MB blocks
2. Each block includes:
   - File metadata (name, size, total blocks)
   - Block information (number, offsets, size)
   - SHA-256 checksum for integrity
   - Timestamp
3. **Kafka** distributes blocks across partitions
4. **Consumers** receive blocks based on partition assignment
5. **Consumers** verify checksums and process blocks
6. **Consumers** send acknowledgments
7. **Monitor** tracks progress and statistics

## Output Structure

Processed data is saved in the following structure:

```
processed_data_machine1/
├── file1_name/
│   ├── block_0000.bin
│   ├── block_0000_metadata.json
│   ├── block_0001.bin
│   ├── block_0001_metadata.json
│   └── ...
├── file2_name/
│   ├── block_0000.bin
│   ├── block_0000_metadata.json
│   └── ...
└── ...
```

## Monitoring and Troubleshooting

### Check Kafka Topics

```bash
# List topics
kafka-topics --list --bootstrap-server 172.16.128.141:29092

# Check topic details
kafka-topics --describe --topic data-blocks --bootstrap-server 172.16.128.141:29092
```

### Monitor Consumer Groups

```bash
# List consumer groups
kafka-consumer-groups --list --bootstrap-server 172.16.128.141:29092

# Check group status
kafka-consumer-groups --describe --group distributed-processors --bootstrap-server 172.16.128.141:29092
```

### View Kafka UI

Access the Kafka UI at: http://localhost:8080

### Common Issues

1. **NoBrokersAvailable**: Check if Kafka cluster is running
2. **Connection refused**: Verify broker addresses and ports
3. **Data loss**: Check acknowledgments and retry settings
4. **Slow processing**: Increase number of consumer machines

## Performance Optimization

### For High Throughput:

1. **Increase partitions**: More partitions = more parallel processing
2. **Add consumers**: More consumer machines = higher throughput
3. **Optimize block size**: Adjust block size based on network and storage
4. **Use compression**: Enable gzip compression for network efficiency

### For Reliability:

1. **Use acknowledgments**: All consumers send acks for processed blocks
2. **Manual commits**: Disable auto-commit for better control
3. **Retry logic**: Exponential backoff for failed operations
4. **Checksums**: Verify data integrity at every step

## Example Workflow

1. **Start Kafka cluster**:
   ```bash
   docker-compose up -d
   ```

2. **Start monitor** (optional):
   ```bash
   python distributed_monitor.py
   ```

3. **Start consumers on multiple machines**:
   ```bash
   # Machine 1
   python distributed_data_processor.py --mode consumer --machine-id machine1
   
   # Machine 2
   python distributed_data_processor.py --mode consumer --machine-id machine2
   ```

4. **Start producer**:
   ```bash
   python distributed_data_processor.py --mode producer --machine-id producer1 --data-folder "D:\feb_to_apr"
   ```

5. **Monitor progress**:
   - Watch the monitor output
   - Check Kafka UI at http://localhost:8080
   - Verify processed data in output directories

## Advanced Features

### Custom Block Processing

You can extend the consumer to add custom processing logic:

```python
def process_data_block(self, message_data: Dict) -> bool:
    # Custom processing logic here
    # e.g., data analysis, transformation, etc.
    return True
```

### Custom Monitoring

Extend the monitor to add custom metrics and alerts:

```python
def custom_alert(self, condition):
    # Send alerts for specific conditions
    pass
```

### Integration with Other Systems

The system can be integrated with:
- Apache Spark for distributed computing
- Elasticsearch for search and analytics
- Hadoop HDFS for storage
- Apache Airflow for workflow orchestration

## Security Considerations

1. **Network Security**: Use SSL/TLS for Kafka communication
2. **Authentication**: Implement SASL authentication
3. **Authorization**: Configure ACLs for topic access
4. **Data Encryption**: Encrypt sensitive data before processing

## Scaling Guidelines

- **Horizontal Scaling**: Add more consumer machines
- **Vertical Scaling**: Increase resources on existing machines
- **Partition Scaling**: Increase topic partitions for more parallelism
- **Storage Scaling**: Use distributed storage systems

This distributed system provides a robust foundation for processing large datasets across multiple machines with reliability, monitoring, and scalability. 