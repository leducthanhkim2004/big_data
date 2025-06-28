from kafka import KafkaConsumer
import subprocess
import time
import os

# === CONFIGURATION ===
TOPIC = 'chunked-data'
KAFKA_BROKER = 'localhost:9092'
GROUP_ID = 'hdfs-writer-group'
LOCAL_TMP_FILE = r"D:\temp\kafka_buffer.txt"
HDFS_DIR = '/shared'
BATCH_SIZE = 10000  # lines per HDFS file

# === SETUP CONSUMER ===
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=lambda x: x.decode('utf-8')
)

# === PROCESS & WRITE BATCHES TO HDFS ===
batch = []

def flush_to_hdfs(batch_data):
    if not batch_data:
        return
    # Write to local temp file
    with open(LOCAL_TMP_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(batch_data) + '\n')

    # Define HDFS target path
    hdfs_file_path ="/shared"

    try:

        # Put file to HDFS
        subprocess.run([r"C:\Users\leduc\hadoop-3.4.1\bin\hdfs.cmd", 'dfs', '-put', '-f', LOCAL_TMP_FILE, hdfs_file_path], check=True)
        print(f"[✓] Wrote {len(batch_data)} lines to HDFS: {hdfs_file_path}")
    except subprocess.CalledProcessError as e:
        print(f"[✗] Failed to write to HDFS: {e}")

    # Clear local temp file
    open(LOCAL_TMP_FILE, 'w').close()

try:
    for message in consumer:
        batch.append(message.value)

        if len(batch) >= BATCH_SIZE:
            flush_to_hdfs(batch)
            batch.clear()

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    # Flush remaining messages
    flush_to_hdfs(batch)
    consumer.close()
