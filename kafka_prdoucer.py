from kafka import KafkaProducer
import os
TOPIC = "chunked-data"
CHUNKS_DIR=r"D:\nov_to_jan"
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    value_serializer= lambda V: V.encode("utf-8")
)
for fname in sorted(os.listdir(CHUNKS_DIR)):
    file_path = os.path.join(CHUNKS_DIR, fname)
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                producer.send(TOPIC, value=line)
                print(f"Sent: {line}")
producer.flush()
producer.close()