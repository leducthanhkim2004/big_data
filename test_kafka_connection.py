#!/usr/bin/env python3
"""
Simple Kafka Connection Test
Tests if Kafka brokers are accessible and ready
"""

import time
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError

def test_kafka_connection():
    """Test Kafka connection with different configurations"""
    
    # Kafka broker addresses
    bootstrap_servers = ['localhost:29092', 'localhost:29093', 'localhost:29094']
    
    print("🔍 Testing Kafka Connection...")
    print("=" * 50)
    print(f"Brokers: {bootstrap_servers}")
    print()
    
    # Test 1: Simple producer connection
    print("1. Testing Producer Connection...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=10000
        )
        producer.close()
        print("✅ Producer connection successful!")
    except Exception as e:
        print(f"❌ Producer connection failed: {e}")
        return False
    
    # Test 2: Admin client connection
    print("\n2. Testing Admin Client Connection...")
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=10000
        )
        
        # Get cluster metadata
        cluster_metadata = admin_client.describe_cluster()
        print(f"✅ Admin client connected!")
        print(f"   Broker Count: {len(cluster_metadata.brokers)}")
        
        admin_client.close()
    except Exception as e:
        print(f"❌ Admin client connection failed: {e}")
        return False
    
    # Test 3: Consumer connection
    print("\n3. Testing Consumer Connection...")
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=10000,
            consumer_timeout_ms=5000
        )
        consumer.close()
        print("✅ Consumer connection successful!")
    except Exception as e:
        print(f"❌ Consumer connection failed: {e}")
        return False
    
    print("\n🎉 All Kafka connection tests passed!")
    return True

def wait_for_kafka(max_wait=60):
    """Wait for Kafka to be ready"""
    print(f"⏳ Waiting for Kafka to be ready (max {max_wait} seconds)...")
    
    start_time = time.time()
    while time.time() - start_time < max_wait:
        try:
            producer = KafkaProducer(
                bootstrap_servers=['localhost:29092'],
                request_timeout_ms=5000
            )
            producer.close()
            print("✅ Kafka is ready!")
            return True
        except Exception as e:
            elapsed = int(time.time() - start_time)
            print(f"⏳ Still waiting... ({elapsed}s elapsed) - {e}")
            time.sleep(5)
    
    print("❌ Kafka did not become ready in time")
    return False

def main():
    print("🚀 Kafka Connection Test")
    print("=" * 60)
    
    # First, wait for Kafka to be ready
    if not wait_for_kafka():
        print("\n💡 Troubleshooting tips:")
        print("1. Make sure Docker containers are running: docker-compose ps")
        print("2. Check container logs: docker-compose logs kafka1")
        print("3. Wait a few more minutes for Kafka to fully start")
        print("4. Restart containers: docker-compose restart")
        return
    
    # Then test the connection
    if test_kafka_connection():
        print("\n✅ Kafka is ready for use!")
        print("You can now run:")
        print("  python simple_data_streamer.py --data-folder 'D:\\feb_to_apr'")
    else:
        print("\n❌ Kafka connection failed!")
        print("Please check the error messages above.")

if __name__ == "__main__":
    main() 