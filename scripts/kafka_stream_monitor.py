import time
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

BOOTSTRAP_SERVERS = 'localhost:19092'
TOPIC_NAME = 'order-events'
GROUP_ID = 'order_processor_group'

def monitor_kafka_lag():
    print(f"--- Kafka Consumer Group & Offset Lag Monitor ---")
    print(f"Cluster: {BOOTSTRAP_SERVERS} | Topic: {TOPIC_NAME} | Group: {GROUP_ID}\n")

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=GROUP_ID,
            enable_auto_commit=False
        )

        partitions = consumer.partitions_for_topic(TOPIC_NAME)
        if not partitions:
            print(f"⚠️ Topic '{TOPIC_NAME}' not found or has no active partitions.")
            consumer.close()
            return

        topic_partitions = [TopicPartition(TOPIC_NAME, p) for p in partitions]
        
        # Query latest offsets produced to the topic (log end offsets)
        end_offsets = consumer.end_offsets(topic_partitions)

        total_lag = 0
        print(f"{'Partition':<12} | {'Log End Offset':<16} | {'Committed Offset':<18} | {'Lag':<10}")
        print("-" * 62)

        for tp in topic_partitions:
            log_end = end_offsets.get(tp, 0)
            committed_metadata = consumer.committed(tp)
            committed_offset = committed_metadata if committed_metadata is not None else 0
            
            lag = max(0, log_end - committed_offset)
            total_lag += lag
            print(f"{tp.partition:<12} | {log_end:<16} | {committed_offset:<18} | {lag:<10}")

        print("-" * 62)
        print(f"Total Consumer Lag Across Partitions: {total_lag} messages\n")
        
        if total_lag > 50:
            print("🚨 Alert: High consumer lag detected! Scale consumers or increase batch size.")
        else:
            print("✅ Consumer health optimal. Processing latency within bounds.")

        consumer.close()

    except Exception as e:
        print(f"❌ Kafka Monitor Failed: {e}")

if __name__ == "__main__":
    monitor_kafka_lag()