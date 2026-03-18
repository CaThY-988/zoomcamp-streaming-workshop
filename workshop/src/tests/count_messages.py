from kafka import KafkaConsumer, TopicPartition

# Connect to your Redpanda/Kafka broker
bootstrap_servers = ['localhost:9092']
topic_name = 'green-trips'

consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)

# Get all partitions for the topic
partitions = consumer.partitions_for_topic(topic_name)
tp_list = [TopicPartition(topic_name, p) for p in partitions]

# Assign partitions to the consumer
consumer.assign(tp_list)

# Get the beginning (earliest) and end (latest) offsets
beginning = consumer.beginning_offsets(tp_list)
end = consumer.end_offsets(tp_list)

# Calculate total messages
total_messages = sum(end[p] - beginning[p] for p in tp_list)
print(f"Total messages in topic '{topic_name}': {total_messages}")