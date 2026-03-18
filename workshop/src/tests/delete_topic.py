from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='admin'
)

# Delete the topic
admin_client.delete_topics(["green-trips"])
print("Topic 'green-trips' deleted")