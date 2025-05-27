from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_BROKER = 'localhost:9092'  # Adjust if your broker is on a different host/port
topic_name = 'payments'

admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, client_id='topic_creator')

# Set num_partitions to 3 for partition-based processing
num_partitions = 3
topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=1)]

try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print(f"Topic '{topic_name}' with {num_partitions} partitions created successfully.")
except TopicAlreadyExistsError:
    print(f"Topic '{topic_name}' already exists.")
finally:
    admin_client.close()
