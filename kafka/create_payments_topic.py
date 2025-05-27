from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_BROKER = 'localhost:9092'  # Adjust if your broker is on a different host/port
topic_name = 'payments'

admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, client_id='topic_creator')

topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]

try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print(f"Topic '{topic_name}' created successfully.")
except TopicAlreadyExistsError:
    print(f"Topic '{topic_name}' already exists.")
finally:
    admin_client.close()
