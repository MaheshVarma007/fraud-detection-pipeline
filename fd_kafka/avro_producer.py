from confluent_kafka.avro import AvroProducer
import avro.schema
import uuid
import time

# Load Avro schema
event_schema = avro.schema.parse(open("../avro/payment_event_schema.avsc").read())

producer = AvroProducer({
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081',
    'acks': 'all',  # Wait for all replicas
    'retries': 5,   # Retry on failure
    'enable.idempotence': True,  # Safe for at-least-once, also enables exactly-once if using transactions
    'transactional.id': 'fd-producer-1'  # Required for exactly-once semantics
}, default_value_schema=event_schema)

producer.init_transactions()

def generate_event():
    return {
        "transaction_id": str(uuid.uuid4()),
        "amount": 10000.0 + 20000.0 * (uuid.uuid4().int % 2),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "location": "New York" if (uuid.uuid4().int % 2) else "London",
        "card_id": f"CARD{uuid.uuid4().hex[:6]}",
        "merchant_id": f"MERCHANT{uuid.uuid4().hex[:6]}"
    }

if __name__ == "__main__":
    producer.begin_transaction()
    try:
        for _ in range(5):
            event = generate_event()
            print("Producing event:", event)
            # Use card_id as the key for partitioning
            producer.produce(topic='payments', value=event, key=event['card_id'])
        producer.commit_transaction()
        print("Transaction committed. Done producing Avro events.")
    except Exception as e:
        print(f"Error occurred: {e}. Aborting transaction.")
        producer.abort_transaction()
