from confluent_kafka.avro import AvroConsumer

consumer = AvroConsumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fd-avro-consumer',
    'auto.offset.reset': 'earliest',
    'schema.registry.url': 'http://localhost:8081',
    'isolation.level': 'read_committed'  # Only read committed messages for exactly-once
})

consumer.subscribe(['payments'])

print("Consuming Avro events from 'payments' topic...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        print(f"Received event: {msg.value()}")
        # Process event here
        consumer.commit()  # Commit after successful processing
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print("Consumer closed.")
