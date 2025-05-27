import asyncio
import json
import logging
import random
import time
from aiokafka import AIOKafkaProducer
from fastavro import schemaless_reader
#import avro.schema
import io

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'payments'
EVENTS_PATH = './avro/payment_events.avro'

logging.basicConfig(filename='producer.log', level=logging.INFO, format='%(asctime)s %(message)s')

# Read Avro events from file
def read_avro_events(path):
    from fastavro import reader
    with open(path, 'rb') as f:
        return [record for record in reader(f)]

async def send_with_retry(producer, topic, value, retries=5):
    delay = 1
    for attempt in range(retries):
        try:
            await producer.send_and_wait(topic, value)
            return True
        except Exception as e:
            logging.warning(f"Send failed (attempt {attempt+1}): {e}. Retrying in {delay}s...")
            await asyncio.sleep(delay)
            delay *= 2
    return False

async def produce():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    await producer.start()
    print("Producer started")
    try:
        print("Reading events from Avro file")
        events = read_avro_events(EVENTS_PATH)
        print(f"Loaded {len(events)} events from {EVENTS_PATH}")
        for i, event in enumerate(events):
            sent = await send_with_retry(producer, TOPIC, event)
            if sent:
                print("inside sent")
                print(f"Sent event {i+1}: {event['transaction_id']}")
                logging.info(f"Sent event {i+1}: {event['transaction_id']}")
            else:
                logging.error(f"Failed to send event {i+1}: {event['transaction_id']}")
            await asyncio.sleep(0.01)  # 100 events/sec
    finally:
        await producer.stop()

if __name__ == '__main__':
    asyncio.run(produce())
