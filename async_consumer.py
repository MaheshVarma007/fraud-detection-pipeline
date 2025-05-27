import asyncio
import json
import logging
import time
from aiokafka import AIOKafkaConsumer
from collections import defaultdict
import aiohttp

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'payments'

logging.basicConfig(filename='consumer.log', level=logging.INFO, format='%(asctime)s %(message)s')

# Track locations per card_id for fraud detection
card_locations = defaultdict(set)

# Track transaction frequency per merchant
merchant_freq = defaultdict(int)

# Simple risk scoring function
def score_risk(event):
    base = 0
    amount = event['amount']
    merchant = event['merchant_id']
    card = event['card_id']
    freq = merchant_freq[merchant]
    # Amount-based risk
    if amount > 20000:
        base += 50
    elif amount > 10000:
        base += 30
    elif amount > 5000:
        base += 10
    # Frequency-based risk
    if freq > 10:
        base += 20
    elif freq > 5:
        base += 10
    # Merchant-based risk (arbitrary: high risk for merchant_id ending with 9)
    if merchant.endswith('9'):
        base += 20
    return min(base, 100)

ALERT_API = 'http://localhost:8000/alerts'
ALERT_LOG = 'alerts.log'

async def send_alert(event, risk, fraud):
    alert = {
        'transaction_id': event['transaction_id'],
        'amount': event['amount'],
        'timestamp': event['timestamp'],
        'location': event['location'],
        'card_id': event['card_id'],
        'merchant_id': event['merchant_id'],
        'risk': risk,
        'fraud': fraud
    }
    # Write to file
    with open(ALERT_LOG, 'a') as f:
        f.write(f"{alert}\n")
    # Optionally send to FastAPI endpoint
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(ALERT_API, json=alert)
    except Exception as e:
        logging.error(f"Failed to send alert to API: {e}")

async def consume():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    print("Starting consumer...")
    await consumer.start()
    alert_count = 0
    start_time = time.time()
    logging.info("Consumer started")
    try:
        async for msg in consumer:
            event = msg.value
            card_id = event['card_id']
            location = event['location']
            amount = event['amount']
            merchant = event['merchant_id']
            merchant_freq[merchant] += 1
            logging.info(f"Processing event: tx={event['transaction_id']} amount={amount} location={location} card={card_id} merchant={merchant}")
            print(f"Processing event: tx={event['transaction_id']} amount={amount} location={location} card={card_id} merchant={merchant}")
            # Fraud detection: transaction > 20,000 from a new location
            is_new_location = location not in card_locations[card_id]
            if amount > 20000 and is_new_location:
                fraud = True
                card_locations[card_id].add(location)
                print(f"Fraud detected: tx={event['transaction_id']} amount={amount} location={location} card={card_id} merchant={merchant}")
            else:
                fraud = False
                card_locations[card_id].add(location)
            risk = score_risk(event)
            if fraud or risk > 80:
                await send_alert(event, risk, fraud)
                alert_count += 1
                logging.warning(f"High-risk event: tx={event['transaction_id']} amount={amount} location={location} card={card_id} merchant={merchant} risk={risk} fraud={fraud}")
                print(f"High-risk event: tx={event['transaction_id']} amount={amount} location={location} card={card_id} merchant={merchant} risk={risk} fraud={fraud}")
            else:
                logging.info(f"Event: tx={event['transaction_id']} amount={amount} location={location} card={card_id} merchant={merchant} risk={risk}")
                print(f"Event: tx={event['transaction_id']} amount={amount} location={location} card={card_id} merchant={merchant} risk={risk}")
            if time.time() - start_time > 60:
                logging.info(f"Summary: {alert_count} alerts in last 60s")
                print(f"Summary: {alert_count} alerts in last 60s")
                alert_count = 0
                start_time = time.time()
    finally:
        await consumer.stop()

if __name__ == '__main__':
    asyncio.run(consume())
