import random
import uuid
from datetime import datetime, timedelta
from fastavro import writer, parse_schema
import json

SCHEMA_PATH = 'avro/payment_event_schema.avsc'
OUTPUT_PATH = 'avro/payment_events.avro'

# Load Avro schema
def load_schema(path):
    with open(path, 'r') as f:
        return parse_schema(json.load(f))

# Generate a random payment event
def generate_event():
    return {
        'transaction_id': str(uuid.uuid4()),
        'amount': round(random.uniform(10, 50000), 2),
        'timestamp': (datetime.utcnow() - timedelta(seconds=random.randint(0, 86400))).isoformat(),
        'location': random.choice(['NY', 'LA', 'SF', 'CHI', 'DAL', 'SEA', 'BOS', 'ATL']),
        'card_id': str(uuid.uuid4()),
        'merchant_id': str(random.randint(1000, 9999))
    }

def main():
    schema = load_schema(SCHEMA_PATH)
    records = [generate_event() for _ in range(5)]
    with open(OUTPUT_PATH, 'wb') as out:
        writer(out, schema, records)
    print(f"Generated 500 payment events and wrote to {OUTPUT_PATH}")

if __name__ == '__main__':
    main()
