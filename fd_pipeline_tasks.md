# Operation Baag-hum-Baag – Kafka/ZeroMQ Async Producers & Consumers Training

## Goal

Build an event-driven pipeline that simulates **real-time payment events**, sends them to **Kafka or ZeroMQ** via an **async producer**, and processes these using **async consumers** to apply:

- Fraud detection logic  
- Risk scoring  
- Alert dispatching (to logs or monitoring endpoint)  

---

## Core Tasks

### Part 1: Kafka/ZeroMQ Setup

- [ ] Use Docker to spin up a Kafka broker with ZooKeeper  
- [ ] Alternatively, install `zeromq` locally and set up basic pub-sub sockets  
- [ ] Create a `payments` topic (Kafka) or `PUB` socket (ZMQ)  

---

### Part 2: Event Schema & Payload Generator

- [ ] Define a `payment_event` schema (JSON or Avro):  
  - `transaction_id`, `amount`, `timestamp`, `location`, `card_id`, `merchant_id`  
- [ ] Implement a Python script that randomly generates and serializes 500+ payment events  
- [ ] Use `fastavro` or `avro-python3` for schema validation and serialization  

---

### Part 3: Async Producer

- [ ] Use `aiokafka` / `pyzmq.asyncio` to send messages asynchronously  
- [ ] Send 100 events per second (simulated real-time)  
- [ ] Include retry with exponential backoff if the broker is unavailable  
- [ ] Log all sent events and timestamps  

---

### Part 4: Async Consumer

- [ ] Build an async consumer that reads from `payments` topic or ZMQ subscriber  
- [ ] Parse and deserialize the event payload  
- [ ] Apply a simple fraud detection rule:  
  - If a transaction > 20,000 made from a new location  
- [ ] Score risk from 0–100 based on merchant, frequency, and amount  
- [ ] Log high-risk events for dispatch  

---

### Part 5: Alert Dispatcher

- [ ] If risk score > 80, push alert to:  
  - A file sink (`alerts.log`)  
  - OR a mock dashboard endpoint (using FastAPI `/alerts`)  
- [ ] Maintain alert count and send a summary every 60 seconds  

---

## Bonus Challenges

- [ ] Use `confluent-kafka-python` with Avro + Schema Registry  
- [ ] Add partition-based processing (e.g., by `card_id`)  
- [ ] Enable exactly-once or at-least-once semantics  
- [ ] Add FastAPI `/metrics` for:  
  - Alerts raised  
  - Max processing latency  
- [ ] Integrate Prometheus + Grafana for real-time alert dashboard  
- [ ] Write integration tests using `pytest-asyncio`  

---

## Learning Outcomes

- Design message-based, decoupled microservices  
- Understand Kafka/ZMQ producers and consumers  
- Gain async experience in stream handling and risk scoring  
- Build a real-time analytics pipeline with event replay and monitoring  
