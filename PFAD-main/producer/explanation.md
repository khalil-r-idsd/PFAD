# Event Producer and GNN Fraud Detection Integration

This document explains the **producer** component, how it generates and serializes events into Kafka, and how these events can be used and enriched by a **GNN‑based fraud detection model** in the PFAD project.[cite:8][cite:9][cite:10][cite:11]

---

## 1. Producer Role in the Architecture

The `producer/` folder implements a **traffic generator** that simulates user activity and sends events into a single Kafka topic.[cite:8][cite:9][cite:10]

High-level flow:

> `producer.py` → generates structured Python events → Avro serialization → Kafka topic (`KAFKA_TOPIC`) → Spark / other consumers build downstream analytics and fraud detection.

This producer acts as the **entry point** of synthetic data into the streaming pipeline that later feeds ClickHouse, Airflow and, potentially, a GNN fraud detection service.[cite:2][cite:8][cite:9][cite:10]

---

## 2. Producer Configuration (`config.py`)

`config.py` defines both the **business space** of events and the **traffic pattern**.[cite:9]

### 2.1 Event types and status

Two enums describe the types of user actions and their outcomes:[cite:9]

- `Events`:
  - `ADD_TO_CART`
  - `CHECKOUT`
  - `PAYMENT`
  - `SEARCH`
  - `VIEW_PRODUCT`
- `Status`:
  - `SUCCESS`
  - `ERROR`

These represent key e‑commerce interactions that your downstream analytics and GNN can reason about.

### 2.2 Simulation parameters

Core parameters controlling load and session behaviour:[cite:9]

- `NUM_WORKERS = 1`  
  Number of producer processes (can be increased for higher throughput).

- `KAFKA_TOPIC = os.environ['KAFKA_TOPIC']`  
  Name of the single Kafka topic that receives all events.

- `EVENT_INTERVAL_SECONDS = 0.01`  
  Controls how often events are produced (~100 events/s/worker).

- `NEW_USER_SESSION_PROBABILITY = 0.01`  
  Probability that the producer switches to a **new user/session**; otherwise it continues the existing one, simulating session continuity.

### 2.3 Kafka producer configuration

`PRODUCER_CONF` configures the `SerializingProducer` from `confluent-kafka`:[cite:9]

- Reliability and throughput:
  - `'acks': 'all'` → strongest durability guarantees.
  - `'batch.size': 32768` → up to 32 KB batches.
  - `'linger.ms': 20` → up to 20 ms wait to fill batches (better throughput, slightly higher latency).
  - `'compression.type': 'snappy'` → efficient compression for high‑volume streams.

- Connectivity:
  - `'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS']` → Kafka cluster endpoint.

- Serialization:
  - `'key.serializer': uuid_serializer` → uses a custom serializer to convert `UUID` keys into a Kafka‑compatible representation.[cite:9]
  - `'value.serializer': avro_serializer` → converts Python dicts into Avro bytes using the `user_event_schema.avsc` schema.[cite:8][cite:9]

**Key idea:** application code generates **normal Python dicts**; the producer configuration controls how they are **serialized and sent to Kafka**.

---

## 3. Schema and Serialization (`user_event_schema.avsc`, `schema_registry.py`)

The file `user_event_schema.avsc` defines the **Avro schema** for the `user_event` messages.[cite:8]  
`schema_registry.py` provides the `avro_serializer` and `uuid_serializer` functions used by the producer.[cite:9]

Conceptually:

- The **schema** defines fields (e.g., `event_id`, `user_id`, `session_id`, `event_type`, `event_timestamp`, `request_latency_ms`, `status`, `error_code`, `product_id`) and their types.[cite:8][cite:10]
- The **Avro serializer**:
  - Checks that each Python dict produced by `generate_event` matches this schema.
  - Fails fast with `ValueSerializationError` if any field is missing or has the wrong type.[cite:10]
- This means **schema validation happens at runtime**, not just in tests.

This combination ensures that **in production** every event respects the same structure enforced in tests and the model/consumers see consistent data.[cite:8][cite:9][cite:10][cite:11]

---

## 4. Event Generation Logic (`producer.py`)

### 4.1 Generating a synthetic user event

`generate_event(user_id: UUID, session_id: UUID) -> Event` creates one realistic e‑commerce event.[cite:10]

Key behavior:

- Draws `event_type` randomly from `Events`.[cite:9][cite:10]
- Samples an `error_probability` uniformly between 0 and 0.5, then decides `has_error` from that.[cite:10]
- Returns a dict with fields aligned with the Avro schema:[cite:10]
  - `event_id`: fresh UUID per event.
  - `user_id`, `session_id`: stringified UUIDs (graph nodes or attributes for GNN).
  - `event_type`: one of the defined enums (categorical behaviour).
  - `event_timestamp`: `int(time.time() * 1000)` in UTC; compatible with Avro `timestamp-millis` and ClickHouse `DateTime64(3, 'UTC')`.
  - `request_latency_ms`: random integer between 50 and 1500 (performance signal).
  - `status`: `SUCCESS` or `ERROR` based on `has_error`.
  - `error_code`: random HTTP-like code 400–599 if error, else `None`.
  - `product_id`: random 1–10000 if `event_type` in `{VIEW_PRODUCT, ADD_TO_CART}`, else `None`.

This is **raw behavioural data** that downstream systems (Spark, GNN) use to infer fraud/anomalies.

### 4.2 Delivery report callback

`delivery_report(err, msg)` is registered as `on_delivery` in `producer.produce`:[cite:10]

- If `err` is not `None`, logs a structured error with:
  - topic, partition, key, error code, reason.[cite:10]
- If no error, it stays quiet (to avoid log noise).

This is important for **observing Kafka delivery issues** without overwhelming logs.

---

## 5. Producer Worker and Multi-Process Setup

### 5.1 Worker loop

`worker(worker_id: int, max_messages: int | None = None)` runs in each process:[cite:10]

1. Logs startup with worker ID and PID.[cite:10]  
2. Constructs a `SerializingProducer(PRODUCER_CONF)` to handle serialization and delivery.[cite:9][cite:10]  
3. Initializes a `user_id` and `session_id` for a current session.[cite:10]  
4. Enters a loop:
   - Increments `count` and generates `user_event = generate_event(user_id, session_id)`.[cite:10]
   - Every 1000 messages, logs throughput stats (messages, seconds, messages per second).[cite:10]
   - Calls `producer.produce(...)` with:
     - `topic=KAFKA_TOPIC`
     - `key=user_id`
     - `value=user_event`
     - `on_delivery=delivery_report`.[cite:10]
   - `producer.poll(0)` to serve callbacks.

5. Handles errors:
   - `BufferError`: producer buffer full → logs and `producer.poll(1)` to drain.[cite:10]
   - `ValueSerializationError`: Avro serialization failure (schema mismatch) → logs exception.[cite:10]
   - `KafkaException`: Kafka errors → logs exception.[cite:10]
   - Generic `Exception`: logs unexpected error and `producer.poll(5)` to recover.[cite:10]

6. Session dynamics:
   - With probability `NEW_USER_SESSION_PROBABILITY`, generates a new `user_id` and `session_id`, simulating a new user/session.[cite:9][cite:10]

7. Rate control:
   - `producer.poll(EVENT_INTERVAL_SECONDS)` to control pace (approx 100 events/s per worker).[cite:9][cite:10]

8. When `max_messages` is provided (used in tests):
   - After loop ends, flushes remaining messages and logs how many are still in queue until fully sent.[cite:10]

### 5.2 Multi-worker management

The `if __name__ == '__main__'` block:[cite:10]

- Sets up logging to stdout at INFO level.[cite:10]
- Spawns `NUM_WORKERS` processes, each running `worker(i+1)`.[cite:9][cite:10]
- Joins on all worker processes.
- On `KeyboardInterrupt`, logs shutdown and terminates workers gracefully.[cite:10]

This design allows scaling load and simulating **multiple independent producers** simply by changing `NUM_WORKERS`.

---

## 6. Single Kafka Topic and Consumers

The producer always writes to a **single Kafka topic**:[cite:9][cite:10]

- The topic name comes from `KAFKA_TOPIC` (environment variable).
- Every event is produced with `topic=KAFKA_TOPIC`.

Main downstream consumer:

- **Spark streaming job** (in `spark/`):
  - Subscribes to this topic.
  - Parses Avro‑encoded messages using the same schema.
  - Writes processed data to ClickHouse (and possibly MinIO) for Airflow ETAR pipelines and dashboarding.[cite:2]

In a more advanced setup, additional consumers (e.g., monitoring services, GNN scoring service) can also subscribe to the same raw topic without changing the producer.

---

## 7. Tests for the Producer (`producer/tests`)

The `producer/tests` folder contains tests that ensure both **correct event structure** and **correct integration with Kafka**.[cite:11]

### 7.1 Shared test setup

- `conftest.py`: pytest fixtures that:
  - Configure environment variables like `KAFKA_TOPIC` and `KAFKA_BOOTSTRAP_SERVERS`.
  - Provide mocked or test Kafka setups and producers.[cite:11]

### 7.2 Unit tests (`test_unit.py`)

Unit tests focus on pure Python logic and small functions:[cite:11][cite:10]

- `generate_event`:
  - Asserts all required fields exist and have expected types.
  - Checks that:
    - `status = ERROR` implies `error_code` is in 400–599.
    - `status = SUCCESS` implies `error_code` is `None`.
    - `product_id` is set only for `VIEW_PRODUCT` or `ADD_TO_CART`, and `None` otherwise.
- Other helper behavior and possibly `delivery_report` logging expectations.

These tests ensure that **event generation logic** is correct **before** any serialization or Kafka interaction.

### 7.3 Integration tests (`test_integration.py`)

Integration tests validate end‑to‑end behavior of the producer with Kafka (real or test cluster):[cite:11]

- Start a worker with `max_messages` set.
- Use a consumer to read messages from `KAFKA_TOPIC`.
- Check that:
  - Messages arrive on the correct topic.
  - Keys/values are present and correctly serialized.
  - Values conform to the Avro schema and match the structure returned by `generate_event`.

Together with Avro schema enforcement at runtime, these tests mean that **in production** each event follows the same validated structure as in tests.[cite:8][cite:9][cite:10][cite:11]

---

## 8. Relation to GNN-Based Fraud Detection

The current producer is deliberately **model‑agnostic**: it generates raw behavioural events without directly labeling them as fraudulent or not.[cite:9][cite:10]

### 8.1 Raw events as input to the GNN

For a fraud detection GNN, these events provide:

- **Nodes and edges**:
  - `user_id`, `session_id`, `product_id` can form nodes in a heterogeneous graph (users, sessions, products, merchants, etc.).
  - Events become edges or attributes connecting these nodes (e.g., user–session, user–product interactions).[cite:10]

- **Node/edge features**:
  - Categorical: `event_type`, `status`.
  - Numerical: `request_latency_ms`, timestamp, error codes.

This is exactly the kind of temporal relational data GNNs can exploit for anomaly and fraud detection.

### 8.2 Where the fraud signal should live

Instead of embedding fraud directly into the producer events, a clean design is:

1. The producer writes **raw events** to the Kafka topic (as now).  
2. A **downstream component** (Spark job or microservice) consumes these events, constructs the graph, and runs the GNN model.  
3. That component outputs **enriched events** with fraud information, such as:
   - `fraud_score`: real-valued score from the GNN (e.g., 0–1).
   - `is_fraud`: boolean label derived from `fraud_score` via a threshold.  

4. Enriched events are written to:
   - A new Kafka topic (e.g., `user_events_scored`) with an extended Avro schema including `fraud_score` and `is_fraud`, **or**
   - Directly into ClickHouse as an enriched table that Airflow and the dashboard can query.

This keeps a clear separation:

- **Producer**: simulates business activity; no assumptions about fraud.  
- **GNN service / Spark job**: infers fraud and enriches events.  
- **ClickHouse / dashboard**: stores and visualizes fraud risk and alerts.

You can describe this in your report as:

> “The producer generates raw user events with a fixed Avro schema. These events are ingested by our streaming layer and later enriched by a GNN-based fraud detection module, which adds fraud scores and flags in a downstream topic or table without changing the original event structure.”

---

## 9. Key Points to Highlight in Documentation / Viva

- The producer **does not send arbitrary JSON**; it sends **Avro-encoded events validated against `user_event_schema.avsc`**.[cite:8][cite:9][cite:10]
- Event generation logic (`generate_event`) is fully tested and then enforced again by the Avro serializer at runtime, so production events respect the same structure used in tests.[cite:10][cite:11]
- Traffic simulation is **realistic**: user sessions, product interactions, variable latency, and error patterns, with tunable rates and session turnover via `EVENT_INTERVAL_SECONDS` and `NEW_USER_SESSION_PROBABILITY`.[cite:9][cite:10]
- Kafka producer is robust: batching, compression, error handling, and delivery callbacks make it suitable as a realistic input source for streaming and GNN-based fraud analytics.[cite:9][cite:10]
- Fraud detection is integrated **downstream**: the GNN uses these raw events (and the graph they induce) to compute fraud scores and labels, which can be attached as a separate enrichment layer without modifying the original producer design.

