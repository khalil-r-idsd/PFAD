# Spark Component in PFAD

This document summarizes the **Spark layer** in the PFAD project: its role in the pipeline, how it is tested, how it is deployed from Docker/Airflow, and how parallelism and schemas are handled end‑to‑end.[cite:2][cite:5][cite:10][cite:12][cite:14][cite:15]

---

## 1. Spark’s Role in the Overall Pipeline

In the global architecture, Spark is the **analytics engine** between storage and orchestration.[cite:2][cite:5][cite:15]

High-level flow:

> Producer → Kafka → ClickHouse → MinIO Parquet → **Spark job** → MinIO JSON → Airflow ETAR DAG → Dashboard[cite:5][cite:14][cite:15]

- **Producer**: sends Avro events (user_id, session_id, event_type, status, error_code, product_id, timestamp, latency) to Kafka topic `KAFKA_TOPIC`.[cite:9][cite:10]
- **ClickHouse + MinIO**: downstream components aggregate events per minute and export them as Parquet files into `MINIO_BUCKET_NAME` in MinIO.[cite:2][cite:5][cite:15]
- **Spark**:
  - Reads a given Parquet file (`s3a://<bucket>/YYYY-MM-DD_HH-MM.parquet`) from MinIO.[cite:14]
  - Computes aggregated statistics:
    - `total_events`
    - `total_errors`
    - `by_event_type` (SUCCESS / ERROR counts per event type).[cite:14]
  - Writes a JSON report back to MinIO as `YYYY-MM-DD_HH-MM.json` with a top-level `report` field.[cite:14]
- **Airflow**:
  - The ETAR DAG `clickHouse_pyspark_dashboard` calls Spark via `SparkSubmitOperator`, then reads the JSON file from MinIO and sends it to the dashboard API endpoint.[cite:5][cite:6][cite:14]

Spark is therefore the **“Analyze & Summarize”** step in the end‑to‑end fraud analytics pipeline.

---

## 2. How Spark Is Deployed (Docker + Airflow)

Spark is packaged and run as its own service in the Docker environment:

- `spark/Dockerfile-Spark` builds a Spark image that:
  - Installs the necessary Spark version and Python dependencies for the project.
  - Copies the Spark application code (including the module that defines `analyze_events` and `main`) into the image.[cite:12][cite:15]
- `docker-compose.yml` defines:
  - A `spark-master` service and one or more `spark-worker` services forming a small Spark cluster.
  - A service for running Spark applications using the `spark` image built from `Dockerfile-Spark`.[cite:12][cite:15]
- The Airflow ETAR DAG (`PFAD-main/airflow/dags/dashboard_etar.py`) configures a `SparkSubmitOperator` that:
  - Points to this cluster (`spark://spark-master:7077`).
  - Uses `SPARK_APPLICATION_PATH` (set via env) to locate the Spark app entrypoint (e.g. `spark.py` inside the container).
  - Sets cluster resources: `num_executors`, `executor_cores`, `driver_memory`, `executor_memory`.[cite:5][cite:6][cite:12]

So you can say: *“The Spark job is packaged as a Docker image and submitted from Airflow to a dedicated Spark cluster, with resources and application path controlled via environment variables and `SparkSubmitOperator`.”*

---

## 3. End‑to‑End Schema Consistency

A key design choice is that the **same logical schema** is respected across producer, storage, and Spark:

- **Producer Avro schema** (`producer/user_event_schema.avsc`):
  - Defines fields like `event_id`, `user_id`, `session_id`, `event_type`, `event_timestamp` (timestamp-millis), `request_latency_ms`, `status`, `error_code`, `product_id`.[cite:8][cite:9][cite:10]
  - Avro serializer (`avro_serializer` in `producer/schema_registry.py`) enforces this schema before sending messages to Kafka.[cite:9][cite:10]
- **ClickHouse / MinIO**:
  - Tables and export logic in `db/` and Airflow use columns that mirror those Avro fields when writing Parquet to MinIO.[cite:2][cite:5][cite:15]
- **Spark tests schema** (`SCHEMA` in `spark/tests/test_spark.py`):
  - Uses PyArrow fields with the same names and compatible types (strings for ids, timestamp with ms precision and timezone, ints for latency/error_code/product_id, nullable where appropriate).[cite:14]

This means:

- Any event that passes Avro validation at the producer will be compatible with ClickHouse tables.
- When exported as Parquet to MinIO, the same field names and types are preserved.
- Spark’s `analyze_events` reads Parquet that matches its expected schema, so there is no schema mismatch between producer, DB, MinIO, and Spark.[cite:8][cite:9][cite:10][cite:14][cite:15]

You can emphasize this as **end‑to‑end schema consistency**, which is important for reliability and for future GNN features.

---

## 4. Spark Tests: What the Job Must Do

All Spark behavior is specified by `spark/tests/test_spark.py`.[cite:12][cite:14]

### 4.1 Common constants and schema

At the top of `test_spark.py`:[cite:14]

- `MINIO_BUCKET_NAME`: MinIO bucket used for test Parquet and JSON objects.
- `EVENTS = {'VIEW_PRODUCT', 'ADD_TO_CART', 'CHECKOUT', 'PAYMENT', 'SEARCH'}`.
- `NUM_ERROR = 3`, `NUM_SUCCESS = 17`.
- `SCHEMA`: PyArrow schema mapping exactly to the event structure: ids, event_type, timestamp (`ms`, tz `'Asia/Tehran'`), latency, status, `error_code`, `product_id`.[cite:14]

### 4.2 SparkSession fixture

The `spark` fixture creates a real `SparkSession`:

- `master='spark://spark-master:7077'` → same master as in Docker.[cite:14][cite:15]
- S3A configs (endpoint `http://minio:9000`, credentials, path-style, no SSL) to talk to MinIO.[cite:14]

This validates Spark + MinIO integration with the same settings used in the cluster.

### 4.3 MinIO client fixture

`minio_client` creates a real `Minio` client (endpoint `minio:9000` with `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`) for uploading Parquet and reading JSON in tests.[cite:14]

---

## 5. Test Data: Fake Parquet ≈ Real Events

### 5.1 Non‑empty Parquet file

The `parquet_file` fixture builds controlled synthetic data:[cite:14]

- Uses timestamp `2025-01-15 10:00` → `object_name = '2025-01-15_10-00.parquet'`.
- For each event type in `EVENTS`:
  - `NUM_ERROR` rows with:
    - `status='ERROR'`, `error_code=500`, `product_id=1000` for VIEW_PRODUCT/ADD_TO_CART, else `None`.
  - `NUM_SUCCESS` rows with:
    - `status='SUCCESS'`, `error_code=None`, same product_id rule.[cite:14]
- Writes this as Parquet with `SCHEMA` and uploads it to `MINIO_BUCKET_NAME`.
- Yields `s3a://<bucket>/2025-01-15_10-00.parquet`, then deletes the object afterwards.[cite:14]

This gives you a realistic dataset with **known counts** per event_type and status.

### 5.2 Empty Parquet file

In the empty-file test, an empty DataFrame (no rows, same columns) is written as `empty-test.parquet` and uploaded to MinIO; its S3 path is passed into `analyze_events`.[cite:14]

---

## 6. Expected Behavior of `analyze_events`

`analyze_events(spark, file_path)` is the core Spark function under test.[cite:14]

### 6.1 Non‑empty data

`test_spark_analyze_events_with_data` requires that:

- `result['total_events'] == len(EVENTS) * (NUM_ERROR + NUM_SUCCESS)`.[cite:14]
- `result['total_errors'] == len(EVENTS) * NUM_ERROR`.[cite:14]
- For each `event_type`:
  - `stats['SUCCESS'] == NUM_SUCCESS`.
  - `stats['ERROR'] == NUM_ERROR`.[cite:14]

So `analyze_events` must:

1. Use Spark to read the Parquet file from MinIO.
2. Compute:
   - total number of events.
   - total number of error events.
   - a dict `by_event_type` with per‑event-type SUCCESS/ERROR counts.

### 6.2 Empty data

`test_spark_analyze_events_empty_file` requires that:

- `result['total_events'] == 0`.
- `result['total_errors'] == 0`.
- `result['by_event_type'] == {}`.[cite:14]

So if the Parquet file exists but contains no rows, `analyze_events` must return zeros and an empty dict, not crash.

---

## 7. Expected Behavior of `main()` (Spark Entrypoint)

The Spark application also exposes a `main()` function, which Airflow calls via SparkSubmit.[cite:5][cite:6][cite:14]

### 7.1 With a Parquet file present

`test_spark_main_with_data`:

- Mocks `sys.argv` as `['spark.py', parquet_file]`.
- Calls `main()` and expects `SystemExit(0)`.
- Derives JSON object name from the Parquet filename by replacing `.parquet` with `.json`.
- Reads that JSON from MinIO and checks that `report.total_events` and `report.total_errors` match the expectations from `parquet_file`.[cite:14]

So `main()` must:

1. Read the S3A path from command-line arguments.
2. Call `analyze_events`.
3. Wrap the result into a JSON object under `report`.
4. Write `YYYY-MM-DD_HH-MM.json` to MinIO in the same bucket.
5. Exit with code 0.[cite:14]

### 7.2 When no Parquet exists

`test_spark_main_no_data`:

- Uses `s3a://<bucket>/2025-01-15_11-00` (no `.parquet` behind it).
- Calls `main()` and still expects `SystemExit(0)`.
- Looks for `2025-01-15_11-00.json` in MinIO and checks:

  - `result_data['report'] == 'No data for 2025-01-15_11-00.'`.[cite:14]

So if no Parquet exists for that minute, `main()` must still succeed and produce a JSON with a friendly “No data for …” message.

---

## 8. Parallelism and In‑Memory Processing

### 8.1 Producer + Kafka (upstream)

From producer code and Kafka config:[cite:2][cite:9][cite:10]

- `NUM_WORKERS` controls how many OS processes run `worker()` in `producer.py`.
- Each worker has a `SerializingProducer` instance and produces events in parallel.[cite:10]
- Kafka topic `KAFKA_TOPIC` is partitioned; messages use `key=user_id`, so all events of one user land in the same partition but users are distributed across partitions for parallel consumption.[cite:9][cite:2]
- `batch.size`, `linger.ms`, `compression.type='snappy'` in `PRODUCER_CONF` configure in‑memory buffering and batching before sending to Kafka.[cite:9]

### 8.2 Spark cluster

From the Airflow DAG’s `SparkSubmitOperator` configuration:[cite:5][cite:6]

- `num_executors = 2`, `executor_cores = 2` → up to 4 cores processing tasks in parallel.
- `driver_memory = '512m'`, `executor_memory = '512m'`.
- Spark reads Parquet from MinIO via S3A and keeps working data in distributed DataFrames in memory on executors.[cite:14][cite:15]

### 8.3 Airflow scheduling

From `clickHouse_pyspark_dashboard` DAG:[cite:5][cite:6]

- `schedule='* * * * *'` → runs once per minute.
- `max_active_runs=2` → up to two overlapping DAG runs.

This yields **time‑window parallelism**:

- Each DAG run handles one minute’s Parquet → Spark → JSON.
- Multiple runs can overlap if previous ones are still running.

---

## 9. Relation to GNN Fraud Detection

The Spark component, as currently written and tested, focuses on **aggregated statistics** but already uses the same event structure the GNN will need:

- Entities and attributes from producer events:
  - `user_id`, `session_id`, `product_id`, `event_type`, `status`, `error_code`, `request_latency_ms`, `event_timestamp`.[cite:10][cite:14]
- These can define nodes (users, sessions, products) and edges (events) in a graph for GNN-based fraud detection.

Possible evolution:

1. Use Spark to build graph features (e.g. edges user–product, user–session, per-user error patterns) from Parquet exports.
2. Feed these features into a GNN model (inside Spark or via a separate microservice).
3. Write fraud metrics (`fraud_score`, `is_fraud`) back to MinIO/ClickHouse or a new Kafka topic following the same pattern:
   - read → compute → write JSON or tables.

Because schemas are consistent end‑to‑end, adding GNN outputs (new columns or JSON fields) becomes an additive change, not a rewrite.

---

## 10. Key Phrases for Your Report / Viva

You can describe the Spark component with phrases like:

- “Spark is packaged as a Dockerized analytics job, submitted from Airflow to a dedicated Spark cluster, reading Parquet from MinIO and writing JSON summaries per minute.”[cite:5][cite:12][cite:15]
- “The behavior of the Spark job is fully specified by integration tests using a real SparkSession and MinIO: they verify `analyze_events` and the CLI `main()` for windows with data, empty files, and no files at all.”[cite:12][cite:14]
- “Schemas are consistent end‑to‑end: Avro at the producer, ClickHouse tables, MinIO Parquet, and Spark’s Arrow schema all share the same fields and types, which simplifies evolution and makes it safe to add GNN-based fraud scoring on top.”[cite:8][cite:9][cite:10][cite:14][cite:15]

