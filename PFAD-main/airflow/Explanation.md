# Airflow Orchestration in the PFAD Project

In this project, Apache Airflow is the **orchestration layer** that runs scheduled, near–real‑time analytics on top of the Kafka + Spark streaming backbone.[cite:2][cite:5] While Kafka and Spark Streaming handle continuous ingestion and real-time processing, Airflow periodically executes pipelines that extract data from ClickHouse, analyze it with Spark, and send summarized results to the dashboard API.[cite:2][cite:5] All Airflow-related assets live under `PFAD-main/airflow/`, including the custom Docker image, configuration, DAGs, and tests.[cite:3][cite:6]

---

## 1. Airflow Components

Main Airflow components in the repository:[cite:3][cite:4][cite:6][cite:7]

- `airflow/Dockerfile-Airflow`, `Dockerfile-Airflow-test`: custom images for runtime and testing.[cite:3]
- `airflow/config/airflow.cfg`: platform configuration (executor, scheduler, logging, DB).[cite:7]
- `airflow/dags/`:
  - `pipeline.py`: main ETAR DAG (`clickHouse_pyspark_dashboard`).[cite:5]
  - `spark.py`: auxiliary Spark-submit DAG.[cite:4]
- `airflow/tests/`: unit, integration, and end‑to‑end tests.[cite:6]

---

## 2. Main ETAR DAG: `clickHouse_pyspark_dashboard`

Defined in `airflow/dags/pipeline.py` using `@dag` and `@task` decorators.[cite:5]

### 2.1 DAG Definition and Scheduling

- `dag_id = 'clickHouse_pyspark_dashboard'`.[cite:5]
- Description: extract from ClickHouse, stream to MinIO, run Spark analysis, report to dashboard.[cite:5]
- `schedule = '* * * * *'` → runs **every minute**.[cite:5]
- `start_date = 2025‑08‑09` (UTC), `catchup = False` (no backfill).[cite:5]
- `default_args`:
  - `retries = 1`, `retry_delay = 3 seconds`.[cite:5]
  - `on_success_callback = on_success_callback_func`.[cite:5]
  - `on_failure_callback = on_failure_callback_func`.[cite:5]
- `max_active_runs = 2` to limit concurrent runs.[cite:5]

**Effect:** each run processes the **previous minute** of data and logs structured success/failure events.[cite:5]

### 2.2 Step 1 – Extract & Stream (`stream_from_clickhouse_to_minio`)

Task type: `@task` Python function.[cite:5]

1. **Connections and clients**  
   - Gets ClickHouse connection via `BaseHook.get_connection(CLICKHOUSE_CONN_NAME)`.[cite:5]  
   - Creates ClickHouse client with host, port, user, password, database.[cite:5]  
   - Gets MinIO connection `MINIO_CONN_NAME`.[cite:5]  
   - Creates `pyarrow.fs.S3FileSystem` pointing to MinIO endpoint with access/secret keys.[cite:5]

2. **Time window and file path**  
   - Receives `data_interval_start` from Airflow.[cite:5]  
   - Converts to `Asia/Tehran` timezone.[cite:5]  
   - Computes **previous minute**: `timestamp = data_interval_start - 1 minute` (second and microsecond set to 0).[cite:5]  
   - Formats `timestamp_str = 'YYYY-MM-DD_HH-MM'`.[cite:5]  
   - Builds Parquet path: `parquet_path = f'{MINIO_BUCKET_NAME}/{timestamp_str}.parquet'`.[cite:5]

3. **Query and streaming write**  
   - Reads `CLICKHOUSE_TABLE` from environment.[cite:5]  
   - SQL query:

     ```sql
     SELECT event_type, status
     FROM <CLICKHOUSE_TABLE>
     WHERE event_minute = <timestamp>;
     ```

   - Uses `clickhouse_client.query_df_stream` with parameters `{table, timestamp}` and streaming settings.[cite:5]  
   - Opens S3 output stream to `parquet_path`.[cite:5]  
   - Opens `pyarrow.parquet.ParquetWriter` with schema `event_type` (string), `status` (string).[cite:5]  
   - For each Pandas chunk:
     - If `df_chunk` empty: break.  
     - Accumulate `total_rows += len(df_chunk)`.  
     - Convert `df_chunk` to PyArrow Table and `writer.write_table(table)`. [cite:5]

4. **Error handling**  
   - Catches `ClickHouseError`, logs and re‑raises.[cite:5]  
   - Catches `S3Error` from MinIO, logs and re‑raises.[cite:5]  
   - Catches generic exceptions, logs and re‑raises.[cite:5]  
   - Closes ClickHouse client in `finally` block.[cite:5]

5. **Return value and cleanup**  
   - If `total_rows == 0`:
     - Logs warning “No data found for minute …”.[cite:5]  
     - Deletes `parquet_path` from MinIO.[cite:5]  
     - Returns `'s3a://' + parquet_path.replace('.parquet', '')`.[cite:5]  
   - Else:
     - Logs success with number of rows.[cite:5]  
     - Returns `'s3a://' + parquet_path`.[cite:5]

**Role:** Extracts previous minute events from ClickHouse and writes them as a Parquet file in MinIO, or signals an empty window.[cite:5]

### 2.3 Step 2 – Analyze with Spark (`spark_analysis`)

Task type: `SparkSubmitOperator`.[cite:5]

1. **Operator configuration**  
   - `task_id = 'spark_analysis'`.[cite:5]  
   - `conn_id = SPARK_CONN_NAME` (Airflow Spark connection).[cite:5]  
   - `application = SPARK_APPLICATION_PATH` (path to Spark app).[cite:5]  
   - `application_args = [file_path]` (output of Step 1).[cite:5]  
   - `deploy_mode = 'client'`.[cite:5]

2. **Spark configuration for MinIO (S3A)**  
   `conf` includes:[cite:5]

   - `spark.hadoop.fs.s3a.endpoint = {{ conn.MINIO_CONN_NAME.extra_dejson.get("host") }}`.[cite:5]  
   - `spark.hadoop.fs.s3a.access.key = {{ conn.MINIO_CONN_NAME.login }}`.[cite:5]  
   - `spark.hadoop.fs.s3a.secret.key = {{ conn.MINIO_CONN_NAME.password }}`.[cite:5]  
   - `spark.hadoop.fs.s3a.path.style.access = true`.[cite:5]  
   - `spark.hadoop.fs.s3a.impl = org.apache.hadoop.fs.s3a.S3AFileSystem`.[cite:5]  
   - `spark.hadoop.fs.s3a.connection.ssl.enabled = false`.[cite:5]  
   - `spark.eventLog.enabled = SPARK_EVENT_LOG_ENABLED` (from env).[cite:5]  
   - `spark.eventLog.dir = '/opt/airflow/logs/spark'`.[cite:5]

3. **Resources**  
   - `driver_memory = '512m'`.[cite:5]  
   - `executor_memory = '512m'`.[cite:5]  
   - `executor_cores = 2`.[cite:5]  
   - `num_executors = 2`.[cite:5]

**Role:** Reads the Parquet file from MinIO, runs Spark analysis (aggregations/anomaly detection), and writes a JSON report back to MinIO.[cite:5]

### 2.4 Step 3 – Report to Dashboard (`send_to_dashboard`)

Task type: `@task` Python function.[cite:5]

1. **File path normalization**  
   - Input: `file_path` from previous tasks (S3A path).[cite:5]  
   - If `'parquet'` in `file_path`: replace `'parquet'` with `'json'`.[cite:5]  
   - Else: append `'.json'`.[cite:5]  
   - Extract `file_name` as last path segment.[cite:5]

2. **MinIO read**  
   - Uses `get_minio_client()` to create a `Minio` client from `MINIO_CONN_NAME`.[cite:5]  
   - Calls `minio_client.get_object(bucket_name=MINIO_BUCKET_NAME, object_name=file_name)`.[cite:5]  
   - Reads the object body and parses JSON with `json.loads`.[cite:5]

3. **Dashboard API call**  
   - Sends `result` to `DASHBOARD_API_URL` via `requests.post(url=..., json=result)`.[cite:5]  
   - Calls `dashboard_response.raise_for_status()` to catch HTTP errors.[cite:5]

4. **Error handling**  
   - Catches `S3Error` → logs failure to fetch from MinIO and re‑raises.[cite:5]  
   - Catches `json.JSONDecodeError` → logs invalid JSON and re‑raises.[cite:5]  
   - Catches `requests.RequestException` → logs dashboard API failure and re‑raises.[cite:5]  
   - Catches generic exceptions, logs, and re‑raises.[cite:5]  
   - In `finally`:
     - Closes MinIO response and releases connection if it exists.[cite:5]

**Role:** Reads Spark’s analysis result from MinIO and pushes it into the dashboard API as JSON.[cite:5]

### 2.5 Task Dependency Chain

Final dependency wiring:[cite:5]

```text
stream_from_clickhouse_to_minio
    >> spark_analysis
    >> send_to_dashboard





End‑to‑end flow each minute:

ClickHouse (previous minute) → MinIO Parquet → Spark analysis → MinIO JSON → Dashboard API.[cite:5]

3. Auxiliary DAG: spark.py
Located at airflow/dags/spark.py, this DAG is a lighter Spark-submit workflow.[cite:4]

Uses one or more SparkSubmitOperator tasks.[cite:4]

Reuses SPARK_CONN_NAME and similar MinIO S3A configuration.[cite:4]

Intended for ad‑hoc Spark jobs, backfills, or experiments outside the main ETAR pipeline.[cite:4]

4. Airflow Test Suite
Tests live under airflow/tests/ and cover multiple levels.[cite:6][cite:5]

4.1 Shared Utilities
common.py:

Helpers for creating test DAGs and Airflow contexts.[cite:6]

Helpers for fake/minimal external clients and assertions.[cite:6]

conftest.py:

Pytest fixtures for environment variables, mocked ClickHouse/MinIO clients, Spark connections, and HTTP endpoints.[cite:6]

4.2 Unit Tests
test_callbacks.py:

Tests on_success_callback_func and on_failure_callback_func using fake Airflow contexts.[cite:6]

test_clients.py:

Tests get_minio_client and related helpers.[cite:6]

test_unit_stream.py:

Tests stream_from_clickhouse_to_minio in scenarios: data present, no data, ClickHouse error, MinIO error.[cite:6]

test_unit_report.py:

Tests send_to_dashboard for:

Correct JSON filename mapping.

Successful POST to the dashboard.

Handling JSON/HTTP errors.[cite:6]

test_unit_spark.py:

Validates that SparkSubmitOperator is created with correct application path, args, and conf.[cite:6]

4.3 Integration and E2E Tests
test_integration_stream.py:

Integration-style tests for streaming ClickHouse data to MinIO and writing Parquet.[cite:6]

test_integration_report.py:

Integration tests for reading JSON from MinIO and POSTing to a mocked dashboard API.[cite:6]

test_dag_integrity.py:

Confirms DAG imports, schedules, and structural integrity.[cite:6]

test_e2e.py:

End‑to‑end ETAR pipeline test (ClickHouse → MinIO → Spark → MinIO → Dashboard) with mocks.[cite:6][cite:5]

Result: Airflow logic is validated at unit, integration, and E2E levels, improving robustness.

5. Airflow Configuration (airflow.cfg)
Airflow is configured via airflow/config/airflow.cfg.[cite:7]

Key conceptual areas:

[core]

Executor type (e.g. LocalExecutor/CeleryExecutor).[cite:7]

dags_folder → where pipeline.py and spark.py are discovered.[cite:7]

Base log directories.[cite:7]

[scheduler]

Controls DAG directory scan interval and scheduling behavior.[cite:7]

Ensures clickHouse_pyspark_dashboard runs according to its cron schedule.

[webserver]

Web UI host, port, and auth settings.[cite:7]

Used to monitor DAGs, runs, and task logs.

[database]

sql_alchemy_conn for the metadata database (e.g. Postgres) storing DAG runs, task instances, and XComs.[cite:7]

[logging]

Log format and storage (local and optional remote).[cite:7]

Service endpoints and credentials (ClickHouse, MinIO, Spark, dashboard) are managed via Airflow Connections and environment variables, not hard-coded in airflow.cfg; DAG code accesses them through BaseHook.get_connection and os.environ.[cite:5][cite:7]

6. Overall Role of Airflow in PFAD
Airflow provides a production‑oriented orchestration layer that:[cite:2][cite:3][cite:5][cite:6][cite:7]

Runs a minute‑level ETAR DAG (clickHouse_pyspark_dashboard) linking ClickHouse, MinIO, Spark, and the dashboard API.

Offers a secondary Spark DAG (spark.py) for independent Spark workloads.

Uses Connections and environment variables to encapsulate external services.

Implements structured logging and error handling through callbacks and well‑designed tasks.

Is backed by a layered test suite and a custom airflow.cfg, making the orchestration robust, observable, and reproducible in Docker.

Together with Kafka and Spark, this Airflow layer ensures that fraud analytics and other insights are produced and delivered to the dashboard reliably, transparently, and repeatably.

text

If you want, the next step could be a super short 4–5 line version of this for your slides; which section do you want compressed first?
