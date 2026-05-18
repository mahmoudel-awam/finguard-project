# 🛡️ FinGuard — Real-Time Financial Fraud Detection Pipeline

> A production-grade streaming data pipeline that detects financial fraud in real time,
> built on the modern Big Data stack and fully containerised with Docker Compose.

![FinGuard System Architecture](dashboard/architecture.png)

---

## Architecture — Medallion Pattern

FinGuard follows the **Medallion Architecture** — a data design pattern that organises 
data into three progressive layers, each adding more structure and business value.

### 🥉 Bronze Layer — Raw Ingestion
> Raw data as-is from the source. No transformations, no filtering.

| Component | Technology | Role |
|---|---|---|
| Message Bus | Apache Kafka | Receives 1 transaction/sec on `transactions_topic` |
| Producer | Python | Generates synthetic Egyptian bank transactions |
| Schema Validation | Schema Registry | Enforces message format at ingest time |
| Coordination | Apache Zookeeper | Manages Kafka cluster membership |

### 🥈 Silver Layer — Processed & Enriched
> Cleaned, validated, and fraud-classified data ready for storage.

| Component | Technology | Role |
|---|---|---|
| Stream Processor | Apache Spark 3.5.1 | Reads Kafka stream, applies fraud rules |
| Detection Engine | fraud_detector.py | Classifies every transaction in real time |
| Trigger | every 5 seconds | Micro-batch processing window |

**Fraud Detection Rules (evaluated in priority order):**

| Priority | Condition | Classification |
|---|---|---|
| 1 | Amount > 50,000 EGP | MONEY LAUNDERING |
| 2 | Type = fraud | FRAUD DETECTED |
| 3 | Type = money_laundering | MONEY LAUNDERING |
| 4 | Type = suspicious | SUSPICIOUS |
| 5 | Receiver: Russia / Nigeria | HIGH RISK COUNTRY |
| 6 | All others | NORMAL |

### 🥇 Golden Layer — Analytics-Ready
> Aggregated, business-ready data for consumption and reporting.

| Component | Technology | Role |
|---|---|---|
| Relational Store | PostgreSQL | 5-table schema · fraud_alerts · daily_summary |
| Data Warehouse | Apache Hive | Batch analytics on HDFS · TEXTFILE format |
| NoSQL Store | Apache HBase | Low-latency lookups · CF: info · CF: alert |
| Dashboard | Power BI | Executive fraud detection dashboard |

---

## Dashboard Preview

![FinGuard Fraud Detection Dashboard](dashboard/dashboard.png)

> Connected directly to PostgreSQL `finguard` database. Shows live transaction counts,
> fraud type distribution, high-risk country analysis, and bank-level breakdown.

---

## Quick Start

```bash
git clone https://github.com/mahmoudel-awam/finguard-project.git
cd finguard-project/infrastructure
docker-compose up -d
```

Wait ~60 seconds, then:

```bash
# Start the transaction producer
python ingestion/transaction_producer.py

# Submit the Spark fraud detector
docker exec -it finguard-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/apps/fraud_detector.py
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Ingestion | Apache Kafka | 7.5 |
| Processing | Apache Spark | 3.5.1 |
| Storage (NoSQL) | Apache HBase | 1.4 |
| Storage (DW) | Apache Hive | 3.1.3 |
| Storage (Relational) | PostgreSQL | 15 |
| Orchestration | Apache Airflow | 2.8.1 |
| Visualisation | Power BI | Desktop |
| Infrastructure | Docker Compose | 12 services |

---

## Project Structure

```
ingestion/          Kafka producer — weighted risk distribution
                    70% Normal · 15% Suspicious · 10% Fraud · 5% Laundering

processing/         Spark Structured Streaming — core detection engine
                    readStream → from_json → withColumn alert → writeStream

consumers/          3 independent Kafka consumers (different group_ids)
                    PostgreSQL · HBase · Hive — zero interference

orchestration/      Airflow DAGs — health checks every 5 min
                    check_kafka >> generate_report · retries: 1

infrastructure/     Docker Compose — 12 services · finguard-net bridge
                    Custom Spark image with pre-loaded Kafka JARs

sql/                PostgreSQL schema — 5 tables with indexes and seed data
                    transactions · fraud_alerts · audit_log · 
                    high_risk_countries · daily_summary

docs/               Architecture diagram · dashboard screenshot · setup guide
```

---

## Services & Ports

| Service | External Port | UI |
|---|---|---|
| Kafka | 29092 | — |
| Spark Master | 7077 | localhost:8080 |
| HBase Thrift | 9090 | localhost:16010 |
| Hive JDBC | 10000 | localhost:10002 |
| Airflow | 8088 | localhost:8088 |
| HDFS NameNode | 9870 | localhost:9870 |
| PostgreSQL | 5432 | — |

---

## Author

**Mahmoud Ramdan** — Aspiring Data Engineer

Capstone project demonstrating end-to-end real-time pipeline design
following the Medallion Architecture pattern.
