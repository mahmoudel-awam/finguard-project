# FinGuard — System Architecture

## Data Flow Diagram

```mermaid
flowchart TD
    subgraph INGESTION["⚡ Ingestion Layer"]
        P("🐍 transaction_producer.py\nSynthetic Egyptian Bank Transactions\n70% Normal · 15% Suspicious · 10% Fraud · 5% Laundering")
        ZK("🔧 Zookeeper\nCluster Coordination")
        K("📨 Apache Kafka\ntransactions_topic\n:9092 internal · :29092 external")
        SR("📋 Schema Registry\n:8081")
    end

    subgraph PROCESSING["⚙️ Processing Layer — Apache Spark 3.5.1"]
        SM("🔷 Spark Master\n:7077 · UI :8080")
        SW("🔷 Spark Worker\n2GB RAM · 2 Cores")
        FD("🔍 fraud_detector.py\nreadStream → from_json → withColumn alert\ntrigger every 5 seconds")
    end

    subgraph STORAGE["🗄️ Storage Layer"]
        HB("🟠 HBase\nTable: transactions\nCF: info · CF: alert\nThrift :9090")
        HV("🟡 Hive\nTable: transactions\nHDFS · JDBC :10000")
        PG("🐘 PostgreSQL\ndb: finguard\n5 Tables")
        HDFS("📁 HDFS\nNameNode + DataNode\n:9870")
    end

    subgraph CONSUMERS["🔄 Parallel Kafka Consumers"]
        C1("hbase_consumer.py\ngroup: hbase-group")
        C2("hive_consumer.py\ngroup: hive-group")
        C3("consumer.py\ngroup: pg-group")
    end

    subgraph ORCHESTRATION["🎯 Orchestration — Apache Airflow :8088"]
        DAG1("finguard_pipeline DAG\nEvery 5 minutes\ncheck_kafka → generate_report")
        DAG2("postgres_pipeline DAG\nEvery hour\nschema init · seed data")
        AIRDB("🐘 Airflow PostgreSQL\ndb: airflow\nDAG state · task logs")
    end

    P -->|"localhost:29092"| K
    ZK -->|"cluster mgmt"| K
    K --- SR

    K -->|"kafka:9092 INTERNAL"| FD
    K -->|"group: hbase-group"| C1
    K -->|"group: hive-group"| C2
    K -->|"group: pg-group"| C3

    SM --> SW
    SW --> FD

    FD -->|"foreachBatch"| HB
    C1 --> HB
    C2 -->|"CSV micro-batch\nevery 50 records"| HV
    C3 -->|"ON CONFLICT DO NOTHING"| PG
    HV --- HDFS

    DAG1 -->|"socket check :9092"| K
    DAG2 --> PG
    DAG1 & DAG2 --> AIRDB

    style INGESTION fill:#2a1f00,stroke:#BA7517,color:#f5f5f5
    style PROCESSING fill:#001a12,stroke:#0F6E56,color:#f5f5f5
    style STORAGE fill:#0d0d2b,stroke:#534AB7,color:#f5f5f5
    style CONSUMERS fill:#1a0808,stroke:#993C1D,color:#f5f5f5
    style ORCHESTRATION fill:#1a0a1a,stroke:#7B3FA0,color:#f5f5f5
    style P fill:#3d2800,stroke:#BA7517,color:#f5f5f5
    style K fill:#3d2800,stroke:#BA7517,color:#f5f5f5
    style ZK fill:#1a1a1a,stroke:#888,color:#f5f5f5
    style SR fill:#1a1a1a,stroke:#888,color:#f5f5f5
    style SM fill:#003322,stroke:#0F6E56,color:#f5f5f5
    style SW fill:#003322,stroke:#0F6E56,color:#f5f5f5
    style FD fill:#004d33,stroke:#1D9E75,color:#f5f5f5
    style HB fill:#2b1500,stroke:#E8740C,color:#f5f5f5
    style HV fill:#2b2200,stroke:#D4A800,color:#f5f5f5
    style PG fill:#001433,stroke:#185FA5,color:#f5f5f5
    style HDFS fill:#1a1a1a,stroke:#888,color:#f5f5f5
    style C1 fill:#2b0d00,stroke:#993C1D,color:#f5f5f5
    style C2 fill:#2b0d00,stroke:#993C1D,color:#f5f5f5
    style C3 fill:#2b0d00,stroke:#993C1D,color:#f5f5f5
    style DAG1 fill:#1a0a2b,stroke:#7B3FA0,color:#f5f5f5
    style DAG2 fill:#1a0a2b,stroke:#7B3FA0,color:#f5f5f5
    style AIRDB fill:#0d0d1a,stroke:#534AB7,color:#f5f5f5
```

---

## Engineering Logic — 3 Key Design Decisions

### 1. Why Three Independent Consumers Instead of One?
Each consumer has its own `group_id`, meaning Kafka delivers the same message to all three simultaneously with zero interference. This gives us **three different storage backends optimised for three different query patterns** — HBase for millisecond point lookups by `transaction_id`, Hive for heavy batch analytics across millions of records, and PostgreSQL for structured relational queries and reporting. One consumer failing never affects the others.

### 2. Why Spark Structured Streaming Instead of a Simple Loop?
A Python `while True` loop processes one message at a time on one machine. Spark's `readStream` distributes the workload across the entire cluster — every micro-batch is split across all worker cores in parallel. The `trigger(processingTime="5 seconds")` means Spark collects 5 seconds of transactions, processes them all simultaneously as a batch, then immediately starts the next window. This design scales horizontally — add more workers, get more throughput, zero code changes.

### 3. Why Two Kafka Ports (9092 and 29092)?
This is the most common Docker networking mistake in Kafka deployments. Services **inside** Docker (Spark, consumers) reach Kafka via the internal Docker bridge network on `kafka:9092`. Services **outside** Docker (the Python producer running on your laptop) cannot use that hostname, so Kafka exposes a second listener on `localhost:29092` mapped to the host machine. Without this dual-listener configuration, either the producer cannot publish or Spark cannot consume — never both working at the same time.

---

## Service Ports Reference

| Service | Internal Port | External Port | UI |
|---|---|---|---|
| Kafka | 9092 | 29092 | — |
| Zookeeper | 2181 | 2181 | — |
| Spark Master | 7077 | 7077 | :8080 |
| HBase Thrift | 9090 | 9090 | :16010 |
| Hive JDBC | 10000 | 10000 | :10002 |
| HDFS NameNode | 9000 | 9000 | :9870 |
| Airflow | 8080 | 8088 | :8088 |
| PostgreSQL (finguard) | 5432 | 5432 | — |
| PostgreSQL (airflow) | 5432 | 5433 | — |
