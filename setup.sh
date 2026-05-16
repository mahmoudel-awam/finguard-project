#!/bin/bash

echo "================================================"
echo "  FinGuard - Real-Time Fraud Detection Pipeline"
echo "================================================"

# 1. Create runtime folders that are gitignored
echo "[1/4] Creating runtime directories..."
mkdir -p spark-logs
mkdir -p checkpoints

# 2. Pull all Docker images
echo "[2/4] Pulling Docker images (this takes 3-5 minutes first time)..."
cd infrastructure
docker-compose pull

# 3. Build custom Spark image with Kafka JARs
echo "[3/4] Building custom Spark image..."
docker-compose build spark-master spark-worker

# 4. Start all services
echo "[4/4] Starting all 12 services..."
docker-compose up -d

echo ""
echo "Waiting 60 seconds for services to become healthy..."
sleep 60

# 5. Create Kafka topic
echo "Creating Kafka topic: transactions_topic..."
docker exec finguard-kafka kafka-topics \
  --create \
  --topic transactions_topic \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1

# 6. Create HBase table
echo "Creating HBase table..."
docker exec finguard-hbase hbase shell <<EOF
create 'transactions', 'info', 'alert'
exit
EOF

# 7. Create Hive table
echo "Creating Hive table..."
docker exec finguard-hive beeline -u jdbc:hive2://localhost:10000 <<EOF
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id STRING,
    ts STRING,
    sender_account STRING,
    receiver_account STRING,
    amount DOUBLE,
    currency STRING,
    sender_bank STRING,
    receiver_bank STRING,
    sender_country STRING,
    receiver_country STRING,
    transaction_type STRING,
    alert STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
!quit
EOF

echo ""
echo "================================================"
echo "  FinGuard is ready!"
echo ""
echo "  Airflow UI   →  http://localhost:8088"
echo "  Spark UI     →  http://localhost:8080"
echo "  HBase UI     →  http://localhost:16010"
echo "  HDFS UI      →  http://localhost:9870"
echo ""
echo "  Next step: python ingestion/transaction_producer.py"
echo "================================================"#!/bin/bash

echo "================================================"
echo "  FinGuard - Real-Time Fraud Detection Pipeline"
echo "================================================"

# 1. Create runtime folders that are gitignored
echo "[1/4] Creating runtime directories..."
mkdir -p spark-logs
mkdir -p checkpoints

# 2. Pull all Docker images
echo "[2/4] Pulling Docker images (this takes 3-5 minutes first time)..."
cd infrastructure
docker-compose pull

# 3. Build custom Spark image with Kafka JARs
echo "[3/4] Building custom Spark image..."
docker-compose build spark-master spark-worker

# 4. Start all services
echo "[4/4] Starting all 12 services..."
docker-compose up -d

echo ""
echo "Waiting 60 seconds for services to become healthy..."
sleep 60

# 5. Create Kafka topic
echo "Creating Kafka topic: transactions_topic..."
docker exec finguard-kafka kafka-topics \
  --create \
  --topic transactions_topic \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1

# 6. Create HBase table
echo "Creating HBase table..."
docker exec finguard-hbase hbase shell <<EOF
create 'transactions', 'info', 'alert'
exit
EOF

# 7. Create Hive table
echo "Creating Hive table..."
docker exec finguard-hive beeline -u jdbc:hive2://localhost:10000 <<EOF
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id STRING,
    ts STRING,
    sender_account STRING,
    receiver_account STRING,
    amount DOUBLE,
    currency STRING,
    sender_bank STRING,
    receiver_bank STRING,
    sender_country STRING,
    receiver_country STRING,
    transaction_type STRING,
    alert STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
!quit
EOF

echo ""
echo "================================================"
echo "  FinGuard is ready!"
echo ""
echo "  Airflow UI   →  http://localhost:8088"
echo "  Spark UI     →  http://localhost:8080"
echo "  HBase UI     →  http://localhost:16010"
echo "  HDFS UI      →  http://localhost:9870"
echo ""
echo "  Next step: python ingestion/transaction_producer.py"
echo "================================================"
