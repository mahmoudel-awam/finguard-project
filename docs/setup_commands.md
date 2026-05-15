المرحلة 1: تشغيل الـ Infrastructure
الخطوة 1: تشغيل كل الـ Services
powershelldocker-compose up -d zookeeper kafka schema-registry
docker-compose up -d spark-master spark-worker
docker-compose up -d hbase hive airflow
التحقق إن كل حاجة شغالة:
powershelldocker ps
المفروض تشوف 8 containers:

 zookeeper
 kafka
 schema-registry
 spark-master
 spark-worker
 hbase
 hive
 airflow

-------------------------------
المرحلة 2: إعداد Kafka
إنشاء الـ Topic:
docker exec -it kafka kafka-topics --create --topic transactions_topic --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1

التحقق إن الـ Topic اتعمل:
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:9092

------------------------------
المرحلة 3: إعداد HBase
الدخول على HBase Shell:

docker exec -it hbase hbase shell

إنشاء الـ Table:
create 'transactions', 'info', 'alert'
exit

----------------------------
المرحلة 4: إعداد Hive
الدخول على Hive:

docker exec -it hive beeline -u jdbc:hive2://localhost:10000


إنشاء الـ Table:

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

الخروج:

!quit
------------------------------------

المرحلة 5: تشغيل الـ Scripts
محتاج 3 terminals في نفس الوقت:

Terminal 1 - Producer (بيولّد المعاملات): 
python transaction_producer.py

Terminal 2 - HBase Consumer (بيحفظ في HBase):  
python hbase_consumer.py

Terminal 3 - Hive Consumer (بيحفظ في Hive):  
python hive_consumer.py

-----------------------------------------
عشان تشغل spark (fraud_detector)

docker exec -it finguard-spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --jars /opt/spark/extra-jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/spark/extra-jars/kafka-clients-3.4.1.jar,/opt/spark/extra-jars/spark-token-provider-kafka-0-102.12-3.5.1.jar,/opt/spark/extra-jars/commons-pool2-2.11.1.jar `
  /opt/spark/work-dir/fraud_detector.py
