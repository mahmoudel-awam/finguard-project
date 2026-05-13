from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FinGuard-Kafka-Test") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# نطبع الـ messages مباشرة بدون groupBy
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()