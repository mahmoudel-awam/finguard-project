from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("FinGuard-Fraud-Detector") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("sender_account", StringType()),
    StructField("receiver_account", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("sender_bank", StringType()),
    StructField("receiver_bank", StringType()),
    StructField("sender_country", StringType()),
    StructField("receiver_country", StringType()),
    StructField("transaction_type", StringType()),
    StructField("status", StringType())
])

df_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "finguard-kafka:9092") \
    .option("subscribe", "transactions_topic") \
    .option("startingOffsets", "latest") \
    .load()

df = df_raw \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

df_alerts = df.withColumn("alert",
    when(col("amount") > 50000, "MONEY LAUNDERING - مبلغ ضخم جداً")
    .when(col("transaction_type") == "fraud", "FRAUD DETECTED - احتيال")
    .when(col("transaction_type") == "money_laundering", "MONEY LAUNDERING - غسيل أموال")
    .when(col("transaction_type") == "suspicious", "SUSPICIOUS - مشبوه")
    .when(col("receiver_country").isin("Russia", "Nigeria"), "HIGH RISK COUNTRY")
    .otherwise("NORMAL - عادية")
)

query = df_alerts \
    .select("transaction_id", "amount", "sender_bank", "receiver_country", "alert") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()