from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import happybase
import json

# -------------------------
# إنشاء Spark Session
# -------------------------
spark = SparkSession.builder \
    .appName("FinGuard-Fraud-Detector-HBase") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------
# شكل البيانات
# -------------------------
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

# -------------------------
# القراءة من Kafka
# -------------------------
df_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions_topic") \
    .option("startingOffsets", "latest") \
    .load()

df = df_raw \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# -------------------------
# قواعد الكشف
# -------------------------
df_alerts = df.withColumn("alert",
    when(col("amount") > 50000, "MONEY_LAUNDERING")
    .when(col("transaction_type") == "fraud", "FRAUD")
    .when(col("transaction_type") == "money_laundering", "MONEY_LAUNDERING")
    .when(col("transaction_type") == "suspicious", "SUSPICIOUS")
    .when(col("receiver_country").isin("Russia", "Nigeria"), "HIGH_RISK_COUNTRY")
    .otherwise("NORMAL")
)

# -------------------------
# دالة الحفظ في HBase
# -------------------------
def save_to_hbase(batch_df, batch_id):
    rows = batch_df.collect()  # جيب كل الصفوف
    
    if len(rows) == 0:
        return
    
    try:
        # اتصل بـ HBase
        connection = happybase.Connection('localhost', port=16000)
        table = connection.table('transactions')
        
        for row in rows:
            # الـ row key هو الـ transaction_id
            row_key = row['transaction_id'].encode('utf-8')
            
            # احفظ البيانات
            table.put(row_key, {
                b'info:timestamp': str(row['timestamp']).encode(),
                b'info:sender_account': str(row['sender_account']).encode(),
                b'info:receiver_account': str(row['receiver_account']).encode(),
                b'info:amount': str(row['amount']).encode(),
                b'info:sender_bank': str(row['sender_bank']).encode(),
                b'info:receiver_bank': str(row['receiver_bank']).encode(),
                b'info:receiver_country': str(row['receiver_country']).encode(),
                b'alert:type': str(row['alert']).encode(),
            })
            
            print(f"✅ Saved: {row['transaction_id']} | {row['alert']}")
        
        connection.close()
        
    except Exception as e:
        print(f"❌ HBase Error: {e}")

# -------------------------
# تشغيل الـ Stream
# -------------------------
query = df_alerts \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(save_to_hbase) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()