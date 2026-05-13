import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

# -------------------------
# الاتصال بـ PostgreSQL
# -------------------------
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="finguard",
    user="airflow",
    password="airflow"
)
cursor = conn.cursor()
print("✅ Connected to PostgreSQL")

# -------------------------
# الاتصال بـ Kafka
# -------------------------
consumer = KafkaConsumer(
    'transactions_topic',
    bootstrap_servers='localhost:29092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='finguard-consumer'
)
print("✅ Connected to Kafka")
print("👂 Listening for transactions...")
print("-" * 50)

# -------------------------
# القراءة والتخزين
# -------------------------
for message in consumer:
    tx = message.value

    try:
        # اكتب في transactions
        cursor.execute("""
            INSERT INTO transactions (
                transaction_id, timestamp, sender_account, receiver_account,
                amount, currency, sender_bank, receiver_bank,
                sender_country, receiver_country, transaction_type, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
        """, (
            tx['transaction_id'],
            tx['timestamp'],
            tx['sender_account'],
            tx['receiver_account'],
            tx['amount'],
            tx['currency'],
            tx['sender_bank'],
            tx['receiver_bank'],
            tx['sender_country'],
            tx['receiver_country'],
            tx['transaction_type'],
            tx['status']
        ))

        # لو مشبوهة اكتب في fraud_alerts
        if tx['transaction_type'] in ['fraud', 'suspicious', 'money_laundering']:
            cursor.execute("""
                INSERT INTO fraud_alerts (
                    transaction_id, alert_type, amount,
                    sender_account, receiver_account,
                    sender_bank, receiver_country
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                tx['transaction_id'],
                tx['transaction_type'],
                tx['amount'],
                tx['sender_account'],
                tx['receiver_account'],
                tx['sender_bank'],
                tx['receiver_country']
            ))

        conn.commit()
        print(f"✅ Saved: {tx['transaction_id']} | {tx['transaction_type'].upper()} | {tx['amount']} EGP")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")