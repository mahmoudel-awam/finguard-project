import json
import csv
import os
from kafka import KafkaConsumer
from datetime import datetime

# -------------------------
# الاتصال بـ Kafka
# -------------------------
consumer = KafkaConsumer(
    'transactions_topic',
    bootstrap_servers='localhost:29092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='hive-consumer-group'
)

print("✅ Connected to Kafka!")
print("📥 Saving transactions to Hive...")
print("-" * 50)

# -------------------------
# قواعد الكشف
# -------------------------
def detect_alert(transaction):
    amount = transaction['amount']
    tx_type = transaction['transaction_type']
    country = transaction['receiver_country']
    
    if amount > 50000:
        return "MONEY_LAUNDERING"
    elif tx_type == "fraud":
        return "FRAUD"
    elif tx_type == "money_laundering":
        return "MONEY_LAUNDERING"
    elif tx_type == "suspicious":
        return "SUSPICIOUS"
    elif country in ["Russia", "Nigeria"]:
        return "HIGH_RISK_COUNTRY"
    else:
        return "NORMAL"

# -------------------------
# الحفظ في ملف CSV
# -------------------------
output_file = "transactions_hive.csv"
count = 0

with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    # إضافة الـ headers
    writer.writerow([
        'transaction_id', 'timestamp', 'sender_account', 'receiver_account',
        'amount', 'currency', 'sender_bank', 'receiver_bank',
        'sender_country', 'receiver_country', 'transaction_type', 'alert'
    ])
    
    for message in consumer:
        transaction = message.value
        alert = detect_alert(transaction)
        
        writer.writerow([
            transaction['transaction_id'],
            transaction['timestamp'],
            transaction['sender_account'],
            transaction['receiver_account'],
            transaction['amount'],
            transaction['currency'],
            transaction['sender_bank'],
            transaction['receiver_bank'],
            transaction['sender_country'],
            transaction['receiver_country'],
            transaction['transaction_type'],
            alert
        ])
        f.flush()  # احفظ فوراً
        
        count += 1
        print(f"[{count}] Saved: {transaction['transaction_id']} | "
              f"{transaction['amount']:>10} EGP | {alert}")
        
        # كل 50 معاملة ارفع الملف لـ Hive
        if count % 50 == 0:
            os.system(f'docker exec -i hive beeline -u jdbc:hive2://localhost:10000 '
                     f'-e "LOAD DATA LOCAL INPATH \'/tmp/transactions_hive.csv\' '
                     f'OVERWRITE INTO TABLE transactions;"')
            print(f"✅ Loaded {count} transactions to Hive!")