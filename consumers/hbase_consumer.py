import happybase
import json
from kafka import KafkaConsumer

# -------------------------
# الاتصال بـ HBase
# -------------------------
try:
    connection = happybase.Connection('localhost', port=9090)
    table = connection.table('transactions')
    print("✅ Connected to HBase!")
except Exception as e:
    print(f"❌ HBase Connection Error: {e}")
    exit()

# -------------------------
# الاتصال بـ Kafka
# -------------------------
consumer = KafkaConsumer(
    'transactions_topic',
    bootstrap_servers='localhost:29092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='hbase-consumer-group'
)

print("✅ Connected to Kafka!")
print("📥 Saving transactions to HBase...")
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
# الحفظ في HBase
# -------------------------
count = 0
for message in consumer:
    transaction = message.value
    alert = detect_alert(transaction)
    
    row_key = transaction['transaction_id'].encode('utf-8')
    
    table.put(row_key, {
        b'info:timestamp': str(transaction['timestamp']).encode(),
        b'info:sender_account': str(transaction['sender_account']).encode(),
        b'info:receiver_account': str(transaction['receiver_account']).encode(),
        b'info:amount': str(transaction['amount']).encode(),
        b'info:sender_bank': str(transaction['sender_bank']).encode(),
        b'info:receiver_bank': str(transaction['receiver_bank']).encode(),
        b'info:receiver_country': str(transaction['receiver_country']).encode(),
        b'alert:type': alert.encode(),
    })
    
    count += 1
    print(f"[{count}] Saved: {transaction['transaction_id']} | "
          f"{transaction['amount']:>10} EGP | {alert}")