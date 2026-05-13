import json      # بيحول البيانات لـ JSON (نص منظم)
import random    # بيولّد أرقام عشوائية
import time      # بيتحكم في الوقت (الانتظار بين المعاملات)
from datetime import datetime  # بيجيب التاريخ والوقت الحالي
from kafka import KafkaProducer  # المكتبة اللي بتتكلم مع Kafka

# -------------------------
# الاتصال بـ Kafka
# -------------------------
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 5, 0)
)

# -------------------------
# البيانات الوهمية
# -------------------------
banks = ['CIB', 'NBE', 'QNB', 'HSBC', 'Banque Misr']  # بنوك مصرية
countries = ['Egypt', 'UAE', 'Saudi', 'USA', 'Russia', 'Nigeria']  # دول مختلفة

# -------------------------
# دالة توليد المعاملة
# -------------------------
def generate_transaction():
    
    # بيقرر نوع المعاملة بنسب معينة
    transaction_type = random.choices(
        ['normal', 'suspicious', 'fraud', 'money_laundering'],
        weights=[70, 15, 10, 5]  # 70% عادية، 15% مشبوهة، 10% احتيال، 5% غسيل
    )[0]
    
    # بيحدد المبلغ حسب نوع المعاملة
    if transaction_type == 'normal':
        amount = round(random.uniform(100, 5000), 2)       # عادية: 100 - 5000
    elif transaction_type == 'suspicious':
        amount = round(random.uniform(8000, 15000), 2)     # مشبوهة: 8000 - 15000
    elif transaction_type == 'fraud':
        amount = round(random.uniform(500, 3000), 2)       # احتيال: 500 - 3000
    else:
        amount = round(random.uniform(50000, 500000), 2)   # غسيل: 50000 - 500000
    
    # تفاصيل المعاملة الكاملة
    transaction = {
        'transaction_id': f'TXN-{random.randint(100000, 999999)}',  # رقم تعريفي فريد
        'timestamp': datetime.now().isoformat(),                      # وقت المعاملة
        'sender_account': f'ACC-{random.randint(1000, 9999)}',       # حساب المرسل
        'receiver_account': f'ACC-{random.randint(1000, 9999)}',     # حساب المستقبل
        'amount': amount,                                              # المبلغ
        'currency': 'EGP',                                            # العملة
        'sender_bank': random.choice(banks),                          # بنك المرسل
        'receiver_bank': random.choice(banks),                        # بنك المستقبل
        'sender_country': 'Egypt',                                    # بلد المرسل
        'receiver_country': random.choice(countries),                 # بلد المستقبل
        'transaction_type': transaction_type,                         # نوع المعاملة
        'status': 'pending'                                           # الحالة: في الانتظار
    }
    
    return transaction

# -------------------------
# تشغيل الـ Producer
# -------------------------
print("🚀 FinGuard Producer Started...")
print("📤 Sending transactions to Kafka...")
print("-" * 50)

count = 0
while True:
    transaction = generate_transaction()
    
    producer.send('transactions_topic', value=transaction)
    producer.flush()  # تأكد إن الـ message اتبعت فعلاً
    
    count += 1
    
    print(f"[{count}] {transaction['transaction_type'].upper():20} | "
          f"Amount: {transaction['amount']:>10} EGP | "
          f"From: {transaction['sender_bank']:>12} → {transaction['receiver_bank']} | "
          f"To: {transaction['receiver_country']}")
    
    time.sleep(1)  # استنى ثانية وبعدين ولّد معاملة تانية