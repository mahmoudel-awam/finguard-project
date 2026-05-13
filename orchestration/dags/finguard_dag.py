from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess

# -------------------------
# إعدادات الـ DAG
# -------------------------
default_args = {
    'owner': 'finguard',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# -------------------------
# المهام
# -------------------------
def check_kafka():
    """تتأكد إن Kafka شغال"""
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('kafka', 9092))
        sock.close()
        if result == 0:
            print("✅ Kafka is running!")
            return True
        else:
            raise Exception("❌ Kafka is not running!")
    except Exception as e:
        raise Exception(f"❌ Kafka check failed: {e}")

def generate_report():
    """بتعمل تقرير بسيط"""
    import json
    from datetime import datetime
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'status': 'FinGuard Pipeline Running',
        'message': 'All systems operational'
    }
    
    print(f"📊 FinGuard Report: {json.dumps(report, indent=2)}")
    return report

# -------------------------
# تعريف الـ DAG
# -------------------------
with DAG(
    dag_id='finguard_pipeline',
    default_args=default_args,
    description='FinGuard Financial Monitoring Pipeline',
    schedule_interval='*/5 * * * *',  # كل 5 دقايق
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['finguard', 'kafka', 'fraud'],
) as dag:

    # المهمة 1: تتأكد من Kafka
    task_check_kafka = PythonOperator(
        task_id='check_kafka',
        python_callable=check_kafka,
    )

    # المهمة 2: تعمل تقرير
    task_generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
    )

    # الترتيب: أول تتأكد من Kafka، بعدين تعمل تقرير
    task_check_kafka >> task_generate_report