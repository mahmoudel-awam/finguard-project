from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="postgres_pipeline",
    start_date=datetime(2026, 5, 11),
    schedule="@hourly",
    catchup=False,
    tags=["postgres", "finguard"]
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_fraud_table",
        conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS fraud_transactions (
            id SERIAL PRIMARY KEY,
            transaction_id VARCHAR(100),
            amount NUMERIC,
            prediction INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    insert_data = SQLExecuteQueryOperator(
        task_id="insert_sample_data",
        conn_id="postgres_default",
        sql="""
        INSERT INTO fraud_transactions
        (transaction_id, amount, prediction)
        VALUES
        ('TXN001', 5000, 1),
        ('TXN002', 150, 0);
        """
    )

    create_table >> insert_data