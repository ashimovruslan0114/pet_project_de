from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import psycopg2


DB_CONFIG = {
    "host": "postgres_dwh",
    "port": 5432,
    "dbname": "dwh",
    "user": "postgres",
    "password": "postgres",
}


def load_raw_to_stg_transactions():
    print("🚀 Start loading raw -> stg transactions")

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    try:
        with conn.cursor() as cursor:
            insert_sql = """
            INSERT INTO stg.transactions_stg (
                transaction_id,
                user_id,
                merchant_id,
                amount,
                currency,
                status,
                transaction_ts,
                source_system,
                load_date,
                load_ts
            )
            SELECT DISTINCT ON (transaction_id)
                r.transaction_id,
                r.user_id,
                r.merchant_id,
                r.amount,
                r.currency,
                r.status,
                r.transaction_ts,
                r.source_system,
                r.load_date,
                r.load_ts
            FROM raw.transactions_raw r
            WHERE r.transaction_id IS NOT NULL
            ORDER BY transaction_id, load_ts DESC
            ON CONFLICT (transaction_id) DO NOTHING;
            """

            cursor.execute(insert_sql)

            print(f"✅ Inserted rows into stg.transactions_stg: {cursor.rowcount}")

    except Exception as e:
        print(f"❌ Error during raw -> stg transactions load: {e}")
        raise

    finally:
        conn.close()
        print("🏁 Finished loading raw -> stg transactions")


with DAG(
    dag_id="raw_to_stg_transactions",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # manual режим
    catchup=False,
    tags=["stg", "transactions"],
) as dag:

    load_transactions = PythonOperator(
        task_id="load_raw_to_stg_transactions",
        python_callable=load_raw_to_stg_transactions,
    )
