import random
import uuid
from datetime import datetime, timedelta

import psycopg2


DB_CONFIG = {
    "host": "postgres_dwh",
    "port": 5432,
    "dbname": "dwh",
    "user": "postgres",
    "password": "postgres",
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def random_ts(days_back=30):
    return datetime.now() - timedelta(days=random.randint(0, days_back))


def load_reference_ids(cursor):
    cursor.execute("SELECT user_id FROM raw.users_raw")
    users = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT merchant_id FROM raw.merchants_raw")
    merchants = [r[0] for r in cursor.fetchall()]

    return users, merchants


def generate_transactions(cursor, users, merchants, n=50):
    rows = []

    for i in range(n):
        tx_id = str(uuid.uuid4())

        user_id = random.choice(users + [999999])  # edge: unknown user
        merchant_id = random.choice(merchants + [888888])  # edge

        amount = random.choice([
            round(random.uniform(10, 500), 2),
            None,          # edge: null amount
            -50.0          # edge: negative
        ])

        currency = random.choice(["KZT", "USD", None])  # edge null

        status = random.choice([
            "success",
            "failed",
            "pending",
            "weird_status"  # edge
        ])

        tx_ts = random_ts()

        rows.append(
            (
                tx_id,
                user_id,
                merchant_id,
                amount,
                currency,
                status,
                tx_ts,
                "generator",
                datetime.now().date(),
                datetime.now(),
            )
        )

        # 🔥 edge: дубликат transaction_id (редко)
        if i == 5:
            rows.append(rows[-1])

    insert_sql = """
        INSERT INTO raw.transactions_raw (
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
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cursor.executemany(insert_sql, rows)
    print(f"Inserted transactions: {len(rows)}")


def generate_events(cursor, n=40):
    cursor.execute("SELECT transaction_id FROM raw.transactions_raw")
    tx_ids = [r[0] for r in cursor.fetchall()]

    rows = []

    for i in range(n):
        event_id = str(uuid.uuid4())

        tx_id = random.choice(tx_ids + ["missing_tx"])  # edge

        event_type = random.choice([
            "created",
            "authorized",
            "captured",
            "unknown_event"  # edge
        ])

        event_ts = random_ts()

        rows.append(
            (
                event_id,
                tx_id,
                event_type,
                event_ts,
                "{}",
                "generator",
                datetime.now().date(),
                datetime.now(),
            )
        )

    insert_sql = """
        INSERT INTO raw.transaction_events_raw (
            event_id,
            transaction_id,
            event_type,
            event_ts,
            details,
            source_system,
            load_date,
            load_ts
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cursor.executemany(insert_sql, rows)
    print(f"Inserted events: {len(rows)}")


def generate_refunds(cursor, n=20):
    cursor.execute("SELECT transaction_id FROM raw.transactions_raw")
    tx_ids = [r[0] for r in cursor.fetchall()]

    rows = []

    for _ in range(n):
        refund_id = str(uuid.uuid4())

        tx_id = random.choice(tx_ids + ["missing_tx"])  # edge

        refund_amount = random.choice([
            round(random.uniform(5, 200), 2),
            None  # edge
        ])

        status = random.choice([
            "success",
            "failed",
            "strange_status"  # edge
        ])

        refund_ts = random_ts()

        rows.append(
            (
                refund_id,
                tx_id,
                refund_amount,
                status,
                refund_ts,
                "generator",
                datetime.now().date(),
                datetime.now(),
            )
        )

    insert_sql = """
        INSERT INTO raw.refunds_raw (
            refund_id,
            transaction_id,
            refund_amount,
            status,
            refund_ts,
            source_system,
            load_date,
            load_ts
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cursor.executemany(insert_sql, rows)
    print(f"Inserted refunds: {len(rows)}")


def main():
    conn = get_connection()
    conn.autocommit = True

    try:
        with conn.cursor() as cursor:
            users, merchants = load_reference_ids(cursor)

            generate_transactions(cursor, users, merchants)
            generate_events(cursor)
            generate_refunds(cursor)

        print("✅ Data generation completed")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
