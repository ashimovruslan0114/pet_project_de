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


# =============================
# LOAD REFERENCE IDS
# =============================

def load_reference_ids(cursor):
    cursor.execute("SELECT user_id FROM raw.users_raw")
    users = [str(r[0]) for r in cursor.fetchall()]

    cursor.execute("SELECT merchant_id FROM raw.merchants_raw")
    merchants = [str(r[0]) for r in cursor.fetchall()]

    return users, merchants


# =============================
# TRANSACTIONS
# =============================

def generate_transactions(cursor, users, merchants, n=50):
    rows = []

    for i in range(n):
        tx_id = str(uuid.uuid4())

        # edge: unknown user
        if users and random.random() > 0.1:
            user_id = random.choice(users)
        else:
            user_id = str(uuid.uuid4())

        # edge: unknown merchant
        if merchants and random.random() > 0.1:
            merchant_id = random.choice(merchants)
        else:
            merchant_id = str(uuid.uuid4())

        amount = random.choice([
            round(random.uniform(10, 500), 2),
            None,      # edge: null
            -50.0      # edge: negative
        ])

        currency = random.choice([
            "KZT",
            "USD",
            None       # edge: null currency
        ])

        status = random.choice([
            "success",
            "failed",
            "pending",
            "weird_status"  # edge
        ])

        tx_ts = random_ts()

        row = (
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

        rows.append(row)

        # 🔥 edge: дубликат transaction_id
        if i == 5:
            rows.append(row)

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


# =============================
# EVENTS
# =============================

def generate_events(cursor, n=40):
    cursor.execute("SELECT transaction_id FROM raw.transactions_raw")
    tx_ids = [str(r[0]) for r in cursor.fetchall()]

    rows = []

    for _ in range(n):
        event_id = str(uuid.uuid4())

        # edge: событие без транзакции
        if tx_ids and random.random() > 0.15:
            tx_id = random.choice(tx_ids)
        else:
            tx_id = str(uuid.uuid4())

        event_type = random.choice([
            "created",
            "authorized",
            "captured",
            "unknown_event"
        ])

        row = (
            event_id,
            tx_id,
            event_type,
            random_ts(),
            "{}",
            "generator",
            datetime.now().date(),
            datetime.now(),
        )

        rows.append(row)

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


# =============================
# REFUNDS
# =============================

def generate_refunds(cursor, n=20):
    cursor.execute("SELECT transaction_id FROM raw.transactions_raw")
    tx_ids = [str(r[0]) for r in cursor.fetchall()]

    rows = []

    for _ in range(n):
        refund_id = str(uuid.uuid4())

        # edge: refund без транзакции
        if tx_ids and random.random() > 0.15:
            tx_id = random.choice(tx_ids)
        else:
            tx_id = str(uuid.uuid4())

        refund_amount = random.choice([
            round(random.uniform(5, 200), 2),
            None
        ])

        status = random.choice([
            "success",
            "failed",
            "strange_status"
        ])

        row = (
            refund_id,
            tx_id,
            refund_amount,
            status,
            random_ts(),
            "generator",
            datetime.now().date(),
            datetime.now(),
        )

        rows.append(row)

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


# =============================
# MAIN
# =============================

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


