from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='customer_orders_taskflow',
    default_args=default_args,
    description='Fusion CSV + PostgreSQL avec TaskFlow API',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def customer_orders_pipeline():

    @task()
    def load_orders_to_postgres():
        df = pd.read_csv('/opt/airflow/data/bdd_tp_2/orders.csv')
        hook = PostgresHook(postgres_conn_id='postgres_default')
        hook.run("""
            CREATE TABLE IF NOT EXISTS orders (
                customer_id INT,
                order_id INT,
                amount FLOAT
            );
            DELETE FROM orders;
        """)
        rows = [tuple(row) for row in df.itertuples(index=False)]
        hook.insert_rows(table='orders', rows=rows)
        print(f"{len(rows)} lignes insérées dans orders ✅")

    @task()
    def extract_csv():
        df = pd.read_csv('/opt/airflow/data/bdd_tp_2/customers_data.csv')
        print(f"CSV chargé : {len(df)} lignes")
        return df.to_json()      # ← return au lieu de xcom_push

    @task()
    def extract_postgres():
        hook = PostgresHook(postgres_conn_id='postgres_default')
        df = hook.get_pandas_df("SELECT * FROM orders;")
        print(f"Orders chargés : {len(df)} lignes")
        return df.to_json()      # ← return au lieu de xcom_push

    @task()
    def transform(customers_json, orders_json):   # ← reçoit directement
        customers = pd.read_json(customers_json)
        orders = pd.read_json(orders_json)
        merged = customers.merge(orders, on='customer_id')
        merged['discount'] = merged['amount'] * 0.1
        print(f"Données fusionnées : {len(merged)} lignes")
        return merged.to_json()  # ← return au lieu de xcom_push

    @task()
    def insert_result(merged_json):               # ← reçoit directement
        merged = pd.read_json(merged_json)
        hook = PostgresHook(postgres_conn_id='postgres_default')
        hook.run("""
            CREATE TABLE IF NOT EXISTS customer_orders (
                customer_id INT,
                name VARCHAR(50),
                age INT,
                city VARCHAR(50),
                order_id INT,
                amount FLOAT,
                discount FLOAT
            );
            DELETE FROM customer_orders;
        """)
        rows = [tuple(row) for row in merged.itertuples(index=False)]
        hook.insert_rows(table='customer_orders', rows=rows)
        print(f"{len(rows)} lignes insérées ✅")
        merged.to_csv('/opt/airflow/output/results_tp_4/customer_orders_taskflow.csv', index=False)
        print("CSV sauvegardé ✅")

    # Orchestration — beaucoup plus lisible !
    load_orders_to_postgres()
    customers = extract_csv()
    orders = extract_postgres()
    merged = transform(customers, orders)
    insert_result(merged)

customer_orders_pipeline()