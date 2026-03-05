from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def load_orders_to_postgres():
    '''
    Charge orders.csv dans une table PostgreSQL
    pour simuler la source MySQL
    '''
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
    print(f"{len(rows)} lignes insérées dans orders")

def extract_csv(ti):
    '''
    Tâche 1 : Lire customers_data.csv
    '''
    df = pd.read_csv('/opt/airflow/data/bdd_tp_2/customers_data.csv')
    print(f"CSV chargé : {len(df)} lignes")
    ti.xcom_push(key='customers', value=df.to_json())

def extract_postgres(ti):
    '''
    Tâche 2 : Lire orders depuis PostgreSQL
    '''
    hook = PostgresHook(postgres_conn_id='postgres_default')
    df = hook.get_pandas_df("SELECT * FROM orders;")
    print(f"Orders chargés : {len(df)} lignes")
    ti.xcom_push(key='orders', value=df.to_json())

def transform(ti):
    '''
    Tâche 3 : Fusionner et ajouter colonne discount
    '''
    customers = pd.read_json(ti.xcom_pull(task_ids='extract_csv', key='customers'))
    orders = pd.read_json(ti.xcom_pull(task_ids='extract_postgres', key='orders'))

    merged = customers.merge(orders, on='customer_id')
    merged['discount'] = merged['amount'] * 0.1

    print("Données fusionnées :")
    print(merged)
    ti.xcom_push(key='merged', value=merged.to_json())

def insert_result(ti):
    '''
    Tâche 4 : Sauvegarder dans customer_orders
    '''
    merged = pd.read_json(ti.xcom_pull(task_ids='transform', key='merged'))
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
    print(f"{len(rows)} lignes insérées dans customer_orders ✅")
    merged.to_csv('/opt/airflow/output/results_tp_2/customer_orders.csv', index=False)
    print("CSV sauvegardé")



with DAG(
    dag_id='customer_orders_pipeline',
    default_args=default_args,
    description='Fusion CSV + PostgreSQL simulant MySQL',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    task_load_orders = PythonOperator(
        task_id='load_orders_to_postgres',
        python_callable=load_orders_to_postgres,
    )

    task_extract_csv = PythonOperator(
        task_id='extract_csv',
        python_callable=extract_csv,
    )

    task_extract_postgres = PythonOperator(
        task_id='extract_postgres',
        python_callable=extract_postgres,
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    task_insert = PythonOperator(
        task_id='insert_result',
        python_callable=insert_result,
    )

    task_load_orders >> [task_extract_csv, task_extract_postgres] >> task_transform >> task_insert