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

def extract_csv(ti):
    '''
    Tache 1 : Charger customers_data.csv et transmettre le chemin via XCom.
    '''
    df = pd.read_csv('/opt/airflow/data/bdd_tp_2/customers_data.csv')
    tmp_path = '/tmp/customers_extract.csv'
    df.to_csv(tmp_path, index=False)
    print(f"CSV extrait : {len(df)} lignes -> {tmp_path}")
    ti.xcom_push(key='customers_path', value=tmp_path)

def extract_postgres(ti):
    '''
    Tache 2 : Lire orders depuis PostgreSQL et transmettre le chemin via XCom.
    '''
    hook = PostgresHook(postgres_conn_id='postgres_default')
    df = hook.get_pandas_df("SELECT * FROM orders;")
    tmp_path = '/tmp/orders_extract.csv'
    df.to_csv(tmp_path, index=False)
    print(f"Orders extraits : {len(df)} lignes -> {tmp_path}")
    ti.xcom_push(key='orders_path', value=tmp_path)

def transform(ti):
    '''
    Tache 3 : Fusionner les fichiers intermediaires et calculer total_amount.
    '''
    customers_path = ti.xcom_pull(task_ids='extract_csv', key='customers_path')
    orders_path = ti.xcom_pull(task_ids='extract_postgres', key='orders_path')

    customers = pd.read_csv(customers_path)
    orders = pd.read_csv(orders_path)

    merged = customers.merge(orders, on='customer_id')
    merged['total_amount'] = merged['amount'] + merged['age']

    tmp_path = '/tmp/transformed_orders.csv'
    merged.to_csv(tmp_path, index=False)
    print(f"Données fusionnées : {len(merged)} lignes -> {tmp_path}")
    ti.xcom_push(key='transformed_path', value=tmp_path)

def insert_result(ti):
    '''
    Tache 4 : Sauvegarder les resultats dans PostgreSQL.
    '''
    transformed_path = ti.xcom_pull(task_ids='transform', key='transformed_path')
    merged = pd.read_csv(transformed_path)

    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run("""
        CREATE TABLE IF NOT EXISTS customer_orders_v2 (
            customer_id INT,
            name VARCHAR(50),
            age INT,
            city VARCHAR(50),
            order_id INT,
            amount FLOAT,
            total_amount FLOAT
        );
        DELETE FROM customer_orders_v2;
    """)
    rows = [tuple(row) for row in merged.itertuples(index=False)]
    hook.insert_rows(table='customer_orders_v2', rows=rows)
    print(f"{len(rows)} lignes insérées dans customer_orders_v2")

    output_path = '/opt/airflow/output/results_tp_2/customer_orders_v2.csv'
    merged.to_csv(output_path, index=False)
    print(f"CSV sauvegardé : {output_path}")


with DAG(
    dag_id='customer_orders_xcom_paths',
    default_args=default_args,
    description='Pipeline avec chemins intermediaires via XCom',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

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

    [task_extract_csv, task_extract_postgres] >> task_transform >> task_insert
