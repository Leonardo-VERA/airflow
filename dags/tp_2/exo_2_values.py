from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extraction(ti):
    '''
    Lire le fichier CSV et calculer la somme de column1.
    '''
    file_path = '/opt/airflow/data/bdd_tp_2/values_data.csv'
    data = pd.read_csv(file_path)
    sum_column1 = data['column1'].sum()
    print(f"Somme calculée : {sum_column1}")
    ti.xcom_push(
        key='sum_column1', 
        value=int(sum_column1)
        )

def communication(ti):
    '''
    Récupérer la valeur via XCom et la transmettre.
    '''
    sum_value = ti.xcom_pull(task_ids='extraction', key='sum_column1')
    print(f"Valeur reçue via XCom : {sum_value}")
    ti.xcom_push(key='result', value=sum_value)

def affichage(ti):
    '''
    Afficher la valeur finale reçue dans les logs.
    '''
    result = ti.xcom_pull(task_ids='communication', key='result')
    print(f"Valeur finale : {result}")


with DAG(
    dag_id='values_xcom_pipeline',
    default_args=default_args,
    description='Pipeline XCom avec values_data',
    schedule=None,
    start_date=datetime(2024, 6, 1),
    catchup=False
) as dag:

    task_extraction = PythonOperator(
        task_id='extraction',
        python_callable=extraction,
    )

    task_communication = PythonOperator(
        task_id='communication',
        python_callable=communication,
    )

    task_affichage = PythonOperator(
        task_id='affichage',
        python_callable=affichage,
    )

    task_extraction >> task_communication >> task_affichage
