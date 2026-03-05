from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_and_transform():
    # Le chemin /opt/airflow/dags est le standard dans le conteneur Docker Airflow
    file_path = '/opt/airflow/data/bdd_tp_1/sales_data.csv'
    
    # Extraction
    data = pd.read_csv(file_path)
    
    # Transformation : Nettoyage et ajout d'une colonne
    data = data.drop_duplicates()
    data['profit_margin'] = (data['revenue'] - data['cost']) / data['revenue']
    
    print("Données transformées :")
    print(data)
    
    output_path = '/opt/airflow/output/results_tp_1/result.csv'
    data.to_csv(output_path, index=False)
    print(f"Fichier sauvegardé : {output_path}")

with DAG(
    dag_id='etl_pipeline_example',
    default_args=default_args,
    description='Un pipeline ETL simple',
    schedule=None, 
    # '@daily', '@once', '@weekly', '@monthly'
    # '0 8 * * *' pour une exécution à 8h tous les jours, '0 8 * * 1' tous les lundis à 8h
    start_date=datetime(2024, 6, 1),
    catchup=False
) as dag:

    task_transform = PythonOperator(
        task_id='extract_and_transform',
        python_callable=extract_and_transform
    )

