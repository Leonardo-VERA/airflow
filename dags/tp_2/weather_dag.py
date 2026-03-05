from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract(ti):
    '''
    on charge un fichier csv depuis la data/bdd_tp_2/weather.csv
    '''
    file_path = '/opt/airflow/data/bdd_tp_2/weather.csv'
    data = pd.read_csv(file_path)
    ti.xcom_push(key='weather_data', value=data.to_json())

def transform(ti):
    '''
    on transforme les données extraites
    Nettoyer les valeurs nulles et ajouter une colonne calculee (feels_like_temp).
    '''
    raw_json = ti.xcom_pull(task_ids='extract', key='weather_data')
    data = pd.read_json(raw_json)

    data = data.ffill()
    # Formule simplifiée du ressenti thermique :
    # température moyenne ajustée par le vent et l'humidité
    data['feels_like_temp'] = ((data['temp_min'] + data['temp_max']) / 2
                               - 0.4 * data['wind_speed']
                               + 0.1 * data['humidity'])

    print("Données transformées :")
    print(data)
    ti.xcom_push(key='transformed_data', value=data.to_json())

def load(ti):
    '''
    on charge les données transformées dans un fichier csv dans output/results_tp_2/weather_result.csv
    '''
    raw_json = ti.xcom_pull(task_ids='transform', key='transformed_data')
    data = pd.read_json(raw_json)

    output_path = '/opt/airflow/output/results_tp_2/weather_result.csv'
    data.to_csv(output_path, index=False)
    print(f"Fichier sauvegardé : {output_path}")


with DAG(
    dag_id='weather_pipeline',
    default_args=default_args,
    description='Un pipeline du climat',
    schedule=None,
    start_date=datetime(2024, 6, 1),
    catchup=False
) as dag:

    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    task_load = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    task_extract >> task_transform >> task_load
