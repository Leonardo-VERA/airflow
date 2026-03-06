from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def process_file():
    file_path = '/opt/airflow/data/bdd_tp_4/test_exo3.csv'
    
    # Vérifier si le fichier est vide
    import os
    taille = os.path.getsize(file_path)
    print(f"📁 Fichier détecté : test_exo3.csv")
    print(f"📏 Taille : {taille} bytes")
    
    if taille == 0:
        print("⚠️ Fichier vide ! Rien à traiter.")
        return
    
    df = pd.read_csv(file_path)
    print(f"📊 Nombre de lignes : {len(df)}")
    print(f"📋 Colonnes : {list(df.columns)}")
    print(f"👀 Aperçu :")
    print(df.head())
    print("✅ Traitement terminé !")

with DAG(
    dag_id="sensor_example",
    start_date=datetime(2024, 6, 1),
    schedule=None,
    catchup=False
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/opt/airflow/data/bdd_tp_4/test_exo3.csv",
        poke_interval=10,
        timeout=300
    )

    file_ready_task = PythonOperator(
        task_id="file_ready",
        python_callable=process_file
    )

    wait_for_file >> file_ready_task