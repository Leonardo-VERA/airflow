from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

cities = ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux"]

with DAG(
    dag_id="dynamic_dag_example",
    start_date=datetime(2024, 6, 1),
    schedule=None,
    catchup=False
) as dag:

    for city in cities:
        BashOperator(
            task_id=f"process_{city.lower()}",
            bash_command=f"echo '🌤️ Traitement météo pour {city} en cours...'"
        )