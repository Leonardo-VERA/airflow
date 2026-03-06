from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def push_data(ti):
    data = {
        "message": "Bonjour depuis push_task !",
        "heure_envoi": datetime.now().strftime('%H:%M:%S'),
        "valeur": 42
    }
    ti.xcom_push(key='my_key', value=data)
    print(f"📤 Valeur poussée : {data}")

def pull_data(ti):
    data = ti.xcom_pull(task_ids='push_task', key='my_key')
    print(f"📥 Valeur XCom reçue : {data}")
    print(f"   → Message  : {data['message']}")
    print(f"   → Envoyé à : {data['heure_envoi']}")
    print(f"   → Valeur   : {data['valeur']}")
    print("✅ Communication entre tâches réussie !")

with DAG(
    dag_id="xcom_example",
    start_date=datetime(2024, 6, 1),
    schedule=None,
    catchup=False
) as dag:

    push_task = PythonOperator(
        task_id="push_task",
        python_callable=push_data
    )

    pull_task = PythonOperator(
        task_id="pull_task",
        python_callable=pull_data
    )

    push_task >> pull_task