from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timezone, timedelta

def choose_task():
    paris_tz = timezone(timedelta(hours=1))
    heure = datetime.now(paris_tz).hour
    print(f"Heure actuelle (Paris) : {heure}")
    if heure < 13:
        print("→ Matin détecté, on lance task_A")
        return "task_A"
    else:
        print("→ Après-midi détecté, on lance task_B")
        return "task_B"

with DAG(
    dag_id="branching_example",
    start_date=datetime(2024, 6, 1),
    schedule=None,
    catchup=False
) as dag:

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=choose_task
    )

    task_A = BashOperator(
        task_id="task_A",
        bash_command="echo 'Bonjour ! Il est moins de 13h'"
    )

    task_B = BashOperator(
        task_id="task_B",
        bash_command="echo 'Bonsoir ! Il est plus de 13h'"
    )

    branch_task >> [task_A, task_B]