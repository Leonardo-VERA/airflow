from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="trigger_rules_example",
    start_date=datetime(2024, 6, 1),
    schedule=None,
    catchup=False
) as dag:

    task_success = BashOperator(
        task_id="task_success",
        bash_command="echo 'Success Task'"
    )

    task_fail = BashOperator(
        task_id="task_fail",
        bash_command="exit 1"
    )

    task_final = BashOperator(
        task_id="task_final",
        bash_command="echo 'Une tâche a échoué mais je tourne quand même!'",
        trigger_rule="one_failed"
    )

    [task_success, task_fail] >> task_final
