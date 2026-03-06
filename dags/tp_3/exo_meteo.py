from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="tp_postgres_etl_pipeline",
    default_args=default_args,
    description="TP : Orchestration ETL avec PostgreSQL et PostgresOperator",
    schedule=None,
    start_date=datetime(2024, 6, 1),
    catchup=False
) as dag:

    # Tâche 1 : Créer la table weather_data
    create_table_task = SQLExecuteQueryOperator(
        task_id="create_weather_table",
        conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS weather_data (
            station_id INT,
            date DATE,
            temperature FLOAT,
            humidity FLOAT,
            wind_speed FLOAT,
            weather_condition VARCHAR(50)
        );
        """
    )

    # Tâche 2 : Insérer des données météo
    insert_data_task = SQLExecuteQueryOperator(
        task_id="insert_weather_data",
        conn_id="postgres_default",
        sql="""
        DELETE FROM weather_data;
        INSERT INTO weather_data (station_id, date, temperature, humidity, wind_speed, weather_condition)
        VALUES
            (101, '2024-06-01', 25, 60, 15, 'Sunny'),
            (102, '2024-06-01', 30, 50, 20, 'Clear'),
            (103, '2024-06-01', 22, 70, 10, 'Cloudy'),
            (104, '2024-06-02', 28, 55, 25, 'Windy'),
            (105, '2024-06-02', 18, 80,  5, 'Rainy');
        """
    )

    # Tâche 3 : Transformer — créer weather_summary avec temperature_category
    transform_data_task = SQLExecuteQueryOperator(
        task_id="transform_weather_data",
        conn_id="postgres_default",
        sql="""
        DROP TABLE IF EXISTS weather_summary;
        CREATE TABLE weather_summary AS
        SELECT
            station_id,
            date,
            temperature,
            humidity,
            wind_speed,
            weather_condition,
            CASE
                WHEN temperature > 28 THEN 'Hot'
                WHEN temperature >= 20 AND temperature <= 28 THEN 'Warm'
                ELSE 'Cold'
            END AS temperature_category
        FROM weather_data;
        """
    )

    # Tâche 4 : Vérifier les résultats
    verify_results_task = SQLExecuteQueryOperator(
        task_id="select_weather_summary",
        conn_id="postgres_default",
        sql="SELECT * FROM weather_summary;"
    )

    create_table_task >> insert_data_task >> transform_data_task >> verify_results_task