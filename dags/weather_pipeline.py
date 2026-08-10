"""
weather_pipeline.py

Airflow DAG that orchestrates the weather ETL pipeline end-to-end:

    1. Ingest  – fetch weather data from the OpenWeather API (Bronze)
    2. Transform – PySpark Bronze → Silver transformation
    3. Load   – Silver → Gold upsert into MongoDB

Each task runs the corresponding Python script via BashOperator
inside the Airflow worker container, which has Java + PySpark +
pymongo pre-installed (see Dockerfile.airflow).

Schedule: @hourly  |  Catchup: disabled
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# ============================================================
# Default arguments applied to every task in the DAG
# ============================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# DAG definition
# ============================================================

with DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    description="End-to-end weather ETL: API ingestion → PySpark transformation → MongoDB loading",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "etl", "pipeline"],
) as dag:

    # --------------------------------------------------------
    # Task 1 – Ingest weather data from OpenWeather API
    # --------------------------------------------------------
    ingest_weather = BashOperator(
        task_id="ingest_weather",
        bash_command="python /opt/airflow/src/ingestion/weather_api.py",
    )

    # --------------------------------------------------------
    # Task 2 – PySpark Bronze → Silver transformation
    # --------------------------------------------------------
    transform_weather = BashOperator(
        task_id="transform_weather",
        bash_command="python /opt/airflow/src/transformation/transform_weather.py",
    )

    # --------------------------------------------------------
    # Task 3 – Silver → Gold MongoDB loading
    # --------------------------------------------------------
    load_to_mongodb = BashOperator(
        task_id="load_to_mongodb",
        bash_command="python /opt/airflow/src/loading/weather/load_mongodb.py",
    )

    # --------------------------------------------------------
    # Task dependencies: ingest → transform → load
    # --------------------------------------------------------
    ingest_weather >> transform_weather >> load_to_mongodb
