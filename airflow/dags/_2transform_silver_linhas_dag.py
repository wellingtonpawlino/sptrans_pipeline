from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}

with DAG(
    dag_id="2transform_silver_linhas",
    start_date=datetime(2025, 11, 23),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["sptrans", "silver", "etl"]
) as dag:

    task_silver = BashOperator(
        task_id="transformar_bronze_para_silver_linhas",
        bash_command="docker exec sptrans_spark_master /opt/spark/bin/spark-submit --master spark://spark:7077 /opt/airflow/processors/_2transform_silver_linhas.py"
    )