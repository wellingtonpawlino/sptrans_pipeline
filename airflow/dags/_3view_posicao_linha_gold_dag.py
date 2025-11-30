
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
    dag_id="3view_gold_posicao_linha",
    start_date=datetime(2025, 11, 29),
    schedule_interval="@daily",  # ou @hourly se quiser mais frequência
    catchup=False,
    default_args=default_args,
    tags=["sptrans", "gold", "etl"]
) as dag:

    criar_gold_ultima_posicao = BashOperator(
        task_id="criar_gold_ultima_posicao",
        bash_command=(
            "docker exec sptrans_spark_master /opt/spark/bin/spark-submit "
            "--master spark://spark:7077 "
            "/opt/airflow/processors/_3create_view_posicao_linhas_gold.py"
        )
    )
