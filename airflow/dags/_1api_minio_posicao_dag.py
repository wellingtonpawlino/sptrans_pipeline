from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from processors._1extract_posicao_api import extrair_posicao_e_salvar_minio

# -----------------------------
# Configurações padrão
# -----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2)
}

# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id="1api_to_minio_posicao",
    description="Extrai posição dos veículos da API SPTrans e salva diretamente no MinIO, depois dispara DAG para carregar no Postgres",
    start_date=datetime(2025, 11, 10),
    schedule_interval="*/10 * * * *",  # a cada 5 minutos
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["sptrans", "posicao", "hive.properties"]
) as dag:

    # Task 1: Extrair posição e salvar no MinIO (sem salvar local)
    extrair_posicao_task = PythonOperator(
        task_id="extrair_posicao_e_salvar_minio",
        python_callable=extrair_posicao_e_salvar_minio
    )


    trigger_silver_task = TriggerDagRunOperator(
        task_id="trigger_transform_silver_posicao",
        trigger_dag_id="2transform_silver_posicao",  # DAG 2
        wait_for_completion=False  # Não bloqueia a DAG 1
    )

    extrair_posicao_task >> trigger_silver_task