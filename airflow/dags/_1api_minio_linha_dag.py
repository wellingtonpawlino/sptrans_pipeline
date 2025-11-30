from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from processors._1extract_linha_api import extrair_e_salvar_minio

with DAG(
    dag_id="1api_to_minio_linha",
    description="Extrai dados da API SPTrans e salva no MinIO (camada bronze)",
    start_date=datetime(2025, 11, 5),
    schedule_interval="0 4 * * *",  # Executa diariamente às 04h
    catchup=False,
    tags=["sptrans", "api", "hive.properties"]
) as dag:

    tarefa_extrair_e_salvar = PythonOperator(
        task_id="extrair_e_salvar_minio",
        python_callable=extrair_e_salvar_minio,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    tarefa_trigger_silver = TriggerDagRunOperator(
        task_id="disparar_bronze_para_silver",
        trigger_dag_id="2transform_silver_linhas"
    )

    tarefa_extrair_e_salvar >> tarefa_trigger_silver