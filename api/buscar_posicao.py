import os
import pandas as pd
import requests
from minio import Minio
from dotenv import load_dotenv
from api.autenticacao import autenticar

load_dotenv()

# Função para salvar no MinIO e localmente
def salvar_no_minio(df, nome_arquivo):
    try:
        caminho_csv = f"/opt/airflow/data/{nome_arquivo}.csv"
        os.makedirs(os.path.dirname(caminho_csv), exist_ok=True)

        # Salvar localmente
        df.to_csv(caminho_csv, index=False)
        print(f"📁 CSV salvo em: {caminho_csv}")

        # Conexão com MinIO
        client = Minio(
            os.getenv("MINIO_ENDPOINT", "hive.properties:9000"),
            access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            secure=False
        )

        bucket_name = "sptrans-data"
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"✅ Bucket '{bucket_name}' criado no MinIO")

        destino = f"bronze/{nome_arquivo}.csv"
        client.fput_object(bucket_name, destino, caminho_csv)
        print(f"✅ Arquivo enviado para {destino} no bucket '{bucket_name}'")

    except Exception as e:
        print(f"❌ Erro ao salvar no MinIO: {e}")


# Função para buscar posição dos veículos
def buscar_posicao_veiculos(session):
    url = "https://api.olhovivo.sptrans.com.br/v2.1/Posicao"
    print(f"➡️ Consultando: {url}")
    response = session.get(url)
    if response.status_code == 200:
        dados = response.json()
        hr = dados.get("hr")  # horário de referência
        veiculos = []
        for linha in dados.get("l", []):
            for v in linha.get("vs", []):
                veiculos.append({
                    "codigo_linha": linha.get("c"),
                    "sentido": linha.get("sl"),  # Sentido da linha
                    "latitude": v.get("py"),
                    "longitude": v.get("px"),
                    "hr_referencia": hr,
                    "hr_atualizacao": v.get("ta"),
                    "acessivel": v.get("a")
                })

        return veiculos
    else:
        print(f"❌ Erro HTTP {response.status_code}")
        return []