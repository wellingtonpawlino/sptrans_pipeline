import os
import io
import pandas as pd
import boto3
from dotenv import load_dotenv
from api.autenticacao import autenticar
from api.buscar_linhas import (
    buscar_linhas_zona_sul,
    buscar_linhas_zona_leste,
    buscar_linhas_zona_norte,
    buscar_linhas_zona_oeste,
    buscar_linhas_zona_central
)

# Carregar variáveis do .env
load_dotenv()


def extrair_e_salvar_minio():
    try:
        # Autenticação na API SPTrans
        session = autenticar()
        if not session:
            raise Exception("❌ Falha na autenticação")
        print("✅ Autenticação realizada com sucesso!")

        # Buscar dados por zona
        dados = {
            "zona_sul": buscar_linhas_zona_sul(session),
            "zona_leste": buscar_linhas_zona_leste(session),
            "zona_norte": buscar_linhas_zona_norte(session),
            "zona_oeste": buscar_linhas_zona_oeste(session),
            "zona_central": buscar_linhas_zona_central(session)
        }

        total_registros = sum(len(v) for v in dados.values() if isinstance(v, list))
        print(f"✅ Dados extraídos: {total_registros} registros")

        # Configurações MinIO
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "hive.properties:9000")
        minio_user = os.getenv("MINIO_ROOT_USER", "minioadmin")
        minio_password = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        bucket_name = "sptrans-data"

        s3_client = boto3.client(
            "s3",
            endpoint_url=f"http://{minio_endpoint}",
            aws_access_key_id=minio_user,
            aws_secret_access_key=minio_password
        )

        # Criar bucket se não existir
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"✅ Bucket '{bucket_name}' criado no MinIO")

        # Diretório local para salvar arquivos
        local_dir = "/opt/airflow/data"
        os.makedirs(local_dir, exist_ok=True)

        # Processar e salvar cada zona
        for zona, linhas in dados.items():
            if not linhas:
                print(f"⚠️ Nenhum dado retornado para {zona}.")
                continue

            arquivo = f"linhas_{zona}.csv"
            caminho_csv = os.path.join(local_dir, arquivo)

            # Criar DataFrame com todas as colunas originais
            df = pd.DataFrame(linhas)

            # Salvar localmente
            df.to_csv(caminho_csv, index=False)
            print(f"📁 CSV salvo em: {caminho_csv}")

            # Converter para CSV em memória
            output = io.StringIO()
            df.to_csv(output, index=False)
            csv_content = output.getvalue()

            # Upload para MinIO
            s3_client.put_object(
                Bucket=bucket_name,
                Key=f"bronze/{arquivo}",
                Body=csv_content,
                ContentType="text/csv"
            )
            print(f"✅ Arquivo enviado para bronze/{arquivo} no bucket '{bucket_name}'")

        print("🎯 Processo concluído com sucesso!")

    except Exception as e:
        print(f"❌ Erro durante a execução: {e}")
        raise


# Executor
if __name__ == "__main__":
    extrair_e_salvar_minio()

