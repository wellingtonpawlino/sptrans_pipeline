
import os
import io
import pandas as pd
import boto3
from datetime import datetime
from dotenv import load_dotenv
from api.autenticacao import autenticar
from api.buscar_posicao import buscar_posicao_veiculos

# Carregar variáveis do .env
load_dotenv()

def extrair_posicao_e_salvar_minio():
    try:
        # Autenticação na API SPTrans
        session = autenticar()
        if not session:
            raise Exception("❌ Falha na autenticação")
        print("✅ Autenticação realizada com sucesso!")

        # Buscar posição dos veículos
        dados = buscar_posicao_veiculos(session)
        if not dados:
            print("⚠️ Nenhum dado retornado para posição, salvando arquivo vazio.")
            df_vazio = pd.DataFrame(columns=[
                "codigo_linha", "sentido", "latitude", "longitude", "hr_referencia", "hr_atualizacao", "acessivel"
            ])
            salvar_parquet_minio(df_vazio, "posicao_veiculos")
            return

        print(f"✅ Dados extraídos: {len(dados)} registros")

        # Criar DataFrame
        df = pd.DataFrame(dados)

        # Colunas esperadas (incluindo acessibilidade e prefixo)
        colunas = ["codigo_linha", "sentido", "latitude", "longitude", "hr_referencia", "hr_atualizacao", "acessivel"]

        # Garantir que todas as colunas existam
        for col in colunas:
            if col not in df.columns:
                df[col] = None

        # Reordenar colunas
        df = df[colunas]

        # Salvar diretamente no MinIO
        salvar_parquet_minio(df, "posicao_veiculos")

        print("🎯 Processo concluído com sucesso!")

    except Exception as e:
        print(f"❌ Erro durante a execução: {e}")
        raise


def salvar_parquet_minio(df, base_nome):
    try:
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

        # Nome do arquivo com timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_arquivo = f"{base_nome}_{timestamp}.parquet"

        # Converter para Parquet em memória (sem salvar local)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
        parquet_buffer.seek(0)

        # Particionamento por data no MinIO
        ano = datetime.now().year
        mes = datetime.now().month
        dia = datetime.now().day
        destino = f"bronze/posicao/ano={ano}/mes={mes}/dia={dia}/{nome_arquivo}"

        # Upload para MinIO
        s3_client.put_object(
            Bucket=bucket_name,
            Key=destino,
            Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream"
        )
        print(f"✅ Arquivo enviado para {destino} no bucket '{bucket_name}'")

    except Exception as e:
        print(f"❌ Erro ao salvar no MinIO: {e}")
        raise


# Executor
if __name__ == "__main__":
    extrair_posicao_e_salvar_minio()
