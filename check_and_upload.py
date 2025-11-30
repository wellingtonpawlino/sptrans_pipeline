from minio import Minio
import os

# Configurações do MinIO (pegue do .env)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "hive.properties:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "senha123")

# Bucket e caminhos
BUCKET = "sptrans-data"
OBJETO = "bronze/dados.csv"
ARQUIVO_LOCAL = "/app/dados.csv"

# Inicializa cliente
client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

# Garante que bucket existe
if not client.bucket_exists(BUCKET):
    print(f"Bucket '{BUCKET}' não existe. Criando...")
    client.make_bucket(BUCKET)
else:
    print(f"Bucket '{BUCKET}' já existe.")

# Verifica se objeto existe
try:
    client.stat_object(BUCKET, OBJETO)
    print(f"Arquivo '{OBJETO}' já existe no MinIO.")
except:
    print(f"Arquivo '{OBJETO}' não encontrado. Fazendo upload...")
    client.fput_object(BUCKET, OBJETO, ARQUIVO_LOCAL)
    print("Upload concluído!")