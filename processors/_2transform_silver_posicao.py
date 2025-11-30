
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Cria sessão Spark com configs para MinIO
spark = SparkSession.builder \
    .appName("SPTransPipeline") \
    .master("spark://spark:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()


# Desativar criação do arquivo _SUCCESS
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# Força as configs no Hadoop (garante que executores também tenham acesso)
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

posicao_df = spark.read.parquet("s3a://sptrans-data/bronze/posicao")

# Criar novas colunas e ajustes
posicao_df = (
    posicao_df
    # Converter hr_atualizacao para timestamp
    .withColumn("hr_atualizacao", F.to_timestamp("hr_atualizacao", "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    # Dia da semana em português
    .withColumn("dia_semana_num", F.dayofweek("hr_atualizacao"))  # 1=Domingo, 7=Sábado
    .withColumn(
        "dia_semana",
        F.when(F.col("dia_semana_num") == 1, "Domingo")
        .when(F.col("dia_semana_num") == 2, "Segunda")
        .when(F.col("dia_semana_num") == 3, "Terca")
        .when(F.col("dia_semana_num") == 4, "Quarta")
        .when(F.col("dia_semana_num") == 5, "Quinta")
        .when(F.col("dia_semana_num") == 6, "Sexta")
        .otherwise("Sabado")
    )

    # Hora e minuto
    .withColumn("hora", F.hour("hr_atualizacao"))
    .withColumn("minuto", F.minute("hr_atualizacao"))

    # Período do dia
    .withColumn(
        "periodo_do_dia",
        F.when(F.col("hora").between(0, 5), "Madrugada")
        .when(F.col("hora").between(6, 11), "Manha")
        .when(F.col("hora").between(12, 17), "Tarde")
        .otherwise("Noite")
    )

    # Acessível: sim/não
    .withColumn("acessivel", F.when(F.col("acessivel") == True, "Sim").otherwise("Nao"))

    # Sentido: 1 = Ida, 2 = Volta
    .withColumn("sentido", F.when(F.col("sentido") == 1, "Ida").otherwise("Volta"))

    # Remover coluna hr_referencia
    .drop("hr_referencia")
)

# Reordenar colunas
colunas_final = [
    "codigo_linha", "sentido", "latitude", "longitude", "hr_atualizacao",
    "dia_semana", "hora", "minuto", "periodo_do_dia", "acessivel", "ano", "mes", "dia"
]

posicao_df = posicao_df.select(colunas_final)

# Visualizar resultado
posicao_df.show(5, truncate=False)


# Caminho no MinIO (ajuste conforme sua configuração)
caminho_silver = "s3a://sptrans-data/silver/posicao"

# Salvar como Parquet com partição por ano, mes e dia
posicao_df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("ano", "mes", "dia") \
    .parquet(caminho_silver)