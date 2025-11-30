
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

linhas_df = spark.read.parquet("s3a://sptrans-data/bronze/linhas")

linhas_df = spark.read.parquet("s3a://sptrans-data/bronze/linhas")

linhas_df = (
    linhas_df
    # Renomeando colunas
    .withColumnRenamed("cl", "CodigoLinha")
    .withColumnRenamed("lc", "LinhaCircular")
    .withColumnRenamed("lt", "NumeroLetreiro")
    .withColumnRenamed("sl", "Sentido")
    .withColumnRenamed("tl", "TipoLinha")
    .withColumnRenamed("tp", "DescricaoPrincipal")
    .withColumnRenamed("ts", "DescricaoSecundario")

    # Ajustando sentido
    .withColumn("Sentido", F.when(F.col("Sentido") == 1, "Ida").otherwise("Volta"))

    # Ajustando sentido
    .withColumn("LinhaCircular", F.when(F.col("LinhaCircular") == True, "Sim").otherwise("Nao"))

    # TipoLinhaDescricao
    .withColumn(
        "TipoLinhaDescricao",
        F.when(F.col("TipoLinha") == 10, "Radial")
        .when(F.col("TipoLinha") == 11, "Noturna")
        .when(F.col("TipoLinha") == 21, "Interbairros")
        .when(F.col("TipoLinha") == 22, "Perimetral")
        .when(F.col("TipoLinha") == 23, "Circular")
        .when(F.col("TipoLinha") == 24, "Especial")
        .when(F.col("TipoLinha") == 25, "Troncal")
        .when(F.col("TipoLinha") == 31, "Semi-Expressa")
        .when(F.col("TipoLinha") == 41, "Expressa")
        .when(F.col("TipoLinha") == 42, "Corredor")
        .otherwise("Desconhecido")
    )

    # LetreiroCompleto
    .withColumn("LetreiroCompleto", F.concat_ws("-", F.col("NumeroLetreiro"), F.col("TipoLinha")))

    # DescricaoCompleto condicional
    .withColumn(
        "DescricaoCompleto",
        F.when(
            F.col("Sentido") == "Ida",
            F.concat_ws(" ", F.col("LetreiroCompleto"), F.lit("-"), F.col("DescricaoPrincipal"))
        ).otherwise(
            F.concat_ws(" ", F.col("LetreiroCompleto"), F.lit("-"), F.col("DescricaoSecundario"))
        )
    )
)

# Reordenar colunas
colunas_final = ["NumeroLetreiro", "LetreiroCompleto", "DescricaoCompleto", "Sentido", "TipoLinhaDescricao",
                 "LinhaCircular"]

linhas_df = linhas_df.select(colunas_final)


# Caminho no MinIO (ajuste conforme sua configuração)
caminho_silver = "s3a://sptrans-data/silver/linhas"

# Salvar como Parquet com partição por ano, mes e dia
linhas_df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(caminho_silver)