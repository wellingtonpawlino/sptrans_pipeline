
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configuração do Spark
spark = SparkSession.builder \
    .appName("SPTransPipeline") \
    .master("spark://spark:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# Leitura Silver
pv = spark.read.parquet("s3a://sptrans-data/silver/posicao")
ls = spark.read.parquet("s3a://sptrans-data/silver/linhas")

# Join
pv = pv.withColumnRenamed("sentido", "sentido_pv")
df = pv.join(ls, (pv["codigo_linha"] == ls["LetreiroCompleto"]) & (pv["sentido_pv"] == ls["Sentido"]), "inner")

colunas_final = [
    "codigo_linha", "LetreiroCompleto", "DescricaoCompleto", "Sentido",
    "TipoLinhaDescricao", "LinhaCircular", "latitude", "longitude",
    "acessivel", "periodo_do_dia", "dia_semana", "hr_atualizacao"
]
df = df.select(colunas_final)

# Última posição por linha (mais recente por hr_atualizacao)
windowSpec = Window.partitionBy("DescricaoCompleto", "Sentido").orderBy(F.col("hr_atualizacao").desc())
df_ranked = df.withColumn("rank", F.row_number().over(windowSpec))
vw_ultima_posicao_linha = df_ranked.filter(F.col("rank") == 1).drop("rank").withColumn("data_ingestao", F.current_timestamp())

# Caminho Parquet (camada ouro)
gold_path = "s3a://sptrans-data/gold/ultima_posicao"

# Salvar sobrescrevendo SEM partição
vw_ultima_posicao_linha.write.mode("overwrite").parquet(gold_path)
