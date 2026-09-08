from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum, stddev, expr, to_json, struct
from esquemas_spark import obtener_esquema_trade

spark = SparkSession.builder.appName("NRT_Metricas_Ventana").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df_crudo = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "trades.crudo") \
    .option("startingOffsets", "latest") \
    .load()

df_parseado = df_crudo.select(
    from_json(col("value").cast("string"), obtener_esquema_trade()).alias("data")
).select("data.*")

# Deduplicación, Watermark de 30s y Ventana Tumbling de 1 minuto
df_agregado = df_parseado \
    .withWatermark("ts_evento", "30 seconds") \
    .dropDuplicates(["id_trade"]) \
    .groupBy(
        window(col("ts_evento"), "1 minute"),
        col("simbolo")
    ).agg(
        (sum(col("precio") * col("cantidad")) / sum("cantidad")).alias("vwap"),
        stddev("precio").alias("volatilidad_real"),
        sum("cantidad").alias("volumen_total")
    )

# Preparar el formato para enviar a Kafka (requiere columna 'value' en string)
df_salida = df_agregado.select(
    to_json(struct(
        col("window.start").alias("inicio_ventana"),
        col("window.end").alias("fin_ventana"),
        col("simbolo"),
        col("vwap"),
        col("volatilidad_real"),
        col("volumen_total")
    )).alias("value")
)

# Escribir salida al topic metricas.1min
query = df_salida.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "metricas.1min") \
    .option("checkpointLocation", "/opt/spark/work-dir/checkpoints") \
    .start()

query.awaitTermination()