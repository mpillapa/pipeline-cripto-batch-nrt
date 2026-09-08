from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Inicializar Spark (las librerías de Kafka ya están en la imagen)
spark = SparkSession.builder \
    .appName("NRT_Metricas_Ventana") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Esquema basado en el contrato de datos
esquema_trade = StructType([
    StructField("simbolo", StringType(), True),
    StructField("precio", DoubleType(), True),
    StructField("cantidad", DoubleType(), True),
    StructField("ts_evento", TimestampType(), True)
])

# 3. Leer el stream desde Kafka
df_crudo = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "trades.crudo") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parsear el JSON
df_parseado = df_crudo.select(
    from_json(col("value").cast("string"), esquema_trade).alias("data")
).select("data.*")

# 5. Agrupación por ventanas de 1 minuto (A refinar con Manuel)
df_agregado = df_parseado \
    .withWatermark("ts_evento", "1 minute") \
    .groupBy(
        window(col("ts_evento"), "1 minute"),
        col("simbolo")
    ).agg(
        avg("precio").alias("precio_promedio"),
        sum("cantidad").alias("cantidad_total")
    )

# 6. Imprimir resultados en consola temporalmente para depuración
query = df_agregado.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()