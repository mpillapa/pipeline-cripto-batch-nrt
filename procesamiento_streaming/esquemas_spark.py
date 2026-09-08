from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Se añade id_trade para la deduplicación requerida
def obtener_esquema_trade():
    return StructType([
        StructField("id_trade", StringType(), True),
        StructField("simbolo", StringType(), True),
        StructField("precio", DoubleType(), True),
        StructField("cantidad", DoubleType(), True),
        StructField("ts_evento", TimestampType(), True)
    ])