"""Esquema del evento de trade, para que Spark parsee el JSON de Kafka.

TIENE QUE COINCIDIR CON contratos/CONTRATO_DATOS.md SECCION 3.

Spark descarta en silencio cualquier campo del JSON que no este declarado aqui.
Por eso se declaran TODOS los del contrato, aunque el job de ventanas solo use
cinco: el dia que haga falta `origen` para separar dato real de simulado en un
panel, o `comprador_es_maker` para medir presion compradora, el campo ya esta
llegando y no hay que tocar el esquema.

`ts_evento` y `ts_ingesta` como TimestampType y no como texto: el watermark y la
ventana necesitan un tipo temporal. Declarados como cadena, Spark no dejaria
usarlos en `window()` y el fallo aparece en tiempo de ejecucion, no al arrancar.
"""

from pyspark.sql.types import (
    BooleanType, DoubleType, LongType, StringType, StructField, StructType,
    TimestampType,
)


def obtener_esquema_trade():
    return StructType([
        StructField("id_evento", StringType(), True),
        StructField("tipo_fuente", StringType(), True),
        StructField("simbolo", StringType(), True),
        # LongType, no StringType: el contrato lo define como entero, y como es
        # la clave de deduplicacion conviene que compare como numero. Un
        # id_trade que llegue como texto en un mensaje y como numero en otro
        # produciria dos claves distintas para el mismo trade.
        StructField("id_trade", LongType(), True),
        StructField("precio", DoubleType(), True),
        StructField("cantidad", DoubleType(), True),
        StructField("importe_usdt", DoubleType(), True),
        StructField("comprador_es_maker", BooleanType(), True),
        StructField("ts_evento", TimestampType(), True),
        StructField("ts_ingesta", TimestampType(), True),
        StructField("origen", StringType(), True),
    ])
