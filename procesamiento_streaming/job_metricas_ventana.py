"""Agregacion por ventanas del flujo near real-time.

Lee trades de Kafka, los agrega en ventanas de un minuto y publica las metricas
en Kafka. ES LA UNICA PIEZA QUE HACE PROCESAMIENTO EN TIEMPO REAL: el resto del
camino NRT ingiere, enruta e indexa, pero no agrega.

FUENTE KAFKA -> AGREGACION -> DESTINO KAFKA. No escribe en Elasticsearch ni en
MySQL a proposito: eso elimina el conector Elasticsearch-Spark y el driver JDBC,
que son las dos dependencias mas fragiles del montaje. Logstash sigue siendo el
unico que escribe hacia Elasticsearch.

LOS CAMPOS DE SALIDA NO SON NEGOCIABLES. Estan fijados en
contratos/CONTRATO_DATOS.md seccion 4, y `comun/conciliacion.py` los lee tal
cual. En particular `volumen_usdt` y `n_trades`: sin el primero no se puede
agregar las 60 ventanas de una hora ponderando por volumen, y sin el segundo no
hay cobertura, que es la metrica que da sentido a la desviacion.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, date_format, expr, from_json, lit, max as f_max,
    min as f_min, struct, sum as f_sum, to_json, when, window,
)

from esquemas_spark import obtener_esquema_trade

# ---------------------------------------------------------------------------
# PARAMETROS
# ---------------------------------------------------------------------------
# Por variable de entorno para poder apuntar a otro broker o cambiar la ventana
# sin reconstruir la imagen.
KAFKA = os.environ.get("CRIPTO_KAFKA", "kafka:29092")
TOPIC_ENTRADA = os.environ.get("CRIPTO_TOPIC_TRADES", "trades.crudo")
TOPIC_SALIDA = os.environ.get("CRIPTO_TOPIC_METRICAS", "metricas.1min")

VENTANA = os.environ.get("CRIPTO_VENTANA", "1 minute")
WATERMARK = os.environ.get("CRIPTO_WATERMARK", "30 seconds")

# El checkpoint va al volumen montado desde el host, NO a /opt/spark/work-dir,
# que es donde se monta el codigo. Un checkpoint dentro de la carpeta de codigo
# es el mismo error que ya se corrigio en el camino batch, y ademas se perderia
# al recrear el contenedor, con lo que la prueba de recuperacion ante fallo no
# demostraria nada.
CHECKPOINT = os.environ.get("CRIPTO_CHECKPOINT", "/opt/spark/checkpoints/metricas")

# Cada cuanto se cierra un micro-lote. Fijarlo hace que la latencia sea
# predecible y que los cubos de la demo aparezcan a ritmo constante; sin el,
# Spark procesa tan rapido como puede y produce micro-lotes irregulares.
INTERVALO = os.environ.get("CRIPTO_INTERVALO_LOTE", "30 seconds")


def construir_sesion():
    sesion = (
        SparkSession.builder
        .appName("cripto-metricas-ventana")
        .getOrCreate()
    )
    sesion.sparkContext.setLogLevel("WARN")
    return sesion


def leer_trades(sesion):
    """Fuente Kafka.

    `failOnDataLoss=false`: los topics tienen 24 horas de retencion. Si el job
    estuvo parado mas tiempo, los offsets guardados en el checkpoint ya no
    existen en el broker. Con el valor por defecto el job se niega a arrancar y
    hay que borrar el checkpoint a mano; asi continua desde lo que haya y lo
    avisa en el log.
    """
    return (
        sesion.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", TOPIC_ENTRADA)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parsear(df_crudo):
    """Convierte el JSON de Kafka en columnas tipadas."""
    return (
        df_crudo
        .select(from_json(col("value").cast("string"), obtener_esquema_trade()).alias("d"))
        .select("d.*")
        # Un mensaje que no cumple el esquema deja todas las columnas nulas.
        # Se descarta aqui en vez de dejar que contamine las agregaciones con
        # nulos: una ventana con un trade sin precio daria un VWAP incorrecto.
        .filter(col("simbolo").isNotNull() & col("precio").isNotNull()
                & col("cantidad").isNotNull() & col("ts_evento").isNotNull())
    )


def preparar(df):
    """Declara el watermark y deduplica. Es la parte que EXIGE un flujo.

    ORDEN DE LAS OPERACIONES. El watermark se declara ANTES de deduplicar y de
    agrupar: es lo que permite a Spark liberar el estado de las ventanas que ya
    no pueden recibir eventos tardios.

    POR QUE dropDuplicatesWithinWatermark Y NO dropDuplicates. Un
    `dropDuplicates` sobre una columna que no es la de tiempo de evento no
    recibe limpieza de estado del watermark: Spark guarda todos los id_trade
    vistos, para siempre. En una demo de diez minutos no se nota; en la prueba
    de carga a 2000 eventos por segundo el job se queda sin memoria.

    POR QUE LA CLAVE ES (simbolo, id_trade). El id_trade es unico POR SIMBOLO en
    el exchange, no globalmente. Deduplicar solo por id_trade descartaria trades
    legitimos de otro par que casualmente compartan numero.
    """
    return (
        df
        .withWatermark("ts_evento", WATERMARK)
        .dropDuplicatesWithinWatermark(["simbolo", "id_trade"])
    )


def calcular_metricas(df, ventana=None):
    """Ventana tumbling y metricas del contrato. Funciona en flujo Y en lote.

    ESTA SEPARADA DE preparar() PARA PODER PROBARLA. `withWatermark` y
    `dropDuplicatesWithinWatermark` solo tienen sentido sobre un flujo, asi que
    mientras estuvieran mezcladas con la aritmetica no habia forma de verificar
    el VWAP ni el OHLC sin levantar Kafka.

    La prueba anterior lo resolvia COPIANDO la logica del job, lo que la hacia
    inutil: pasaba igual aunque el job estuviera roto, porque no probaba el job
    sino la copia. De hecho la copia deduplicaba por `id_trade` a secas, que no
    es lo que hace el job.

    Ahora pruebas/prueba_logica_streaming.py llama a ESTA funcion con un
    DataFrame estatico y comprueba la aritmetica que se ejecuta en produccion.
    """
    return (
        df
        .groupBy(window(col("ts_evento"), ventana or VENTANA), col("simbolo"))
        .agg(
            # Los dos que necesita la conciliacion. volumen_usdt se acumula como
            # suma de precio*cantidad, que es lo que permite que el VWAP horario
            # salga de dividir las dos sumas sin ninguna formula ponderada.
            f_sum(col("precio") * col("cantidad")).alias("volumen_usdt"),
            f_sum(col("cantidad")).alias("volumen_base"),
            expr("count(*)").alias("n_trades"),

            # OHLC de la ventana. min_by y max_by y no first/last: en una
            # agregacion de streaming no hay orden garantizado dentro del grupo,
            # asi que first() devolveria un precio cualquiera. min_by(precio,
            # ts_evento) devuelve el precio del trade MAS ANTIGUO, que es la
            # definicion de apertura.
            expr("min_by(precio, ts_evento)").alias("precio_apertura"),
            f_max(col("precio")).alias("precio_maximo"),
            f_min(col("precio")).alias("precio_minimo"),
            expr("max_by(precio, ts_evento)").alias("precio_cierre"),

            # De que fuente salieron los trades de esta ventana. Se agrega con
            # min y max y NO con collect_set: las agregaciones de coleccion no
            # son fiables en streaming, y con solo dos valores posibles
            # ("exchange_ws" y "simulador") comparar el minimo con el maximo
            # distingue exactamente los tres casos.
            #
            # POR QUE IMPORTA. La conciliacion del DAG 05 solo significa algo si
            # ambos lados leen el mismo mercado. Una ventana alimentada por el
            # simulador comparada contra la vela real del exchange produce una
            # desviacion enorme que NO es un fallo del pipeline. Sin este campo
            # esa distincion no se puede hacer despues de los hechos.
            f_min(col("origen")).alias("_origen_min"),
            f_max(col("origen")).alias("_origen_max"),
        )
        .withColumn(
            "origen_datos",
            when(col("_origen_min") == col("_origen_max"), col("_origen_min"))
            .otherwise(expr("concat(_origen_min, '+', _origen_max)")),
        )
        # VWAP y volatilidad se derivan de las sumas ya calculadas, no son
        # agregaciones nuevas.
        .withColumn("vwap", col("volumen_usdt") / col("volumen_base"))
        # volatilidad_pct = rango relativo, segun el contrato. NO es la
        # desviacion estandar: se elige por ser calculable de forma incremental
        # en una ventana corta, y porque stddev sobre una ventana con una sola
        # operacion devuelve nulo.
        .withColumn(
            "volatilidad_pct",
            (col("precio_maximo") - col("precio_minimo")) / col("vwap") * 100,
        )
    )


def agregar(df):
    """El pipeline completo de agregacion: preparar el flujo y calcular.

    Se conserva como una sola llamada porque es como la usa main(), y porque el
    orden de las dos partes no es intercambiable: el watermark tiene que estar
    declarado antes del groupBy.
    """
    return calcular_metricas(preparar(df))


def formatear_salida(df):
    """Arma el mensaje exacto que fija el contrato de datos.

    La clave del mensaje es el simbolo: garantiza que todas las ventanas de un
    mismo activo caigan en la misma particion y conserven el orden.

    Las marcas de tiempo se formatean a mano en ISO 8601 UTC con milisegundos y
    sufijo Z. `to_json` sobre un timestamp produce otro formato, y Logstash
    tendria que adivinarlo.
    """
    iso = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

    return df.select(
        col("simbolo").alias("key"),
        to_json(struct(
            lit("nrt_metrica").alias("tipo_fuente"),
            col("simbolo"),
            date_format(col("window.start"), iso).alias("ventana_inicio"),
            date_format(col("window.end"), iso).alias("ventana_fin"),
            col("n_trades"),
            col("volumen_base"),
            col("volumen_usdt"),
            col("precio_apertura"),
            col("precio_maximo"),
            col("precio_minimo"),
            col("precio_cierre"),
            col("vwap"),
            col("volatilidad_pct"),
            # Permite medir la latencia de procesamiento como
            # ts_procesado - ventana_fin. Sin este campo la prueba de latencia
            # p50/p95/p99 no se puede hacer.
            date_format(current_timestamp(), iso).alias("ts_procesado"),
            lit("spark_streaming").alias("origen"),
            # `origen` dice quien CALCULO la metrica; `origen_datos` dice de
            # donde venian los trades que la alimentaron. No son lo mismo y
            # confundirlos deja la conciliacion sin forma de saber si es
            # comparable.
            col("origen_datos"),
        )).alias("value"),
    )


def main():
    sesion = construir_sesion()

    print("Kafka      : " + KAFKA)
    print("Entrada    : " + TOPIC_ENTRADA)
    print("Salida     : " + TOPIC_SALIDA)
    print("Ventana    : " + VENTANA + "  watermark: " + WATERMARK)
    print("Checkpoint : " + CHECKPOINT)

    salida = formatear_salida(agregar(parsear(leer_trades(sesion))))

    consulta = (
        salida.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("topic", TOPIC_SALIDA)
        .option("checkpointLocation", CHECKPOINT)
        # `append` y no `update`: con append, una ventana se emite UNA sola vez,
        # cuando el watermark garantiza que ya no puede recibir mas eventos. Con
        # update se emitiria varias veces con valores parciales, y la
        # conciliacion sumaria la misma ventana dos veces.
        .outputMode("append")
        .trigger(processingTime=INTERVALO)
        .start()
    )

    consulta.awaitTermination()


if __name__ == "__main__":
    main()
