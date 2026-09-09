"""Prueba de la aritmetica del job de Spark. Necesita pyspark, no Kafka.

Se ejecuta DENTRO del contenedor de Spark, que es donde vive pyspark. Desde la
raiz del repositorio, en PowerShell:

    docker compose run --rm --no-deps -v "${PWD}/pruebas:/pruebas" `
        spark-streaming /opt/spark/bin/spark-submit /pruebas/prueba_logica_streaming.py

Con `spark-submit` y NO con `python`. En la imagen de Spark el ejecutable se
llama `python3`, y aunque se invoque bien, `pyspark` no esta en el PYTHONPATH:
vive en /opt/spark/python y es spark-submit quien lo pone en su sitio.

En Git Bash hay que anteponer `MSYS_NO_PATHCONV=1`, o convierte `/pruebas` en una
ruta de Windows antes de que docker la vea.

POR QUE ESTA PRUEBA SE REESCRIBIO
---------------------------------
La version anterior decia "aplicamos la misma logica del job principal" y a
continuacion la COPIABA dentro del propio archivo de prueba. Eso la volvia
inservible por dos motivos:

  1. Si el job cambiaba, la prueba seguia pasando. No probaba el job: probaba
     una copia del job que solo existia en la prueba.
  2. La copia no era fiel. Deduplicaba con `dropDuplicates(["id_trade"])`,
     mientras que el job usa `dropDuplicatesWithinWatermark(["simbolo",
     "id_trade"])`. Y los datos de ejemplo no tenian ningun id_trade repetido
     entre simbolos distintos, asi que las dos versiones daban el mismo
     resultado y la diferencia no se veia.

Ademas declaraba `id_trade` como StringType, cuando el contrato lo define como
`long`.

Ahora se importa `calcular_metricas` del job y se ejerce la aritmetica que corre
en produccion. Para que eso fuera posible se separo `agregar()` en `preparar()`
(watermark y deduplicacion, que exigen un flujo) y `calcular_metricas()` (ventana
y agregaciones, que funciona igual sobre un DataFrame estatico).

LO QUE NO CUBRE
---------------
La deduplicacion y el watermark, que solo existen sobre un flujo real. Se
verifican de otra manera: la prueba de carga reenvia trades ya procesados y
comprueba que las metricas no cambian.
"""

import os
import sys
import unittest
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampType,
)

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(RAIZ, "procesamiento_streaming"))
# El contenedor monta el codigo del job en su directorio de trabajo.
sys.path.insert(0, "/opt/spark/work-dir")

import job_metricas_ventana as job  # noqa: E402


# El esquema es el del contrato, con los tipos del contrato. `id_trade` es
# LongType y no StringType: los id reales del exchange pasan de 2^31.
ESQUEMA = StructType([
    StructField("id_trade", LongType(), True),
    StructField("simbolo", StringType(), True),
    StructField("precio", DoubleType(), True),
    StructField("cantidad", DoubleType(), True),
    StructField("ts_evento", TimestampType(), True),
    StructField("origen", StringType(), True),
])


class PruebaAritmeticaDelJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .appName("prueba_logica_streaming")
            .master("local[2]")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _agregar(self, filas):
        df = self.spark.createDataFrame(filas, ESQUEMA)
        return {
            (r["simbolo"], r["window"].start): r
            for r in job.calcular_metricas(df).collect()
        }

    def test_vwap_pondera_por_cantidad(self):
        """El VWAP es sum(precio*cantidad)/sum(cantidad), no el promedio simple.

        Es la diferencia que mas caro sale: con cantidades desiguales, el
        promedio simple y el ponderado se separan mucho, y la conciliacion
        reportaria como error del pipeline lo que seria un error de formula.
        """
        filas = [
            (1, "BTCUSDT", 60000.0, 1.0, datetime(2026, 9, 8, 14, 0, 10), "exchange_ws"),
            (2, "BTCUSDT", 61000.0, 2.0, datetime(2026, 9, 8, 14, 0, 20), "exchange_ws"),
        ]
        r = self._agregar(filas)[("BTCUSDT", datetime(2026, 9, 8, 14, 0))]

        # Ponderado: (60000*1 + 61000*2) / 3 = 60666.67
        self.assertAlmostEqual(r["vwap"], 60666.666666, places=4)
        # El promedio simple seria 60500. Si la prueba pasara con ese valor,
        # no estaria comprobando nada.
        self.assertNotAlmostEqual(r["vwap"], 60500.0, places=1)
        self.assertAlmostEqual(r["volumen_base"], 3.0, places=6)
        self.assertAlmostEqual(r["volumen_usdt"], 182000.0, places=4)
        self.assertEqual(r["n_trades"], 2)

    def test_ohlc_usa_el_orden_temporal_no_el_de_llegada(self):
        """Apertura y cierre salen de min_by/max_by sobre ts_evento.

        En una agregacion no hay orden garantizado dentro del grupo, asi que
        first()/last() devolverian un precio cualquiera. Para que la prueba lo
        detecte, las filas se pasan DESORDENADAS a proposito: el trade mas
        antiguo va en medio y el mas reciente, primero.
        """
        filas = [
            (3, "BTCUSDT", 63000.0, 1.0, datetime(2026, 9, 8, 14, 0, 50), "exchange_ws"),
            (1, "BTCUSDT", 60000.0, 1.0, datetime(2026, 9, 8, 14, 0, 5), "exchange_ws"),
            (2, "BTCUSDT", 65000.0, 1.0, datetime(2026, 9, 8, 14, 0, 30), "exchange_ws"),
            (4, "BTCUSDT", 59000.0, 1.0, datetime(2026, 9, 8, 14, 0, 40), "exchange_ws"),
        ]
        r = self._agregar(filas)[("BTCUSDT", datetime(2026, 9, 8, 14, 0))]

        self.assertEqual(r["precio_apertura"], 60000.0, "apertura = trade mas ANTIGUO")
        self.assertEqual(r["precio_cierre"], 63000.0, "cierre = trade mas RECIENTE")
        self.assertEqual(r["precio_maximo"], 65000.0)
        self.assertEqual(r["precio_minimo"], 59000.0)

    def test_volatilidad_es_rango_relativo_y_no_desviacion(self):
        """El contrato define volatilidad_pct como (max-min)/vwap*100.

        No es la desviacion estandar. Con una sola operacion en la ventana,
        stddev devuelve nulo, y una ventana de un minuto puede tener una sola
        operacion en los pares de menor volumen.
        """
        filas = [
            (1, "SOLUSDT", 100.0, 1.0, datetime(2026, 9, 8, 14, 0, 10), "exchange_ws"),
            (2, "SOLUSDT", 110.0, 1.0, datetime(2026, 9, 8, 14, 0, 20), "exchange_ws"),
        ]
        r = self._agregar(filas)[("SOLUSDT", datetime(2026, 9, 8, 14, 0))]

        # vwap = 105; (110-100)/105*100 = 9.5238
        self.assertAlmostEqual(r["volatilidad_pct"], 9.523809, places=4)

        # Una sola operacion: rango 0, no nulo.
        una = [(9, "SOLUSDT", 100.0, 1.0, datetime(2026, 9, 8, 14, 0, 10), "exchange_ws")]
        sola = self._agregar(una)[("SOLUSDT", datetime(2026, 9, 8, 14, 0))]
        self.assertIsNotNone(sola["volatilidad_pct"], "stddev daria nulo aqui")
        self.assertAlmostEqual(sola["volatilidad_pct"], 0.0, places=6)

    def test_las_ventanas_no_mezclan_simbolos(self):
        """Se agrupa por (ventana, simbolo). Sin el simbolo, el VWAP de BTC y
        el de ETH se promediarian juntos y darian un numero sin significado."""
        filas = [
            (1, "BTCUSDT", 60000.0, 1.0, datetime(2026, 9, 8, 14, 0, 10), "exchange_ws"),
            (2, "ETHUSDT", 3000.0, 5.0, datetime(2026, 9, 8, 14, 0, 30), "exchange_ws"),
        ]
        resultado = self._agregar(filas)

        self.assertEqual(resultado[("BTCUSDT", datetime(2026, 9, 8, 14, 0))]["vwap"], 60000.0)
        self.assertEqual(resultado[("ETHUSDT", datetime(2026, 9, 8, 14, 0))]["vwap"], 3000.0)

    def test_los_trades_caen_en_la_ventana_correcta(self):
        """ventana_inicio es inclusivo y ventana_fin exclusivo: un trade en
        14:01:00.000 pertenece a la ventana siguiente, no a la que cierra."""
        filas = [
            (1, "BTCUSDT", 60000.0, 1.0, datetime(2026, 9, 8, 14, 0, 59), "exchange_ws"),
            (2, "BTCUSDT", 70000.0, 1.0, datetime(2026, 9, 8, 14, 1, 0), "exchange_ws"),
        ]
        resultado = self._agregar(filas)

        self.assertEqual(len(resultado), 2, "deberian ser dos ventanas distintas")
        self.assertEqual(resultado[("BTCUSDT", datetime(2026, 9, 8, 14, 0))]["vwap"], 60000.0)
        self.assertEqual(resultado[("BTCUSDT", datetime(2026, 9, 8, 14, 1))]["vwap"], 70000.0)

    def test_origen_datos_distingue_la_fuente_de_los_trades(self):
        """Es el campo del que depende la regla C01 de la conciliacion."""
        reales = [
            (1, "BTCUSDT", 60000.0, 1.0, datetime(2026, 9, 8, 14, 0, 10), "exchange_ws"),
            (2, "BTCUSDT", 61000.0, 1.0, datetime(2026, 9, 8, 14, 0, 20), "exchange_ws"),
        ]
        r = self._agregar(reales)[("BTCUSDT", datetime(2026, 9, 8, 14, 0))]
        self.assertEqual(r["origen_datos"], "exchange_ws")

        simulados = [
            (1, "BTCUSDT", 63000.0, 1.0, datetime(2026, 9, 8, 14, 0, 10), "simulador"),
        ]
        r = self._agregar(simulados)[("BTCUSDT", datetime(2026, 9, 8, 14, 0))]
        self.assertEqual(r["origen_datos"], "simulador")

        # Ventana mixta: es lo que ocurre en el minuto en que se cambia de
        # fuente. Tiene que quedar MARCADA como mixta, no elegir una de las dos.
        mixtos = [
            (1, "BTCUSDT", 60000.0, 1.0, datetime(2026, 9, 8, 14, 0, 10), "exchange_ws"),
            (2, "BTCUSDT", 63000.0, 1.0, datetime(2026, 9, 8, 14, 0, 20), "simulador"),
        ]
        r = self._agregar(mixtos)[("BTCUSDT", datetime(2026, 9, 8, 14, 0))]
        self.assertEqual(r["origen_datos"], "exchange_ws+simulador")

    def test_la_salida_lleva_los_campos_del_contrato(self):
        """formatear_salida arma el JSON exacto de la seccion 4 del contrato.

        Un campo que falte aqui no da error: llega a Elasticsearch como ausente
        y la conciliacion lo lee como cero.
        """
        import json

        filas = [
            (1, "BTCUSDT", 60000.0, 1.0, datetime(2026, 9, 8, 14, 0, 10), "exchange_ws"),
        ]
        df = self.spark.createDataFrame(filas, ESQUEMA)
        salida = job.formatear_salida(job.calcular_metricas(df)).collect()

        self.assertEqual(salida[0]["key"], "BTCUSDT", "la clave de Kafka es el simbolo")
        mensaje = json.loads(salida[0]["value"])

        esperados = {
            "tipo_fuente", "simbolo", "ventana_inicio", "ventana_fin", "n_trades",
            "volumen_base", "volumen_usdt", "precio_apertura", "precio_maximo",
            "precio_minimo", "precio_cierre", "vwap", "volatilidad_pct",
            "ts_procesado", "origen", "origen_datos",
        }
        self.assertEqual(set(mensaje), esperados)
        self.assertEqual(mensaje["tipo_fuente"], "nrt_metrica")
        self.assertEqual(mensaje["origen"], "spark_streaming")
        self.assertTrue(mensaje["ventana_inicio"].endswith("Z"))

        # Los campos internos del calculo no deben salir al contrato.
        self.assertNotIn("_origen_min", mensaje)
        self.assertNotIn("_origen_max", mensaje)


if __name__ == "__main__":
    unittest.main(verbosity=2)
