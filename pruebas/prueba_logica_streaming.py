import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, window, sum, stddev
from datetime import datetime

class PruebaLogicaStreaming(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Levantamos Spark en local para probar la lógica funcional sin infraestructura
        cls.spark = SparkSession.builder \
            .appName("Test_Logica_Streaming") \
            .master("local[2]") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_deduplicacion_y_calculo_vwap(self):
        """
        Esta prueba valida:
        1. Que los eventos duplicados (mismo id_trade) se descarten.
        2. Que el VWAP se calcule correctamente: sum(precio*cantidad) / sum(cantidad).
        """
        esquema = StructType([
            StructField("id_trade", StringType(), True),
            StructField("simbolo", StringType(), True),
            StructField("precio", DoubleType(), True),
            StructField("cantidad", DoubleType(), True),
            StructField("ts_evento", TimestampType(), True)
        ])
        
        # Datos estáticos: El primer evento BTC está duplicado
        datos = [
            ("1", "BTCUSDT", 60000.0, 1.0, datetime(2026, 9, 8, 14, 0, 10)),
            ("2", "BTCUSDT", 61000.0, 2.0, datetime(2026, 9, 8, 14, 0, 20)),
            ("1", "BTCUSDT", 60000.0, 1.0, datetime(2026, 9, 8, 14, 0, 10)), # Duplicado exacto
            ("3", "ETHUSDT", 3000.0, 5.0, datetime(2026, 9, 8, 14, 0, 30))
        ]
        
        df = self.spark.createDataFrame(datos, esquema)
        
        # Aplicamos la misma lógica del job principal
        df_agregado = df \
            .dropDuplicates(["id_trade"]) \
            .groupBy(
                window(col("ts_evento"), "1 minute"),
                col("simbolo")
            ).agg(
                (sum(col("precio") * col("cantidad")) / sum("cantidad")).alias("vwap"),
                sum("cantidad").alias("volumen_total")
            )
            
        resultados = df_agregado.collect()
        
        btc_res = next(r for r in resultados if r['simbolo'] == 'BTCUSDT')
        eth_res = next(r for r in resultados if r['simbolo'] == 'ETHUSDT')
        
        # Comprobación de deduplicación: el volumen total de BTC debería ser 3.0, no 4.0
        self.assertEqual(btc_res['volumen_total'], 3.0)
        
        # Comprobación de VWAP BTC: (60000*1 + 61000*2) / 3 = 182000 / 3 = 60666.66
        self.assertAlmostEqual(btc_res['vwap'], 60666.66, places=2)
        
        # ETH solo tiene un evento, vwap igual al precio
        self.assertEqual(eth_res['vwap'], 3000.0)
        self.assertEqual(eth_res['volumen_total'], 5.0)

if __name__ == '__main__':
    unittest.main()
