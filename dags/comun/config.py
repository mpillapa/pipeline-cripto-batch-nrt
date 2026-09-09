"""Parametros del entorno del camino batch.

Todo lo que alguien podria querer cambiar sin leer el resto del codigo vive
aqui: rutas, conexiones, simbolos, umbrales de calidad y ventanas de los
indicadores.

Las reglas de negocio NO estan aqui: estan en reglas_calidad.py y
transformaciones.py como funciones puras. Aqui solo los numeros que las
parametrizan.
"""

import os
from datetime import timedelta

# ---------------------------------------------------------------------------
# RUTAS
# ---------------------------------------------------------------------------
# /opt/airflow/datos esta montado desde ./datos del host (ver docker-compose).
# Los datos NO van en /opt/airflow/dags: el scheduler reescanea esa carpeta
# buscando codigo Python cada 30 segundos, y un Parquet grande ahi lo hace
# trabajar de mas en cada ciclo.
DIR_BASE_DATOS = os.environ.get("CRIPTO_DIR_DATOS", "/opt/airflow/datos")

DIR_BRONCE = os.path.join(DIR_BASE_DATOS, "bronce")          # DAG 01: crudo, tal como llega
DIR_CUARENTENA = os.path.join(DIR_BASE_DATOS, "cuarentena")  # DAG 02: rechazos
DIR_PLATA = os.path.join(DIR_BASE_DATOS, "plata")            # DAG 03: normalizado + indicadores
DIR_EXPORTADO = os.path.join(DIR_BASE_DATOS, "exportado")    # DAG 04: NDJSON que consume Logstash
DIR_REPORTES = os.path.join(DIR_BASE_DATOS, "reportes")      # DAG 05: reporte de conciliacion

# ---------------------------------------------------------------------------
# BASE DE DATOS
# ---------------------------------------------------------------------------
# conn_id propio, definido por AIRFLOW_CONN_MYSQL_CRIPTO en el docker-compose,
# no a mano en la UI: asi sobrevive a `docker compose down -v`.
#
# Deliberadamente NO se usa `mysql_default`: esa es la conexion por defecto del
# provider y pisarla afecta a cualquier otro DAG del mismo Airflow.
CONN_MYSQL = "mysql_cripto"

TABLA_ACTIVOS = "dim_activo"
TABLA_OHLCV = "hechos_ohlcv_diario"
TABLA_LOTES = "control_lotes"
TABLA_CONCILIACION = "conciliacion"

# executemany por lotes: N inserts individuales son N viajes al servidor.
TAMANO_LOTE_INSERT = 500

# ---------------------------------------------------------------------------
# FUENTE REST (F3 del contrato)
# ---------------------------------------------------------------------------
# Endpoint publico de velas. Se deja en variable de entorno para poder apuntar
# a un espejo o a un servidor de pruebas sin tocar codigo.
API_BASE = os.environ.get("CRIPTO_API_BASE", "https://api.binance.com")
API_RUTA_KLINES = "/api/v3/klines"

# Simbolos a procesar. Tres, no mas: cada uno multiplica el volumen descargado,
# el numero de particiones de Kafka y la memoria que necesita el entorno.
# BTC de alto volumen y SOL bastante menor, para que las ventanas de la demo no
# se vean todas iguales.
SIMBOLOS = os.environ.get("CRIPTO_SIMBOLOS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")

# Intervalo de la vela que descarga el DAG 01.
INTERVALO_DIARIO = "1d"
# Intervalo que usa el DAG 05 para conciliar: la vela mas fina que se puede
# traer sin explotar el volumen de la API. Ver seccion 7 del contrato.
INTERVALO_HORARIO = "1h"

DIAS_HISTORIA = int(os.environ.get("CRIPTO_DIAS_HISTORIA", "365"))

# La API limita a 1000 velas por peticion. El cliente pagina con este tamano.
MAXIMO_VELAS_POR_PETICION = 1000

# Reintentos ante error de red o codigo 429 (limite de tasa excedido).
API_REINTENTOS = 3
API_ESPERA_INICIAL = 2      # segundos; se duplica en cada reintento
API_TIEMPO_LIMITE = 20      # segundos por peticion

# ---------------------------------------------------------------------------
# UMBRALES DE CALIDAD  (ver docs/REGLAS_NEGOCIO.md)
# ---------------------------------------------------------------------------
# Si el porcentaje de filas rechazadas supera este umbral, el DAG 02 no promueve
# el lote: lo bloquea. Es el criterio que decide la bifurcacion del DAG 02.
UMBRAL_RECHAZO = 0.15

# Un lote con menos filas validas que esto no alcanza para calcular una media
# movil de 30 dias, asi que no vale la pena procesarlo.
MINIMO_FILAS_VALIDAS = 60

# Tolerancia de la regla R06: volumen_usdt deberia parecerse a
# volumen_base * precio_medio. No son iguales porque el precio medio real es
# ponderado por operacion y aqui solo se tiene el OHLC.
TOLERANCIA_VOLUMEN_PCT = 25.0

# Tasa de defectos que inyecta el modo de prueba del DAG 01. Existe para que la
# cuarentena y la bifurcacion tengan algo real que rechazar; sin esto nunca se
# podrian demostrar. Se sobrescribe por ejecucion:
#   Trigger DAG w/ config -> {"tasa_defectos": 0.30}
TASA_DEFECTOS_PRUEBA = 0.0

# ---------------------------------------------------------------------------
# INDICADORES  (ver docs/REGLAS_NEGOCIO.md, seccion Transformaciones)
# ---------------------------------------------------------------------------
VENTANA_SMA_CORTA = 7
VENTANA_SMA_LARGA = 30
VENTANA_VOLATILIDAD = 30

# Cortes de la clasificacion de volatilidad, en puntos porcentuales de
# desviacion estandar diaria a 30 dias. Son valores elegidos para este trabajo,
# no un estandar del sector.
CORTE_VOLATILIDAD_BAJA = 2.0    # <= 2.0 % -> BAJA
CORTE_VOLATILIDAD_MEDIA = 5.0   # <= 5.0 % -> MEDIA, por encima -> ALTA

# ---------------------------------------------------------------------------
# OBSERVABILIDAD  (F6 y F7 del contrato)
# ---------------------------------------------------------------------------
# Los DAGs publican hitos por HTTP y logs por TCP a Logstash, reutilizando los
# dos inputs que el entorno ya tiene configurados.
LOGSTASH_HTTP_URL = os.environ.get("CRIPTO_LOGSTASH_HTTP", "http://logstash:8088")
LOGSTASH_TCP_HOST = os.environ.get("CRIPTO_LOGSTASH_TCP_HOST", "logstash")
LOGSTASH_TCP_PUERTO = int(os.environ.get("CRIPTO_LOGSTASH_TCP_PUERTO", "5000"))

# Tiempo limite corto y deliberado: si Logstash esta caido, reportar telemetria
# no puede bloquear al DAG. Ver seccion 8 del contrato.
OBSERVABILIDAD_TIEMPO_LIMITE = 2  # segundos

# ---------------------------------------------------------------------------
# ELASTICSEARCH  (lo consulta el DAG 05 para la conciliacion)
# ---------------------------------------------------------------------------
ELASTICSEARCH_URL = os.environ.get("CRIPTO_ELASTICSEARCH", "http://elasticsearch:9200")
INDICE_METRICAS_NRT = "cripto-nrt_metrica-*"

# Cortes del veredicto de conciliacion.
CONCILIACION_DESVIACION_ACEPTABLE = 0.5   # % de diferencia entre vwap y cierre
CONCILIACION_COBERTURA_MINIMA = 60.0      # % de trades vistos por el streaming

# Solo se concilian las ventanas alimentadas por ESTE origen de trades.
#
# POR QUE EXISTE ESTE FILTRO. La conciliacion compara el VWAP del streaming
# contra el cierre de la vela real del exchange. Eso solo mide algo si ambos
# lados leen el mismo mercado. Con el simulador (que genera precios a partir de
# constantes escritas a mano) la comparacion mide la distancia entre esas
# constantes y el mercado: medido el 9/9/2026, -20 %, +36 % y +40 % de
# desviacion con cobertura del 8 %, 10 % y 33 %. Ninguno de esos numeros dice
# nada sobre la exactitud del pipeline.
#
# El campo lo publica el job de Spark agregando el `origen` de los trades de
# cada ventana. Poner None desactiva el filtro y concilia todo, incluidas las
# ventanas mixtas; util solo para depurar.
CONCILIACION_ORIGEN_DATOS = os.environ.get("CRIPTO_CONCILIACION_ORIGEN", "exchange_ws") or None

# ---------------------------------------------------------------------------
# CONFIGURACION COMUN DE LOS DAGS
# ---------------------------------------------------------------------------
ARGS_POR_DEFECTO = {
    "owner": "equipo",
    "retries": 1,
    # timedelta, no un entero. Un entero suelto se interpreta en segundos en
    # unas versiones y da error en otras; el objeto es inequivoco.
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
}

ETIQUETAS_BASE = ["proyecto-final", "batch", "cripto"]

# Cada cuanto consulta el TriggerDagRunOperator si el DAG hijo ya termino.
INTERVALO_ESPERA_TRIGGER = 10  # segundos
