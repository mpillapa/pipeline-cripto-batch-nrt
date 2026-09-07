"""Helpers transversales a los DAGs del camino batch.

Concentra cuatro cosas que de otro modo se repetirian en cada DAG:

  1. El manejo del `lote_id`, que es el hilo que conecta los cinco DAGs.
  2. Las rutas de archivos por lote.
  3. Lectura y escritura de JSON y NDJSON con una unica convencion.
  4. Marcas de tiempo en UTC, en el formato que exige el contrato de datos.
"""

import json
import os
from datetime import datetime, timezone

from comun import config


# ---------------------------------------------------------------------------
# TIEMPO
# ---------------------------------------------------------------------------
def ahora_utc():
    """Momento actual con zona horaria explicita.

    `datetime.now()` a secas devuelve un objeto sin zona, y comparar uno de esos
    con una marca del exchange (que si trae zona) lanza TypeError. Peor todavia
    seria que no fallara: se estarian comparando dos horas distintas como si
    fueran la misma.
    """
    return datetime.now(timezone.utc)


def a_iso(momento):
    """Formatea en ISO 8601 UTC con milisegundos y sufijo Z.

    Es el formato que fija el contrato de datos para todos los campos `ts_*`.
    Se recorta a milisegundos porque los microsegundos de Python no aportan y
    Elasticsearch los trunca de todos modos.
    """
    if momento.tzinfo is None:
        momento = momento.replace(tzinfo=timezone.utc)
    return momento.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
        "{:03d}Z".format(momento.microsecond // 1000)


def desde_milisegundos(milisegundos):
    """Convierte una marca epoch en milisegundos (lo que devuelve la API) a datetime UTC."""
    return datetime.fromtimestamp(int(milisegundos) / 1000, tz=timezone.utc)


# ---------------------------------------------------------------------------
# LOTE_ID: el identificador que viaja por los cinco DAGs
# ---------------------------------------------------------------------------
def nuevo_lote_id(momento=None):
    """Genera un identificador de lote unico y ordenable alfabeticamente.

    Formato: L20260907_143012

    Lleva la hora, no solo la fecha, porque durante las pruebas se ejecuta el
    pipeline varias veces el mismo dia y cada corrida necesita su propio espacio
    de archivos. Un lote_id con solo la fecha hace que la segunda corrida del
    dia pise a la primera.
    """
    momento = momento or ahora_utc()
    return "L" + momento.strftime("%Y%m%d_%H%M%S")


def resolver_lote_id(context):
    """Obtiene el lote_id de la ejecucion actual.

    Prioridad:
      1. `conf` del dag_run, que es lo que envia el DAG anterior por
         TriggerDagRunOperator y tambien lo que se puede escribir a mano en la
         UI con "Trigger DAG w/ config".
      2. El ultimo lote presente en disco.

    El punto 2 es lo que permite ejecutar cualquiera de los DAGs 02..05 de forma
    aislada desde la UI, sin relanzar toda la cadena. Sin este respaldo, disparar
    el DAG 03 solo fallaria por falta de lote_id.
    """
    dag_run = context.get("dag_run")
    if dag_run is not None and dag_run.conf:
        lote_id = dag_run.conf.get("lote_id")
        if lote_id:
            return lote_id

    lote_id = ultimo_lote_en_disco()
    if lote_id is None:
        raise ValueError(
            "No se recibio lote_id en la configuracion del dag_run y no hay "
            "ningun lote en " + config.DIR_BRONCE + ". Ejecuta primero "
            "dag_01_ingesta_batch, o dispara este DAG con la configuracion "
            '{"lote_id": "L20260907_143012"}.'
        )

    print("Sin lote_id en conf; se usa el ultimo lote disponible: " + lote_id)
    return lote_id


def ultimo_lote_en_disco():
    """Devuelve el lote mas reciente de DIR_BRONCE, o None si no hay ninguno.

    Funciona por orden alfabetico porque el formato del lote_id (fecha y hora en
    posiciones fijas) hace que el orden alfabetico coincida con el cronologico.
    """
    if not os.path.isdir(config.DIR_BRONCE):
        return None

    lotes = sorted(
        nombre for nombre in os.listdir(config.DIR_BRONCE)
        if os.path.isdir(os.path.join(config.DIR_BRONCE, nombre))
    )
    return lotes[-1] if lotes else None


def leer_conf(context, clave, por_defecto):
    """Lee un parametro de la configuracion del dag_run con valor por defecto.

    Permite ajustar una ejecucion puntual desde la UI sin tocar el codigo.
    Ejemplo:  {"tasa_defectos": 0.35}  fuerza la ruta de lote bloqueado.
    """
    dag_run = context.get("dag_run")
    if dag_run is not None and dag_run.conf and clave in dag_run.conf:
        return dag_run.conf[clave]
    return por_defecto


# ---------------------------------------------------------------------------
# RUTAS POR LOTE
# ---------------------------------------------------------------------------
def dir_lote(directorio_base, lote_id, crear=True):
    """Subcarpeta propia de un lote dentro de una de las zonas de datos.

    Una carpeta por lote, en vez de una ruta fija compartida, evita que dos
    ejecuciones simultaneas se sobrescriban los archivos entre si.
    """
    ruta = os.path.join(directorio_base, lote_id)
    if crear:
        os.makedirs(ruta, exist_ok=True)
    return ruta


def ruta_en_lote(directorio_base, lote_id, nombre_archivo, crear=True):
    """Ruta completa de un archivo dentro de la carpeta de un lote."""
    return os.path.join(dir_lote(directorio_base, lote_id, crear), nombre_archivo)


# ---------------------------------------------------------------------------
# LECTURA / ESCRITURA
# ---------------------------------------------------------------------------
def escribir_json(ruta, datos):
    """Escribe un JSON legible (indentado, con acentos sin escapar)."""
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    with open(ruta, "w", encoding="utf-8") as archivo:
        json.dump(datos, archivo, indent=2, ensure_ascii=False, default=str)
    return ruta


def leer_json(ruta):
    if not os.path.exists(ruta):
        raise FileNotFoundError("No existe el archivo esperado: " + ruta)
    with open(ruta, encoding="utf-8") as archivo:
        return json.load(archivo)


def escribir_ndjson(ruta, filas):
    """Escribe un objeto JSON por linea. Devuelve el numero de lineas escritas.

    Es el formato que consume Logstash con `input { file { codec => json_lines } }`.
    No lleva indentacion a proposito: un objeto indentado ocupa varias lineas
    fisicas y Logstash lo leeria como varios eventos rotos.

    `default=str` convierte fechas y decimales a texto. Sin eso, un Decimal de
    MySQL o un date de Python revientan la serializacion.
    """
    os.makedirs(os.path.dirname(ruta), exist_ok=True)

    escritas = 0
    with open(ruta, "w", encoding="utf-8") as archivo:
        for fila in filas:
            archivo.write(json.dumps(fila, ensure_ascii=False, default=str))
            archivo.write("\n")
            escritas += 1

    return escritas


# ---------------------------------------------------------------------------
# SALIDA EN LOG
# ---------------------------------------------------------------------------
def encabezado(titulo):
    """Separador visible en el log de la tarea.

    Los logs de Airflow se leen en la UI durante la revision; un encabezado hace
    evidente donde empieza cada paso.
    """
    print("=" * 70)
    print(titulo)
    print("=" * 70)


def resumen(pares):
    """Imprime un bloque `clave: valor` alineado."""
    if not pares:
        return
    ancho = max(len(str(clave)) for clave, _ in pares)
    for clave, valor in pares:
        print("  " + str(clave).ljust(ancho) + " : " + str(valor))
