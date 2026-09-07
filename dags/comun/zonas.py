"""Acceso a las zonas de datos del camino batch.

Unico lugar donde se decide el formato fisico en que aterrizan los datos. Los
DAGs piden "guarda estas velas en bronce del lote X" y no saben si por debajo es
Parquet, CSV o cualquier otra cosa. Cambiar de formato se hace aqui.

POR QUE PARQUET Y NO CSV:

  - Es columnar y comprimido: la serie de 365 dias por 3 simbolos ocupa una
    fraccion de lo que ocuparia en texto.
  - Lleva el esquema dentro. Un CSV devuelve todo como texto y obliga a que cada
    lector vuelva a decidir que es numero y que es fecha; dos lectores que
    decidan distinto producen resultados distintos sin que nadie lo note.
  - Un precio como 63250.10 sobrevive el viaje de ida y vuelta. En CSV depende
    del formato con que se escribio.

La contrapartida es que no se puede abrir con un editor de texto. Para eso esta
`describir_parquet`, que imprime esquema y primeras filas en el log de la tarea.
"""

import os

import pandas as pd

from comun import config

# Compresion snappy: es la predeterminada de Parquet y la que mejor equilibra
# velocidad y tamano. gzip comprime mas pero cuesta bastante mas CPU, y aqui el
# cuello de botella del entorno es la memoria, no el disco.
COMPRESION = "snappy"


# ---------------------------------------------------------------------------
# ESCRITURA
# ---------------------------------------------------------------------------
def escribir_zona(directorio_base, lote_id, nombre, marco):
    """Guarda un DataFrame como Parquet dentro de la carpeta del lote.

    Devuelve (ruta, numero_de_filas).

    `index=False` es obligatorio: sin eso pandas escribe una columna extra con
    el indice, que reaparece como `__index_level_0__` al leer y ensucia el
    esquema.
    """
    if marco is None:
        raise ValueError("No se puede escribir un DataFrame nulo en " + nombre)

    ruta_lote = os.path.join(directorio_base, lote_id)
    os.makedirs(ruta_lote, exist_ok=True)
    ruta = os.path.join(ruta_lote, nombre + ".parquet")

    marco.to_parquet(ruta, engine="pyarrow", compression=COMPRESION, index=False)
    return ruta, len(marco)


def escribir_bronce(lote_id, simbolo, marco):
    """Zona bronce: las velas tal como llegaron de la API, sin transformar.

    Un archivo por simbolo, no uno solo con todo. Si la descarga de un simbolo
    falla, los que ya bajaron quedan en disco y el reintento no repite el
    trabajo completo.
    """
    return escribir_zona(config.DIR_BRONCE, lote_id, "bronce_" + simbolo.lower(), marco)


def escribir_cuarentena(lote_id, marco):
    """Zona de cuarentena: filas rechazadas, con la columna `motivo_rechazo`.

    Se guardan siempre, incluso cuando el lote se promueve. Sin el archivo no
    hay forma de revisar despues por que se descarto un dato, y la regla de
    calidad se vuelve una caja negra.
    """
    return escribir_zona(config.DIR_CUARENTENA, lote_id, "cuarentena", marco)


def escribir_plata(lote_id, marco):
    """Zona plata: velas normalizadas, tipadas y con los indicadores calculados."""
    return escribir_zona(config.DIR_PLATA, lote_id, "plata", marco)


# ---------------------------------------------------------------------------
# LECTURA
# ---------------------------------------------------------------------------
def leer_zona(directorio_base, lote_id, nombre):
    """Lee un Parquet concreto de la carpeta de un lote."""
    ruta = os.path.join(directorio_base, lote_id, nombre + ".parquet")
    if not os.path.exists(ruta):
        raise FileNotFoundError(
            "No existe " + ruta + ". Es probable que el DAG anterior de la "
            "cadena no haya llegado a escribirlo: revisa su log antes de "
            "reintentar este."
        )
    return pd.read_parquet(ruta, engine="pyarrow")


def leer_bronce(lote_id):
    """Lee y concatena todos los archivos de bronce del lote, uno por simbolo.

    Devuelve un solo DataFrame. El orden de concatenacion no importa: las
    transformaciones posteriores ordenan explicitamente por simbolo y fecha,
    porque las medias moviles dependen del orden y confiar en el que traiga el
    archivo es una fuente silenciosa de error.
    """
    ruta_lote = os.path.join(config.DIR_BRONCE, lote_id)
    if not os.path.isdir(ruta_lote):
        raise FileNotFoundError("No existe la carpeta del lote: " + ruta_lote)

    archivos = sorted(
        os.path.join(ruta_lote, nombre)
        for nombre in os.listdir(ruta_lote)
        if nombre.startswith("bronce_") and nombre.endswith(".parquet")
    )
    if not archivos:
        raise FileNotFoundError(
            "La carpeta " + ruta_lote + " existe pero no contiene ningun "
            "archivo bronce_*.parquet. El DAG 01 la creo y fallo antes de "
            "escribir."
        )

    marcos = [pd.read_parquet(archivo, engine="pyarrow") for archivo in archivos]
    return pd.concat(marcos, ignore_index=True)


def leer_plata(lote_id):
    return leer_zona(config.DIR_PLATA, lote_id, "plata")


# ---------------------------------------------------------------------------
# INSPECCION
# ---------------------------------------------------------------------------
def describir_parquet(ruta, filas=5):
    """Imprime esquema, conteo y primeras filas de un Parquet en el log.

    Existe porque un Parquet no se puede abrir con un editor de texto. Durante
    la revision del trabajo, poder ver en el log de Airflow que tipos quedaron
    guardados evita tener que levantar un notebook para comprobarlo.
    """
    marco = pd.read_parquet(ruta, engine="pyarrow")
    print("Archivo : " + ruta)
    print("Filas   : " + str(len(marco)))
    print("Columnas y tipos:")
    for columna, tipo in marco.dtypes.items():
        print("  " + str(columna).ljust(20) + " " + str(tipo))
    print("Primeras " + str(filas) + " filas:")
    print(marco.head(filas).to_string())
    return len(marco)


def tamano_zona(directorio_base, lote_id):
    """Bytes ocupados por un lote en una zona. Va al reporte final."""
    ruta_lote = os.path.join(directorio_base, lote_id)
    if not os.path.isdir(ruta_lote):
        return 0
    return sum(
        os.path.getsize(os.path.join(ruta_lote, nombre))
        for nombre in os.listdir(ruta_lote)
        if os.path.isfile(os.path.join(ruta_lote, nombre))
    )
